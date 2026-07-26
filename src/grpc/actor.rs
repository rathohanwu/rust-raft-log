use std::collections::HashMap;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::runtime::Handle;
use tokio::sync::oneshot;

use super::client::RaftGrpcClient;
use super::handle::{ClientResult, NodeViewData, RaftHandle};
use crate::consensus::{RaftNode, RaftStateSnapshot};
use crate::models::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterConfig, NodeId, RequestVoteRequest,
    RequestVoteResponse, ServerState,
};

pub(crate) enum Event {
    Tick,
    RequestVote {
        request: RequestVoteRequest,
        reply: oneshot::Sender<RequestVoteResponse>,
    },
    AppendEntries {
        request: AppendEntriesRequest,
        reply: oneshot::Sender<AppendEntriesResponse>,
    },
    VoteResponse {
        from: NodeId,
        response: RequestVoteResponse,
    },
    AppendResponse {
        from: NodeId,
        request: AppendEntriesRequest,
        response: AppendEntriesResponse,
    },
    ClientProposal {
        request_id: u64,
        payload: Vec<u8>,
        reply: oneshot::Sender<ClientResult>,
    },
    CancelClientRequest {
        request_id: u64,
    },
}

pub(crate) enum Command {
    Event(Event),
    Query {
        response: mpsc::Sender<NodeViewData>,
    },
    QueryEntry {
        index: u64,
        response: mpsc::Sender<Option<crate::models::LogEntry>>,
    },
    Shutdown,
}

struct Waiter {
    term: u64,
    index: u64,
    reply: oneshot::Sender<ClientResult>,
}

pub struct RaftActor {
    node: RaftNode,
    config: ClusterConfig,
    command_rx: mpsc::Receiver<Command>,
    command_tx: mpsc::Sender<Command>,
    snapshot: Arc<RwLock<RaftStateSnapshot>>,
    leader: Arc<RwLock<Option<NodeId>>>,
    runtime: Handle,
    client: RaftGrpcClient,
    waiters: HashMap<u64, Waiter>,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
}

impl RaftActor {
    pub fn spawn(node: RaftNode) -> RaftHandle {
        let runtime_handle = Handle::current();
        let config = node.get_config().clone();
        let snapshot = Arc::new(RwLock::new(node.get_state()));
        let leader = Arc::new(RwLock::new(node.get_current_leader()));
        let (command_tx, command_rx) = mpsc::channel();
        let handle = RaftHandle::new(
            command_tx.clone(),
            Arc::clone(&snapshot),
            Arc::clone(&leader),
            config.node_id,
        );
        thread::Builder::new()
            .name(format!("raft-actor-{}", config.node_id))
            .spawn(move || {
                RaftActor::new(
                    node,
                    config,
                    command_rx,
                    command_tx,
                    snapshot,
                    leader,
                    runtime_handle,
                )
                .run()
            })
            .expect("failed to spawn Raft actor thread");
        handle
    }

    fn new(
        node: RaftNode,
        config: ClusterConfig,
        command_rx: mpsc::Receiver<Command>,
        command_tx: mpsc::Sender<Command>,
        snapshot: Arc<RwLock<RaftStateSnapshot>>,
        leader: Arc<RwLock<Option<NodeId>>>,
        runtime: Handle,
    ) -> Self {
        let now = Instant::now();
        let election_deadline = now + Self::election_timeout(&config);
        let heartbeat_deadline = now + Duration::from_millis(config.heartbeat_interval);
        Self {
            node,
            client: RaftGrpcClient::new(config.clone()),
            config,
            command_rx,
            command_tx,
            snapshot,
            leader,
            runtime,
            waiters: HashMap::new(),
            election_deadline,
            heartbeat_deadline,
        }
    }

    fn election_timeout(config: &ClusterConfig) -> Duration {
        let (min, max) = config.election_timeout_range;
        let jitter = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64
            % (max - min + 1);
        Duration::from_millis(min + jitter)
    }

    fn run(mut self) {
        loop {
            let now = Instant::now();
            let deadline = if self.node.get_server_state() == ServerState::Leader {
                self.heartbeat_deadline
            } else {
                self.election_deadline
            };
            match self
                .command_rx
                .recv_timeout(deadline.saturating_duration_since(now))
            {
                Ok(command) => {
                    if !self.handle(command) {
                        break;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => self.drive(Event::Tick),
            }
        }
        self.fail_all("Raft actor is shut down");
    }

    fn handle(&mut self, command: Command) -> bool {
        match command {
            Command::Event(event) => self.drive(event),
            Command::Query { response } => {
                let _ = response.send(NodeViewData {
                    log_length: self.node.get_log_length(),
                    last_log_info: self.node.get_last_log_info(),
                });
            }
            Command::QueryEntry { index, response } => {
                let _ = response.send(self.node.get_entry(index));
            }
            Command::Shutdown => return false,
        }
        true
    }

    fn drive(&mut self, event: Event) {
        let state_before = self.node.get_state();
        match event {
            Event::Tick => self.tick(),
            Event::RequestVote { request, reply } => {
                let response = self.node.handle_request_vote(request);
                if response.vote_granted {
                    self.reset_election();
                }
                let _ = reply.send(response);
            }
            Event::AppendEntries { request, reply } => {
                let response = self.node.handle_append_entries(request);
                if response.success {
                    self.reset_election();
                }
                let _ = reply.send(response);
            }
            Event::VoteResponse { from, response } => {
                if self.node.handle_vote_response(from, response) {
                    self.heartbeat_deadline = Instant::now();
                    self.send_replication();
                }
            }
            Event::AppendResponse {
                from,
                request,
                response,
            } => {
                let retry = !response.success || !request.entries.is_empty();
                self.node
                    .handle_append_entries_response(from, &request, response);
                if retry {
                    self.send_replication();
                }
            }
            Event::ClientProposal {
                request_id,
                payload,
                reply,
            } => self.propose(request_id, payload, reply),
            Event::CancelClientRequest { request_id } => {
                self.waiters.remove(&request_id);
            }
        }
        let state = self.node.get_state();
        if state.server_state != ServerState::Leader
            && state_before.server_state == ServerState::Leader
        {
            self.fail_term(state_before.current_term);
        }
        self.complete_committed(state.commit_index);
        *self.snapshot.write().unwrap() = state;
        *self.leader.write().unwrap() = self.node.get_current_leader();
    }

    fn tick(&mut self) {
        let now = Instant::now();
        if self.node.get_server_state() == ServerState::Leader {
            if now >= self.heartbeat_deadline {
                self.heartbeat_deadline =
                    now + Duration::from_millis(self.config.heartbeat_interval);
                self.send_replication();
            }
        } else if now >= self.election_deadline {
            self.reset_election();
            if let Some(request) = self.node.create_vote_request() {
                self.send_votes(request);
            }
        }
    }

    fn reset_election(&mut self) {
        self.election_deadline = Instant::now() + Self::election_timeout(&self.config);
    }
    fn propose(&mut self, request_id: u64, payload: Vec<u8>, reply: oneshot::Sender<ClientResult>) {
        if self.node.get_server_state() != ServerState::Leader {
            Self::reject(reply, "Not the leader");
            return;
        }
        match self.node.append_new_entry(payload) {
            Ok(index) => {
                let term = self.node.get_current_term();
                self.waiters
                    .insert(request_id, Waiter { term, index, reply });
                self.send_replication();
            }
            Err(error) => Self::reject(reply, &format!("Failed to append entry: {error}")),
        }
    }
    fn reject(reply: oneshot::Sender<ClientResult>, message: &str) {
        let _ = reply.send(Err(message.into()));
    }
    fn complete_committed(&mut self, commit: u64) {
        for (_, waiter) in self.waiters.extract_if(|_, waiter| waiter.index <= commit) {
            let _ = waiter.reply.send(Ok((waiter.term, waiter.index)));
        }
    }
    fn fail_term(&mut self, term: u64) {
        for (_, waiter) in self.waiters.extract_if(|_, waiter| waiter.term == term) {
            let _ = waiter
                .reply
                .send(Err("Leadership changed before the entry committed".into()));
        }
    }
    fn fail_all(&mut self, message: &str) {
        for (_, waiter) in self.waiters.drain() {
            let _ = waiter.reply.send(Err(message.into()));
        }
    }

    fn send_votes(&self, request: RequestVoteRequest) {
        for peer in self.config.get_other_nodes() {
            let command_tx = self.command_tx.clone();
            let client = self.client.clone();
            let to = peer.node_id;
            let request = request.clone();
            self.runtime.spawn(async move {
                if let Ok(response) = client.request_vote(to, request).await {
                    let _ =
                        command_tx.send(Command::Event(Event::VoteResponse { from: to, response }));
                }
            });
        }
    }
    fn send_replication(&self) {
        for (to, request) in self.node.build_replication_requests() {
            let command_tx = self.command_tx.clone();
            let client = self.client.clone();
            self.runtime.spawn(async move {
                if let Ok(response) = client.append_entries(to, request.clone()).await {
                    let _ = command_tx.send(Command::Event(Event::AppendResponse {
                        from: to,
                        request,
                        response,
                    }));
                }
            });
        }
    }
}
