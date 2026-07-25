use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::runtime::Handle;
use tokio::sync::oneshot;

use super::client::RaftGrpcClient;
use crate::consensus::{RaftNode, RaftStateSnapshot};
use crate::models::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterConfig, NodeId, RequestVoteRequest,
    RequestVoteResponse, ServerState,
};

type ClientResult = Result<(u64, u64), String>;

enum Event {
    Tick,
    RequestVote {
        rpc_id: u64,
        request: RequestVoteRequest,
    },
    AppendEntries {
        rpc_id: u64,
        request: AppendEntriesRequest,
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
    },
    CancelClientRequest {
        request_id: u64,
    },
}

enum Command {
    Event(Event),
    RequestVote {
        rpc_id: u64,
        request: RequestVoteRequest,
        response: oneshot::Sender<RequestVoteResponse>,
    },
    AppendEntries {
        rpc_id: u64,
        request: AppendEntriesRequest,
        response: oneshot::Sender<AppendEntriesResponse>,
    },
    ClientProposal {
        request_id: u64,
        payload: Vec<u8>,
        response: oneshot::Sender<ClientResult>,
    },
    CancelClientRequest {
        request_id: u64,
    },
    Query {
        response: mpsc::Sender<NodeViewData>,
    },
    QueryEntry {
        index: u64,
        response: mpsc::Sender<Option<crate::models::LogEntry>>,
    },
    QueryAppliedState {
        response: mpsc::Sender<Option<Vec<u8>>>,
    },
    LegacyAppend {
        payload: Vec<u8>,
        response: mpsc::Sender<Result<u64, String>>,
    },
    Shutdown,
}

/// Cloneable ingress to the single Raft actor. It contains no Raft state.
#[derive(Clone)]
pub struct RaftHandle {
    tx: mpsc::Sender<Command>,
    snapshot: Arc<RwLock<RaftStateSnapshot>>,
    leader: Arc<RwLock<Option<NodeId>>>,
    node_id: NodeId,
}

#[derive(Clone)]
pub struct RaftNodeView {
    raft: RaftHandle,
}
struct NodeViewData {
    log_length: u64,
    last_log_info: (u64, u64),
}

impl RaftHandle {
    pub fn snapshot(&self) -> RaftStateSnapshot {
        self.snapshot.read().unwrap().clone()
    }
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        *self.leader.read().unwrap()
    }
    pub fn shutdown(&self) {
        let _ = self.tx.send(Command::Shutdown);
    }
    pub fn cancel_client_request(&self, request_id: u64) {
        let _ = self.tx.send(Command::CancelClientRequest { request_id });
    }

    pub async fn request_vote(
        &self,
        rpc_id: u64,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, String> {
        let (response, rx) = oneshot::channel();
        self.tx
            .send(Command::RequestVote {
                rpc_id,
                request,
                response,
            })
            .map_err(|_| "Raft actor is shut down".to_string())?;
        rx.await
            .map_err(|_| "Raft actor dropped vote reply".to_string())
    }

    pub async fn append_entries(
        &self,
        rpc_id: u64,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, String> {
        let (response, rx) = oneshot::channel();
        self.tx
            .send(Command::AppendEntries {
                rpc_id,
                request,
                response,
            })
            .map_err(|_| "Raft actor is shut down".to_string())?;
        rx.await
            .map_err(|_| "Raft actor dropped append reply".to_string())
    }

    pub async fn propose(&self, request_id: u64, payload: Vec<u8>) -> ClientResult {
        let (response, rx) = oneshot::channel();
        self.tx
            .send(Command::ClientProposal {
                request_id,
                payload,
                response,
            })
            .map_err(|_| "Raft actor is shut down".to_string())?;
        rx.await
            .map_err(|_| "Raft actor dropped proposal reply".to_string())?
    }
    pub fn node_view(&self) -> Arc<Mutex<RaftNodeView>> {
        Arc::new(Mutex::new(RaftNodeView { raft: self.clone() }))
    }
    fn view(&self) -> Option<NodeViewData> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Command::Query { response: tx }).ok()?;
        rx.recv().ok()
    }
    fn entry(&self, index: u64) -> Option<crate::models::LogEntry> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(Command::QueryEntry {
                index,
                response: tx,
            })
            .ok()?;
        rx.recv().ok().flatten()
    }
    pub fn applied_state(&self) -> Option<Vec<u8>> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(Command::QueryAppliedState { response: tx })
            .ok()?;
        rx.recv().ok().flatten()
    }
    fn legacy_append(&self, payload: Vec<u8>) -> Result<u64, String> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(Command::LegacyAppend {
                payload,
                response: tx,
            })
            .map_err(|_| "Raft actor is shut down".to_string())?;
        rx.recv()
            .map_err(|_| "Raft actor dropped append reply".to_string())?
    }
}

impl RaftNodeView {
    pub fn get_state(&self) -> RaftStateSnapshot {
        self.raft.snapshot()
    }
    pub fn get_current_term(&self) -> u64 {
        self.get_state().current_term
    }
    pub fn get_current_leader(&self) -> Option<NodeId> {
        self.raft.leader_id()
    }
    pub fn get_server_state(&self) -> ServerState {
        self.get_state().server_state
    }
    pub fn get_log_length(&self) -> u64 {
        self.raft.view().map_or(0, |view| view.log_length)
    }
    pub fn get_last_log_info(&self) -> (u64, u64) {
        self.raft.view().map_or((0, 0), |view| view.last_log_info)
    }
    pub fn get_entry(&self, index: u64) -> Option<crate::models::LogEntry> {
        self.raft.entry(index)
    }
    pub fn append_new_entry(&mut self, payload: Vec<u8>) -> Result<u64, String> {
        self.raft.legacy_append(payload)
    }
}

pub struct RaftActor;

impl RaftActor {
    pub fn spawn(node: RaftNode, io_handle: Handle) -> RaftHandle {
        let config = node.get_config().clone();
        let snapshot = Arc::new(RwLock::new(node.get_state()));
        let leader = Arc::new(RwLock::new(node.get_current_leader()));
        let (tx, rx) = mpsc::channel();
        let handle = RaftHandle {
            tx: tx.clone(),
            snapshot: Arc::clone(&snapshot),
            leader: Arc::clone(&leader),
            node_id: config.node_id,
        };
        thread::Builder::new()
            .name(format!("raft-actor-{}", config.node_id))
            .spawn(move || Actor::new(node, config, rx, tx, snapshot, leader, io_handle).run())
            .expect("failed to spawn Raft actor thread");
        handle
    }
}

struct Actor {
    node: RaftNode,
    config: ClusterConfig,
    rx: mpsc::Receiver<Command>,
    tx: mpsc::Sender<Command>,
    snapshot: Arc<RwLock<RaftStateSnapshot>>,
    leader: Arc<RwLock<Option<NodeId>>>,
    io: Handle,
    client: RaftGrpcClient,
    vote_replies: HashMap<u64, oneshot::Sender<RequestVoteResponse>>,
    append_replies: HashMap<u64, oneshot::Sender<AppendEntriesResponse>>,
    pending: HashMap<u64, oneshot::Sender<ClientResult>>,
    waiters: HashMap<u64, (u64, u64, oneshot::Sender<ClientResult>)>,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
}

impl Actor {
    fn new(
        node: RaftNode,
        config: ClusterConfig,
        rx: mpsc::Receiver<Command>,
        tx: mpsc::Sender<Command>,
        snapshot: Arc<RwLock<RaftStateSnapshot>>,
        leader: Arc<RwLock<Option<NodeId>>>,
        io: Handle,
    ) -> Self {
        let now = Instant::now();
        let election_deadline = now + Self::election_timeout(&config);
        let heartbeat_deadline = now + Duration::from_millis(config.heartbeat_interval);
        Self {
            node,
            client: RaftGrpcClient::new(config.clone()),
            config,
            rx,
            tx,
            snapshot,
            leader,
            io,
            vote_replies: HashMap::new(),
            append_replies: HashMap::new(),
            pending: HashMap::new(),
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
                .rx
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
            Command::RequestVote {
                rpc_id,
                request,
                response,
            } => {
                self.vote_replies.insert(rpc_id, response);
                self.drive(Event::RequestVote { rpc_id, request });
            }
            Command::AppendEntries {
                rpc_id,
                request,
                response,
            } => {
                self.append_replies.insert(rpc_id, response);
                self.drive(Event::AppendEntries { rpc_id, request });
            }
            Command::ClientProposal {
                request_id,
                payload,
                response,
            } => {
                self.pending.insert(request_id, response);
                self.drive(Event::ClientProposal {
                    request_id,
                    payload,
                });
            }
            Command::CancelClientRequest { request_id } => {
                self.drive(Event::CancelClientRequest { request_id })
            }
            Command::Query { response } => {
                let _ = response.send(NodeViewData {
                    log_length: self.node.get_log_length(),
                    last_log_info: self.node.get_last_log_info(),
                });
            }
            Command::QueryEntry { index, response } => {
                let _ = response.send(self.node.get_entry(index));
            }
            Command::QueryAppliedState { response } => {
                let _ = response.send(self.node.get_application_state());
            }
            Command::LegacyAppend { payload, response } => {
                let result = self
                    .node
                    .append_new_entry(payload)
                    .map_err(|error| error.to_string());
                if result.is_ok() {
                    self.send_replication();
                }
                let _ = response.send(result);
            }
            Command::Shutdown => return false,
        }
        true
    }

    fn drive(&mut self, event: Event) {
        let state_before = self.node.get_state();
        match event {
            Event::Tick => self.tick(),
            Event::RequestVote { rpc_id, request } => {
                let response = self.node.handle_request_vote(request);
                if response.vote_granted {
                    self.reset_election();
                }
                if let Some(reply) = self.vote_replies.remove(&rpc_id) {
                    let _ = reply.send(response);
                }
            }
            Event::AppendEntries { rpc_id, request } => {
                let response = self.node.handle_append_entries(request);
                if response.success {
                    self.reset_election();
                }
                if let Some(reply) = self.append_replies.remove(&rpc_id) {
                    let _ = reply.send(response);
                }
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
            } => self.propose(request_id, payload),
            Event::CancelClientRequest { request_id } => {
                self.pending.remove(&request_id);
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
    fn propose(&mut self, request_id: u64, payload: Vec<u8>) {
        if self.node.get_server_state() != ServerState::Leader {
            self.reject(request_id, "Not the leader");
            return;
        }
        match self.node.append_new_entry(payload) {
            Ok(index) => {
                let term = self.node.get_current_term();
                if let Some(waiter) = self.pending.remove(&request_id) {
                    self.waiters.insert(request_id, (term, index, waiter));
                }
                self.send_replication();
            }
            Err(error) => self.reject(request_id, &format!("Failed to append entry: {error}")),
        }
    }
    fn reject(&mut self, request_id: u64, message: &str) {
        if let Some(reply) = self.pending.remove(&request_id) {
            let _ = reply.send(Err(message.into()));
        }
    }
    fn complete_committed(&mut self, commit: u64) {
        let ready: Vec<_> = self
            .waiters
            .iter()
            .filter_map(|(&id, &(_, index, _))| (index <= commit).then_some(id))
            .collect();
        for id in ready {
            if let Some((term, index, reply)) = self.waiters.remove(&id) {
                let _ = reply.send(Ok((term, index)));
            }
        }
    }
    fn fail_term(&mut self, term: u64) {
        let stale: Vec<_> = self
            .waiters
            .iter()
            .filter_map(|(&id, &(waiter_term, _, _))| (waiter_term == term).then_some(id))
            .collect();
        for id in stale {
            if let Some((_, _, reply)) = self.waiters.remove(&id) {
                let _ = reply.send(Err("Leadership changed before the entry committed".into()));
            }
        }
    }
    fn fail_all(&mut self, message: &str) {
        for (_, reply) in self.pending.drain() {
            let _ = reply.send(Err(message.into()));
        }
        for (_, (_, _, reply)) in self.waiters.drain() {
            let _ = reply.send(Err(message.into()));
        }
    }

    fn send_votes(&self, request: RequestVoteRequest) {
        for peer in self.config.get_other_nodes() {
            let tx = self.tx.clone();
            let client = self.client.clone();
            let to = peer.node_id;
            let request = request.clone();
            self.io.spawn(async move {
                if let Ok(response) = client.request_vote(to, request).await {
                    let _ = tx.send(Command::Event(Event::VoteResponse { from: to, response }));
                }
            });
        }
    }
    fn send_replication(&self) {
        for (to, request) in self.node.build_replication_requests() {
            let tx = self.tx.clone();
            let client = self.client.clone();
            self.io.spawn(async move {
                if let Ok(response) = client.append_entries(to, request.clone()).await {
                    let _ = tx.send(Command::Event(Event::AppendResponse {
                        from: to,
                        request,
                        response,
                    }));
                }
            });
        }
    }
}
