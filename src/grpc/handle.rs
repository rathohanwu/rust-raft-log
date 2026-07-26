use std::sync::{mpsc, Arc, RwLock};

use tokio::sync::oneshot;

use super::actor::{Command, Event};
use crate::consensus::RaftStateSnapshot;
use crate::models::{
    AppendEntriesRequest, AppendEntriesResponse, NodeId, RequestVoteRequest, RequestVoteResponse,
    ServerState,
};

pub(crate) type ClientResult = Result<(u64, u64), String>;

pub(crate) struct NodeViewData {
    pub(crate) log_length: u64,
    pub(crate) last_log_info: (u64, u64),
}

/// Cloneable ingress to the single Raft actor. It contains no Raft state.
#[derive(Clone)]
pub struct RaftHandle {
    command_tx: mpsc::Sender<Command>,
    snapshot: Arc<RwLock<RaftStateSnapshot>>,
    leader: Arc<RwLock<Option<NodeId>>>,
    node_id: NodeId,
}

#[derive(Clone)]
pub struct RaftNodeView {
    raft_handle: RaftHandle,
}

impl RaftHandle {
    pub(crate) fn new(
        command_tx: mpsc::Sender<Command>,
        snapshot: Arc<RwLock<RaftStateSnapshot>>,
        leader: Arc<RwLock<Option<NodeId>>>,
        node_id: NodeId,
    ) -> Self {
        Self {
            command_tx,
            snapshot,
            leader,
            node_id,
        }
    }

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
        let _ = self.command_tx.send(Command::Shutdown);
    }
    pub fn cancel_client_request(&self, request_id: u64) {
        let _ = self
            .command_tx
            .send(Command::Event(Event::CancelClientRequest { request_id }));
    }

    pub async fn request_vote(
        &self,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, String> {
        self.call(
            |reply| Command::Event(Event::RequestVote { request, reply }),
            "Raft actor dropped vote reply",
        )
        .await
    }

    pub async fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, String> {
        self.call(
            |reply| Command::Event(Event::AppendEntries { request, reply }),
            "Raft actor dropped append reply",
        )
        .await
    }

    pub async fn propose(&self, request_id: u64, payload: Vec<u8>) -> ClientResult {
        self.call(
            |reply| {
                Command::Event(Event::ClientProposal {
                    request_id,
                    payload,
                    reply,
                })
            },
            "Raft actor dropped proposal reply",
        )
        .await?
    }
    pub fn node_view(&self) -> RaftNodeView {
        RaftNodeView {
            raft_handle: self.clone(),
        }
    }
    async fn call<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<T>) -> Command,
        dropped: &'static str,
    ) -> Result<T, String> {
        let (reply, rx) = oneshot::channel();
        self.command_tx
            .send(command(reply))
            .map_err(|_| "Raft actor is shut down".to_string())?;
        rx.await.map_err(|_| dropped.to_string())
    }
    fn query<T>(&self, command: impl FnOnce(mpsc::Sender<T>) -> Command) -> Option<T> {
        let (reply, rx) = mpsc::channel();
        self.command_tx.send(command(reply)).ok()?;
        rx.recv().ok()
    }
    fn view(&self) -> Option<NodeViewData> {
        self.query(|response| Command::Query { response })
    }
    fn entry(&self, index: u64) -> Option<crate::models::LogEntry> {
        self.query(|response| Command::QueryEntry { index, response })?
    }
}

impl RaftNodeView {
    pub fn get_state(&self) -> RaftStateSnapshot {
        self.raft_handle.snapshot()
    }
    pub fn get_current_term(&self) -> u64 {
        self.get_state().current_term
    }
    pub fn get_current_leader(&self) -> Option<NodeId> {
        self.raft_handle.leader_id()
    }
    pub fn get_server_state(&self) -> ServerState {
        self.get_state().server_state
    }
    pub fn get_log_length(&self) -> u64 {
        self.raft_handle.view().map_or(0, |view| view.log_length)
    }
    pub fn get_last_log_info(&self) -> (u64, u64) {
        self.raft_handle
            .view()
            .map_or((0, 0), |view| view.last_log_info)
    }
    pub fn get_entry(&self, index: u64) -> Option<crate::models::LogEntry> {
        self.raft_handle.entry(index)
    }
}
