use super::actor::RaftHandle;
use super::proto::raft_service_server::RaftService;
use crate::models::types_proto::{
    ClientRequestMessage, ClientResponseMessage, GetAppliedStateRequest, GetAppliedStateResponse,
    ProtoAppendEntriesRequest, ProtoAppendEntriesResponse, ProtoRequestVoteRequest,
    ProtoRequestVoteResponse,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::time::{timeout, Duration};
use tonic::{Request, Response, Status};

pub struct RaftGrpcService {
    raft: RaftHandle,
    available: Arc<AtomicBool>,
    next_rpc_id: AtomicU64,
    next_client_request_id: AtomicU64,
}
impl RaftGrpcService {
    pub fn new(raft: RaftHandle, available: Arc<AtomicBool>) -> Self {
        Self {
            raft,
            available,
            next_rpc_id: AtomicU64::new(1),
            next_client_request_id: AtomicU64::new(1),
        }
    }
    fn available(&self) -> Result<(), Status> {
        self.available
            .load(Ordering::Acquire)
            .then_some(())
            .ok_or_else(|| Status::unavailable("Raft node is shut down"))
    }
    fn id(counter: &AtomicU64) -> u64 {
        counter.fetch_add(1, Ordering::Relaxed).max(1)
    }
}
#[tonic::async_trait]
impl RaftService for RaftGrpcService {
    async fn request_vote(
        &self,
        request: Request<ProtoRequestVoteRequest>,
    ) -> Result<Response<ProtoRequestVoteResponse>, Status> {
        self.available()?;
        Ok(Response::new(
            self.raft
                .request_vote(Self::id(&self.next_rpc_id), request.into_inner().into())
                .await
                .map_err(Status::unavailable)?
                .into(),
        ))
    }
    async fn append_entries(
        &self,
        request: Request<ProtoAppendEntriesRequest>,
    ) -> Result<Response<ProtoAppendEntriesResponse>, Status> {
        self.available()?;
        Ok(Response::new(
            self.raft
                .append_entries(Self::id(&self.next_rpc_id), request.into_inner().into())
                .await
                .map_err(Status::unavailable)?
                .into(),
        ))
    }
    async fn client_request(
        &self,
        request: Request<ClientRequestMessage>,
    ) -> Result<Response<ClientResponseMessage>, Status> {
        self.available()?;
        let id = Self::id(&self.next_client_request_id);
        let raft = self.raft.clone();
        let result = timeout(
            Duration::from_secs(2),
            raft.propose(id, request.into_inner().payload),
        )
        .await;
        let response = match result {
            Ok(Ok((_term, index))) => ClientResponseMessage {
                success: true,
                leader_id: self.raft.node_id(),
                log_index: index,
                error_message: String::new(),
            },
            Ok(Err(error)) => ClientResponseMessage {
                success: false,
                leader_id: self.raft.leader_id().unwrap_or(0),
                log_index: 0,
                error_message: error,
            },
            Err(_) => {
                self.raft.cancel_client_request(id);
                ClientResponseMessage {
                    success: false,
                    leader_id: self.raft.leader_id().unwrap_or(0),
                    log_index: 0,
                    error_message: "Timed out waiting for the entry to commit".into(),
                }
            }
        };
        Ok(Response::new(response))
    }
    async fn get_applied_state(
        &self,
        _request: Request<GetAppliedStateRequest>,
    ) -> Result<Response<GetAppliedStateResponse>, Status> {
        self.available()?;
        let last_applied = self.raft.snapshot().last_applied;
        let response = match self.raft.applied_state() {
            Some(state_json) => GetAppliedStateResponse {
                available: true,
                state_json,
                last_applied,
            },
            None => GetAppliedStateResponse {
                available: false,
                state_json: Vec::new(),
                last_applied,
            },
        };
        Ok(Response::new(response))
    }
}
