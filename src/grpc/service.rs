use super::handle::RaftHandle;
use super::proto::raft_service_server::RaftService;
use crate::models::types_proto::{
    ClientRequestMessage, ClientResponseMessage, ProtoAppendEntriesRequest,
    ProtoAppendEntriesResponse, ProtoRequestVoteRequest, ProtoRequestVoteResponse,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::time::{timeout, Duration};
use tonic::{Request, Response, Status};

pub struct RaftGrpcService {
    raft_handle: RaftHandle,
    available: Arc<AtomicBool>,
    next_rpc_id: AtomicU64,
    next_client_request_id: AtomicU64,
}
impl RaftGrpcService {
    pub fn new(raft_handle: RaftHandle, available: Arc<AtomicBool>) -> Self {
        Self {
            raft_handle,
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
            self.raft_handle
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
            self.raft_handle
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
        let raft_handle = self.raft_handle.clone();
        let result = timeout(
            Duration::from_secs(2),
            raft_handle.propose(id, request.into_inner().payload),
        )
        .await;
        let response = match result {
            Ok(Ok((_term, index))) => ClientResponseMessage {
                success: true,
                leader_id: self.raft_handle.node_id(),
                log_index: index,
                error_message: String::new(),
            },
            Ok(Err(error)) => ClientResponseMessage {
                success: false,
                leader_id: self.raft_handle.leader_id().unwrap_or(0),
                log_index: 0,
                error_message: error,
            },
            Err(_) => {
                self.raft_handle.cancel_client_request(id);
                ClientResponseMessage {
                    success: false,
                    leader_id: self.raft_handle.leader_id().unwrap_or(0),
                    log_index: 0,
                    error_message: "Timed out waiting for the entry to commit".into(),
                }
            }
        };
        Ok(Response::new(response))
    }
}
