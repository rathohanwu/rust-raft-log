use super::handle::RaftHandle;
use super::proto::raft_service_server::RaftService;
use crate::models::types_proto::{
    ClientRequestMessage, ClientResponseMessage, ProtoAppendEntriesRequest,
    ProtoAppendEntriesResponse, ProtoRequestVoteRequest, ProtoRequestVoteResponse,
};
use crate::models::NodeId;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;
use tokio::time::{timeout, Duration};
use tonic::{Request, Response, Status};

const COMMIT_WAIT: Duration = Duration::from_secs(2);

pub struct RaftGrpcService {
    raft_handle: RaftHandle,
    shutdown_rx: watch::Receiver<bool>,
    next_client_request_id: AtomicU64,
}
impl RaftGrpcService {
    pub fn new(raft_handle: RaftHandle, shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            raft_handle,
            shutdown_rx,
            next_client_request_id: AtomicU64::new(1),
        }
    }
    fn available(&self) -> Result<(), Status> {
        if *self.shutdown_rx.borrow() {
            Err(Status::unavailable("Raft node is shut down"))
        } else {
            Ok(())
        }
    }

    fn committed(leader_id: NodeId, log_index: u64) -> ClientResponseMessage {
        ClientResponseMessage {
            success: true,
            leader_id,
            log_index,
            error_message: String::new(),
        }
    }

    fn failed(leader_id: NodeId, error_message: impl Into<String>) -> ClientResponseMessage {
        ClientResponseMessage {
            success: false,
            leader_id,
            log_index: 0,
            error_message: error_message.into(),
        }
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
                .request_vote(request.into_inner().into())
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
                .append_entries(request.into_inner().into())
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
        let id = self
            .next_client_request_id
            .fetch_add(1, Ordering::Relaxed)
            .max(1);
        let result = timeout(
            COMMIT_WAIT,
            self.raft_handle.propose(id, request.into_inner().payload),
        )
        .await;
        let response = match result {
            Ok(Ok((_term, index))) => Self::committed(self.raft_handle.node_id(), index),
            Ok(Err(error)) => Self::failed(self.raft_handle.leader_id().unwrap_or(0), error),
            Err(_) => {
                self.raft_handle.cancel_client_request(id);
                Self::failed(
                    self.raft_handle.leader_id().unwrap_or(0),
                    "Timed out waiting for the entry to commit",
                )
            }
        };
        Ok(Response::new(response))
    }
}
