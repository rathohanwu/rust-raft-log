use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::time::{sleep, timeout, Duration};
use tonic::{Request, Response, Status};

use super::event_loop::RaftEventLoop;
use super::proto::raft_service_server::RaftService;
use crate::models::types_proto::{
    ClientRequestMessage, ClientResponseMessage, ProtoAppendEntriesRequest,
    ProtoAppendEntriesResponse, ProtoRequestVoteRequest, ProtoRequestVoteResponse,
};
use crate::ServerState;
use crate::consensus::RaftNode;

/// gRPC service implementation that wraps RaftNode
pub struct RaftGrpcService {
    raft_node: Arc<Mutex<RaftNode>>,
    event_loop: RaftEventLoop,
    available: Arc<AtomicBool>,
}

impl RaftGrpcService {
    pub fn new(
        raft_node: Arc<Mutex<RaftNode>>,
        event_loop: RaftEventLoop,
        available: Arc<AtomicBool>,
    ) -> Self {
        Self {
            raft_node,
            event_loop,
            available,
        }
    }

    fn ensure_available(&self) -> Result<(), Status> {
        if self.available.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(Status::unavailable("Raft node is shut down"))
        }
    }
}

#[tonic::async_trait]
impl RaftService for RaftGrpcService {
    async fn request_vote(
        &self,
        request: Request<ProtoRequestVoteRequest>,
    ) -> Result<Response<ProtoRequestVoteResponse>, Status> {
        self.ensure_available()?;
        let req = request.into_inner();
        let rust_request: crate::models::RequestVoteRequest = req.into();

        // Use spawn_blocking to handle the synchronous RaftNode method
        let raft_node = Arc::clone(&self.raft_node);
        let event_loop = self.event_loop.clone();
        let response = tokio::task::spawn_blocking(move || {
            let mut node = raft_node
                .lock()
                .map_err(|_| Status::internal("Failed to acquire lock on RaftNode"))?;

            let rust_response = node.handle_request_vote(rust_request);
            if rust_response.vote_granted {
                event_loop.reset_election_timeout();
            }
            Ok::<_, Status>(rust_response)
        })
        .await
        .map_err(|_| Status::internal("Task join error"))??;

        let proto_response: ProtoRequestVoteResponse = response.into();
        Ok(Response::new(proto_response))
    }

    async fn append_entries(
        &self,
        request: Request<ProtoAppendEntriesRequest>,
    ) -> Result<Response<ProtoAppendEntriesResponse>, Status> {
        self.ensure_available()?;
        let req = request.into_inner();
        let rust_request: crate::models::AppendEntriesRequest = req.into();
        let raft_node = Arc::clone(&self.raft_node);
        let event_loop = self.event_loop.clone();

        let response = tokio::task::spawn_blocking(move || {
            let mut node = raft_node
                .lock()
                .map_err(|_| Status::internal("Failed to acquire lock on RaftNode"))?;

            let rust_response = node.handle_append_entries(rust_request);

            // If this was a successful AppendEntries from leader, reset election timeout
            if rust_response.success {
                event_loop.reset_election_timeout();
            }

            Ok::<_, Status>(rust_response)
        })
        .await
        .map_err(|_| Status::internal("Task join error"))??;

        let proto_response: ProtoAppendEntriesResponse = response.into();
        Ok(Response::new(proto_response))
    }

    async fn client_request(
        &self,
        request: Request<ClientRequestMessage>,
    ) -> Result<Response<ClientResponseMessage>, Status> {
        self.ensure_available()?;
        let proto_request = request.into_inner();

        let response = tokio::task::spawn_blocking({
            let raft_node = self.raft_node.clone();
            move || -> Result<Result<(u32, u64, u64), ClientResponseMessage>, Status> {
                let mut node = raft_node
                    .lock()
                    .map_err(|_| Status::internal("Failed to acquire lock on RaftNode"))?;

                // Check if this node is the leader
                if node.get_server_state() != ServerState::Leader {
                    // Not the leader - return error with leader hint if we know it
                    let leader_id = node.get_current_leader().unwrap_or(0);
                    let error_message = if leader_id != 0 {
                        format!("Not the leader. Current leader is node {}", leader_id)
                    } else {
                        "Not the leader. Current leader is unknown".to_string()
                    };

                    return Ok(Err(ClientResponseMessage {
                        success: false,
                        leader_id,
                        log_index: 0,
                        error_message,
                    }));
                }

                // We are the leader - append the entry durably. The response is
                // held below until a majority commits it.
                match node.append_new_entry(proto_request.payload) {
                    Ok(log_index) => Ok::<Result<(u32, u64, u64), ClientResponseMessage>, Status>(
                        Ok((node.get_node_id(), node.get_current_term(), log_index)),
                    ),
                    Err(e) => Ok::<Result<(u32, u64, u64), ClientResponseMessage>, Status>(Err(
                        ClientResponseMessage {
                            success: false,
                            leader_id: 0,
                            log_index: 0,
                            error_message: format!("Failed to append entry: {}", e),
                        },
                    )),
                }
            }
        })
        .await
        .map_err(|_| Status::internal("Task join error"))??;

        let (leader_id, entry_term, log_index) = match response {
            Ok(value) => value,
            Err(response) => return Ok(Response::new(response)),
        };

        let raft_node = Arc::clone(&self.raft_node);
        let committed = timeout(Duration::from_secs(2), async move {
            loop {
                let result = {
                    let node = raft_node
                        .lock()
                        .map_err(|_| Status::internal("Failed to acquire lock on RaftNode"))?;
                    if node.get_server_state() != ServerState::Leader
                        || node.get_node_id() != leader_id
                        || node.get_current_term() != entry_term
                    {
                        Some(false)
                    } else if node.get_state().commit_index >= log_index {
                        Some(true)
                    } else {
                        None
                    }
                };
                if let Some(result) = result {
                    return Ok::<bool, Status>(result);
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;

        let response = match committed {
            Ok(Ok(true)) => ClientResponseMessage {
                success: true,
                leader_id,
                log_index,
                error_message: String::new(),
            },
            Ok(Ok(false)) => ClientResponseMessage {
                success: false,
                leader_id: 0,
                log_index: 0,
                error_message: "Leadership changed before the entry committed".to_string(),
            },
            Ok(Err(status)) => return Err(status),
            Err(_) => ClientResponseMessage {
                success: false,
                leader_id: 0,
                log_index: 0,
                error_message: "Timed out waiting for the entry to commit".to_string(),
            },
        };

        Ok(Response::new(response))
    }
}
