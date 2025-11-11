use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

use super::event_loop::RaftEventLoop;
use super::proto::raft_service_server::RaftService;
use crate::models::types_proto::{
    ProtoRequestVoteRequest, ProtoRequestVoteResponse,
    ProtoAppendEntriesRequest, ProtoAppendEntriesResponse,
    ClientRequestMessage, ClientResponseMessage,
};
use crate::{models::ClusterConfig, consensus::RaftNode};
use crate::ServerState;

/// gRPC service implementation that wraps RaftNode
pub struct RaftGrpcService {
    raft_node: Arc<Mutex<RaftNode>>,
    event_loop: RaftEventLoop,
}

impl RaftGrpcService {
    pub fn new(raft_node: Arc<Mutex<RaftNode>>, event_loop: RaftEventLoop) -> Self {
        Self {
            raft_node,
            event_loop,
        }
    }
}

#[tonic::async_trait]
impl RaftService for RaftGrpcService {
    async fn request_vote(
        &self,
        request: Request<ProtoRequestVoteRequest>,
    ) -> Result<Response<ProtoRequestVoteResponse>, Status> {
        let req = request.into_inner();
        let rust_request: crate::models::RequestVoteRequest = req.into();

        // Use spawn_blocking to handle the synchronous RaftNode method
        let raft_node = Arc::clone(&self.raft_node);
        let response = tokio::task::spawn_blocking(move || {
            let mut node = raft_node
                .lock()
                .map_err(|_| Status::internal("Failed to acquire lock on RaftNode"))?;

            let rust_response = node.handle_request_vote(rust_request);
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
        let proto_request = request.into_inner();

        let response = tokio::task::spawn_blocking({
            let raft_node = self.raft_node.clone();
            move || {
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

                    return Ok::<ClientResponseMessage, Status>(ClientResponseMessage {
                        success: false,
                        leader_id,
                        log_index: 0,
                        error_message,
                    });
                }

                // We are the leader - append the entry
                match node.append_new_entry(proto_request.payload) {
                    Ok(log_index) => {
                        Ok::<ClientResponseMessage, Status>(ClientResponseMessage {
                            success: true,
                            leader_id: node.get_node_id(),
                            log_index,
                            error_message: String::new(),
                        })
                    }
                    Err(e) => {
                        Ok::<ClientResponseMessage, Status>(ClientResponseMessage {
                            success: false,
                            leader_id: 0,
                            log_index: 0,
                            error_message: format!("Failed to append entry: {}", e),
                        })
                    }
                }
            }
        })
        .await
        .map_err(|_| Status::internal("Task join error"))??;

        Ok(Response::new(response))
    }
}
