use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status, transport::Server};

use crate::log::{RaftNode, models::ClusterConfig};
use super::proto::{self, raft_service_server::{RaftService, RaftServiceServer}};
use super::client::RaftGrpcClient;

/// gRPC service implementation that wraps RaftNode
pub struct RaftGrpcService {
    raft_node: Arc<Mutex<RaftNode>>,
}

impl RaftGrpcService {
    pub fn new(raft_node: Arc<Mutex<RaftNode>>) -> Self {
        Self { raft_node }
    }
}

#[tonic::async_trait]
impl RaftService for RaftGrpcService {
    async fn request_vote(
        &self,
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let rust_request: crate::log::raft_rpc::RequestVoteRequest = req.into();
        
        // Use spawn_blocking to handle the synchronous RaftNode method
        let raft_node = Arc::clone(&self.raft_node);
        let response = tokio::task::spawn_blocking(move || {
            let mut node = raft_node.lock().map_err(|_| {
                Status::internal("Failed to acquire lock on RaftNode")
            })?;
            
            let rust_response = node.handle_request_vote(rust_request);
            Ok::<_, Status>(rust_response)
        })
        .await
        .map_err(|_| Status::internal("Task join error"))??;
        
        let proto_response: proto::RequestVoteResponse = response.into();
        Ok(Response::new(proto_response))
    }

    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let rust_request: crate::log::raft_rpc::AppendEntriesRequest = req.into();
        
        // Use spawn_blocking to handle the synchronous RaftNode method
        let raft_node = Arc::clone(&self.raft_node);
        let response = tokio::task::spawn_blocking(move || {
            let mut node = raft_node.lock().map_err(|_| {
                Status::internal("Failed to acquire lock on RaftNode")
            })?;
            
            let rust_response = node.handle_append_entries(rust_request);
            Ok::<_, Status>(rust_response)
        })
        .await
        .map_err(|_| Status::internal("Task join error"))??;
        
        let proto_response: proto::AppendEntriesResponse = response.into();
        Ok(Response::new(proto_response))
    }
}

/// gRPC server that hosts the Raft service and includes client for outbound communication
pub struct RaftGrpcServer {
    raft_node: Arc<Mutex<RaftNode>>,
    grpc_client: RaftGrpcClient,
    config: ClusterConfig,
}

impl RaftGrpcServer {
    pub fn new(raft_node: RaftNode) -> Self {
        let config = raft_node.get_config().clone();
        let grpc_client = RaftGrpcClient::new(config.clone());
        Self {
            raft_node: Arc::new(Mutex::new(raft_node)),
            grpc_client,
            config,
        }
    }

    /// Start the gRPC server using the port from NodeInfo configuration
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let this_node = self.config.get_this_node()
            .ok_or("Current node not found in cluster configuration")?;
        
        let addr = format!("0.0.0.0:{}", this_node.port).parse()?;
        
        let service = RaftGrpcService::new(Arc::clone(&self.raft_node));
        let svc = RaftServiceServer::new(service);

        println!("Starting Raft gRPC server on {}", addr);
        
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await?;

        Ok(())
    }

    /// Get a reference to the wrapped RaftNode for external access
    pub fn get_raft_node(&self) -> Arc<Mutex<RaftNode>> {
        Arc::clone(&self.raft_node)
    }

    /// Get a reference to the gRPC client for outbound communication
    pub fn get_grpc_client(&self) -> &RaftGrpcClient {
        &self.grpc_client
    }
}

impl Clone for RaftGrpcServer {
    fn clone(&self) -> Self {
        Self {
            raft_node: Arc::clone(&self.raft_node),
            grpc_client: self.grpc_client.clone(),
            config: self.config.clone(),
        }
    }
}
