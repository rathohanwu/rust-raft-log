use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tonic::{transport::Server, Request, Response, Status};

use super::client::RaftGrpcClient;
use super::event_loop::RaftEventLoop;
use super::proto::{
    self,
    raft_service_server::{RaftService, RaftServiceServer},
};
use crate::log::{models::ClusterConfig, RaftNode};

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
        request: Request<proto::RequestVoteRequest>,
    ) -> Result<Response<proto::RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let rust_request: crate::log::raft_rpc::RequestVoteRequest = req.into();

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

        let proto_response: proto::AppendEntriesResponse = response.into();
        Ok(Response::new(proto_response))
    }
}

/// gRPC server that hosts the Raft service (only receives requests)
pub struct RaftGrpcServer {
    raft_node: Arc<Mutex<RaftNode>>,
    event_loop: RaftEventLoop,
    config: ClusterConfig,
}

impl RaftGrpcServer {
    pub fn new(raft_node: RaftNode) -> Self {
        let config = raft_node.get_config().clone();
        let raft_node_arc = Arc::new(Mutex::new(raft_node));

        // Event loop creates and owns the gRPC client
        let grpc_client = RaftGrpcClient::new(config.clone());
        let event_loop = RaftEventLoop::new(Arc::clone(&raft_node_arc), grpc_client, &config);

        Self {
            raft_node: raft_node_arc,
            event_loop,
            config,
        }
    }

    /// Start the gRPC server and event loop
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let this_node = self
            .config
            .get_this_node()
            .ok_or("Current node not found in cluster configuration")?;

        let addr = format!("0.0.0.0:{}", this_node.port).parse()?;

        let service = RaftGrpcService::new(
            Arc::clone(&self.raft_node),
            self.event_loop.clone(),
        );
        let svc = RaftServiceServer::new(service);

        println!("Starting Raft gRPC server on {}", addr);

        // Start gRPC server
        let server_result = Server::builder().add_service(svc).serve(addr).await;

        // Start event loop in background
        let event_loop_handle = {
            let event_loop = self.event_loop.clone();
            tokio::spawn(async move {
                event_loop.run().await;
            })
        };

        // Shutdown event loop when server stops
        self.event_loop.shutdown();
        event_loop_handle.abort();

        server_result?;
        Ok(())
    }

    /// Start the gRPC server and event loop, returning handles for manual control
    pub async fn start_with_handles(
        &self,
    ) -> Result<
        (
            JoinHandle<()>,
            JoinHandle<Result<(), tonic::transport::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let this_node = self
            .config
            .get_this_node()
            .ok_or("Current node not found in cluster configuration")?;

        let addr = format!("0.0.0.0:{}", this_node.port).parse()?;

        let service = RaftGrpcService::new(
            Arc::clone(&self.raft_node),
            self.event_loop.clone(),
        );
        let svc = RaftServiceServer::new(service);

        println!("Starting Raft gRPC server on {}", addr);

        // Start event loop in background
        let event_loop_handle = {
            let event_loop = self.event_loop.clone();
            tokio::spawn(async move {
                event_loop.run().await;
            })
        };

        // Start gRPC server
        let server_handle =
            tokio::spawn(async move { Server::builder().add_service(svc).serve(addr).await });

        Ok((event_loop_handle, server_handle))
    }

    /// Get a reference to the wrapped RaftNode for external access
    pub fn get_raft_node(&self) -> Arc<Mutex<RaftNode>> {
        Arc::clone(&self.raft_node)
    }

    /// Get a reference to the gRPC client from the event loop
    pub fn get_grpc_client(&self) -> &RaftGrpcClient {
        self.event_loop.get_grpc_client()
    }

    /// Get a reference to the event loop
    pub fn get_event_loop(&self) -> &RaftEventLoop {
        &self.event_loop
    }

    /// Shutdown the server and event loop
    pub fn shutdown(&self) {
        self.event_loop.shutdown();
    }
}

impl Clone for RaftGrpcServer {
    fn clone(&self) -> Self {
        Self {
            raft_node: Arc::clone(&self.raft_node),
            event_loop: self.event_loop.clone(),
            config: self.config.clone(),
        }
    }
}
