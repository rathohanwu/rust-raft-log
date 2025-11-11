use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tonic::transport::Server;
use log::info;

use super::client::RaftGrpcClient;
use super::event_loop::RaftEventLoop;
use super::service::RaftGrpcService;
use super::proto::raft_service_server::RaftServiceServer;
use crate::{models::ClusterConfig, consensus::RaftNode};



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

        info!("Starting Raft gRPC server on {}", addr);

        // Start event loop in background FIRST
        let event_loop_handle = {
            let event_loop = self.event_loop.clone();
            tokio::spawn(async move {
                event_loop.run().await;
            })
        };

        // Start gRPC server (this blocks until server stops)
        let server_result = Server::builder().add_service(svc).serve(addr).await;

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

        info!("Starting Raft gRPC server on {}", addr);

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
