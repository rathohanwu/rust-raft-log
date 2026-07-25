use log::info;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::actor::RaftActor;
use super::handle::{RaftHandle, RaftNodeView};
use super::proto::raft_service_server::RaftServiceServer;
use super::service::RaftGrpcService;
use crate::{consensus::RaftNode, models::ClusterConfig};

/// gRPC server backed by a single actor that exclusively owns RaftNode.
pub struct RaftGrpcServer {
    raft_handle: RaftHandle,
    config: ClusterConfig,
    available: Arc<AtomicBool>,
}

impl RaftGrpcServer {
    pub fn new(node: RaftNode) -> Self {
        let config = node.get_config().clone();
        let raft_handle = RaftActor::spawn(node);
        Self {
            raft_handle,
            config,
            available: Arc::new(AtomicBool::new(true)),
        }
    }
    fn service(&self) -> RaftServiceServer<RaftGrpcService> {
        RaftServiceServer::new(RaftGrpcService::new(
            self.raft_handle.clone(),
            Arc::clone(&self.available),
        ))
    }
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let node = self
            .config
            .get_this_node()
            .ok_or("Current node not found in cluster configuration")?;
        let addr = format!("0.0.0.0:{}", node.port).parse()?;
        info!("Starting Raft gRPC server on {}", addr);
        Server::builder()
            .add_service(self.service())
            .serve(addr)
            .await?;
        self.shutdown();
        Ok(())
    }
    pub fn start_with_listener(
        &self,
        listener: tokio::net::TcpListener,
    ) -> (
        JoinHandle<()>,
        JoinHandle<Result<(), tonic::transport::Error>>,
    ) {
        let service = self.service();
        (
            tokio::spawn(async {}),
            tokio::spawn(async move {
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
            }),
        )
    }
    pub fn node_view(&self) -> Arc<Mutex<RaftNodeView>> {
        self.raft_handle.node_view()
    }
    pub fn shutdown(&self) {
        self.available.store(false, Ordering::Release);
        self.raft_handle.shutdown();
    }
}
impl Clone for RaftGrpcServer {
    fn clone(&self) -> Self {
        Self {
            raft_handle: self.raft_handle.clone(),
            config: self.config.clone(),
            available: Arc::clone(&self.available),
        }
    }
}
