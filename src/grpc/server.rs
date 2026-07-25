use log::info;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::actor::{RaftActor, RaftHandle, RaftNodeView};
use super::client::RaftGrpcClient;
use super::proto::raft_service_server::RaftServiceServer;
use super::service::RaftGrpcService;
use crate::{consensus::RaftNode, models::ClusterConfig};

/// gRPC server backed by a single actor that exclusively owns RaftNode.
pub struct RaftGrpcServer {
    raft: RaftHandle,
    grpc_client: RaftGrpcClient,
    config: ClusterConfig,
    available: Arc<AtomicBool>,
}

impl RaftGrpcServer {
    pub fn new(node: RaftNode) -> Self {
        let config = node.get_config().clone();
        let grpc_client = RaftGrpcClient::new(config.clone());
        let raft = RaftActor::spawn(node, tokio::runtime::Handle::current());
        Self {
            raft,
            grpc_client,
            config,
            available: Arc::new(AtomicBool::new(true)),
        }
    }
    fn service(&self) -> RaftServiceServer<RaftGrpcService> {
        RaftServiceServer::new(RaftGrpcService::new(
            self.raft.clone(),
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
    pub async fn start_with_handles(
        &self,
    ) -> Result<
        (
            JoinHandle<()>,
            JoinHandle<Result<(), tonic::transport::Error>>,
        ),
        Box<dyn std::error::Error>,
    > {
        let node = self
            .config
            .get_this_node()
            .ok_or("Current node not found in cluster configuration")?;
        let addr = format!("0.0.0.0:{}", node.port).parse()?;
        let service = self.service();
        Ok((
            tokio::spawn(async {}),
            tokio::spawn(async move { Server::builder().add_service(service).serve(addr).await }),
        ))
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
    pub fn raft_handle(&self) -> RaftHandle {
        self.raft.clone()
    }
    pub fn get_raft_node(&self) -> Arc<Mutex<RaftNodeView>> {
        self.raft.node_view()
    }
    pub fn get_grpc_client(&self) -> &RaftGrpcClient {
        &self.grpc_client
    }
    pub fn shutdown(&self) {
        self.available.store(false, Ordering::Release);
        self.raft.shutdown();
    }
}
impl Clone for RaftGrpcServer {
    fn clone(&self) -> Self {
        Self {
            raft: self.raft.clone(),
            grpc_client: self.grpc_client.clone(),
            config: self.config.clone(),
            available: Arc::clone(&self.available),
        }
    }
}
