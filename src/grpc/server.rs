use log::info;
use std::future::Future;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use super::actor::RaftActor;
use super::handle::{RaftHandle, RaftNodeView};
use super::proto::raft_service_server::RaftServiceServer;
use super::service::RaftGrpcService;
use crate::{consensus::RaftNode, models::ClusterConfig};

/// Owns a Raft actor and the gRPC services that expose it.
///
/// The runtime can be embedded in another tonic server through
/// [`Self::grpc_service`], or it can bind and serve its configured address with
/// [`Self::serve`]. Shutting it down stops both the actor and any server started
/// through this runtime.
#[derive(Clone)]
pub struct RaftRuntime {
    raft_handle: RaftHandle,
    config: ClusterConfig,
    shutdown_tx: watch::Sender<bool>,
}

impl RaftRuntime {
    pub fn new(node: RaftNode) -> Self {
        let config = node.get_config().clone();
        let raft_handle = RaftActor::spawn(node);
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            raft_handle,
            config,
            shutdown_tx,
        }
    }

    /// Returns the tonic service backed by this runtime's Raft actor.
    pub fn grpc_service(&self) -> RaftServiceServer<RaftGrpcService> {
        RaftServiceServer::new(RaftGrpcService::new(
            self.raft_handle.clone(),
            self.shutdown_tx.subscribe(),
        ))
    }

    /// Serves the configured node address until [`Self::shutdown`] is called.
    pub async fn serve(&self) -> Result<(), Box<dyn std::error::Error>> {
        let node = self
            .config
            .get_this_node()
            .ok_or("Current node not found in cluster configuration")?;
        let addr = format!("0.0.0.0:{}", node.port).parse()?;
        info!("Starting Raft gRPC server on {}", addr);
        Server::builder()
            .add_service(self.grpc_service())
            .serve_with_shutdown(addr, self.shutdown_signal())
            .await?;
        self.shutdown();
        Ok(())
    }

    /// Serves an already-bound listener until [`Self::shutdown`] is called.
    pub fn serve_with_listener(
        &self,
        listener: tokio::net::TcpListener,
    ) -> JoinHandle<Result<(), tonic::transport::Error>> {
        let service = self.grpc_service();
        let shutdown = self.shutdown_signal();
        tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
                .await
        })
    }

    pub fn node_view(&self) -> RaftNodeView {
        self.raft_handle.node_view()
    }

    pub fn shutdown(&self) {
        self.shutdown_tx.send_replace(true);
        self.raft_handle.shutdown();
    }

    fn shutdown_signal(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        async move {
            if !*shutdown_rx.borrow() {
                let _ = shutdown_rx.changed().await;
            }
        }
    }
}
