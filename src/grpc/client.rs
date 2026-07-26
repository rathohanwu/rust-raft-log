use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use super::proto::raft_service_client::RaftServiceClient;
use crate::models::types_proto::{
    ClientRequestMessage, ClientResponseMessage, ProtoAppendEntriesRequest, ProtoRequestVoteRequest,
};
use crate::models::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterConfig, NodeId, RequestVoteRequest,
    RequestVoteResponse,
};

const RAFT_RPC_TIMEOUT: Duration = Duration::from_millis(500);
const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

/// gRPC client with persistent connection management for Raft RPCs
#[derive(Clone)]
pub struct RaftGrpcClient {
    /// Persistent connections to other nodes in the cluster
    connections: Arc<RwLock<HashMap<NodeId, RaftServiceClient<Channel>>>>,
    /// Cluster configuration for node discovery
    config: ClusterConfig,
}

impl RaftGrpcClient {
    /// Create a new RaftGrpcClient with the given cluster configuration
    pub fn new(config: ClusterConfig) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Get or create a connection to the specified node
    async fn get_connection(&self, node_id: NodeId) -> Result<RaftServiceClient<Channel>, Status> {
        // First, try to get existing connection
        {
            let connections = self.connections.read().await;
            if let Some(client) = connections.get(&node_id) {
                return Ok(client.clone());
            }
        }

        // Connection doesn't exist, create a new one
        let node_info = self
            .config
            .get_node(node_id)
            .ok_or_else(|| Status::not_found(format!("Node {} not found in cluster", node_id)))?;

        let endpoint = Endpoint::from_shared(format!("http://{}", node_info.get_address()))
            .map_err(|e| Status::internal(format!("Invalid endpoint: {}", e)))?
            .connect_timeout(Duration::from_millis(250));

        let channel = endpoint.connect().await.map_err(|e| {
            Status::unavailable(format!("Failed to connect to node {}: {}", node_id, e))
        })?;

        let client = RaftServiceClient::new(channel);

        // Store the connection for future use
        {
            let mut connections = self.connections.write().await;
            connections.insert(node_id, client.clone());
        }

        Ok(client)
    }

    /// Send a RequestVote RPC to the specified node
    pub async fn request_vote(
        &self,
        node_id: NodeId,
        request: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, Status> {
        let mut client = self.get_connection(node_id).await?;

        let proto_request: ProtoRequestVoteRequest = request.into();
        let mut request = Request::new(proto_request);
        request.set_timeout(RAFT_RPC_TIMEOUT);
        let response = client.request_vote(request).await;
        if response.is_err() {
            self.close_connection(node_id).await;
        }
        Ok(response?.into_inner().into())
    }

    /// Send an AppendEntries RPC to the specified node
    pub async fn append_entries(
        &self,
        node_id: NodeId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Status> {
        let mut client = self.get_connection(node_id).await?;

        let proto_request: ProtoAppendEntriesRequest = request.into();
        let mut request = Request::new(proto_request);
        request.set_timeout(RAFT_RPC_TIMEOUT);
        let response = client.append_entries(request).await;
        if response.is_err() {
            self.close_connection(node_id).await;
        }
        Ok(response?.into_inner().into())
    }

    /// Send a client request to the specified node
    pub async fn client_request(
        &self,
        node_id: NodeId,
        payload: Vec<u8>,
    ) -> Result<ClientResponseMessage, Status> {
        let mut client = self.get_connection(node_id).await?;

        let proto_request = ClientRequestMessage { payload };
        let mut request = Request::new(proto_request);
        // The server deliberately waits up to two seconds for majority commit.
        request.set_timeout(CLIENT_REQUEST_TIMEOUT);
        let response = client.client_request(request).await;
        if response.is_err() {
            self.close_connection(node_id).await;
        }
        Ok(response?.into_inner())
    }

    /// Close connection to a specific node (useful for handling connection errors)
    pub async fn close_connection(&self, node_id: NodeId) {
        let mut connections = self.connections.write().await;
        connections.remove(&node_id);
    }
}
