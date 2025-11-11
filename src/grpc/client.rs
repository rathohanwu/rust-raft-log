use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use log::{error};

use crate::models::{
    NodeId, NodeInfo, ClusterConfig,
    RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
};
use super::proto::raft_service_client::RaftServiceClient;
use crate::models::types_proto::{
    ProtoRequestVoteRequest, ProtoAppendEntriesRequest,
    ClientRequestMessage, ClientResponseMessage,
};
use crate::models::conversions::*;

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
        let node_info = self.config.get_node(node_id)
            .ok_or_else(|| Status::not_found(format!("Node {} not found in cluster", node_id)))?;

        let endpoint = Endpoint::from_shared(format!("http://{}", node_info.get_address()))
            .map_err(|e| Status::internal(format!("Invalid endpoint: {}", e)))?;

        let channel = endpoint.connect().await
            .map_err(|e| Status::unavailable(format!("Failed to connect to node {}: {}", node_id, e)))?;

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
        let response = client.request_vote(Request::new(proto_request)).await?;
        
        let rust_response: RequestVoteResponse = response.into_inner().into();
        Ok(rust_response)
    }

    /// Send an AppendEntries RPC to the specified node
    pub async fn append_entries(
        &self,
        node_id: NodeId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, Status> {
        let mut client = self.get_connection(node_id).await?;
        
        let proto_request: ProtoAppendEntriesRequest = request.into();
        let response = client.append_entries(Request::new(proto_request)).await?;
        
        let rust_response: AppendEntriesResponse = response.into_inner().into();
        Ok(rust_response)
    }

    /// Send a client request to the specified node
    pub async fn client_request(
        &self,
        node_id: NodeId,
        payload: Vec<u8>,
    ) -> Result<ClientResponseMessage, Status> {
        let mut client = self.get_connection(node_id).await?;

        let proto_request = ClientRequestMessage { payload };
        let response = client.client_request(Request::new(proto_request)).await?;

        Ok(response.into_inner())
    }

    /// Send RequestVote RPCs to all other nodes in the cluster
    /// Returns a vector of (node_id, result) pairs
    pub async fn broadcast_request_vote(
        &self,
        request: RequestVoteRequest,
    ) -> Vec<(NodeId, Result<RequestVoteResponse, Status>)> {
        let other_nodes = self.config.get_other_nodes();
        let mut results = Vec::new();

        // Send requests concurrently to all nodes
        let mut handles = Vec::new();
        
        for node in other_nodes {
            let node_id = node.node_id;
            let request_clone = request.clone();
            let client = self.clone();
            
            let handle = tokio::spawn(async move {
                let result = client.request_vote(node_id, request_clone).await;
                (node_id, result)
            });
            
            handles.push(handle);
        }

        // Collect results
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => {
                    // Handle join error - this shouldn't normally happen
                    error!("Task join error: {}", e);
                }
            }
        }

        results
    }

    /// Send AppendEntries RPCs to specific nodes
    /// Returns a vector of (node_id, result) pairs
    pub async fn send_append_entries(
        &self,
        requests: Vec<(NodeId, AppendEntriesRequest)>,
    ) -> Vec<(NodeId, Result<AppendEntriesResponse, Status>)> {
        let mut results = Vec::new();
        let mut handles = Vec::new();

        // Send requests concurrently
        for (node_id, request) in requests {
            let client = self.clone();
            
            let handle = tokio::spawn(async move {
                let result = client.append_entries(node_id, request).await;
                (node_id, result)
            });
            
            handles.push(handle);
        }

        // Collect results
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => {
                    error!("Task join error: {}", e);
                }
            }
        }

        results
    }

    /// Close connection to a specific node (useful for handling connection errors)
    pub async fn close_connection(&self, node_id: NodeId) {
        let mut connections = self.connections.write().await;
        connections.remove(&node_id);
    }

    /// Close all connections
    pub async fn close_all_connections(&self) {
        let mut connections = self.connections.write().await;
        connections.clear();
    }

    /// Get the number of active connections
    pub async fn connection_count(&self) -> usize {
        let connections = self.connections.read().await;
        connections.len()
    }
}


