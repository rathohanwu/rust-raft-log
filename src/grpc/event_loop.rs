use rand::Rng;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{interval, sleep, Instant};

use super::client::RaftGrpcClient;
use crate::log::{models::ServerState, RaftNode};


/// Configuration for Raft timing parameters
#[derive(Debug, Clone)]
pub struct RaftTimingConfig {
    /// Election timeout range (min, max) in milliseconds
    pub election_timeout_range: (u64, u64),
    /// Heartbeat interval in milliseconds (should be much smaller than election timeout)
    pub heartbeat_interval: u64,
}

impl Default for RaftTimingConfig {
    fn default() -> Self {
        Self {
            // Raft paper suggests 150-300ms election timeout
            election_timeout_range: (150, 300),
            // Heartbeat should be ~10x faster than election timeout
            heartbeat_interval: 50,
        }
    }
}

/// Raft event loop that handles timeouts, elections, and heartbeats
pub struct RaftEventLoop {
    raft_node: Arc<Mutex<RaftNode>>,
    grpc_client: RaftGrpcClient,
    timing_config: RaftTimingConfig,
    shutdown_signal: Arc<Mutex<bool>>,
    last_heartbeat: Arc<Mutex<Instant>>,
}

impl RaftEventLoop {
    pub fn new(
        raft_node: Arc<Mutex<RaftNode>>,
        grpc_client: RaftGrpcClient,
        timing_config: Option<RaftTimingConfig>,
    ) -> Self {
        Self {
            raft_node,
            grpc_client,
            timing_config: timing_config.unwrap_or_default(),
            shutdown_signal: Arc::new(Mutex::new(false)),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Start the event loop (runs indefinitely until shutdown)
    pub async fn run(&self) {
        println!("🔄 Starting Raft event loop...");

        let mut election_timeout = self.generate_election_timeout();
        let mut heartbeat_interval = interval(Duration::from_millis(self.timing_config.heartbeat_interval));
        let mut candidate_started_election = false;

        loop {
            // Check for shutdown signal
            if *self.shutdown_signal.lock().unwrap() {
                println!("🛑 Raft event loop shutting down...");
                break;
            }

            let current_state = {
                let node = self.raft_node.lock().unwrap();
                node.get_server_state()
            };

            match current_state {
                ServerState::Follower => {
                    // Reset candidate flag when becoming follower
                    candidate_started_election = false;

                    // Only job: check election timeout
                    if self.is_election_timeout_expired(&election_timeout) {
                        println!("⏰ Election timeout! Transitioning to candidate...");
                        // The create_vote_request() method will handle the state transition
                        election_timeout = self.generate_election_timeout();
                    }
                }
                ServerState::Candidate => {
                    // Only start election once when entering candidate state
                    if !candidate_started_election {
                        println!("📢 Starting election as candidate...");
                        self.send_vote_requests().await;
                        candidate_started_election = true;
                        election_timeout = self.generate_election_timeout();
                    }

                    // Check for election timeout (split vote scenario)
                    if self.is_election_timeout_expired(&election_timeout) {
                        println!("🔄 Election timeout as candidate - restarting election...");
                        candidate_started_election = false; // Will restart election next loop
                        election_timeout = self.generate_election_timeout();
                    }
                }
                ServerState::Leader => {
                    // Reset candidate flag when becoming leader
                    candidate_started_election = false;

                    // Send periodic heartbeats
                    heartbeat_interval.tick().await;
                    self.send_heartbeats().await;
                }
            }

            // Small sleep to prevent busy waiting
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Check if election timeout has expired
    fn is_election_timeout_expired(&self, election_timeout: &Duration) -> bool {
        let last_heartbeat = self.last_heartbeat.lock().unwrap();
        last_heartbeat.elapsed() >= *election_timeout
    }

    /// Send vote requests using RaftNode's optimized create_vote_request() method
    async fn send_vote_requests(&self) {
        // Use RaftNode's optimized create_vote_request() method to get a single vote request
        let vote_request = {
            let mut node = self.raft_node.lock().unwrap();
            node.create_vote_request() // Returns Option<RequestVoteRequest>
        };

        let vote_request = match vote_request {
            Some(request) => request,
            None => {
                println!("⚠️ Failed to create vote request (election cannot be started)");
                return;
            }
        };

        let node_id = {
            let node = self.raft_node.lock().unwrap();
            node.get_node_id()
        };

        println!("📢 Node {} starting election with single optimized vote request...", node_id);

        // Get target nodes (all other nodes in cluster)
        let other_nodes = {
            let node = self.raft_node.lock().unwrap();
            node.get_config().get_other_nodes().iter().map(|n| n.node_id).collect::<Vec<_>>()
        };

        println!("📤 Broadcasting vote request to {} nodes...", other_nodes.len());

        // Send the single vote request to all other nodes
        for target_node_id in other_nodes {
            match self.grpc_client.request_vote(target_node_id, vote_request.clone()).await {
                Ok(response) => {
                    println!("📥 Vote response from Node {}: granted={}, term={}",
                             target_node_id, response.vote_granted, response.term);

                    // Use RaftNode's tested handle_vote_response() method
                    let won_election = {
                        let mut node = self.raft_node.lock().unwrap();
                        node.handle_vote_response(target_node_id, response)
                    };

                    if won_election {
                        println!("👑 Won election! Becoming leader...");
                        let mut node = self.raft_node.lock().unwrap();
                        node.become_leader();
                        return;
                    }
                }
                Err(e) => {
                    println!("❌ Vote request to Node {} failed: {}", target_node_id, e);
                }
            }
        }

        // Log final election status
        let (vote_count, cluster_size) = {
            let node = self.raft_node.lock().unwrap();
            (node.get_vote_count(), node.get_config().cluster_size())
        };
        println!("🗳️ Election result: {}/{} votes", vote_count, cluster_size);
    }

    /// Send heartbeats using RaftNode's tested create_heartbeats() method
    async fn send_heartbeats(&self) {
        // Use RaftNode's tested create_heartbeats() method
        let heartbeat_requests = {
            let node = self.raft_node.lock().unwrap();
            node.create_heartbeats() // Returns Vec<(NodeId, AppendEntriesRequest)>
        };

        if heartbeat_requests.is_empty() {
            return;
        }

        println!("💓 Sending {} heartbeats...", heartbeat_requests.len());

        // Store original requests for response handling
        let original_requests: std::collections::HashMap<_, _> = heartbeat_requests.iter().cloned().collect();

        // Send heartbeats via gRPC
        let results = self.grpc_client.send_append_entries(heartbeat_requests).await;

        // Process responses using RaftNode's tested handle_append_entries_response() method
        for (node_id, result) in results {
            match result {
                Ok(response) => {
                    if let Some(original_request) = original_requests.get(&node_id) {
                        let response_success = response.success;
                        let mut node = self.raft_node.lock().unwrap();
                        let still_leader = node.handle_append_entries_response(node_id, original_request, response);

                        if !still_leader {
                            println!("📉 Stepped down from leader due to higher term from Node {}", node_id);
                            return;
                        }

                        if response_success {
                            println!("✅ Heartbeat to Node {} successful", node_id);
                        } else {
                            println!("⚠️ Heartbeat to Node {} failed (term conflict)", node_id);
                        }
                    }
                }
                Err(e) => {
                    println!("❌ Heartbeat to Node {} failed: {}", node_id, e);
                }
            }
        }
    }

    /// Generate a random election timeout within the configured range
    fn generate_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let timeout_ms = rng.gen_range(
            self.timing_config.election_timeout_range.0
                ..=self.timing_config.election_timeout_range.1,
        );
        Duration::from_millis(timeout_ms)
    }

    /// Signal the event loop to shutdown
    pub fn shutdown(&self) {
        *self.shutdown_signal.lock().unwrap() = true;
    }

    /// Reset election timeout (called when receiving valid AppendEntries from leader)
    pub fn reset_election_timeout(&self) {
        let mut last_heartbeat = self.last_heartbeat.lock().unwrap();
        *last_heartbeat = Instant::now();
        println!("💓 Heartbeat received - election timeout reset");
    }

    /// Get a reference to the gRPC client (only the event loop should send requests)
    pub fn get_grpc_client(&self) -> &RaftGrpcClient {
        &self.grpc_client
    }
}

impl Clone for RaftEventLoop {
    fn clone(&self) -> Self {
        Self {
            raft_node: Arc::clone(&self.raft_node),
            grpc_client: self.grpc_client.clone(),
            timing_config: self.timing_config.clone(),
            shutdown_signal: Arc::clone(&self.shutdown_signal),
            last_heartbeat: Arc::clone(&self.last_heartbeat),
        }
    }
}
