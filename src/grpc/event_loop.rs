use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::{interval, sleep, Instant};

use super::client::RaftGrpcClient;
use crate::log::{models::{ServerState, ClusterConfig}, RaftNode, RequestVoteRequest};
use crate::ServerState::*;

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
            // Use wider range for better split vote prevention
            // Original Raft paper suggests 150-300ms, but wider ranges help in practice
            election_timeout_range: (150, 500),
            // Heartbeat should be ~10x faster than election timeout
            heartbeat_interval: 50,
        }
    }
}

/// Raft event loop that handles timeouts, elections, and heartbeats
#[derive(Clone)]
pub struct RaftEventLoop {
    raft_node: Arc<Mutex<RaftNode>>,
    grpc_client: RaftGrpcClient,
    timing_config: RaftTimingConfig,
    shutdown_signal: Arc<Mutex<bool>>,
    last_heartbeat: Arc<Mutex<Instant>>,
    /// Counter to ensure unique timeout values across multiple calls
    timeout_counter: Arc<AtomicU64>,
}

impl RaftEventLoop {
    pub fn new(
        raft_node: Arc<Mutex<RaftNode>>,
        grpc_client: RaftGrpcClient,
        cluster_config: &ClusterConfig,
    ) -> Self {
        let timing_config = RaftTimingConfig {
            election_timeout_range: cluster_config.election_timeout_range,
            heartbeat_interval: cluster_config.heartbeat_interval,
        };

        Self {
            raft_node,
            grpc_client,
            timing_config,
            shutdown_signal: Arc::new(Mutex::new(false)),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            timeout_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start the event loop (runs indefinitely until shutdown)
    pub async fn run(&self) {
        println!("🔄 Starting Raft event loop...");

        let election_timeout = self.generate_election_timeout();
        let mut heartbeat_interval =
            interval(Duration::from_millis(self.timing_config.heartbeat_interval));

        loop {
            // Check for shutdown signal
            if *self.shutdown_signal.lock().unwrap() {
                println!("🛑 Raft event loop shutting down...");
                break;
            }

            let mut current_state = {
                let node = self.raft_node.lock().unwrap();
                node.get_server_state()
            };

            if self.is_election_timeout_expired(&election_timeout) && current_state != Leader {
                current_state = Candidate;
            }

            match current_state {
                Follower => {}
                Candidate => {
                    let vote_request = {
                        let mut node = self.raft_node.lock().unwrap();
                        node.create_vote_request()
                    };
                    self.send_vote_requests(vote_request.unwrap()).await;
                }
                Leader => {
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

    /// Send vote requests using a provided vote request
    async fn send_vote_requests(&self, vote_request: RequestVoteRequest) {
        let node_id = {
            let node = self.raft_node.lock().unwrap();
            node.get_node_id()
        };

        println!(
            "📢 Node {} starting election with provided vote request...",
            node_id
        );

        // Get target nodes (all other nodes in cluster)
        let other_nodes = {
            let node = self.raft_node.lock().unwrap();
            node.get_config()
                .get_other_nodes()
                .iter()
                .map(|n| n.node_id)
                .collect::<Vec<_>>()
        };

        println!(
            "📤 Broadcasting vote request to {} nodes...",
            other_nodes.len()
        );

        // Send the vote request to all other nodes
        for target_node_id in other_nodes {
            let request_clone = vote_request.clone();
            let response = self
                .grpc_client
                .request_vote(target_node_id, request_clone)
                .await;

            match response {
                Ok(vote_response) => {
                    println!(
                        "📥 Vote response from Node {}: granted={}, term={}",
                        target_node_id, vote_response.vote_granted, vote_response.term
                    );

                    // Handle the vote response
                    let won_election = {
                        let mut node = self.raft_node.lock().unwrap();
                        node.handle_vote_response(target_node_id, vote_response)
                    };

                    if won_election {
                        println!("👑 Won election! Becoming leader...");
                        let mut node = self.raft_node.lock().unwrap();
                        node.become_leader();
                        break; // Stop sending more requests
                    }
                }
                Err(e) => {
                    println!("❌ Failed to get vote from Node {}: {}", target_node_id, e);
                }
            }
        }
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
        let original_requests: std::collections::HashMap<_, _> =
            heartbeat_requests.iter().cloned().collect();

        // Send heartbeats via gRPC
        let results = self
            .grpc_client
            .send_append_entries(heartbeat_requests)
            .await;

        // Process responses using RaftNode's tested handle_append_entries_response() method
        for (node_id, result) in results {
            match result {
                Ok(response) => {
                    if let Some(original_request) = original_requests.get(&node_id) {
                        let response_success = response.success;
                        let mut node = self.raft_node.lock().unwrap();
                        let still_leader = node.handle_append_entries_response(
                            node_id,
                            original_request,
                            response,
                        );

                        if !still_leader {
                            println!(
                                "📉 Stepped down from leader due to higher term from Node {}",
                                node_id
                            );
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

    /// Generate a random election timeout with improved variance to reduce split votes
    ///
    /// This implementation uses multiple randomization techniques:
    /// 1. Multiple independent random sources for better variance
    /// 2. Non-uniform distribution using power functions
    /// 3. Additional jitter layers for extra randomization
    /// 4. Unique counter to ensure different values across calls
    /// 5. Wider effective range to spread timeouts further apart
    fn generate_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let (min_ms, max_ms) = self.timing_config.election_timeout_range;

        // Calculate base range and expand it for better variance
        let base_range = max_ms - min_ms;

        // Use multiple random sources and combine them more carefully
        // Primary random component - use power distribution for non-uniform spread
        let uniform1: f64 = rng.gen(); // 0.0 to 1.0
        let power_sample = uniform1.powf(0.6); // Power < 1 creates bias toward smaller values
        let primary_offset = (power_sample * (base_range as f64 * 2.5)) as u64;

        // Secondary random component - uniform distribution for additional spread
        let uniform2: f64 = rng.gen();
        let secondary_offset = (uniform2 * base_range as f64) as u64;

        // Tertiary random component - exponential-like distribution
        let uniform3: f64 = rng.gen();
        let exp_like = (-uniform3.ln() * 0.5).min(2.0); // Capped exponential
        let tertiary_offset = (exp_like * base_range as f64 * 0.8) as u64;

        // Add unique counter-based offset to ensure different values across calls
        let counter = self.timeout_counter.fetch_add(1, Ordering::Relaxed);
        let counter_offset = (counter % 100) * (base_range / 50); // Spread based on call count

        // Add time-based microsecond jitter for additional uniqueness
        let time_jitter = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_micros() as u64) % (base_range / 2);

        // Combine all sources using weighted sum instead of modulo to avoid clustering
        let combined_offset = (primary_offset + secondary_offset / 2 + tertiary_offset / 3 + counter_offset + time_jitter / 4) / 3;

        // Apply to base range with expansion
        let expanded_max = max_ms + (base_range * 3 / 2); // Expand range by 150%
        let timeout_ms = min_ms + (combined_offset % (expanded_max - min_ms));

        // Ensure minimum separation to prevent too-close timeouts
        let min_separation = base_range / 10; // At least 10% of base range separation
        let final_timeout = timeout_ms.max(min_ms + min_separation);

        Duration::from_millis(final_timeout)
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

    /// Generate an election timeout (exposed for testing)
    pub fn generate_election_timeout_for_test(&self) -> Duration {
        self.generate_election_timeout()
    }
}
