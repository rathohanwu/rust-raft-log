use log::{debug, info, warn};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;
use tokio::time::{interval, sleep, Instant};

use super::client::RaftGrpcClient;
use crate::ServerState::*;
use crate::{
    consensus::RaftNode,
    models::{ClusterConfig, RequestVoteRequest},
};

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
        }
    }

    /// Start the event loop (runs indefinitely until shutdown)
    pub async fn run(&self) {
        info!("🔄 Starting Raft event loop...");

        let mut election_timeout = self.generate_election_timeout();
        let mut heartbeat_interval =
            interval(Duration::from_millis(self.timing_config.heartbeat_interval));

        loop {
            // Check for shutdown signal
            if *self.shutdown_signal.lock().unwrap() {
                info!("🛑 Raft event loop shutting down...");
                break;
            }

            let current_state = {
                let node = self.raft_node.lock().unwrap();
                node.get_server_state()
            };

            match current_state {
                Follower | Candidate if self.is_election_timeout_expired(&election_timeout) => {
                    let vote_request = {
                        let mut node = self.raft_node.lock().unwrap();
                        node.create_vote_request()
                    };
                    // Each election gets a fresh timeout and its own timer start.
                    election_timeout = self.generate_election_timeout();
                    self.reset_election_timeout();
                    if let Some(vote_request) = vote_request {
                        self.send_vote_requests(vote_request).await;
                    }
                }
                Follower => {}
                Candidate => {
                    // Wait for this election's timer instead of starting another
                    // election on every event-loop tick.
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

        info!(
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

        debug!(
            "📤 Broadcasting vote request to {} nodes...",
            other_nodes.len()
        );

        // Process vote RPCs as they arrive. A dead peer must not delay a vote
        // from a reachable peer long enough to lose this election round.
        let mut requests = JoinSet::new();
        for target_node_id in other_nodes {
            let request_clone = vote_request.clone();
            let client = self.grpc_client.clone();
            requests.spawn(async move {
                let response = client.request_vote(target_node_id, request_clone).await;
                (target_node_id, response)
            });
        }

        while let Some(result) = requests.join_next().await {
            let Ok((target_node_id, response)) = result else {
                continue;
            };

            match response {
                Ok(vote_response) => {
                    debug!(
                        "📥 Vote response from Node {}: granted={}, term={}",
                        target_node_id, vote_response.vote_granted, vote_response.term
                    );

                    // Handle the vote response
                    let won_election = {
                        let mut node = self.raft_node.lock().unwrap();
                        node.handle_vote_response(target_node_id, vote_response)
                    };

                    if won_election {
                        requests.abort_all();
                        return;
                    }
                }
                Err(e) => {
                    warn!("❌ Failed to get vote from Node {}: {}", target_node_id, e);
                }
            }
        }
    }

    /// Send replication/heartbeat work using RaftNode's unified builder.
    async fn send_heartbeats(&self) {
        // Lagging peers receive entries; caught-up peers receive heartbeats.
        let heartbeat_requests = {
            let node = self.raft_node.lock().unwrap();
            node.build_replication_requests()
        };

        if heartbeat_requests.is_empty() {
            return;
        }

        debug!("💓 Sending {} heartbeats...", heartbeat_requests.len());

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
                        node.handle_append_entries_response(node_id, original_request, response);
                        if response_success {
                            debug!("✅ Heartbeat to Node {} successful", node_id);
                        } else {
                            warn!("⚠️ Heartbeat to Node {} failed (term conflict)", node_id);
                        }
                    }
                }
                Err(e) => {
                    warn!("❌ Heartbeat to Node {} failed: {}", node_id, e);
                }
            }
        }
    }

    /// Draw a new timeout for every election. A tiny local PRNG avoids coupling
    /// timeout selection to a static node ID while keeping the runtime dependency-free.
    fn generate_election_timeout(&self) -> Duration {
        let (min_ms, max_ms) = self.timing_config.election_timeout_range;
        let span = max_ms.saturating_sub(min_ms);
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let wall_clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let seed = wall_clock ^ COUNTER.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed);
        let mixed = seed ^ (seed >> 30);
        Duration::from_millis(min_ms + if span == 0 { 0 } else { mixed % (span + 1) })
    }

    /// Signal the event loop to shutdown
    pub fn shutdown(&self) {
        *self.shutdown_signal.lock().unwrap() = true;
    }

    /// Reset election timeout (called when receiving valid AppendEntries from leader)
    pub fn reset_election_timeout(&self) {
        let mut last_heartbeat = self.last_heartbeat.lock().unwrap();
        *last_heartbeat = Instant::now();
        debug!("💓 Heartbeat received - election timeout reset");
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
