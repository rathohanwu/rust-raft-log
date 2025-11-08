use clap::Parser;
use std::process;
use std::time::Duration;
use tokio::time::sleep;
use log::{info, error, warn, debug};

use raft_log::{
    YamlClusterConfig, RaftGrpcClient,
    NodeId, ClusterConfig,
};

#[derive(Parser)]
#[command(name = "raft-client")]
#[command(about = "A Raft cluster client for sending log entries")]
#[command(version = "0.1.0")]
struct Args {
    /// Path to the YAML configuration file
    #[arg(short, long)]
    config: String,

    /// Payload/command to append to the Raft log
    #[arg(short, long)]
    payload: String,

    /// Optional initial target node ID (will try to discover leader if not specified)
    #[arg(short, long)]
    node_id: Option<u32>,

    /// Maximum number of retry attempts for leader discovery
    #[arg(long, default_value = "10")]
    max_retries: u32,

    /// Delay between retry attempts in milliseconds
    #[arg(long, default_value = "1000")]
    retry_delay_ms: u64,
}

#[tokio::main]
async fn main() {
    // Initialize logging
    env_logger::init();

    let args = Args::parse();

    info!("🚀 Starting Raft client...");

    // Load cluster configuration
    let yaml_config = match YamlClusterConfig::from_file(&args.config) {
        Ok(config) => config,
        Err(e) => {
            error!("❌ Failed to load configuration file '{}': {}", args.config, e);
            process::exit(1);
        }
    };

    info!("📋 Loaded cluster configuration with {} nodes", yaml_config.nodes.len());

    // Create gRPC client
    let cluster_config = match yaml_config.to_cluster_config(1) { // Use node 1 as dummy for client
        Ok(config) => config,
        Err(e) => {
            error!("❌ Failed to create cluster configuration: {}", e);
            process::exit(1);
        }
    };
    let grpc_client = RaftGrpcClient::new(cluster_config.clone());

    // Determine initial target node
    let initial_target = match args.node_id {
        Some(id) => {
            if cluster_config.get_node(id).is_some() {
                id
            } else {
                error!("❌ Node ID {} not found in cluster configuration", id);
                process::exit(1);
            }
        }
        None => {
            // Start with the first node in the cluster
            cluster_config.nodes[0].node_id
        }
    };

    info!("🎯 Initial target node: {}", initial_target);
    info!("📦 Payload to send: '{}'", args.payload);

    // Attempt to send the log entry with leader discovery and retry logic
    match send_log_entry_with_retry(
        &grpc_client,
        &cluster_config,
        args.payload.as_bytes().to_vec(),
        initial_target,
        args.max_retries,
        args.retry_delay_ms,
    ).await {
        Ok((leader_id, index)) => {
            info!("✅ Successfully appended log entry!");
            info!("   Leader: Node {}", leader_id);
            info!("   Log index: {}", index);
        }
        Err(e) => {
            error!("❌ Failed to append log entry after {} retries: {}", args.max_retries, e);
            process::exit(1);
        }
    }
}

/// Sends a log entry with automatic leader discovery and retry logic
async fn send_log_entry_with_retry(
    client: &RaftGrpcClient,
    cluster_config: &ClusterConfig,
    payload: Vec<u8>,
    initial_target: NodeId,
    max_retries: u32,
    retry_delay_ms: u64,
) -> Result<(NodeId, u64), Box<dyn std::error::Error>> {
    let mut current_target = initial_target;
    let mut tried_nodes = std::collections::HashSet::new();

    for attempt in 1..=max_retries {
        debug!("🔄 Attempt {} - Trying node {}", attempt, current_target);

        match client.client_request(current_target, payload.clone()).await {
            Ok(response) => {
                if response.success {
                    info!("✅ Node {} accepted the entry (it's the leader!)", current_target);
                    return Ok((current_target, response.log_index));
                } else {
                    warn!("❌ Node {} rejected the entry: {}", current_target, response.error_message);

                    // Mark current node as tried
                    tried_nodes.insert(current_target);

                    // If the response includes a leader hint, prioritize trying that node
                    if response.leader_id != 0 {
                        // Check if the suggested leader is valid and different from current
                        if cluster_config.get_node(response.leader_id).is_some() && response.leader_id != current_target {
                            current_target = response.leader_id;
                            debug!("🎯 Server suggested leader is node {}, trying immediately", current_target);

                            // Don't wait for retry delay when we have a leader hint - try immediately
                            continue;
                        } else {
                            warn!("⚠️  Server suggested invalid or same leader ({}), falling back to round-robin", response.leader_id);
                        }
                    }

                    // No valid leader hint, try next available node
                    current_target = find_next_node_to_try(cluster_config, &tried_nodes)?;
                    debug!("🔍 Switching to node {} for next attempt", current_target);
                }
            }
            Err(e) => {
                warn!("❌ Failed to connect to node {}: {}", current_target, e);
                tried_nodes.insert(current_target);

                // Try to find the next node to try
                match find_next_node_to_try(cluster_config, &tried_nodes) {
                    Ok(next_node) => {
                        current_target = next_node;
                        debug!("🔍 Switching to node {} for next attempt", current_target);
                    }
                    Err(_) => {
                        // All nodes have been tried, but we might want to retry if we haven't reached max attempts
                        if attempt < max_retries {
                            debug!("🔄 All nodes tried, resetting for next round of attempts");
                            tried_nodes.clear();
                            current_target = initial_target;
                        } else {
                            return Err(format!("All nodes have been tried and failed").into());
                        }
                    }
                }
            }
        }

        // Wait before retrying (but skip delay if we just got a leader hint and used continue)
        if attempt < max_retries {
            debug!("⏳ Waiting {}ms before next attempt...", retry_delay_ms);
            sleep(Duration::from_millis(retry_delay_ms)).await;
        }
    }

    Err(format!("Failed to find leader after {} attempts", max_retries).into())
}

/// Finds the next node to try that hasn't been tried yet
/// Returns nodes in order, skipping already tried nodes
fn find_next_node_to_try(
    cluster_config: &ClusterConfig,
    tried_nodes: &std::collections::HashSet<NodeId>,
) -> Result<NodeId, Box<dyn std::error::Error>> {
    // Find the first untried node
    for node in &cluster_config.nodes {
        if !tried_nodes.contains(&node.node_id) {
            return Ok(node.node_id);
        }
    }

    // All nodes have been tried
    Err(format!("All {} nodes in the cluster have been tried", cluster_config.nodes.len()).into())
}
