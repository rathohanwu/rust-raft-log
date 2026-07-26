use clap::Parser;
use log::{error, info};
use std::process;

use raft_log::{RaftNode, RaftRuntime, YamlClusterConfig};

#[derive(Parser)]
#[command(name = "raft-node")]
#[command(about = "A Raft consensus node")]
#[command(version = "0.1.0")]
struct Args {
    /// Node ID for this Raft node
    #[arg(short, long)]
    node_id: u32,

    /// Path to the YAML configuration file
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Parse command line arguments
    let args = Args::parse();

    info!(
        "🚀 Starting Raft node {} with config: {}",
        args.node_id, args.config
    );

    // Load cluster configuration from YAML file
    let yaml_config = match YamlClusterConfig::from_file(&args.config) {
        Ok(config) => config,
        Err(e) => {
            error!(
                "❌ Failed to load configuration from {}: {}",
                args.config, e
            );
            process::exit(1);
        }
    };

    // Convert to ClusterConfig for this specific node
    let cluster_config = match yaml_config.to_cluster_config(args.node_id) {
        Ok(config) => config,
        Err(e) => {
            error!(
                "❌ Failed to create cluster config for node {}: {}",
                args.node_id, e
            );
            process::exit(1);
        }
    };

    info!("📋 Cluster configuration:");
    info!("   Node ID: {}", cluster_config.node_id);
    info!("   Address: {}", cluster_config.get_address());
    info!("   Cluster size: {}", cluster_config.cluster_size());
    info!("   Log directory: {}", cluster_config.log_directory);

    // Create the Raft node
    let raft_node = match RaftNode::new(cluster_config.clone()) {
        Ok(node) => node,
        Err(e) => {
            error!("❌ Failed to create Raft node: {}", e);
            process::exit(1);
        }
    };

    info!("✅ Raft node created successfully");

    // Create the Raft runtime and serve its gRPC interface.
    let runtime = RaftRuntime::new(raft_node);

    info!(
        "🌐 Starting gRPC server on {}",
        cluster_config.get_address()
    );

    match runtime.serve().await {
        Ok(_) => info!("✅ Server started successfully"),
        Err(e) => {
            error!("❌ Failed to start server: {}", e);
            process::exit(1);
        }
    }

    Ok(())
}
