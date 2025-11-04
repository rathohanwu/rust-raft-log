use clap::Parser;
use std::process;

use raft_log::{RaftGrpcServer, RaftNode, YamlClusterConfig};

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
    // Parse command line arguments
    let args = Args::parse();

    println!(
        "🚀 Starting Raft node {} with config: {}",
        args.node_id, args.config
    );

    // Load cluster configuration from YAML file
    let yaml_config = match YamlClusterConfig::from_file(&args.config) {
        Ok(config) => config,
        Err(e) => {
            eprintln!(
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
            eprintln!(
                "❌ Failed to create cluster config for node {}: {}",
                args.node_id, e
            );
            process::exit(1);
        }
    };

    println!("📋 Cluster configuration:");
    println!("   Node ID: {}", cluster_config.node_id);
    println!("   Address: {}", cluster_config.get_address());
    println!("   Cluster size: {}", cluster_config.cluster_size());
    println!("   Log directory: {}", cluster_config.log_directory);

    // Create the Raft node
    let raft_node = match RaftNode::new(cluster_config.clone()) {
        Ok(node) => node,
        Err(e) => {
            eprintln!("❌ Failed to create Raft node: {}", e);
            process::exit(1);
        }
    };

    println!("✅ Raft node created successfully");

    // Create and start the gRPC server
    let server = RaftGrpcServer::new(raft_node);

    println!(
        "🌐 Starting gRPC server on {}",
        cluster_config.get_address()
    );

    match server.start().await {
        Ok(_) => println!("✅ Server started successfully"),
        Err(e) => {
            eprintln!("❌ Failed to start server: {}", e);
            process::exit(1);
        }
    }

    Ok(())
}
