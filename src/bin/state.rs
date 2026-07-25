use clap::Parser;
use std::process;

use raft_log::{RaftGrpcClient, YamlClusterConfig};

#[derive(Parser)]
#[command(name = "raft-state")]
#[command(about = "Read a Raft node's embedded test state machine")]
struct Args {
    #[arg(short, long)]
    config: String,

    #[arg(short, long)]
    node_id: u32,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let yaml_config = YamlClusterConfig::from_file(&args.config).unwrap_or_else(|error| {
        eprintln!("Failed to load configuration: {error}");
        process::exit(1);
    });
    let cluster_config = yaml_config
        .to_cluster_config(args.node_id)
        .unwrap_or_else(|error| {
            eprintln!("Failed to create cluster configuration: {error}");
            process::exit(1);
        });
    let client = RaftGrpcClient::new(cluster_config);
    let response = client
        .get_applied_state(args.node_id)
        .await
        .unwrap_or_else(|error| {
            eprintln!("Failed to query node {}: {error}", args.node_id);
            process::exit(1);
        });

    if !response.available {
        eprintln!("Node {} has no embedded state machine", args.node_id);
        process::exit(2);
    }

    let state: serde_json::Value =
        serde_json::from_slice(&response.state_json).unwrap_or_else(|error| {
            eprintln!("Node {} returned invalid state JSON: {error}", args.node_id);
            process::exit(1);
        });
    println!(
        "{}",
        serde_json::json!({
            "node_id": args.node_id,
            "last_applied": response.last_applied,
            "state": state,
        })
    );
}
