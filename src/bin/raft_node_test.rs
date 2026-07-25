use clap::Parser;
use log::{error, info};
use std::process;

use raft_log::{testkit::ArithmeticStateMachine, RaftGrpcServer, RaftNode, YamlClusterConfig};

#[derive(Parser)]
#[command(name = "raft-node-test")]
#[command(about = "Raft node with the Docker E2E arithmetic state machine")]
struct Args {
    #[arg(short, long)]
    node_id: u32,

    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let args = Args::parse();
    let yaml_config = YamlClusterConfig::from_file(&args.config).unwrap_or_else(|error| {
        error!(
            "Failed to load configuration from {}: {}",
            args.config, error
        );
        process::exit(1);
    });
    let cluster_config = yaml_config
        .to_cluster_config(args.node_id)
        .unwrap_or_else(|error| {
            error!("Failed to configure node {}: {}", args.node_id, error);
            process::exit(1);
        });
    let node = RaftNode::new_with_state_machine(
        cluster_config.clone(),
        Box::new(ArithmeticStateMachine::default()),
    )
    .unwrap_or_else(|error| {
        error!("Failed to create test node: {}", error);
        process::exit(1);
    });

    info!("Starting arithmetic test node {}", args.node_id);
    RaftGrpcServer::new(node).start().await
}
