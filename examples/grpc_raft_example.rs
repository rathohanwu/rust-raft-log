use tokio::time::{sleep, Duration};
use tempfile::TempDir;

use raft_log::{
    ClusterConfig, NodeInfo, RaftNode, 
    RaftGrpcServer, RaftGrpcClient,
    RequestVoteRequest, AppendEntriesRequest
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Raft gRPC Integration Example");

    // Create a 3-node cluster configuration
    let nodes = vec![
        NodeInfo::new(1, "127.0.0.1".to_string(), 8001),
        NodeInfo::new(2, "127.0.0.1".to_string(), 8002),
        NodeInfo::new(3, "127.0.0.1".to_string(), 8003),
    ];

    // Create temporary directories for each node
    let temp_dir1 = TempDir::new()?;
    let temp_dir2 = TempDir::new()?;
    let temp_dir3 = TempDir::new()?;

    // Create cluster configurations for each node
    let config1 = ClusterConfig::new(
        1,
        nodes.clone(),
        temp_dir1.path().join("logs").to_string_lossy().to_string(),
        temp_dir1.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, // 1MB segments
        100,
    );

    let config2 = ClusterConfig::new(
        2,
        nodes.clone(),
        temp_dir2.path().join("logs").to_string_lossy().to_string(),
        temp_dir2.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024,
        100,
    );

    let config3 = ClusterConfig::new(
        3,
        nodes.clone(),
        temp_dir3.path().join("logs").to_string_lossy().to_string(),
        temp_dir3.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024,
        100,
    );

    // Create RaftNodes
    let raft_node1 = RaftNode::new(config1.clone())?;
    let raft_node2 = RaftNode::new(config2.clone())?;
    let raft_node3 = RaftNode::new(config3.clone())?;

    println!("✅ Created 3 RaftNodes");

    // Create gRPC servers
    let server1 = RaftGrpcServer::new(raft_node1);
    let server2 = RaftGrpcServer::new(raft_node2);
    let server3 = RaftGrpcServer::new(raft_node3);

    println!("✅ Created gRPC servers");

    // Start servers in background tasks (move servers directly)
    let server1_handle = tokio::spawn(async move {
        if let Err(e) = server1.start().await {
            eprintln!("Server 1 error: {}", e);
        }
    });

    let server2_handle = tokio::spawn(async move {
        if let Err(e) = server2.start().await {
            eprintln!("Server 2 error: {}", e);
        }
    });

    let server3_handle = tokio::spawn(async move {
        if let Err(e) = server3.start().await {
            eprintln!("Server 3 error: {}", e);
        }
    });

    println!("🌐 Started gRPC servers on ports 8001, 8002, 8003");

    // Wait a moment for servers to start
    sleep(Duration::from_millis(1000)).await;

    // Create gRPC clients
    let client1 = RaftGrpcClient::new(config1.clone());
    let client2 = RaftGrpcClient::new(config2.clone());

    println!("✅ Created gRPC clients");

    // Example 1: Send a RequestVote RPC from node 1 to node 2
    println!("\n📊 Example 1: RequestVote RPC");
    let vote_request = RequestVoteRequest::new(1, 1, 0, 0);

    match client1.request_vote(2, vote_request).await {
        Ok(response) => {
            println!("✅ RequestVote successful: term={}, vote_granted={}", 
                     response.term, response.vote_granted);
        }
        Err(e) => {
            println!("❌ RequestVote failed: {}", e);
        }
    }

    // Example 2: Send a heartbeat AppendEntries RPC from node 1 to node 3
    println!("\n💓 Example 2: Heartbeat AppendEntries RPC");
    let heartbeat = AppendEntriesRequest::heartbeat(1, 1, 0, 0, 0);
    
    match client1.append_entries(3, heartbeat).await {
        Ok(response) => {
            println!("✅ AppendEntries successful: term={}, success={}", 
                     response.term, response.success);
        }
        Err(e) => {
            println!("❌ AppendEntries failed: {}", e);
        }
    }

    // Example 3: Broadcast RequestVote to all other nodes
    println!("\n📢 Example 3: Broadcast RequestVote");
    let broadcast_request = RequestVoteRequest::new(2, 2, 0, 0);
    let results = client2.broadcast_request_vote(broadcast_request).await;
    
    for (node_id, result) in results {
        match result {
            Ok(response) => {
                println!("✅ Node {} responded: term={}, vote_granted={}", 
                         node_id, response.term, response.vote_granted);
            }
            Err(e) => {
                println!("❌ Node {} error: {}", node_id, e);
            }
        }
    }

    println!("\n🎉 gRPC Integration Example completed successfully!");
    println!("   - Demonstrated RequestVote RPC");
    println!("   - Demonstrated AppendEntries RPC");
    println!("   - Demonstrated broadcast functionality");
    println!("   - All servers running with persistent connections");

    // Keep the example running for a bit to show the servers are working
    println!("\n⏳ Servers will continue running for 5 seconds...");
    sleep(Duration::from_secs(5)).await;

    // Cleanup: abort server tasks
    server1_handle.abort();
    server2_handle.abort();
    server3_handle.abort();

    println!("🛑 Servers stopped. Example complete.");

    Ok(())
}
