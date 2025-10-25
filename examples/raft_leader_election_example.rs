use tokio::time::{sleep, Duration};
use tempfile::TempDir;

use raft_log::{
    ClusterConfig, NodeInfo, RaftNode, 
    RaftGrpcServer, ServerState
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🗳️  Starting Raft Leader Election Example");

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
        1, nodes.clone(),
        temp_dir1.path().join("logs").to_string_lossy().to_string(),
        temp_dir1.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
    );

    let config2 = ClusterConfig::new(
        2, nodes.clone(),
        temp_dir2.path().join("logs").to_string_lossy().to_string(),
        temp_dir2.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
    );

    let config3 = ClusterConfig::new(
        3, nodes.clone(),
        temp_dir3.path().join("logs").to_string_lossy().to_string(),
        temp_dir3.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
    );

    // Create RaftNodes
    let raft_node1 = RaftNode::new(config1)?;
    let raft_node2 = RaftNode::new(config2)?;
    let raft_node3 = RaftNode::new(config3)?;

    println!("✅ Created 3 RaftNodes");

    // Create gRPC servers (each with built-in client)
    let server1 = RaftGrpcServer::new(raft_node1);
    let server2 = RaftGrpcServer::new(raft_node2);
    let server3 = RaftGrpcServer::new(raft_node3);

    println!("✅ Created gRPC servers with built-in clients");

    // Get references to the servers for later use
    let server1_raft = server1.get_raft_node();
    let server1_client = server1.get_grpc_client().clone();

    let server2_raft = server2.get_raft_node();
    let server3_raft = server3.get_raft_node();

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

    // Wait for servers to start
    sleep(Duration::from_millis(1000)).await;

    // Simulate leader election process
    println!("\n🗳️  Simulating Leader Election Process");

    // Node 1 starts an election
    let vote_request = {
        let mut node1 = server1_raft.lock().unwrap();
        println!("📢 Node 1 starting election...");
        node1.create_vote_request()
    };

    let request = match vote_request {
        Some(request) => {
            println!("✅ Node 1 created vote request: term={}, candidate_id={}",
                     request.term, request.candidate_id);
            request
        }
        None => {
            println!("❌ Node 1 failed to create vote request");
            return Ok(());
        }
    };

    // Use the built-in client to broadcast vote requests
    println!("📤 Broadcasting vote request using built-in gRPC client...");
    let results = server1_client.broadcast_request_vote(request).await;
        
        let mut votes_received = 1; // Node 1 votes for itself
        for (node_id, result) in results {
            match result {
                Ok(response) => {
                    println!("✅ Node {} vote response: term={}, granted={}", 
                             node_id, response.term, response.vote_granted);
                    if response.vote_granted {
                        votes_received += 1;
                    }
                }
                Err(e) => {
                    println!("❌ Node {} vote error: {}", node_id, e);
                }
            }
        }

        println!("🗳️  Total votes received: {}/3", votes_received);
        
        if votes_received >= 2 { // Majority in 3-node cluster
            let mut node1 = server1_raft.lock().unwrap();
            node1.become_leader();
            println!("👑 Node 1 became LEADER!");
        }

    // Check final states
    println!("\n📊 Final Node States:");
    {
        let node1 = server1_raft.lock().unwrap();
        println!("   Node 1: {:?}, Term: {}", 
                 node1.get_server_state(), node1.get_current_term());
    }
    {
        let node2 = server2_raft.lock().unwrap();
        println!("   Node 2: {:?}, Term: {}", 
                 node2.get_server_state(), node2.get_current_term());
    }
    {
        let node3 = server3_raft.lock().unwrap();
        println!("   Node 3: {:?}, Term: {}", 
                 node3.get_server_state(), node3.get_current_term());
    }

    // Demonstrate leader sending heartbeats
    {
        let node1 = server1_raft.lock().unwrap();
        if node1.get_server_state() == ServerState::Leader {
            println!("\n💓 Leader sending heartbeats...");
            let heartbeat_requests = node1.create_heartbeats();
            drop(node1); // Release lock before async operations
            
            let results = server1_client.send_append_entries(heartbeat_requests).await;
            for (node_id, result) in results {
                match result {
                    Ok(response) => {
                        println!("✅ Heartbeat to Node {}: success={}", node_id, response.success);
                    }
                    Err(e) => {
                        println!("❌ Heartbeat to Node {} failed: {}", node_id, e);
                    }
                }
            }
        }
    }

    println!("\n🎉 Leader Election Example completed successfully!");
    println!("   - Demonstrated built-in gRPC client in server");
    println!("   - Showed leader election process");
    println!("   - Demonstrated heartbeat mechanism");

    // Keep running for a bit
    println!("\n⏳ Servers will continue running for 3 seconds...");
    sleep(Duration::from_secs(3)).await;

    // Cleanup
    server1_handle.abort();
    server2_handle.abort();
    server3_handle.abort();

    println!("🛑 Servers stopped. Example complete.");

    Ok(())
}
