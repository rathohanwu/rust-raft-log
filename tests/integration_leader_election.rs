use tokio::time::{sleep, Duration, timeout};
use tempfile::TempDir;
use std::collections::HashSet;

use raft_log::{
    ClusterConfig, NodeInfo, RaftNode,
    RaftGrpcServer, ServerState
};

/// Integration test that starts three real servers and waits for leader election
#[tokio::test]
async fn test_three_server_leader_election() {
    println!("🧪 Integration Test: Three Server Leader Election");
    println!("   Starting three real gRPC servers and waiting for leader election...");

    // Create a 3-node cluster configuration
    let nodes = vec![
        NodeInfo::new(1, "127.0.0.1".to_string(), 18001), // Use different ports to avoid conflicts
        NodeInfo::new(2, "127.0.0.1".to_string(), 18002),
        NodeInfo::new(3, "127.0.0.1".to_string(), 18003),
    ];

    // Create temporary directories for each node
    let temp_dir1 = TempDir::new().expect("Failed to create temp dir 1");
    let temp_dir2 = TempDir::new().expect("Failed to create temp dir 2");
    let temp_dir3 = TempDir::new().expect("Failed to create temp dir 3");

    // Create cluster configurations for each node with fast timing for testing
    let config1 = ClusterConfig::new(
        1, nodes.clone(),
        temp_dir1.path().join("logs").to_string_lossy().to_string(),
        temp_dir1.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
        (500, 1500), // Wider range to prevent split votes
        100,         // Fast heartbeat for testing
    );

    let config2 = ClusterConfig::new(
        2,
        nodes.clone(),
        temp_dir2.path().join("logs").to_string_lossy().to_string(),
        temp_dir2.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
        (500, 1500), // Wider range to prevent split votes
        100,         // Fast heartbeat for testing
    );

    let config3 = ClusterConfig::new(
        3,
        nodes.clone(),
        temp_dir3.path().join("logs").to_string_lossy().to_string(),
        temp_dir3.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
        (500, 1500), // Wider range to prevent split votes
        100,         // Fast heartbeat for testing
    );

    // Create RaftNodes
    let raft_node1 = RaftNode::new(config1).expect("Failed to create RaftNode 1");
    let raft_node2 = RaftNode::new(config2).expect("Failed to create RaftNode 2");
    let raft_node3 = RaftNode::new(config3).expect("Failed to create RaftNode 3");

    println!("✅ Created 3 RaftNodes");

    // Create gRPC servers (timing is now configured in ClusterConfig)
    let server1 = RaftGrpcServer::new(raft_node1);
    let server2 = RaftGrpcServer::new(raft_node2);
    let server3 = RaftGrpcServer::new(raft_node3);

    println!("✅ Created gRPC servers with fast timing for testing");

    // Get references for monitoring
    let server1_raft = server1.get_raft_node();
    let server2_raft = server2.get_raft_node();
    let server3_raft = server3.get_raft_node();

    // Start servers with event loops
    let (event_loop1, server_handle1) = server1.start_with_handles().await
        .expect("Failed to start server 1");
    let (event_loop2, server_handle2) = server2.start_with_handles().await
        .expect("Failed to start server 2");
    let (event_loop3, server_handle3) = server3.start_with_handles().await
        .expect("Failed to start server 3");

    println!("🌐 Started 3 gRPC servers on ports 18001, 18002, 18003");

    // Wait for servers to start up
    sleep(Duration::from_millis(500)).await;

    // Verify initial state - all should be followers
    {
        let node1 = server1_raft.lock().unwrap();
        let node2 = server2_raft.lock().unwrap();
        let node3 = server3_raft.lock().unwrap();
        
        assert_eq!(node1.get_server_state(), ServerState::Follower);
        assert_eq!(node2.get_server_state(), ServerState::Follower);
        assert_eq!(node3.get_server_state(), ServerState::Follower);
        
        assert_eq!(node1.get_current_term(), 0);
        assert_eq!(node2.get_current_term(), 0);
        assert_eq!(node3.get_current_term(), 0);
    }
    println!("✅ All nodes start as Followers in term 0");

    // Simple function to check if there's exactly one leader
    let check_for_leader = || -> Option<(u32, u64)> {
        let states = [
            (1, server1_raft.lock().unwrap()),
            (2, server2_raft.lock().unwrap()),
            (3, server3_raft.lock().unwrap()),
        ];

        let leaders: Vec<(u32, u64)> = states.iter()
            .filter_map(|(id, node)| {
                if node.get_server_state() == ServerState::Leader {
                    Some((*id, node.get_current_term()))
                } else {
                    None
                }
            })
            .collect();

        if leaders.len() == 1 {
            Some(leaders[0])
        } else {
            None
        }
    };

    // Wait for leader election with timeout
    println!("⏳ Waiting for leader election (max 10 seconds)...");

    let election_result = timeout(Duration::from_secs(10), async {
        loop {
            sleep(Duration::from_millis(200)).await;

            if let Some((leader_id, leader_term)) = check_for_leader() {
                println!("👑 Leader found: Node {} in term {}", leader_id, leader_term);
                return (leader_id, leader_term);
            }
        }
    }).await;

    // Verify election succeeded
    let (leader_id, leader_term) = election_result
        .expect("Leader election timed out after 10 seconds");

    println!("🎉 Leader election completed successfully!");
    println!("   - Leader: Node {}", leader_id);
    println!("   - Term: {}", leader_term);

    // Wait a bit more to observe heartbeats and verify stability
    println!("💓 Observing heartbeat behavior for 2 seconds...");
    sleep(Duration::from_secs(2)).await;

    // Verify leader is still the same
    if let Some((current_leader_id, current_leader_term)) = check_for_leader() {
        assert_eq!(current_leader_id, leader_id, "Leader should remain the same");
        println!("✅ Cluster is stable - Leader: Node {} in term {}", current_leader_id, current_leader_term);
    } else {
        panic!("Leader disappeared after election!");
    }

    // Graceful shutdown
    println!("🛑 Shutting down servers...");
    
    server1.shutdown();
    server2.shutdown();
    server3.shutdown();

    event_loop1.abort();
    event_loop2.abort();
    event_loop3.abort();
    server_handle1.abort();
    server_handle2.abort();
    server_handle3.abort();

    println!("✅ Integration test completed successfully!");
    println!("   - Three servers started and communicated");
    println!("   - Leader election completed within timeout");
    println!("   - Cluster achieved stable state");
    println!("   - Heartbeats maintained leadership");
}
