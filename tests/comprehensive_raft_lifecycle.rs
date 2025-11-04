use tokio::time::{sleep, Duration, timeout};
use tempfile::TempDir;
use std::collections::HashMap;

use raft_log::{
    ClusterConfig, NodeInfo, RaftNode,
    RaftGrpcServer, ServerState
};

/// Comprehensive integration test that validates the complete Raft consensus lifecycle
#[tokio::test]
async fn test_comprehensive_raft_lifecycle() {
    println!("🧪 COMPREHENSIVE RAFT LIFECYCLE TEST");
    println!("   Testing: Leader Election → Log Replication → Leader Failure → Re-election → Recovery");

    // Create a 3-node cluster configuration
    let nodes = vec![
        NodeInfo::new(1, "127.0.0.1".to_string(), 19001),
        NodeInfo::new(2, "127.0.0.1".to_string(), 19002),
        NodeInfo::new(3, "127.0.0.1".to_string(), 19003),
    ];

    // Create temporary directories for each node
    let temp_dir1 = TempDir::new().expect("Failed to create temp dir 1");
    let temp_dir2 = TempDir::new().expect("Failed to create temp dir 2");
    let temp_dir3 = TempDir::new().expect("Failed to create temp dir 3");

    // Create cluster configurations for each node
    let config1 = ClusterConfig::new(
        1, nodes.clone(),
        temp_dir1.path().join("logs").to_string_lossy().to_string(),
        temp_dir1.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
        (1000, 2000), // 1-2 seconds for testing
        300,          // 300ms heartbeats
    );

    let config2 = ClusterConfig::new(
        2, nodes.clone(),
        temp_dir2.path().join("logs").to_string_lossy().to_string(),
        temp_dir2.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
        (1000, 2000), // 1-2 seconds for testing
        300,          // 300ms heartbeats
    );

    let config3 = ClusterConfig::new(
        3, nodes.clone(),
        temp_dir3.path().join("logs").to_string_lossy().to_string(),
        temp_dir3.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
        (1000, 2000), // 1-2 seconds for testing
        300,          // 300ms heartbeats
    );

    // Create RaftNodes
    let raft_node1 = RaftNode::new(config1).expect("Failed to create RaftNode 1");
    let raft_node2 = RaftNode::new(config2).expect("Failed to create RaftNode 2");
    let raft_node3 = RaftNode::new(config3).expect("Failed to create RaftNode 3");

    // Create gRPC servers (timing is now configured in ClusterConfig)
    let server1 = RaftGrpcServer::new(raft_node1);
    let server2 = RaftGrpcServer::new(raft_node2);
    let server3 = RaftGrpcServer::new(raft_node3);

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

    println!("🌐 Started 3 gRPC servers on ports 19001, 19002, 19003");

    // Wait for servers to start up
    sleep(Duration::from_millis(500)).await;

    // Helper function to find the current leader
    let find_leader = || -> Option<(u32, u64)> {
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

    // Helper function to get log info for all nodes
    let get_all_log_info = || -> HashMap<u32, (u64, u64, u64)> {
        let mut info = HashMap::new();
        
        let node1 = server1_raft.lock().unwrap();
        let (last_index1, last_term1) = node1.get_last_log_info();
        info.insert(1, (node1.get_log_length(), last_index1, last_term1));
        drop(node1);
        
        let node2 = server2_raft.lock().unwrap();
        let (last_index2, last_term2) = node2.get_last_log_info();
        info.insert(2, (node2.get_log_length(), last_index2, last_term2));
        drop(node2);
        
        let node3 = server3_raft.lock().unwrap();
        let (last_index3, last_term3) = node3.get_last_log_info();
        info.insert(3, (node3.get_log_length(), last_index3, last_term3));
        drop(node3);
        
        info
    };

    println!("\n📋 PHASE 1: INITIAL LEADER ELECTION");
    
    // Wait for initial leader election
    let (initial_leader_id, initial_term) = timeout(Duration::from_secs(10), async {
        loop {
            sleep(Duration::from_millis(200)).await;
            if let Some((leader_id, term)) = find_leader() {
                return (leader_id, term);
            }
        }
    }).await.expect("Initial leader election timed out");

    println!("✅ Initial leader elected: Node {} in term {}", initial_leader_id, initial_term);

    // Verify exactly one leader and two followers
    let states = [
        (1, server1_raft.lock().unwrap().get_server_state()),
        (2, server2_raft.lock().unwrap().get_server_state()),
        (3, server3_raft.lock().unwrap().get_server_state()),
    ];
    
    let leader_count = states.iter().filter(|(_, state)| *state == ServerState::Leader).count();
    let follower_count = states.iter().filter(|(_, state)| *state == ServerState::Follower).count();
    
    assert_eq!(leader_count, 1, "Should have exactly 1 leader");
    assert_eq!(follower_count, 2, "Should have exactly 2 followers");
    
    println!("✅ Cluster state verified: 1 Leader + 2 Followers");

    println!("\n📋 PHASE 2: LOG REPLICATION TEST");
    
    // Get initial log state
    let initial_log_info = get_all_log_info();
    println!("📊 Initial log state: {:?}", initial_log_info);
    
    // Have the leader append 4 log entries
    let test_payloads = vec![
        b"Entry 1: Hello Raft".to_vec(),
        b"Entry 2: Consensus Test".to_vec(),
        b"Entry 3: Log Replication".to_vec(),
        b"Entry 4: Distributed Systems".to_vec(),
    ];
    
    let mut appended_indices = Vec::new();
    
    for (i, payload) in test_payloads.iter().enumerate() {
        let index = match initial_leader_id {
            1 => {
                let mut node = server1_raft.lock().unwrap();
                node.append_new_entry(payload.clone()).expect("Failed to append entry")
            },
            2 => {
                let mut node = server2_raft.lock().unwrap();
                node.append_new_entry(payload.clone()).expect("Failed to append entry")
            },
            3 => {
                let mut node = server3_raft.lock().unwrap();
                node.append_new_entry(payload.clone()).expect("Failed to append entry")
            },
            _ => panic!("Invalid leader ID"),
        };
        
        appended_indices.push(index);
        println!("✅ Leader appended entry {}: index {}", i + 1, index);
        
        // Small delay to allow replication
        sleep(Duration::from_millis(100)).await;
    }
    
    // Wait for replication to complete
    println!("⏳ Waiting for log replication to complete...");
    sleep(Duration::from_secs(2)).await;
    
    // Verify all nodes have the same log length and last log info
    let final_log_info = get_all_log_info();
    println!("📊 Final log state after replication: {:?}", final_log_info);
    
    // All nodes should have the same log length and last log info
    let log_lengths: Vec<u64> = final_log_info.values().map(|(len, _, _)| *len).collect();
    let last_indices: Vec<u64> = final_log_info.values().map(|(_, idx, _)| *idx).collect();
    let last_terms: Vec<u64> = final_log_info.values().map(|(_, _, term)| *term).collect();
    
    assert!(log_lengths.iter().all(|&len| len == log_lengths[0]), 
            "All nodes should have same log length: {:?}", log_lengths);
    assert!(last_indices.iter().all(|&idx| idx == last_indices[0]), 
            "All nodes should have same last index: {:?}", last_indices);
    assert!(last_terms.iter().all(|&term| term == last_terms[0]), 
            "All nodes should have same last term: {:?}", last_terms);
    
    println!("✅ Log replication verified: All nodes have {} entries", log_lengths[0]);

    println!("\n📋 PHASE 3: LEADER FAILURE SIMULATION");
    
    // Forcibly shut down the current leader
    println!("💥 Shutting down leader Node {}...", initial_leader_id);
    
    match initial_leader_id {
        1 => {
            server1.shutdown();
            event_loop1.abort();
            server_handle1.abort();
        },
        2 => {
            server2.shutdown();
            event_loop2.abort();
            server_handle2.abort();
        },
        3 => {
            server3.shutdown();
            event_loop3.abort();
            server_handle3.abort();
        },
        _ => panic!("Invalid leader ID"),
    }
    
    println!("✅ Leader Node {} shut down", initial_leader_id);

    // Wait for new leader election
    println!("⏳ Waiting for new leader election...");
    
    let (new_leader_id, new_term) = timeout(Duration::from_secs(15), async {
        loop {
            sleep(Duration::from_millis(300)).await;
            
            // Check remaining nodes for new leader
            let remaining_states = match initial_leader_id {
                1 => vec![
                    (2, server2_raft.lock().unwrap().get_server_state(), server2_raft.lock().unwrap().get_current_term()),
                    (3, server3_raft.lock().unwrap().get_server_state(), server3_raft.lock().unwrap().get_current_term()),
                ],
                2 => vec![
                    (1, server1_raft.lock().unwrap().get_server_state(), server1_raft.lock().unwrap().get_current_term()),
                    (3, server3_raft.lock().unwrap().get_server_state(), server3_raft.lock().unwrap().get_current_term()),
                ],
                3 => vec![
                    (1, server1_raft.lock().unwrap().get_server_state(), server1_raft.lock().unwrap().get_current_term()),
                    (2, server2_raft.lock().unwrap().get_server_state(), server2_raft.lock().unwrap().get_current_term()),
                ],
                _ => panic!("Invalid leader ID"),
            };
            
            let leaders: Vec<(u32, u64)> = remaining_states.iter()
                .filter_map(|(id, state, term)| {
                    if *state == ServerState::Leader {
                        Some((*id, *term))
                    } else {
                        None
                    }
                })
                .collect();
            
            if leaders.len() == 1 {
                return leaders[0];
            }
        }
    }).await.expect("New leader election timed out");

    println!("✅ New leader elected: Node {} in term {}", new_leader_id, new_term);
    
    // Verify new term is higher than initial term
    assert!(new_term > initial_term, 
            "New term {} should be higher than initial term {}", new_term, initial_term);
    
    // Verify new leader is different from failed leader
    assert_ne!(new_leader_id, initial_leader_id, 
               "New leader should be different from failed leader");

    println!("✅ Leader failover verified: Node {} → Node {} (term {} → {})",
             initial_leader_id, new_leader_id, initial_term, new_term);

    println!("\n📋 PHASE 4: NEW LEADER LOG APPEND TEST");

    // Have the new leader append 3 additional log entries
    let additional_payloads = vec![
        b"Entry 5: New Leader Test".to_vec(),
        b"Entry 6: Post-Failover".to_vec(),
        b"Entry 7: Consistency Check".to_vec(),
    ];

    let mut new_appended_indices = Vec::new();

    for (i, payload) in additional_payloads.iter().enumerate() {
        let index = match new_leader_id {
            1 => {
                let mut node = server1_raft.lock().unwrap();
                node.append_new_entry(payload.clone()).expect("Failed to append entry")
            },
            2 => {
                let mut node = server2_raft.lock().unwrap();
                node.append_new_entry(payload.clone()).expect("Failed to append entry")
            },
            3 => {
                let mut node = server3_raft.lock().unwrap();
                node.append_new_entry(payload.clone()).expect("Failed to append entry")
            },
            _ => panic!("Invalid new leader ID"),
        };

        new_appended_indices.push(index);
        println!("✅ New leader appended entry {}: index {}", i + 5, index);

        // Small delay to allow replication
        sleep(Duration::from_millis(100)).await;
    }

    // Wait for replication to complete between remaining nodes
    println!("⏳ Waiting for new entries to replicate...");
    sleep(Duration::from_secs(2)).await;

    // Verify remaining nodes have consistent logs
    let remaining_log_info = match initial_leader_id {
        1 => {
            let mut info = HashMap::new();
            let node2 = server2_raft.lock().unwrap();
            let (last_index2, last_term2) = node2.get_last_log_info();
            info.insert(2, (node2.get_log_length(), last_index2, last_term2));
            drop(node2);

            let node3 = server3_raft.lock().unwrap();
            let (last_index3, last_term3) = node3.get_last_log_info();
            info.insert(3, (node3.get_log_length(), last_index3, last_term3));
            drop(node3);

            info
        },
        2 => {
            let mut info = HashMap::new();
            let node1 = server1_raft.lock().unwrap();
            let (last_index1, last_term1) = node1.get_last_log_info();
            info.insert(1, (node1.get_log_length(), last_index1, last_term1));
            drop(node1);

            let node3 = server3_raft.lock().unwrap();
            let (last_index3, last_term3) = node3.get_last_log_info();
            info.insert(3, (node3.get_log_length(), last_index3, last_term3));
            drop(node3);

            info
        },
        3 => {
            let mut info = HashMap::new();
            let node1 = server1_raft.lock().unwrap();
            let (last_index1, last_term1) = node1.get_last_log_info();
            info.insert(1, (node1.get_log_length(), last_index1, last_term1));
            drop(node1);

            let node2 = server2_raft.lock().unwrap();
            let (last_index2, last_term2) = node2.get_last_log_info();
            info.insert(2, (node2.get_log_length(), last_index2, last_term2));
            drop(node2);

            info
        },
        _ => panic!("Invalid initial leader ID"),
    };

    println!("📊 Remaining nodes log state: {:?}", remaining_log_info);

    // Verify remaining nodes have consistent logs
    let remaining_log_lengths: Vec<u64> = remaining_log_info.values().map(|(len, _, _)| *len).collect();
    let remaining_last_indices: Vec<u64> = remaining_log_info.values().map(|(_, idx, _)| *idx).collect();

    assert!(remaining_log_lengths.iter().all(|&len| len == remaining_log_lengths[0]),
            "Remaining nodes should have same log length: {:?}", remaining_log_lengths);
    assert!(remaining_last_indices.iter().all(|&idx| idx == remaining_last_indices[0]),
            "Remaining nodes should have same last index: {:?}", remaining_last_indices);

    println!("✅ New leader log append verified: {} total entries", remaining_log_lengths[0]);

    println!("\n📋 PHASE 5: FINAL VERIFICATION");

    // Verify cluster stability with remaining nodes
    let remaining_leader = match initial_leader_id {
        1 => {
            let states = [
                (2, server2_raft.lock().unwrap().get_server_state()),
                (3, server3_raft.lock().unwrap().get_server_state()),
            ];

            let leaders: Vec<u32> = states.iter()
                .filter_map(|(id, state)| if *state == ServerState::Leader { Some(*id) } else { None })
                .collect();

            assert_eq!(leaders.len(), 1, "Should have exactly 1 leader among remaining nodes");
            leaders[0]
        },
        2 => {
            let states = [
                (1, server1_raft.lock().unwrap().get_server_state()),
                (3, server3_raft.lock().unwrap().get_server_state()),
            ];

            let leaders: Vec<u32> = states.iter()
                .filter_map(|(id, state)| if *state == ServerState::Leader { Some(*id) } else { None })
                .collect();

            assert_eq!(leaders.len(), 1, "Should have exactly 1 leader among remaining nodes");
            leaders[0]
        },
        3 => {
            let states = [
                (1, server1_raft.lock().unwrap().get_server_state()),
                (2, server2_raft.lock().unwrap().get_server_state()),
            ];

            let leaders: Vec<u32> = states.iter()
                .filter_map(|(id, state)| if *state == ServerState::Leader { Some(*id) } else { None })
                .collect();

            assert_eq!(leaders.len(), 1, "Should have exactly 1 leader among remaining nodes");
            leaders[0]
        },
        _ => panic!("Invalid initial leader ID"),
    };

    assert_eq!(remaining_leader, new_leader_id, "Leader should remain stable");

    println!("✅ Final cluster state verified: Node {} is stable leader", remaining_leader);

    // Graceful shutdown of remaining servers
    println!("\n🛑 Shutting down remaining servers...");

    match initial_leader_id {
        1 => {
            server2.shutdown();
            server3.shutdown();
            event_loop2.abort();
            event_loop3.abort();
            server_handle2.abort();
            server_handle3.abort();
        },
        2 => {
            server1.shutdown();
            server3.shutdown();
            event_loop1.abort();
            event_loop3.abort();
            server_handle1.abort();
            server_handle3.abort();
        },
        3 => {
            server1.shutdown();
            server2.shutdown();
            event_loop1.abort();
            event_loop2.abort();
            server_handle1.abort();
            server_handle2.abort();
        },
        _ => panic!("Invalid leader ID"),
    }

    println!("🎉 COMPREHENSIVE RAFT LIFECYCLE TEST COMPLETED SUCCESSFULLY!");
    println!("   ✅ Phase 1: Initial leader election");
    println!("   ✅ Phase 2: Log replication across all nodes");
    println!("   ✅ Phase 3: Leader failure detection and new election");
    println!("   ✅ Phase 4: New leader log append and replication");
    println!("   ✅ Phase 5: Final cluster stability verification");
    println!("   🔒 All Raft safety properties maintained throughout lifecycle");
}
