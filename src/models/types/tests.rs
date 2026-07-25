use super::*;

#[test]
fn test_node_info() {
    let node = NodeInfo::new(1, "192.168.1.100".to_string(), 8001);
    assert_eq!(node.node_id, 1);
    assert_eq!(node.ip_address, "192.168.1.100");
    assert_eq!(node.port, 8001);
    assert_eq!(node.get_address(), "192.168.1.100:8001");
}

#[test]
fn test_cluster_config_creation() {
    let nodes = vec![
        NodeInfo::new(1, "192.168.1.100".to_string(), 8001),
        NodeInfo::new(2, "192.168.1.101".to_string(), 8002),
        NodeInfo::new(3, "192.168.1.102".to_string(), 8003),
    ];

    let config = ClusterConfig::new(
        2, // This is node 2
        nodes,
        "/var/raft/logs".to_string(),
        "/var/raft/state.meta".to_string(),
        1024 * 1024, // 1MB segments
        1000,
        (150, 500), // Election timeout range
        50,         // Heartbeat interval
    );

    assert_eq!(config.node_id, 2);
    assert_eq!(config.cluster_size(), 3);
    assert_eq!(config.majority_size(), 2);
    assert_eq!(config.get_address(), "192.168.1.101:8002");

    // Test finding this node
    let this_node = config.get_this_node().unwrap();
    assert_eq!(this_node.node_id, 2);
    assert_eq!(this_node.get_address(), "192.168.1.101:8002");

    // Test finding other nodes
    let others = config.get_other_nodes();
    assert_eq!(others.len(), 2);
    assert!(others.iter().any(|n| n.node_id == 1));
    assert!(others.iter().any(|n| n.node_id == 3));

    // Test finding specific node
    let node1 = config.get_node(1).unwrap();
    assert_eq!(node1.get_address(), "192.168.1.100:8001");
}

#[test]
fn test_cluster_config_helpers() {
    // Test 3-node cluster config
    let config = ClusterConfig::test_cluster_config(2);
    assert_eq!(config.node_id, 2);
    assert_eq!(config.cluster_size(), 3);
    assert_eq!(config.get_address(), "127.0.0.1:8002");

    // Test single-node config
    let single_config = ClusterConfig::test_config(5);
    assert_eq!(single_config.node_id, 5);
    assert_eq!(single_config.cluster_size(), 1);
    assert_eq!(single_config.majority_size(), 1);
    assert_eq!(single_config.get_address(), "127.0.0.1:8005");
    assert_eq!(single_config.get_other_nodes().len(), 0);
}

#[test]
fn test_raft_log_config_conversion() {
    let cluster_config = ClusterConfig::test_cluster_config(1);
    let raft_log_config = cluster_config.to_raft_log_config();

    assert_eq!(
        raft_log_config.log_directory.to_string_lossy(),
        cluster_config.log_directory
    );
    assert_eq!(
        raft_log_config.segment_size,
        cluster_config.log_segment_size
    );
    assert_eq!(
        raft_log_config.max_entries_per_query,
        cluster_config.max_entries_per_query
    );
}
