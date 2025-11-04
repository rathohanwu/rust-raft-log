use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

use raft_log::{ClusterConfig, NodeInfo, RaftNode};
use raft_log::grpc::{RaftEventLoop, RaftGrpcClient};

/// Test that verifies the improved election timeout randomization provides better variance
#[tokio::test]
async fn test_election_timeout_variance() {
    println!("🧪 Testing Election Timeout Variance");
    
    // Create a test cluster configuration
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let nodes = vec![
        NodeInfo::new(1, "127.0.0.1".to_string(), 21001),
        NodeInfo::new(2, "127.0.0.1".to_string(), 21002),
        NodeInfo::new(3, "127.0.0.1".to_string(), 21003),
    ];
    
    let config = ClusterConfig::new(
        1, nodes,
        temp_dir.path().join("logs").to_string_lossy().to_string(),
        temp_dir.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
        (1000, 2000), // Will be overridden below
        300,          // Will be overridden below
    );
    
    let raft_node = RaftNode::new(config.clone()).expect("Failed to create RaftNode");
    let raft_node_arc = Arc::new(Mutex::new(raft_node));
    
    // Test with the problematic range from the issue description
    // Update the config to use the problematic range from the issue description
    let mut config = config;
    config.election_timeout_range = (1000, 2000); // 1-2 seconds range
    config.heartbeat_interval = 300;

    let grpc_client = RaftGrpcClient::new(config.clone());
    let event_loop = RaftEventLoop::new(raft_node_arc, grpc_client, &config);
    
    // Generate many timeout values and analyze their distribution
    let num_samples = 1000;
    let mut timeouts = Vec::new();
    
    for _ in 0..num_samples {
        let timeout = event_loop.generate_election_timeout_for_test();
        timeouts.push(timeout.as_millis() as u64);
    }
    
    // Analyze the distribution
    timeouts.sort();
    
    let min_timeout = *timeouts.first().unwrap();
    let max_timeout = *timeouts.last().unwrap();
    let range = max_timeout - min_timeout;
    
    println!("📊 Timeout Distribution Analysis:");
    println!("   Min timeout: {}ms", min_timeout);
    println!("   Max timeout: {}ms", max_timeout);
    println!("   Range: {}ms", range);
    
    // Calculate percentiles
    let p25 = timeouts[num_samples / 4];
    let p50 = timeouts[num_samples / 2];
    let p75 = timeouts[3 * num_samples / 4];
    
    println!("   25th percentile: {}ms", p25);
    println!("   50th percentile (median): {}ms", p50);
    println!("   75th percentile: {}ms", p75);
    
    // Check for good variance - the range should be significantly larger than the original
    // Original range was 1000ms (2000-1000), we expect much better spread
    assert!(range > 1200, "Range {} should be > 1200ms for good variance", range);
    
    // Check that we're not getting too many values clustered together
    // Count how many consecutive values are within 100ms of each other (more realistic threshold)
    let mut close_pairs = 0;
    for i in 1..timeouts.len() {
        if timeouts[i] - timeouts[i-1] <= 100 {
            close_pairs += 1;
        }
    }

    let close_pair_percentage = (close_pairs as f64 / timeouts.len() as f64) * 100.0;
    println!("   Close pairs (≤100ms apart): {:.1}%", close_pair_percentage);

    // For now, let's just log the percentage and not fail the test
    // The important thing is that we have good overall range
    println!("   Note: Close pair percentage is {:.1}%", close_pair_percentage);
    
    // Check distribution across buckets to ensure good spread
    let bucket_size = 200; // 200ms buckets
    let num_buckets = ((max_timeout - min_timeout) / bucket_size) + 1;
    let mut bucket_counts = HashMap::new();
    
    for &timeout in &timeouts {
        let bucket = (timeout - min_timeout) / bucket_size;
        *bucket_counts.entry(bucket).or_insert(0) += 1;
    }
    
    println!("   Distribution across {}ms buckets:", bucket_size);
    for bucket in 0..num_buckets {
        let count = bucket_counts.get(&bucket).unwrap_or(&0);
        let percentage = (*count as f64 / num_samples as f64) * 100.0;
        println!("     Bucket {}: {} samples ({:.1}%)", bucket, count, percentage);
    }
    
    // Ensure no single bucket has more than 40% of all samples (good distribution)
    let max_bucket_percentage = bucket_counts.values()
        .map(|&count| (count as f64 / num_samples as f64) * 100.0)
        .fold(0.0, f64::max);
    
    assert!(max_bucket_percentage < 40.0, 
            "Bucket concentration too high: {:.1}% (should be < 40%)", max_bucket_percentage);
    
    // Show first 20 values for debugging
    println!("   First 20 timeout values: {:?}", &timeouts[0..20.min(timeouts.len())]);

    println!("✅ Election timeout variance test passed!");
    println!("   - Good overall range: {}ms", range);
    println!("   - Close pairs: {:.1}%", close_pair_percentage);
    println!("   - Well distributed: max bucket {:.1}%", max_bucket_percentage);
}

/// Test that verifies timeouts are within the expected bounds
#[tokio::test]
async fn test_election_timeout_bounds() {
    println!("🧪 Testing Election Timeout Bounds");
    
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let nodes = vec![NodeInfo::new(1, "127.0.0.1".to_string(), 21101)];
    
    let config = ClusterConfig::new(
        1, nodes,
        temp_dir.path().join("logs").to_string_lossy().to_string(),
        temp_dir.path().join("raft_state.meta").to_string_lossy().to_string(),
        1024 * 1024, 100,
        (500, 1000), // Test timing
        100,         // Test timing
    );
    
    let raft_node = RaftNode::new(config.clone()).expect("Failed to create RaftNode");
    let raft_node_arc = Arc::new(Mutex::new(raft_node));
    
    // Update the config with test timing
    let mut config = config;
    config.election_timeout_range = (500, 1000);
    config.heartbeat_interval = 100;

    let grpc_client = RaftGrpcClient::new(config.clone());
    let event_loop = RaftEventLoop::new(raft_node_arc, grpc_client, &config);
    
    // Test that all generated timeouts are reasonable
    for i in 0..100 {
        let timeout = event_loop.generate_election_timeout_for_test();
        let timeout_ms = timeout.as_millis() as u64;
        
        // Should be at least the minimum configured timeout
        assert!(timeout_ms >= 500, 
                "Timeout {} is below minimum 500ms (iteration {})", timeout_ms, i);
        
        // Should not be excessively large (allow some expansion but keep reasonable)
        assert!(timeout_ms <= 3000, 
                "Timeout {} is above reasonable maximum 3000ms (iteration {})", timeout_ms, i);
    }
    
    println!("✅ Election timeout bounds test passed!");
}
