use super::*;
use crate::models::{ClusterConfig, EntryType};
use tempfile::TempDir;

/// Creates a test config using a temporary directory that gets cleaned up automatically.
/// Use this for most tests where you don't need to inspect the files afterward.
/// Returns both the config and the TempDir (keep the TempDir alive to prevent cleanup).
fn create_test_config() -> (RaftLogConfig, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config = RaftLogConfig {
        log_directory: temp_dir.path().to_path_buf(),
        segment_size: 1024, // Small size to test rotation
        max_entries_per_query: 1000,
    };
    (config, temp_dir)
}

fn create_test_entry(term: u64, payload: &str) -> LogEntry {
    LogEntry::new_with_type(term, 0, EntryType::Normal, payload.as_bytes().to_vec())
}

/// Creates a test config that uses a local directory for file inspection.
/// Use this when you want to examine the generated segment files after the test.
/// Files will be created in a unique "./temp/raft_logs_<test_name>" directory.
fn create_inspectable_test_config(test_name: &str) -> RaftLogConfig {
    let log_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("temp")
        .join(format!("raft_logs_{}", test_name));
    // Clean up any existing test files
    if log_dir.exists() {
        let _ = std::fs::remove_dir_all(&log_dir);
    }
    std::fs::create_dir_all(&log_dir).expect("Failed to create test directory");

    RaftLogConfig {
        log_directory: log_dir,
        segment_size: 1024, // Small size to test rotation
        max_entries_per_query: 1000,
    }
}

#[test]
fn test_new_raft_log() {
    let (config, _temp_dir) = create_test_config();
    let raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    assert_eq!(raft_log.len(), 0);
    assert!(raft_log.is_empty());
    assert_eq!(raft_log.segment_count(), 1); // Should create initial segment
    assert_eq!(raft_log.next_index, 1);
}

#[test]
fn test_append_single_entry() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    let entry = create_test_entry(1, "test entry");
    raft_log
        .append_entry(entry)
        .expect("Failed to append entry");

    assert_eq!(raft_log.len(), 1);
    assert!(!raft_log.is_empty());
    assert_eq!(raft_log.next_index, 2);

    let retrieved = raft_log.get_entry(1).expect("Failed to get entry");
    assert_eq!(retrieved.term, 1);
    assert_eq!(retrieved.index, 1);
    assert_eq!(retrieved.payload, "test entry".as_bytes());
}

#[test]
fn test_append_multiple_entries() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    let entries = vec![
        create_test_entry(1, "entry 1"),
        create_test_entry(1, "entry 2"),
        create_test_entry(2, "entry 3"),
    ];

    raft_log
        .append_entries(entries)
        .expect("Failed to append entries");

    assert_eq!(raft_log.len(), 3);
    assert_eq!(raft_log.next_index, 4);

    for i in 1..=3 {
        let entry = raft_log.get_entry(i).expect("Failed to get entry");
        assert_eq!(entry.index, i);
    }
}

#[test]
fn test_get_entries_range() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Add 5 entries
    for i in 1..=5 {
        let entry = create_test_entry(1, &format!("entry {}", i));
        raft_log
            .append_entry(entry)
            .expect("Failed to append entry");
    }

    let entries = raft_log.get_entries(2, 4).expect("Failed to get entries");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].index, 2);
    assert_eq!(entries[1].index, 3);
    assert_eq!(entries[2].index, 4);
}

#[test]
fn test_get_last_log_entry() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Empty log should return None
    assert!(raft_log.get_last_log_entry().is_none());

    // Add entries
    raft_log
        .append_entry(create_test_entry(1, "first"))
        .expect("Failed to append");
    raft_log
        .append_entry(create_test_entry(2, "last"))
        .expect("Failed to append");

    let last = raft_log
        .get_last_log_entry()
        .expect("Failed to get last entry");
    assert_eq!(last.index, 2);
    assert_eq!(last.term, 2);
    assert_eq!(last.payload, "last".as_bytes());
}

#[test]
fn test_truncate_from() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Add 5 entries
    for i in 1..=5 {
        let entry = create_test_entry(1, &format!("entry {}", i));
        raft_log
            .append_entry(entry)
            .expect("Failed to append entry");
    }

    assert_eq!(raft_log.len(), 5);

    // Truncate from index 3
    let result = raft_log.truncate_from(3).expect("Failed to truncate");
    assert!(result);
    assert_eq!(raft_log.len(), 2);
    assert_eq!(raft_log.next_index, 3);

    // Verify remaining entries
    assert!(raft_log.get_entry(1).is_some());
    assert!(raft_log.get_entry(2).is_some());
    assert!(raft_log.get_entry(3).is_none());

    // Can append new entries after truncation
    raft_log
        .append_entry(create_test_entry(2, "new entry"))
        .expect("Failed to append");
    let new_entry = raft_log.get_entry(3).expect("Failed to get new entry");
    assert_eq!(new_entry.term, 2);
    assert_eq!(new_entry.payload, "new entry".as_bytes());
}

#[test]
fn truncating_across_segments_removes_obsolete_files_before_reopen() {
    let temp_dir = TempDir::new().expect("create temp directory");
    let config = RaftLogConfig {
        log_directory: temp_dir.path().join("logs"),
        // Two 80-byte payload entries fit after the 32-byte header; the
        // third rotates, giving the truncation a whole suffix segment.
        segment_size: 256,
        max_entries_per_query: 10,
    };

    {
        let mut log = RaftLog::new(config.clone()).expect("create log");
        for index in 1..=6 {
            log.append_entry(LogEntry::new_with_type(
                1,
                index,
                EntryType::Normal,
                vec![index as u8; 80],
            ))
            .expect("append original entry");
        }
        assert!(log.segment_count() >= 3);
        assert!(log.truncate_from(3).expect("truncate suffix"));
        log.append_entry(LogEntry::new_with_type(
            2,
            0,
            EntryType::Normal,
            b"replacement-3".to_vec(),
        ))
        .expect("append replacement 3");
        log.append_entry(LogEntry::new_with_type(
            2,
            0,
            EntryType::Normal,
            b"replacement-4".to_vec(),
        ))
        .expect("append replacement 4");
    }

    let reopened = RaftLog::new(config.clone()).expect("reopen replacement log");
    assert_eq!(reopened.len(), 4);
    assert_eq!(reopened.get_entry(1).unwrap().term, 1);
    assert_eq!(reopened.get_entry(2).unwrap().term, 1);
    assert_eq!(reopened.get_entry(3).unwrap().term, 2);
    assert_eq!(reopened.get_entry(3).unwrap().payload, b"replacement-3");
    assert_eq!(reopened.get_entry(4).unwrap().payload, b"replacement-4");
    assert!(reopened.get_entry(5).is_none());

    let segment_files = fs::read_dir(&config.log_directory)
        .expect("read log directory")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".dat"))
        .count();
    assert_eq!(segment_files, reopened.segment_count());
}

#[test]
fn truncating_within_a_segment_recovers_after_reopen() {
    let temp_dir = TempDir::new().expect("create temp directory");
    let config = RaftLogConfig {
        log_directory: temp_dir.path().join("logs"),
        segment_size: 256,
        max_entries_per_query: 10,
    };

    {
        let mut log = RaftLog::new(config.clone()).expect("create log");
        for index in 1..=5 {
            log.append_entry(LogEntry::new_with_type(
                1,
                index,
                EntryType::Normal,
                vec![index as u8; 40],
            ))
            .expect("append original entry");
        }
        assert!(log.segment_count() >= 2);

        assert!(log.truncate_from(2).expect("truncate in segment"));
        log.append_entry(LogEntry::new_with_type(
            2,
            0,
            EntryType::Normal,
            b"replacement-2".to_vec(),
        ))
        .expect("append replacement 2");
        log.append_entry(LogEntry::new_with_type(
            2,
            0,
            EntryType::Normal,
            b"replacement-3".to_vec(),
        ))
        .expect("append replacement 3");
    }

    let reopened = RaftLog::new(config).expect("reopen replacement log");
    assert_eq!(reopened.len(), 3);
    assert_eq!(reopened.get_entry(1).unwrap().term, 1);
    assert_eq!(reopened.get_entry(2).unwrap().term, 2);
    assert_eq!(reopened.get_entry(2).unwrap().payload, b"replacement-2");
    assert_eq!(reopened.get_entry(3).unwrap().payload, b"replacement-3");
    assert!(reopened.get_entry(4).is_none());
}

#[test]
fn reopening_with_a_different_segment_size_preserves_existing_files() {
    let temp_dir = TempDir::new().expect("create temp directory");
    let log_directory = temp_dir.path().join("logs");
    let original = RaftLogConfig {
        log_directory: log_directory.clone(),
        segment_size: 256,
        max_entries_per_query: 10,
    };

    {
        let mut log = RaftLog::new(original).expect("create log");
        log.append_entry(LogEntry::new_with_type(
            1,
            0,
            EntryType::Normal,
            b"durable entry".to_vec(),
        ))
        .expect("append entry");
    }

    let mismatched = RaftLogConfig {
        log_directory,
        segment_size: 512,
        max_entries_per_query: 10,
    };
    assert!(RaftLog::new(mismatched).is_err());
}

#[test]
fn test_segment_rotation() {
    let config = create_inspectable_test_config("segment_rotation");
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    let initial_segments = raft_log.segment_count();
    let total_entry_count = 2000;

    for i in 1..=total_entry_count {
        let large_payload = format!("Entry {} with large payload: testing", i);
        let entry = create_test_entry(1, &large_payload);
        raft_log
            .append_entry(entry)
            .expect("Failed to append entry");
    }

    // Should have created additional segments
    let final_segments = raft_log.segment_count();
    assert!(
        final_segments > initial_segments,
        "Expected segment rotation to occur"
    );

    // Test cross-segment range queries
    let entries_1_to_10 = raft_log
        .get_entries(1, 10)
        .expect("Failed to get entries 1-10");
    assert_eq!(entries_1_to_10.len(), 10);

    let entries_40_to_50 = raft_log
        .get_entries(40, 50)
        .expect("Failed to get entries 40-50");
    assert_eq!(entries_40_to_50.len(), 11);
}

#[test]
fn test_cross_segment_queries() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Force segment rotation by adding large entries
    for _i in 1..=20 {
        let large_payload = "x".repeat(200);
        let entry = create_test_entry(1, &large_payload);
        raft_log
            .append_entry(entry)
            .expect("Failed to append entry");
    }

    // Should have multiple segments
    assert!(raft_log.segment_count() > 1);

    // Query across segments
    let entries = raft_log.get_entries(1, 20).expect("Failed to get entries");
    assert_eq!(entries.len(), 20);

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.index, (i + 1) as u64);
    }
}

#[test]
fn test_error_conditions() {
    let (config, _temp_dir) = create_test_config();
    let raft_log = RaftLog::new(config.clone()).expect("Failed to create RaftLog");

    // Invalid index (0)
    assert!(raft_log.get_entry(0).is_none());

    // Non-existent index
    assert!(raft_log.get_entry(100).is_none());

    // Invalid range
    assert!(raft_log.get_entries(5, 3).is_none());

    // Too many entries requested
    let mut config_small_limit = config.clone();
    config_small_limit.max_entries_per_query = 5;
    let mut raft_log_small = RaftLog::new(config_small_limit).expect("Failed to create RaftLog");

    for _i in 1..=10 {
        raft_log_small
            .append_entry(create_test_entry(1, "test"))
            .expect("Failed to append");
    }

    // Should return None when too many entries requested
    assert!(raft_log_small.get_entries(1, 10).is_none());
}

#[test]
fn test_persistence_and_loading() {
    let (config, _temp_dir) = create_test_config();

    // Create and populate a log
    {
        let mut raft_log = RaftLog::new(config.clone()).expect("Failed to create RaftLog");
        for i in 1..=10 {
            let entry = create_test_entry(1, &format!("entry {}", i));
            raft_log
                .append_entry(entry)
                .expect("Failed to append entry");
        }
    } // raft_log goes out of scope

    // Create a new log with the same directory - should load existing segments
    {
        let raft_log = RaftLog::new(config).expect("Failed to create RaftLog");
        assert_eq!(raft_log.len(), 10);
        assert_eq!(raft_log.next_index, 11);

        // Verify all entries are loaded correctly
        for i in 1..=10 {
            let entry = raft_log.get_entry(i).expect("Failed to get entry");
            assert_eq!(entry.index, i);
            assert_eq!(entry.payload, format!("entry {}", i).as_bytes());
        }
    }
}

#[test]
fn test_recursive_append_with_rotation() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    let initial_segments = raft_log.segment_count();

    // Fill up the first segment to near capacity
    for _i in 0..10 {
        let entry = create_test_entry(1, &"x".repeat(80));
        raft_log
            .append_entry(entry)
            .expect("Failed to append entry");
    }

    // Now add an entry that should trigger rotation and recursive append
    let large_payload = "y".repeat(100);
    let entry = create_test_entry(2, &large_payload);
    raft_log
        .append_entry(entry)
        .expect("Failed to append large entry");

    // Should have created a new segment
    assert!(raft_log.segment_count() > initial_segments);
    assert_eq!(raft_log.next_index, 12); // 10 + 1 + 1

    // Last entry should be retrievable and in the new segment
    let retrieved = raft_log.get_entry(11).expect("Failed to get entry");
    assert_eq!(retrieved.payload, large_payload.as_bytes());
    assert_eq!(retrieved.term, 2);
}

#[test]
fn test_base_index_from_file_header() {
    let (config, _temp_dir) = create_test_config();

    // Create a log with specific base index
    {
        let mut raft_log = RaftLog::new(config.clone()).expect("Failed to create RaftLog");

        // Add entries to fill first segment and trigger rotation
        for _i in 0..15 {
            let entry = create_test_entry(1, &"x".repeat(50));
            raft_log
                .append_entry(entry)
                .expect("Failed to append entry");
        }

        // Should have multiple segments now
        assert!(raft_log.segment_count() > 1);
    } // raft_log goes out of scope, files are written

    // Create a new log instance - should read base indices from file headers
    {
        let raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

        // Verify that segments were loaded correctly with proper base indices
        assert!(raft_log.segment_count() > 1);
        assert_eq!(raft_log.len(), 15);

        // Verify all entries are accessible (proves base indices were read correctly)
        for i in 1..=15 {
            let entry = raft_log
                .get_entry(i)
                .expect(&format!("Failed to get entry {}", i));
            assert_eq!(entry.index, i);
        }

        // Verify next_index is correct
        assert_eq!(raft_log.next_index, 16);
    }
}

#[test]
fn test_comprehensive_cross_segment_operations() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Phase 1: Fill multiple segments with different terms and payloads
    let mut entry_count = 0;
    for term in 1..=5 {
        for entry_in_term in 1..=20 {
            entry_count += 1;
            let payload = format!(
                "Term {} Entry {} - Data: {}",
                term,
                entry_in_term,
                "x".repeat(50)
            );
            let entry = create_test_entry(term, &payload);
            raft_log
                .append_entry(entry)
                .expect("Failed to append entry");
        }
    }

    let total_entries = entry_count;
    let total_segments = raft_log.segment_count();
    assert!(total_segments > 1, "Should have multiple segments");

    // Phase 2: Test individual entry retrieval across all segments
    for i in 1..=total_entries {
        let entry = raft_log
            .get_entry(i)
            .expect(&format!("Failed to get entry {}", i));
        assert_eq!(entry.index, i);

        // Verify term progression
        let expected_term = ((i - 1) / 20) + 1;
        assert_eq!(
            entry.term, expected_term,
            "Entry {} should have term {}",
            i, expected_term
        );
    }

    // Phase 3: Test range queries that span multiple segments

    // Query spanning first two segments
    let early_entries = raft_log
        .get_entries(1, 30)
        .expect("Failed to get early entries");
    assert_eq!(early_entries.len(), 30);
    assert_eq!(early_entries[0].index, 1);
    assert_eq!(early_entries[29].index, 30);

    // Query spanning middle segments
    let middle_entries = raft_log
        .get_entries(40, 70)
        .expect("Failed to get middle entries");
    assert_eq!(middle_entries.len(), 31);
    assert_eq!(middle_entries[0].index, 40);
    assert_eq!(middle_entries[30].index, 70);

    // Query spanning to the end
    let late_entries = raft_log
        .get_entries(80, total_entries)
        .expect("Failed to get late entries");
    assert_eq!(late_entries.len(), (total_entries - 79) as usize);
    assert_eq!(late_entries[0].index, 80);
    assert_eq!(late_entries.last().unwrap().index, total_entries);

    // Phase 4: Test last entry retrieval
    let last_entry = raft_log
        .get_last_log_entry()
        .expect("Failed to get last entry");
    assert_eq!(last_entry.index, total_entries);
    assert_eq!(last_entry.term, 5); // Should be from term 5

    // Phase 5: Test truncation across segments
    let truncate_point = total_entries - 25; // Remove last 25 entries
    let truncated = raft_log
        .truncate_from(truncate_point + 1)
        .expect("Failed to truncate");
    assert!(truncated, "Truncation should have occurred");

    // Verify truncation worked
    assert_eq!(raft_log.len(), truncate_point);
    assert!(raft_log.get_entry(truncate_point).is_some());
    assert!(raft_log.get_entry(truncate_point + 1).is_none());

    // Phase 6: Add new entries after truncation
    for i in 1..=10 {
        let payload = format!("Post-truncation entry {} - {}", i, "y".repeat(40));
        let entry = create_test_entry(6, &payload); // New term
        raft_log
            .append_entry(entry)
            .expect("Failed to append post-truncation entry");
    }

    // Verify new entries
    let new_total = truncate_point + 10;
    assert_eq!(
        raft_log.len(),
        new_total,
        "Expected {} entries after adding 10 post-truncation entries",
        new_total
    );

    let last_new_entry = raft_log
        .get_last_log_entry()
        .expect("Failed to get last new entry");
    assert_eq!(last_new_entry.term, 6);
    assert!(last_new_entry.payload.starts_with(b"Post-truncation"));
}

#[test]
fn test_example_using_temp_dir() {
    // Example: Using temporary directory (files auto-cleaned up)
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Add some entries
    for i in 1..=5 {
        let entry = create_test_entry(1, &format!("temp entry {}", i));
        raft_log
            .append_entry(entry)
            .expect("Failed to append entry");
    }

    assert_eq!(raft_log.len(), 5);
    // Files will be automatically cleaned up when _temp_dir goes out of scope
}

#[test]
fn test_example_using_inspectable_dir() {
    // Example: Using inspectable directory (files remain for inspection)
    let config = create_inspectable_test_config("example");
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Add some entries
    for i in 1..=3 {
        let entry = create_test_entry(1, &format!("inspectable entry {}", i));
        raft_log
            .append_entry(entry)
            .expect("Failed to append entry");
    }

    assert_eq!(raft_log.len(), 3);
    // Files remain in ./temp/raft_logs_example/ for inspection
}

#[test]
fn test_clean_option_api() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Demonstrate clean Option-based API

    // Empty log returns None
    assert!(raft_log.get_entry(1).is_none());
    assert!(raft_log.get_last_log_entry().is_none());
    assert!(raft_log.get_entries(1, 5).is_none());

    // Add some entries
    for i in 1..=5 {
        let entry = create_test_entry(1, &format!("entry {}", i));
        raft_log
            .append_entry(entry)
            .expect("Failed to append entry");
    }

    // Now entries exist - clean Option API
    if let Some(entry) = raft_log.get_entry(3) {
        assert_eq!(entry.index, 3);
    }

    if let Some(last_entry) = raft_log.get_last_log_entry() {
        assert_eq!(last_entry.index, 5);
    }

    if let Some(entries) = raft_log.get_entries(2, 4) {
        assert_eq!(entries.len(), 3);
    }

    // Invalid requests return None (no exceptions!)
    assert!(raft_log.get_entry(0).is_none()); // Invalid index
    assert!(raft_log.get_entry(100).is_none()); // Non-existent
    assert!(raft_log.get_entries(5, 3).is_none()); // Invalid range
}

#[test]
fn test_entry_type_support_in_raft_log() {
    let (config, _temp_dir) = create_test_config();
    let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

    // Test appending entries with different types
    let normal_entry = LogEntry::new_with_type(
        1,
        0,
        EntryType::Normal,
        "normal command".as_bytes().to_vec(),
    );
    raft_log
        .append_entry(normal_entry)
        .expect("Failed to append normal entry");

    let noop_entry = LogEntry::new_with_type(1, 0, EntryType::NoOp, vec![]);
    raft_log
        .append_entry(noop_entry)
        .expect("Failed to append noop entry");

    let another_normal = LogEntry::new_with_type(
        2,
        0,
        EntryType::Normal,
        "another command".as_bytes().to_vec(),
    );
    raft_log
        .append_entry(another_normal)
        .expect("Failed to append another normal entry");

    // Verify entries can be retrieved with correct types
    let entry1 = raft_log.get_entry(1).expect("Should get entry 1");
    assert_eq!(entry1.entry_type, EntryType::Normal);
    assert_eq!(entry1.term, 1);
    assert_eq!(entry1.payload, "normal command".as_bytes());

    let entry2 = raft_log.get_entry(2).expect("Should get entry 2");
    assert_eq!(entry2.entry_type, EntryType::NoOp);
    assert_eq!(entry2.term, 1);
    assert_eq!(entry2.payload, Vec::<u8>::new());

    let entry3 = raft_log.get_entry(3).expect("Should get entry 3");
    assert_eq!(entry3.entry_type, EntryType::Normal);
    assert_eq!(entry3.term, 2);
    assert_eq!(entry3.payload, "another command".as_bytes());

    // Test range queries preserve entry types
    let entries = raft_log.get_entries(1, 3).expect("Should get entries 1-3");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].entry_type, EntryType::Normal);
    assert_eq!(entries[1].entry_type, EntryType::NoOp);
    assert_eq!(entries[2].entry_type, EntryType::Normal);

    // Test last entry
    let last_entry = raft_log
        .get_last_log_entry()
        .expect("Should get last entry");
    assert_eq!(last_entry.entry_type, EntryType::Normal);
    assert_eq!(last_entry.term, 2);
}

#[test]
fn test_multiple_node_cluster_configs() {
    // Test creating configs for multiple nodes in the same cluster
    let node1_config = ClusterConfig::test_cluster_config(1);
    let node2_config = ClusterConfig::test_cluster_config(2);
    let node3_config = ClusterConfig::test_cluster_config(3);

    // All nodes should have the same cluster configuration
    assert_eq!(node1_config.cluster_size(), 3);
    assert_eq!(node2_config.cluster_size(), 3);
    assert_eq!(node3_config.cluster_size(), 3);

    // But different node IDs
    assert_eq!(node1_config.node_id, 1);
    assert_eq!(node2_config.node_id, 2);
    assert_eq!(node3_config.node_id, 3);

    // Each node should know about all other nodes
    let node1_others = node1_config.get_other_nodes();
    assert_eq!(node1_others.len(), 2);
    assert!(node1_others.iter().any(|n| n.node_id == 2));
    assert!(node1_others.iter().any(|n| n.node_id == 3));

    let node2_others = node2_config.get_other_nodes();
    assert_eq!(node2_others.len(), 2);
    assert!(node2_others.iter().any(|n| n.node_id == 1));
    assert!(node2_others.iter().any(|n| n.node_id == 3));

    // Verify each node can find itself
    assert_eq!(node1_config.get_address(), "127.0.0.1:8001");
    assert_eq!(node2_config.get_address(), "127.0.0.1:8002");
    assert_eq!(node3_config.get_address(), "127.0.0.1:8003");

    // Verify unique log directories
    assert_eq!(node1_config.log_directory, "./test_logs_node_1");
    assert_eq!(node2_config.log_directory, "./test_logs_node_2");
    assert_eq!(node3_config.log_directory, "./test_logs_node_3");

    // Verify majority calculation
    assert_eq!(node1_config.majority_size(), 2); // (3/2) + 1 = 2
    assert_eq!(node2_config.majority_size(), 2);
    assert_eq!(node3_config.majority_size(), 2);

    // Test single-node configs (for simple tests)
    let single_node_config = ClusterConfig::test_config(1);
    assert_eq!(single_node_config.cluster_size(), 1);
    assert_eq!(single_node_config.majority_size(), 1);
    assert_eq!(single_node_config.get_other_nodes().len(), 0);
}
