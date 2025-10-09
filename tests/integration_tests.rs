//! Integration tests for the Raft log implementation.
//!
//! These tests cover complex scenarios, edge cases, and integration
//! between different components of the system.

use raft_log::{Error, LogEntry, RaftLog, RaftLogConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_large_scale_operations() {
    let temp_dir = TempDir::new().unwrap();
    let config = RaftLogConfig {
        max_entries_per_segment: 1000,
        data_dir: temp_dir.path().to_path_buf(),
    };
    let log = RaftLog::with_config(config).unwrap();

    // Add 10,000 entries across multiple segments
    const TOTAL_ENTRIES: u64 = 10_000;
    
    for i in 1..=TOTAL_ENTRIES {
        let term = (i - 1) / 1000 + 1; // Change term every 1000 entries
        let entry = LogEntry::new(term, i, format!("large_scale_entry_{}", i).into_bytes());
        log.append(entry).unwrap();
        
        // Validate every 1000 entries
        if i % 1000 == 0 {
            log.flush().unwrap();
            log.validate().unwrap();
        }
    }

    // Verify final state
    assert_eq!(log.last_index(), Some(TOTAL_ENTRIES));
    assert_eq!(log.last_term().unwrap(), Some(10)); // Last term should be 10
    
    let metadata = log.metadata();
    assert_eq!(metadata.total_entries, TOTAL_ENTRIES as u32);
    assert_eq!(metadata.segment_count, 10); // 10 segments of 1000 entries each

    // Test random access
    for _ in 0..100 {
        let random_index = (rand::random::<u64>() % TOTAL_ENTRIES) + 1;
        let entry = log.get_entry(random_index).unwrap();
        assert_eq!(entry.index(), random_index);
        assert_eq!(
            entry.command(),
            format!("large_scale_entry_{}", random_index).as_bytes()
        );
    }
}

#[test]
fn test_segment_boundary_operations() {
    let temp_dir = TempDir::new().unwrap();
    let config = RaftLogConfig {
        max_entries_per_segment: 3, // Small segments for boundary testing
        data_dir: temp_dir.path().to_path_buf(),
    };
    let log = RaftLog::with_config(config).unwrap();

    // Add entries that will span multiple segments
    for i in 1..=10 {
        let entry = LogEntry::new(1, i, format!("boundary_test_{}", i).into_bytes());
        log.append(entry).unwrap();
    }

    // Should have 4 segments: [1,2,3], [4,5,6], [7,8,9], [10]
    assert_eq!(log.segment_count(), 4);

    // Test reading across segment boundaries
    let entries = log.get_entries(2, 8).unwrap(); // Spans 3 segments
    assert_eq!(entries.len(), 7);
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.index(), (i + 2) as u64);
    }

    // Test segment finding at boundaries
    assert_eq!(log.find_segment_for_index(3), Some(1)); // End of first segment
    assert_eq!(log.find_segment_for_index(4), Some(4)); // Start of second segment
    assert_eq!(log.find_segment_for_index(9), Some(7)); // End of third segment
    assert_eq!(log.find_segment_for_index(10), Some(10)); // Single entry segment

    // Test metadata across segments
    let metadata = log.metadata();
    assert_eq!(metadata.first_index, Some(1));
    assert_eq!(metadata.last_index, Some(10));
    assert_eq!(metadata.total_entries, 10);
}

#[test]
fn test_concurrent_read_write_operations() {
    let temp_dir = TempDir::new().unwrap();
    let log = Arc::new(RaftLog::new(temp_dir.path()).unwrap());

    // Add initial entries
    for i in 1..=100 {
        let entry = LogEntry::new(1, i, format!("initial_{}", i).into_bytes());
        log.append(entry).unwrap();
    }

    let log_clone = Arc::clone(&log);
    
    // Spawn writer thread
    let writer_handle = thread::spawn(move || {
        for i in 101..=200 {
            let entry = LogEntry::new(2, i, format!("concurrent_{}", i).into_bytes());
            log_clone.append(entry).unwrap();
            thread::sleep(Duration::from_millis(1)); // Small delay
        }
    });

    // Spawn multiple reader threads
    let mut reader_handles = vec![];
    for thread_id in 0..5 {
        let log_clone = Arc::clone(&log);
        let handle = thread::spawn(move || {
            for _ in 0..50 {
                // Read random entries from the initial set
                let index = (rand::random::<u64>() % 100) + 1;
                if let Ok(entry) = log_clone.get_entry(index) {
                    assert_eq!(entry.index(), index);
                    assert!(entry.command().starts_with(b"initial_"));
                }
                
                // Check metadata
                let metadata = log_clone.metadata();
                assert!(metadata.total_entries >= 100);
                
                thread::sleep(Duration::from_millis(2));
            }
            thread_id // Return thread ID for verification
        });
        reader_handles.push(handle);
    }

    // Wait for all threads to complete
    writer_handle.join().unwrap();
    for handle in reader_handles {
        let thread_id = handle.join().unwrap();
        assert!(thread_id < 5); // Verify thread completed successfully
    }

    // Verify final state
    assert_eq!(log.last_index(), Some(200));
    let metadata = log.metadata();
    assert_eq!(metadata.total_entries, 200);
}

#[test]
fn test_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    
    // Phase 1: Create log and add data
    {
        let config = RaftLogConfig {
            max_entries_per_segment: 50,
            data_dir: temp_dir.path().to_path_buf(),
        };
        let log = RaftLog::with_config(config).unwrap();

        // Add entries with different terms
        for i in 1..=150 {
            let term = (i - 1) / 50 + 1; // Terms 1, 2, 3
            let entry = LogEntry::new(term, i, format!("persistent_data_{}", i).into_bytes());
            log.append(entry).unwrap();
        }
        
        log.flush().unwrap();
        log.validate().unwrap();
    }

    // Phase 2: Restart and verify data
    {
        let config = RaftLogConfig {
            max_entries_per_segment: 50,
            data_dir: temp_dir.path().to_path_buf(),
        };
        let log = RaftLog::with_config(config).unwrap();

        // Verify all data is intact
        assert_eq!(log.last_index(), Some(150));
        assert_eq!(log.last_term().unwrap(), Some(3));
        assert_eq!(log.segment_count(), 3);

        // Verify random entries
        for _ in 0..20 {
            let index = (rand::random::<u64>() % 150) + 1;
            let entry = log.get_entry(index).unwrap();
            assert_eq!(entry.index(), index);
            assert_eq!(
                entry.command(),
                format!("persistent_data_{}", index).as_bytes()
            );
        }

        // Add more data after restart
        for i in 151..=200 {
            let entry = LogEntry::new(4, i, format!("post_restart_{}", i).into_bytes());
            log.append(entry).unwrap();
        }

        assert_eq!(log.last_index(), Some(200));
        assert_eq!(log.last_term().unwrap(), Some(4));
    }

    // Phase 3: Final restart and verification
    {
        let log = RaftLog::new(temp_dir.path()).unwrap();
        assert_eq!(log.last_index(), Some(200));
        
        // Verify data from both phases
        let entry_100 = log.get_entry(100).unwrap();
        assert_eq!(entry_100.command(), b"persistent_data_100");
        
        let entry_175 = log.get_entry(175).unwrap();
        assert_eq!(entry_175.command(), b"post_restart_175");
    }
}

#[test]
fn test_edge_case_empty_operations() {
    let temp_dir = TempDir::new().unwrap();
    let log = RaftLog::new(temp_dir.path()).unwrap();

    // Test operations on empty log
    assert!(log.is_empty());
    assert_eq!(log.first_index(), None);
    assert_eq!(log.last_index(), None);
    assert_eq!(log.last_term().unwrap(), None);
    
    // Test reading from empty log
    assert!(matches!(log.get_entry(1), Err(Error::EntryNotFound { index: 1 })));
    assert_eq!(log.get_entries(1, 10).unwrap(), Vec::new());
    assert_eq!(log.get_entries_with_limit(1, 10).unwrap(), Vec::new());
    
    // Test metadata on empty log
    let metadata = log.metadata();
    assert_eq!(metadata.total_entries, 0);
    assert_eq!(metadata.segment_count, 0);
    
    // Test validation on empty log
    log.validate().unwrap();
    
    // Test flush on empty log
    log.flush().unwrap();
}

#[test]
fn test_edge_case_single_entry() {
    let temp_dir = TempDir::new().unwrap();
    let log = RaftLog::new(temp_dir.path()).unwrap();

    // Add single entry
    let entry = LogEntry::new(1, 1, b"single_entry".to_vec());
    log.append(entry.clone()).unwrap();

    // Test all operations with single entry
    assert!(!log.is_empty());
    assert_eq!(log.first_index(), Some(1));
    assert_eq!(log.last_index(), Some(1));
    assert_eq!(log.last_term().unwrap(), Some(1));
    
    let retrieved = log.get_entry(1).unwrap();
    assert_eq!(retrieved, entry);
    
    let entries = log.get_entries(1, 1).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], entry);
    
    // Test metadata
    let metadata = log.metadata();
    assert_eq!(metadata.total_entries, 1);
    assert_eq!(metadata.segment_count, 1);
    assert_eq!(metadata.first_index, Some(1));
    assert_eq!(metadata.last_index, Some(1));
}

#[test]
fn test_error_handling_invalid_sequences() {
    let temp_dir = TempDir::new().unwrap();
    let log = RaftLog::new(temp_dir.path()).unwrap();

    // Test invalid first index
    let invalid_entry = LogEntry::new(1, 5, b"invalid".to_vec()); // Should be 1
    assert!(matches!(
        log.append(invalid_entry),
        Err(Error::InvalidEntry { .. })
    ));

    // Add valid first entry
    let entry1 = LogEntry::new(1, 1, b"valid_1".to_vec());
    log.append(entry1).unwrap();

    // Test gap in sequence
    let gap_entry = LogEntry::new(1, 3, b"gap".to_vec()); // Should be 2
    assert!(matches!(
        log.append(gap_entry),
        Err(Error::InvalidEntry { .. })
    ));

    // Test duplicate index
    let duplicate_entry = LogEntry::new(1, 1, b"duplicate".to_vec());
    assert!(matches!(
        log.append(duplicate_entry),
        Err(Error::InvalidEntry { .. })
    ));

    // Add valid second entry
    let entry2 = LogEntry::new(1, 2, b"valid_2".to_vec());
    log.append(entry2).unwrap();

    // Verify only valid entries were added
    assert_eq!(log.last_index(), Some(2));
    assert_eq!(log.metadata().total_entries, 2);
}

#[test]
fn test_compaction_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let config = RaftLogConfig {
        max_entries_per_segment: 5,
        data_dir: temp_dir.path().to_path_buf(),
    };
    let log = RaftLog::with_config(config).unwrap();

    // Add entries across multiple segments
    for i in 1..=15 {
        let entry = LogEntry::new(1, i, format!("compact_test_{}", i).into_bytes());
        log.append(entry).unwrap();
    }

    assert_eq!(log.segment_count(), 3); // [1-5], [6-10], [11-15]

    // Test compaction at segment boundary
    log.compact(6).unwrap(); // Should remove first segment
    assert_eq!(log.segment_count(), 2);

    // Verify compacted entries are gone
    assert!(matches!(log.get_entry(1), Err(Error::EntryNotFound { .. })));
    assert!(matches!(log.get_entry(5), Err(Error::EntryNotFound { .. })));

    // Verify remaining entries are accessible
    assert!(log.get_entry(6).is_ok());
    assert!(log.get_entry(15).is_ok());

    // Test compaction beyond all data
    log.compact(20).unwrap();
    assert_eq!(log.segment_count(), 0);

    // Test compaction on empty log
    log.compact(1).unwrap(); // Should not panic
}

#[test]
fn test_batch_operations_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let log = RaftLog::new(temp_dir.path()).unwrap();

    // Test empty batch
    log.append_entries(vec![]).unwrap();
    assert!(log.is_empty());

    // Test single entry batch
    let single_batch = vec![LogEntry::new(1, 1, b"single".to_vec())];
    log.append_entries(single_batch).unwrap();
    assert_eq!(log.last_index(), Some(1));

    // Test large batch
    let large_batch: Vec<_> = (2..=1000)
        .map(|i| LogEntry::new(1, i, format!("batch_{}", i).into_bytes()))
        .collect();
    log.append_entries(large_batch).unwrap();
    assert_eq!(log.last_index(), Some(1000));

    // Test invalid batch (gap in sequence)
    let invalid_batch = vec![
        LogEntry::new(1, 1001, b"valid".to_vec()),
        LogEntry::new(1, 1003, b"invalid_gap".to_vec()), // Gap at 1002
    ];
    assert!(matches!(
        log.append_entries(invalid_batch),
        Err(Error::InvalidEntry { .. })
    ));

    // Log should remain unchanged after failed batch
    assert_eq!(log.last_index(), Some(1000));
}

#[test]
fn test_term_operations_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let log = RaftLog::new(temp_dir.path()).unwrap();

    // Test term operations on empty log
    assert_eq!(log.last_index_for_term(1), None);
    assert!(matches!(log.term_at(1), Err(Error::EntryNotFound { .. })));

    // Add entries with various terms
    let entries = vec![
        LogEntry::new(1, 1, b"term1_a".to_vec()),
        LogEntry::new(1, 2, b"term1_b".to_vec()),
        LogEntry::new(3, 3, b"term3_a".to_vec()), // Skip term 2
        LogEntry::new(3, 4, b"term3_b".to_vec()),
        LogEntry::new(1, 5, b"term1_c".to_vec()), // Term regression
    ];

    for entry in entries {
        log.append(entry).unwrap();
    }

    // Test finding last index for various terms
    assert_eq!(log.last_index_for_term(1), Some(5)); // Last occurrence
    assert_eq!(log.last_index_for_term(2), None);    // Non-existent term
    assert_eq!(log.last_index_for_term(3), Some(4)); // Middle term
    assert_eq!(log.last_index_for_term(4), None);    // Future term

    // Test term_at for all entries
    assert_eq!(log.term_at(1).unwrap(), 1);
    assert_eq!(log.term_at(3).unwrap(), 3);
    assert_eq!(log.term_at(5).unwrap(), 1);
}

#[test]
fn test_size_and_limit_operations() {
    let temp_dir = TempDir::new().unwrap();
    let log = RaftLog::new(temp_dir.path()).unwrap();

    // Add entries with varying sizes
    let entries = vec![
        LogEntry::new(1, 1, b"small".to_vec()),                    // 5 bytes
        LogEntry::new(1, 2, b"medium_sized_entry".to_vec()),       // 18 bytes
        LogEntry::new(1, 3, b"much_larger_entry_with_more_data".to_vec()), // 34 bytes
    ];

    for entry in &entries {
        log.append(entry.clone()).unwrap();
    }

    // Test size calculations
    let total_size = log.entries_size(1, 3).unwrap();
    let expected_size: usize = entries.iter().map(|e| e.serialized_size()).sum();
    assert_eq!(total_size, expected_size);

    // Test partial size
    let partial_size = log.entries_size(2, 2).unwrap();
    assert_eq!(partial_size, entries[1].serialized_size());

    // Test invalid range
    assert_eq!(log.entries_size(5, 3).unwrap(), 0);

    // Test get_entries_with_limit
    let limited = log.get_entries_with_limit(1, 2).unwrap();
    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].index(), 1);
    assert_eq!(limited[1].index(), 2);

    // Test limit larger than available
    let all_limited = log.get_entries_with_limit(1, 10).unwrap();
    assert_eq!(all_limited.len(), 3);

    // Test limit from non-existent index
    let empty_limited = log.get_entries_with_limit(10, 5).unwrap();
    assert_eq!(empty_limited.len(), 0);
}

#[test]
fn test_segment_statistics_edge_cases() {
    let temp_dir = TempDir::new().unwrap();

    // Test empty log statistics
    let log = RaftLog::new(temp_dir.path()).unwrap();
    let stats = log.segment_statistics();
    assert_eq!(stats.segment_count, 0);
    assert_eq!(stats.total_entries, 0);
    assert_eq!(stats.min_entries_per_segment, 0);
    assert_eq!(stats.max_entries_per_segment, 0);
    assert_eq!(stats.avg_entries_per_segment, 0.0);

    // Create uneven segments
    let config = RaftLogConfig {
        max_entries_per_segment: 10,
        data_dir: temp_dir.path().to_path_buf(),
    };
    let log = RaftLog::with_config(config).unwrap();

    // Add 25 entries (segments: 10, 10, 5)
    for i in 1..=25 {
        let entry = LogEntry::new(1, i, format!("stats_test_{}", i).into_bytes());
        log.append(entry).unwrap();
    }

    let stats = log.segment_statistics();
    assert_eq!(stats.segment_count, 3);
    assert_eq!(stats.total_entries, 25);
    assert_eq!(stats.min_entries_per_segment, 5);
    assert_eq!(stats.max_entries_per_segment, 10);
    assert!((stats.avg_entries_per_segment - 25.0/3.0).abs() < 0.001);
}
