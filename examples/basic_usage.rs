//! Basic usage example for the Raft log implementation.
//!
//! This example demonstrates the core functionality of the Raft log,
//! including creating a log, appending entries, reading data, and
//! working with segments.

use raft_log::{LogEntry, RaftLog, RaftLogConfig};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Raft Log Basic Usage Example");
    println!("================================\n");

    // Create a temporary directory for this example
    let data_dir = "./example-raft-data";
    std::fs::create_dir_all(data_dir)?;

    // Clean up any existing data
    if Path::new(data_dir).exists() {
        std::fs::remove_dir_all(data_dir)?;
        std::fs::create_dir_all(data_dir)?;
    }

    // 1. Create a new Raft log with custom configuration
    println!("1. Creating Raft log...");
    let config = RaftLogConfig {
        max_entries_per_segment: 5, // Small segments for demonstration
        data_dir: data_dir.into(),
    };
    let log = RaftLog::with_config(config)?;
    println!("   ✓ Log created successfully\n");

    // 2. Append individual entries
    println!("2. Appending individual entries...");
    for i in 1..=8 {
        let term = if i <= 3 { 1 } else if i <= 6 { 2 } else { 3 };
        let command = format!("SET key{} = value{}", i, i);
        let entry = LogEntry::new(term, i, command.into_bytes());
        
        log.append(entry)?;
        println!("   ✓ Appended entry {} (term {})", i, term);
    }
    println!();

    // 3. Append a batch of entries
    println!("3. Appending batch of entries...");
    let batch_entries = vec![
        LogEntry::new(3, 9, b"DELETE key1".to_vec()),
        LogEntry::new(3, 10, b"UPDATE key2 = new_value".to_vec()),
        LogEntry::new(4, 11, b"CREATE TABLE users".to_vec()),
    ];
    log.append_entries(batch_entries)?;
    println!("   ✓ Appended batch of 3 entries\n");

    // 4. Read individual entries
    println!("4. Reading individual entries...");
    let entry_5 = log.get_entry(5)?;
    println!("   Entry 5: term={}, command={}", 
             entry_5.term(), 
             String::from_utf8_lossy(entry_5.command()));
    
    let entry_10 = log.get_entry(10)?;
    println!("   Entry 10: term={}, command={}", 
             entry_10.term(), 
             String::from_utf8_lossy(entry_10.command()));
    println!();

    // 5. Read ranges of entries
    println!("5. Reading ranges of entries...");
    let entries_3_to_7 = log.get_entries(3, 7)?;
    println!("   Entries 3-7:");
    for entry in entries_3_to_7 {
        println!("     Index {}: {}", 
                 entry.index(), 
                 String::from_utf8_lossy(entry.command()));
    }
    println!();

    // 6. Get entries with limit
    println!("6. Reading with limit...");
    let limited_entries = log.get_entries_with_limit(8, 3)?;
    println!("   Next 3 entries from index 8:");
    for entry in limited_entries {
        println!("     Index {}: {}", 
                 entry.index(), 
                 String::from_utf8_lossy(entry.command()));
    }
    println!();

    // 7. Examine log metadata
    println!("7. Log metadata...");
    let metadata = log.metadata();
    println!("   First index: {:?}", metadata.first_index);
    println!("   Last index: {:?}", metadata.last_index);
    println!("   Last term: {:?}", metadata.last_term);
    println!("   Total entries: {}", metadata.total_entries);
    println!("   Segment count: {}", metadata.segment_count);
    println!("   Append offset: {:?}", metadata.append_offset);
    println!();

    // 8. Examine segment statistics
    println!("8. Segment statistics...");
    let stats = log.segment_statistics();
    println!("   Segments: {}", stats.segment_count);
    println!("   Total entries: {}", stats.total_entries);
    println!("   Min entries per segment: {}", stats.min_entries_per_segment);
    println!("   Max entries per segment: {}", stats.max_entries_per_segment);
    println!("   Avg entries per segment: {:.2}", stats.avg_entries_per_segment);
    println!();

    // 9. Term-based operations
    println!("9. Term-based operations...");
    println!("   Last index for term 1: {:?}", log.last_index_for_term(1));
    println!("   Last index for term 2: {:?}", log.last_index_for_term(2));
    println!("   Last index for term 3: {:?}", log.last_index_for_term(3));
    println!("   Term at index 5: {}", log.term_at(5)?);
    println!("   Term at index 9: {}", log.term_at(9)?);
    println!();

    // 10. Size calculations
    println!("10. Size calculations...");
    let size_1_to_5 = log.entries_size(1, 5)?;
    let size_6_to_11 = log.entries_size(6, 11)?;
    println!("   Size of entries 1-5: {} bytes", size_1_to_5);
    println!("   Size of entries 6-11: {} bytes", size_6_to_11);
    println!();

    // 11. Segment information
    println!("11. Segment information...");
    let segment_info = log.segment_info();
    for (i, (base_index, entry_count, last_index)) in segment_info.iter().enumerate() {
        println!("   Segment {}: base_index={}, entries={}, last_index={:?}", 
                 i + 1, base_index, entry_count, last_index);
    }
    println!();

    // 12. Validation and flush
    println!("12. Validation and persistence...");
    log.validate()?;
    println!("   ✓ Log validation passed");
    
    log.flush()?;
    println!("   ✓ All data flushed to disk");
    println!();

    // 13. Demonstrate persistence by creating a new log instance
    println!("13. Testing persistence...");
    drop(log); // Close the current log
    
    let new_log = RaftLog::new(data_dir)?;
    println!("   ✓ Reopened log from disk");
    println!("   Last index after restart: {:?}", new_log.last_index());
    println!("   Total entries after restart: {}", new_log.metadata().total_entries);
    
    // Verify data integrity
    let entry_after_restart = new_log.get_entry(7)?;
    println!("   Entry 7 after restart: {}", 
             String::from_utf8_lossy(entry_after_restart.command()));
    println!();

    // 14. Compaction example
    println!("14. Log compaction...");
    println!("   Segments before compaction: {}", new_log.segment_count());
    new_log.compact(6)?; // Remove entries before index 6
    println!("   Segments after compaction: {}", new_log.segment_count());
    
    // Verify compacted entries are gone
    match new_log.get_entry(3) {
        Ok(_) => println!("   ❌ Entry 3 should have been compacted"),
        Err(_) => println!("   ✓ Entry 3 successfully compacted"),
    }
    
    // Verify remaining entries are still accessible
    let entry_8 = new_log.get_entry(8)?;
    println!("   ✓ Entry 8 still accessible: {}", 
             String::from_utf8_lossy(entry_8.command()));
    println!();

    println!("🎉 Example completed successfully!");
    println!("\nThe Raft log implementation provides:");
    println!("  • Thread-safe concurrent operations");
    println!("  • Automatic segment management and rotation");
    println!("  • Comprehensive validation and error handling");
    println!("  • Efficient binary storage format");
    println!("  • Rich metadata and statistics");
    println!("  • Persistent storage with crash recovery");

    // Clean up
    std::fs::remove_dir_all(data_dir)?;
    println!("\n🧹 Cleaned up example data directory");

    Ok(())
}
