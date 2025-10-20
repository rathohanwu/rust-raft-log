use super::log_file_segment::LogFileSegment;
use super::models::{AppendResult, LogEntry, RaftLogConfig, RaftLogError};
use super::utils::create_memory_mapped_file;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

/// RaftLog manages a collection of log file segments for the Raft consensus algorithm.
/// It provides methods to append, retrieve, and truncate log entries across multiple segments.
pub struct RaftLog {
    /// Configuration for the RaftLog
    config: RaftLogConfig,
    /// Map of base_index -> LogFileSegment for efficient segment lookup
    segments: BTreeMap<u64, LogFileSegment>,
    /// The next index to be assigned to a new log entry
    next_index: u64,
}

impl RaftLog {
    /// Creates a new RaftLog with the given configuration
    pub fn new(config: RaftLogConfig) -> Result<Self, RaftLogError> {
        // Create log directory if it doesn't exist
        if !config.log_directory.exists() {
            fs::create_dir_all(&config.log_directory).map_err(|e| {
                RaftLogError::DirectoryError(format!(
                    "Failed to create log directory {:?}: {}",
                    config.log_directory, e
                ))
            })?;
        }

        let mut raft_log = RaftLog {
            config,
            segments: BTreeMap::new(),
            next_index: 1,
        };

        // Load existing segments
        raft_log.load_existing_segments()?;

        Ok(raft_log)
    }

    /// Generates a segment file name based on a sequential number
    fn generate_segment_filename(&self) -> String {
        // Find the highest numbered segment file and increment
        let mut max_number = 0;
        if let Ok(entries) = fs::read_dir(&self.config.log_directory) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();
                if file_name_str.starts_with("log-segment-") && file_name_str.ends_with(".dat") {
                    if let Some(number_part) = file_name_str
                        .strip_prefix("log-segment-")
                        .and_then(|s| s.strip_suffix(".dat"))
                    {
                        if let Ok(number) = number_part.parse::<u64>() {
                            max_number = max_number.max(number);
                        }
                    }
                }
            }
        }
        format!("log-segment-{:010}.dat", max_number + 1)
    }

    /// Gets the full path for a new segment file
    fn get_new_segment_path(&self) -> std::path::PathBuf {
        self.config
            .log_directory
            .join(self.generate_segment_filename())
    }

    /// Creates a new segment with the given base index
    fn create_new_segment(&mut self, base_index: u64) -> Result<(), RaftLogError> {
        let segment_path = self.get_new_segment_path();
        let memory_map =
            create_memory_mapped_file(segment_path.to_str().unwrap(), self.config.segment_size)
                .map_err(|e| {
                    RaftLogError::SegmentFileError(format!(
                        "Failed to create segment file {:?}: {}",
                        segment_path, e
                    ))
                })?;

        let segment = LogFileSegment::new(memory_map, base_index);
        self.segments.insert(base_index, segment);
        Ok(())
    }

    /// Gets the segment that contains the given index
    fn get_segment_for_index(&self, index: u64) -> Option<&LogFileSegment> {
        // Find the segment with the largest base_index <= index
        self.segments
            .range(..=index)
            .next_back()
            .map(|(_, segment)| segment)
    }

    /// Gets the mutable segment that contains the given index
    fn get_segment_for_index_mut(&mut self, index: u64) -> Option<&mut LogFileSegment> {
        // Find the segment with the largest base_index <= index
        self.segments
            .range_mut(..=index)
            .next_back()
            .map(|(_, segment)| segment)
    }

    /// Gets the last (most recent) segment
    fn get_last_segment(&self) -> Option<&LogFileSegment> {
        self.segments.values().last()
    }

    /// Gets the last (most recent) segment mutably
    fn get_last_segment_mut(&mut self) -> Option<&mut LogFileSegment> {
        self.segments.values_mut().last()
    }

    /// Loads existing segment files from the log directory
    /// Assumes only .dat files exist in the directory (metadata will be in separate folder)
    fn load_existing_segments(&mut self) -> Result<(), RaftLogError> {
        let entries = fs::read_dir(&self.config.log_directory).map_err(|e| {
            RaftLogError::DirectoryError(format!(
                "Failed to read log directory {:?}: {}",
                self.config.log_directory, e
            ))
        })?;

        let mut segment_files = Vec::new();

        // Collect all .dat files (assuming they are all segment files)
        for entry in entries {
            let entry = entry.map_err(|e| {
                RaftLogError::DirectoryError(format!("Failed to read directory entry: {}", e))
            })?;

            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Only process .dat files
            if file_name_str.ends_with(".dat") {
                segment_files.push(entry.path());
            }
        }

        // Load each segment and get base index from file header
        let mut segments_with_base_index = Vec::new();
        for path in segment_files {
            if let Ok((base_index, segment)) = self.load_segment_file(&path) {
                segments_with_base_index.push((base_index, segment));
            }
        }

        // Sort by base index
        segments_with_base_index.sort_by_key(|(base_index, _)| *base_index);

        // Insert segments into the map
        for (base_index, segment) in segments_with_base_index {
            self.segments.insert(base_index, segment);
        }

        // Update next_index based on loaded segments
        self.update_next_index();

        // If no segments exist, create the first one
        if self.segments.is_empty() {
            self.create_new_segment(1)?;
        }

        Ok(())
    }

    /// Loads a single segment file and returns the base index and segment
    fn load_segment_file(&self, path: &Path) -> Result<(u64, LogFileSegment), RaftLogError> {
        // Use configured segment size for existing files too
        let memory_map = create_memory_mapped_file(
            path.to_str().unwrap(),
            self.config.segment_size,
        )
        .map_err(|e| {
            RaftLogError::SegmentFileError(format!("Failed to open segment file {:?}: {}", path, e))
        })?;

        // Don't call new() which initializes header - the file already has a header
        let segment = LogFileSegment::from_existing(memory_map);

        // Read the base index from the segment header
        let base_index = segment.get_base_index();

        Ok((base_index, segment))
    }

    /// Updates the next_index based on the last entry in the log
    fn update_next_index(&mut self) {
        if let Some(last_segment) = self.get_last_segment() {
            if let Some(last_index) = last_segment.get_last_index() {
                self.next_index = last_index + 1;
            } else {
                self.next_index = last_segment.get_base_index();
            }
        }
    }

    /// Appends a single log entry to the log
    pub fn append_entry(&mut self, mut log_entry: LogEntry) -> Result<(), RaftLogError> {
        // Set the index for the entry
        log_entry.index = self.next_index;

        // Get the last segment (guaranteed to exist since new() creates one if empty)
        let last_segment = self
            .get_last_segment_mut()
            .expect("No segments exist - this should never happen");

        match last_segment.append_entry(log_entry.clone()) {
            AppendResult::Success => {
                self.next_index += 1;
                Ok(())
            }
            AppendResult::RotationNeeded => {
                // Create new segment and recursively call append_entry
                self.create_new_segment(self.next_index)?;
                self.append_entry(LogEntry::new(log_entry.term, 0, log_entry.payload))
            }
        }
    }

    /// Appends multiple log entries to the log
    pub fn append_entries(&mut self, log_entries: Vec<LogEntry>) -> Result<(), RaftLogError> {
        for log_entry in log_entries {
            self.append_entry(log_entry)?;
        }
        Ok(())
    }

    /// Gets a single log entry by index
    pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
        if index == 0 {
            return None;
        }

        if let Some(segment) = self.get_segment_for_index(index) {
            segment.get_entry_at(index)
        } else {
            None
        }
    }

    /// Gets multiple log entries from start_index to end_index (inclusive)
    /// Returns at most max_entries_per_query entries
    pub fn get_entries(&self, start_index: u64, end_index: u64) -> Option<Vec<LogEntry>> {
        if start_index == 0 || end_index == 0 || start_index > end_index {
            return None;
        }

        let requested_count = (end_index - start_index + 1) as usize;
        if requested_count > self.config.max_entries_per_query {
            return None;
        }

        let mut entries = Vec::new();
        let mut current_index = start_index;

        while current_index <= end_index && entries.len() < self.config.max_entries_per_query {
            if let Some(entry) = self.get_entry(current_index) {
                entries.push(entry);
                current_index += 1;
            } else {
                // Entry not found, might be beyond the log
                break;
            }
        }

        if entries.is_empty() {
            None
        } else {
            Some(entries)
        }
    }

    /// Gets the last log entry in the log
    pub fn get_last_log_entry(&self) -> Option<LogEntry> {
        self.last_index().and_then(|last_index| self.get_entry(last_index))
    }

    /// Truncates the log from the given index (inclusive)
    /// Removes all entries from the given index onwards and deletes empty segments
    pub fn truncate_from(&mut self, from_index: u64) -> Result<bool, RaftLogError> {
        if from_index == 0 {
            return Err(RaftLogError::InvalidIndex(from_index));
        }

        let mut segments_to_remove = Vec::new();
        let mut truncated = false;

        // Find all segments that need to be truncated or removed
        for (&base_index, segment) in self.segments.iter_mut() {
            let segment_last_index = segment.get_last_index();

            if let Some(last_index) = segment_last_index {
                if from_index <= last_index {
                    // This segment contains entries that need to be truncated
                    if from_index <= base_index {
                        // The entire segment should be removed
                        segments_to_remove.push(base_index);
                    } else {
                        // Truncate within this segment
                        if segment.truncate_from(from_index) {
                            truncated = true;
                            // Check if segment is now empty
                            if segment.get_entry_count() == 0 {
                                segments_to_remove.push(base_index);
                            }
                        }
                    }
                }
            } else if from_index <= base_index {
                // Empty segment that should be removed
                segments_to_remove.push(base_index);
            }
        }

        // Remove empty segments from memory
        // Note: File cleanup is handled separately since we don't track file paths by base_index
        for base_index in segments_to_remove {
            self.segments.remove(&base_index);
            truncated = true;
        }

        // Update next_index
        self.update_next_index();

        // If all segments were removed, create a new one
        if self.segments.is_empty() {
            self.create_new_segment(1)?;
        }

        Ok(truncated)
    }

    /// Gets the total number of entries in the log
    pub fn len(&self) -> u64 {
        self.segments.values().map(|s| s.get_entry_count()).sum()
    }

    /// Checks if the log is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Gets the index of the first entry in the log
    pub fn first_index(&self) -> Option<u64> {
        self.segments.keys().next().copied()
    }

    /// Gets the index of the last entry in the log
    pub fn last_index(&self) -> Option<u64> {
        self.get_last_segment()?.get_last_index()
    }

    /// Gets the number of segments
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        LogEntry::new(term, 0, payload.as_bytes().to_vec()) // index will be set by append_entry
    }

    /// Creates a test config that uses a local directory for file inspection.
    /// Use this when you want to examine the generated segment files after the test.
    /// Files will be created in a unique "./raft_logs_<test_name>" directory.
    fn create_inspectable_test_config(test_name: &str) -> RaftLogConfig {
        let log_dir = std::path::PathBuf::from(format!("./raft_logs_{}", test_name));
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
    fn test_segment_rotation() {
        let config = create_inspectable_test_config("segment_rotation");
        let mut raft_log = RaftLog::new(config).expect("Failed to create RaftLog");

        let initial_segments = raft_log.segment_count();
        println!("Initial segments: {}", initial_segments);
        println!("Log directory: {:?}", raft_log.config.log_directory);

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
        println!("Final segments: {}", final_segments);
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

        // Verify entries span multiple segments
        println!(
            "Successfully verified {} entries across {} segments",
            50, final_segments
        );
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
        let mut raft_log_small =
            RaftLog::new(config_small_limit).expect("Failed to create RaftLog");

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

        println!("=== Comprehensive Cross-Segment Test ===");
        println!("Log directory: {:?}", raft_log.config.log_directory);

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

                if entry_count % 25 == 0 {
                    println!(
                        "Added {} entries, segments: {}",
                        entry_count,
                        raft_log.segment_count()
                    );
                }
            }
        }

        let total_entries = entry_count;
        let total_segments = raft_log.segment_count();
        println!(
            "Total entries: {}, Total segments: {}",
            total_entries, total_segments
        );
        assert!(total_segments > 1, "Should have multiple segments");

        // Phase 2: Test individual entry retrieval across all segments
        println!("Testing individual entry retrieval...");
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
        println!("Testing cross-segment range queries...");

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
        println!("Testing truncation across segments...");
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
        println!("Testing append after truncation...");
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

        println!("=== Cross-segment test completed successfully ===");
        println!(
            "Final state: {} entries across {} segments",
            raft_log.len(),
            raft_log.segment_count()
        );
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

        println!(
            "Files will be created in: {:?}",
            raft_log.config.log_directory
        );

        // Add some entries
        for i in 1..=3 {
            let entry = create_test_entry(1, &format!("inspectable entry {}", i));
            raft_log
                .append_entry(entry)
                .expect("Failed to append entry");
        }

        assert_eq!(raft_log.len(), 3);
        println!("Check ./raft_logs/ directory to inspect the segment files");
        // Files remain in ./raft_logs/ for inspection
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
            raft_log.append_entry(entry).expect("Failed to append entry");
        }

        // Now entries exist - clean Option API
        if let Some(entry) = raft_log.get_entry(3) {
            assert_eq!(entry.index, 3);
            println!("Found entry 3: {:?}", String::from_utf8(entry.payload).unwrap());
        }

        if let Some(last_entry) = raft_log.get_last_log_entry() {
            assert_eq!(last_entry.index, 5);
            println!("Last entry: {:?}", String::from_utf8(last_entry.payload).unwrap());
        }

        if let Some(entries) = raft_log.get_entries(2, 4) {
            assert_eq!(entries.len(), 3);
            println!("Retrieved {} entries from range 2-4", entries.len());
        }

        // Invalid requests return None (no exceptions!)
        assert!(raft_log.get_entry(0).is_none());        // Invalid index
        assert!(raft_log.get_entry(100).is_none());      // Non-existent
        assert!(raft_log.get_entries(5, 3).is_none());   // Invalid range
    }
}
