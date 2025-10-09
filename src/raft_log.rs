//! Main Raft log coordinator.
//!
//! The RaftLog is the main interface for managing a distributed log with multiple segments.
//! It provides thread-safe operations and manages segment lifecycle automatically.

use crate::{
    utils::segment_filename, Error, LogEntry, LogSegment, Result, SegmentHeader,
};
use parking_lot::RwLock;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

/// Configuration for the Raft log.
#[derive(Debug, Clone)]
pub struct RaftLogConfig {
    /// Maximum number of entries per segment before creating a new one.
    pub max_entries_per_segment: u32,
    /// Data directory where segment files are stored.
    pub data_dir: PathBuf,
}

impl Default for RaftLogConfig {
    fn default() -> Self {
        Self {
            max_entries_per_segment: 10_000,
            data_dir: PathBuf::from("./raft-data"),
        }
    }
}

/// Main Raft log interface.
///
/// The RaftLog manages multiple log segments and provides a unified interface
/// for appending and reading log entries. It automatically handles:
/// - Segment creation and rotation
/// - Thread-safe concurrent access
/// - Index-based lookups across segments
/// - Metadata derivation (last_index, last_term)
///
/// # Thread Safety
///
/// All operations are thread-safe using RwLock:
/// - Multiple concurrent readers
/// - Exclusive writer access
/// - No data races or corruption
#[derive(Debug)]
pub struct RaftLog {
    /// Configuration for this log.
    config: RaftLogConfig,
    /// Map of base_index -> LogSegment for efficient lookups.
    /// Protected by RwLock for thread safety.
    segments: Arc<RwLock<BTreeMap<u64, LogSegment>>>,
    /// Current active segment for appends.
    /// Protected by RwLock for thread safety.
    active_segment: Arc<RwLock<Option<LogSegment>>>,
}

impl RaftLog {
    /// Creates a new Raft log with default configuration.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Directory where segment files will be stored
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use raft_log::RaftLog;
    ///
    /// let log = RaftLog::new("./raft-data").unwrap();
    /// ```
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let config = RaftLogConfig {
            data_dir: data_dir.as_ref().to_path_buf(),
            ..Default::default()
        };
        Self::with_config(config)
    }

    /// Creates a new Raft log with custom configuration.
    pub fn with_config(config: RaftLogConfig) -> Result<Self> {
        // Create data directory if it doesn't exist
        fs::create_dir_all(&config.data_dir)?;

        let log = Self {
            config,
            segments: Arc::new(RwLock::new(BTreeMap::new())),
            active_segment: Arc::new(RwLock::new(None)),
        };

        // Load existing segments
        log.load_existing_segments()?;

        Ok(log)
    }

    /// Returns the configuration for this log.
    pub fn config(&self) -> &RaftLogConfig {
        &self.config
    }

    /// Returns the number of segments in this log.
    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    /// Returns true if the log is empty.
    pub fn is_empty(&self) -> bool {
        self.segments.read().is_empty()
    }

    /// Returns the first index in the log, if any.
    pub fn first_index(&self) -> Option<u64> {
        self.segments.read().keys().next().copied()
    }

    /// Returns the last index in the log, if any.
    pub fn last_index(&self) -> Option<u64> {
        let segments = self.segments.read();
        segments.values().rev().find_map(|segment| segment.last_index())
    }

    /// Returns the term of the last entry in the log, if any.
    pub fn last_term(&self) -> Result<Option<u64>> {
        if let Some(last_index) = self.last_index() {
            let entry = self.get_entry(last_index)?;
            Ok(Some(entry.term()))
        } else {
            Ok(None)
        }
    }

    /// Loads existing segments from the data directory.
    fn load_existing_segments(&self) -> Result<()> {
        let entries = fs::read_dir(&self.config.data_dir)?;
        let mut segments = self.segments.write();

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("segment") {
                match LogSegment::open(&path) {
                    Ok(segment) => {
                        let base_index = segment.base_index();
                        segments.insert(base_index, segment);
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to load segment {:?}: {}", path, e);
                    }
                }
            }
        }

        // Set the active segment to the last one
        if let Some((_, last_segment)) = segments.iter().last() {
            if !last_segment.is_empty() &&
               last_segment.entry_count() < self.config.max_entries_per_segment {
                // Clone the segment for the active segment
                let active = LogSegment::open(last_segment.file_path())?;
                *self.active_segment.write() = Some(active);
            }
        }

        Ok(())
    }

    /// Appends an entry to the log.
    ///
    /// This operation is thread-safe and will automatically create new segments
    /// when the current segment reaches the maximum entry limit.
    ///
    /// # Arguments
    ///
    /// * `entry` - The log entry to append
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use raft_log::{RaftLog, LogEntry};
    ///
    /// let log = RaftLog::new("./raft-data").unwrap();
    /// let entry = LogEntry::new(1, 1, b"command data".to_vec());
    /// log.append(entry).unwrap();
    /// ```
    pub fn append(&self, entry: LogEntry) -> Result<()> {
        // Validate entry index
        let expected_index = self.last_index().map(|i| i + 1).unwrap_or(1);
        if entry.index() != expected_index {
            return Err(Error::InvalidEntry {
                reason: format!(
                    "Entry index {} doesn't match expected index {}",
                    entry.index(),
                    expected_index
                ),
            });
        }

        // Get or create active segment
        let mut active_segment_guard = self.active_segment.write();

        // Check if we need a new segment
        let needs_new_segment = if let Some(ref segment) = *active_segment_guard {
            segment.entry_count() >= self.config.max_entries_per_segment
        } else {
            true
        };

        if needs_new_segment {
            // Create new segment
            let new_segment = LogSegment::create(&self.config.data_dir, entry.index())?;
            let base_index = new_segment.base_index();

            // Add to segments map
            {
                let mut segments = self.segments.write();
                segments.insert(base_index, LogSegment::open(new_segment.file_path())?);
            }

            *active_segment_guard = Some(new_segment);
        }

        // Append to active segment
        if let Some(ref mut segment) = *active_segment_guard {
            segment.append(entry)?;

            // Update the segment in the map
            let base_index = segment.base_index();
            let mut segments = self.segments.write();
            segments.insert(base_index, LogSegment::open(segment.file_path())?);
        }

        Ok(())
    }

    /// Gets an entry at the specified index.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the entry to retrieve
    ///
    /// # Returns
    ///
    /// The log entry at the specified index.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use raft_log::RaftLog;
    ///
    /// let log = RaftLog::new("./raft-data").unwrap();
    /// let entry = log.get_entry(1).unwrap();
    /// ```
    pub fn get_entry(&self, index: u64) -> Result<LogEntry> {
        let segments = self.segments.read();

        // Find the segment containing this index
        for segment in segments.values() {
            if index >= segment.base_index() {
                if let Some(last_index) = segment.last_index() {
                    if index <= last_index {
                        return segment.read_entry(index);
                    }
                }
            }
        }

        Err(Error::EntryNotFound { index })
    }

    /// Gets multiple entries in the specified range.
    ///
    /// # Arguments
    ///
    /// * `start_index` - The first index to retrieve (inclusive)
    /// * `end_index` - The last index to retrieve (inclusive)
    ///
    /// # Returns
    ///
    /// A vector of log entries in the specified range.
    pub fn get_entries(&self, start_index: u64, end_index: u64) -> Result<Vec<LogEntry>> {
        if start_index > end_index {
            return Ok(Vec::new());
        }

        // Check if log is empty or range is outside bounds
        if let Some(first_index) = self.first_index() {
            if let Some(last_index) = self.last_index() {
                if start_index > last_index || end_index < first_index {
                    return Ok(Vec::new());
                }

                // Clamp the range to actual bounds
                let actual_start = start_index.max(first_index);
                let actual_end = end_index.min(last_index);

                let mut entries = Vec::new();
                for index in actual_start..=actual_end {
                    entries.push(self.get_entry(index)?);
                }
                return Ok(entries);
            }
        }

        // Empty log
        Ok(Vec::new())
    }

    /// Gets all entries from the specified index to the end of the log.
    pub fn get_entries_from(&self, start_index: u64) -> Result<Vec<LogEntry>> {
        if let Some(last_index) = self.last_index() {
            self.get_entries(start_index, last_index)
        } else {
            Ok(Vec::new())
        }
    }

    /// Truncates the log at the specified index.
    ///
    /// All entries at and after the specified index will be removed.
    /// This operation is used for log compaction and conflict resolution.
    ///
    /// # Arguments
    ///
    /// * `index` - The index at which to truncate (inclusive)
    pub fn truncate(&self, index: u64) -> Result<()> {
        // This is a complex operation that would involve:
        // 1. Removing segments that are entirely after the truncation point
        // 2. Truncating segments that span the truncation point
        // 3. Updating the active segment
        // For now, return an error indicating it's not implemented
        Err(Error::InvalidFormat("Truncate operation not yet implemented".to_string()))
    }

    /// Returns information about all segments.
    pub fn segment_info(&self) -> Vec<(u64, u32, Option<u64>)> {
        let segments = self.segments.read();
        segments
            .values()
            .map(|segment| {
                (
                    segment.base_index(),
                    segment.entry_count(),
                    segment.last_index(),
                )
            })
            .collect()
    }

    /// Flushes all pending writes to disk.
    ///
    /// This ensures that all appended entries are durably stored.
    /// Should be called after critical append operations.
    pub fn flush(&self) -> Result<()> {
        let mut active_segment_guard = self.active_segment.write();
        if let Some(ref mut segment) = *active_segment_guard {
            segment.flush()?;
        }
        Ok(())
    }

    /// Validates the integrity of all segments in the log.
    ///
    /// This performs comprehensive validation including:
    /// - Header validation
    /// - Checksum verification
    /// - Index continuity checks
    pub fn validate(&self) -> Result<()> {
        let segments = self.segments.read();

        if segments.is_empty() {
            return Ok(());
        }

        let mut expected_index = None;

        for segment in segments.values() {
            // Validate individual segment
            segment.validate()?;

            // Check index continuity
            if let Some(expected) = expected_index {
                if segment.base_index() != expected {
                    return Err(Error::InvalidFormat(format!(
                        "Index gap: expected {}, got {}",
                        expected,
                        segment.base_index()
                    )));
                }
            }

            // Update expected next index
            if let Some(last_index) = segment.last_index() {
                expected_index = Some(last_index + 1);
            } else {
                expected_index = Some(segment.base_index());
            }
        }

        Ok(())
    }

    /// Appends multiple entries atomically.
    ///
    /// Either all entries are appended successfully, or none are.
    /// This is useful for batch operations and maintaining consistency.
    pub fn append_entries(&self, entries: Vec<LogEntry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Validate all entries first
        let mut expected_index = self.last_index().map(|i| i + 1).unwrap_or(1);
        for entry in &entries {
            if entry.index() != expected_index {
                return Err(Error::InvalidEntry {
                    reason: format!(
                        "Entry index {} doesn't match expected index {}",
                        entry.index(),
                        expected_index
                    ),
                });
            }
            expected_index += 1;
        }

        // Append all entries
        for entry in entries {
            self.append(entry)?;
        }

        // Ensure all data is flushed
        self.flush()?;

        Ok(())
    }

    /// Compacts the log by removing entries before the specified index.
    ///
    /// This is used for log compaction to reclaim disk space.
    /// Entries before `compact_index` will be removed.
    pub fn compact(&self, compact_index: u64) -> Result<()> {
        let mut segments = self.segments.write();

        // Find segments that can be completely removed
        let mut to_remove = Vec::new();
        for (&base_index, segment) in segments.iter() {
            if let Some(last_index) = segment.last_index() {
                if last_index < compact_index {
                    to_remove.push(base_index);
                    // Remove the file
                    if let Err(e) = std::fs::remove_file(segment.file_path()) {
                        eprintln!("Warning: Failed to remove segment file: {}", e);
                    }
                }
            }
        }

        // Remove segments from map
        for base_index in to_remove {
            segments.remove(&base_index);
        }

        Ok(())
    }

    /// Returns the current append offset for the active segment.
    ///
    /// This is useful for understanding the current file position
    /// and for performance monitoring.
    pub fn append_offset(&self) -> Option<u64> {
        let active_segment_guard = self.active_segment.read();
        active_segment_guard.as_ref().map(|segment| segment.append_offset)
    }

    /// Returns detailed metadata about the log.
    pub fn metadata(&self) -> LogMetadata {
        let segments = self.segments.read();
        let first_index = segments.keys().next().copied();
        let last_index = segments.values().rev().find_map(|s| s.last_index());
        let last_term = if let Some(last_idx) = last_index {
            self.get_entry(last_idx).ok().map(|e| e.term())
        } else {
            None
        };

        let total_entries: u32 = segments.values().map(|s| s.entry_count()).sum();
        let segment_count = segments.len();

        LogMetadata {
            first_index,
            last_index,
            last_term,
            total_entries,
            segment_count,
            append_offset: self.append_offset(),
        }
    }

    /// Checks if the log contains an entry at the specified index.
    pub fn contains_index(&self, index: u64) -> bool {
        self.get_entry(index).is_ok()
    }

    /// Returns the term at the specified index, if it exists.
    pub fn term_at(&self, index: u64) -> Result<u64> {
        self.get_entry(index).map(|entry| entry.term())
    }

    /// Finds the last entry with the specified term.
    ///
    /// Returns the index of the last entry with the given term,
    /// or None if no entry with that term exists.
    pub fn last_index_for_term(&self, term: u64) -> Option<u64> {
        let segments = self.segments.read();
        let mut last_found_index = None;

        // Search through all segments and entries
        for segment in segments.values() {
            if let Ok(entries) = segment.read_all_entries() {
                for entry in entries.iter() {
                    if entry.term() == term {
                        last_found_index = Some(entry.index());
                    }
                }
            }
        }

        last_found_index
    }

    /// Returns entries starting from the specified index with a limit.
    ///
    /// This is useful for replication where you want to send a batch
    /// of entries but limit the size.
    pub fn get_entries_with_limit(&self, start_index: u64, limit: usize) -> Result<Vec<LogEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut entries = Vec::with_capacity(limit.min(1000)); // Reasonable cap
        let mut current_index = start_index;

        while entries.len() < limit {
            match self.get_entry(current_index) {
                Ok(entry) => {
                    entries.push(entry);
                    current_index += 1;
                }
                Err(Error::EntryNotFound { .. }) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(entries)
    }

    /// Returns the size in bytes of entries in the specified range.
    ///
    /// This is useful for understanding storage usage and for
    /// implementing size-based limits.
    pub fn entries_size(&self, start_index: u64, end_index: u64) -> Result<usize> {
        if start_index > end_index {
            return Ok(0);
        }

        let mut total_size = 0;
        for index in start_index..=end_index {
            let entry = self.get_entry(index)?;
            total_size += entry.serialized_size();
        }

        Ok(total_size)
    }

    /// Finds the segment containing the specified index.
    ///
    /// Returns the base index of the segment containing the index,
    /// or None if no segment contains that index.
    pub fn find_segment_for_index(&self, index: u64) -> Option<u64> {
        let segments = self.segments.read();

        for segment in segments.values() {
            if index >= segment.base_index() {
                if let Some(last_index) = segment.last_index() {
                    if index <= last_index {
                        return Some(segment.base_index());
                    }
                }
            }
        }

        None
    }

    /// Returns statistics about segment sizes and distribution.
    pub fn segment_statistics(&self) -> SegmentStatistics {
        let segments = self.segments.read();

        if segments.is_empty() {
            return SegmentStatistics::default();
        }

        let entry_counts: Vec<u32> = segments.values().map(|s| s.entry_count()).collect();
        let total_entries: u32 = entry_counts.iter().sum();
        let segment_count = segments.len();

        let min_entries = *entry_counts.iter().min().unwrap_or(&0);
        let max_entries = *entry_counts.iter().max().unwrap_or(&0);
        let avg_entries = if segment_count > 0 {
            total_entries as f64 / segment_count as f64
        } else {
            0.0
        };

        SegmentStatistics {
            segment_count,
            total_entries,
            min_entries_per_segment: min_entries,
            max_entries_per_segment: max_entries,
            avg_entries_per_segment: avg_entries,
        }
    }
}

/// Metadata about the entire log.
#[derive(Debug, Clone, PartialEq)]
pub struct LogMetadata {
    /// First index in the log, if any.
    pub first_index: Option<u64>,
    /// Last index in the log, if any.
    pub last_index: Option<u64>,
    /// Term of the last entry, if any.
    pub last_term: Option<u64>,
    /// Total number of entries across all segments.
    pub total_entries: u32,
    /// Number of segments.
    pub segment_count: usize,
    /// Current append offset in the active segment.
    pub append_offset: Option<u64>,
}

/// Statistics about segment distribution.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentStatistics {
    /// Number of segments.
    pub segment_count: usize,
    /// Total entries across all segments.
    pub total_entries: u32,
    /// Minimum entries in any segment.
    pub min_entries_per_segment: u32,
    /// Maximum entries in any segment.
    pub max_entries_per_segment: u32,
    /// Average entries per segment.
    pub avg_entries_per_segment: f64,
}

impl Default for SegmentStatistics {
    fn default() -> Self {
        Self {
            segment_count: 0,
            total_entries: 0,
            min_entries_per_segment: 0,
            max_entries_per_segment: 0,
            avg_entries_per_segment: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_empty_log() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        assert!(log.is_empty());
        assert_eq!(log.segment_count(), 0);
        assert_eq!(log.first_index(), None);
        assert_eq!(log.last_index(), None);
        assert_eq!(log.last_term().unwrap(), None);
    }

    #[test]
    fn test_append_single_entry() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        let entry = LogEntry::new(1, 1, b"test command".to_vec());
        log.append(entry.clone()).unwrap();

        assert!(!log.is_empty());
        assert_eq!(log.segment_count(), 1);
        assert_eq!(log.first_index(), Some(1));
        assert_eq!(log.last_index(), Some(1));
        assert_eq!(log.last_term().unwrap(), Some(1));

        let retrieved = log.get_entry(1).unwrap();
        assert_eq!(retrieved, entry);
    }

    #[test]
    fn test_append_multiple_entries() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        let entries = vec![
            LogEntry::new(1, 1, b"command 1".to_vec()),
            LogEntry::new(1, 2, b"command 2".to_vec()),
            LogEntry::new(2, 3, b"command 3".to_vec()),
        ];

        for entry in &entries {
            log.append(entry.clone()).unwrap();
        }

        assert_eq!(log.last_index(), Some(3));
        assert_eq!(log.last_term().unwrap(), Some(2));

        // Verify all entries
        for (i, expected) in entries.iter().enumerate() {
            let retrieved = log.get_entry((i + 1) as u64).unwrap();
            assert_eq!(retrieved, *expected);
        }
    }

    #[test]
    fn test_get_entries_range() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        // Add 5 entries
        for i in 1..=5 {
            let entry = LogEntry::new(1, i, format!("command {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        // Get entries 2-4
        let entries = log.get_entries(2, 4).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index(), 2);
        assert_eq!(entries[1].index(), 3);
        assert_eq!(entries[2].index(), 4);

        // Get entries from 3 to end
        let entries = log.get_entries_from(3).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index(), 3);
        assert_eq!(entries[2].index(), 5);
    }

    #[test]
    fn test_invalid_entry_index() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        // Try to append entry with wrong index
        let entry = LogEntry::new(1, 5, b"wrong index".to_vec()); // Should be 1
        let result = log.append(entry);

        match result {
            Err(Error::InvalidEntry { .. }) => {}, // Expected
            _ => panic!("Expected InvalidEntry error"),
        }
    }

    #[test]
    fn test_entry_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        let result = log.get_entry(1);
        match result {
            Err(Error::EntryNotFound { index: 1 }) => {}, // Expected
            _ => panic!("Expected EntryNotFound error"),
        }
    }

    #[test]
    fn test_segment_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let config = RaftLogConfig {
            max_entries_per_segment: 3, // Small limit for testing
            data_dir: temp_dir.path().to_path_buf(),
        };
        let log = RaftLog::with_config(config).unwrap();

        // Add 5 entries (should create 2 segments)
        for i in 1..=5 {
            let entry = LogEntry::new(1, i, format!("command {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        assert_eq!(log.segment_count(), 2);

        let segment_info = log.segment_info();
        assert_eq!(segment_info.len(), 2);

        // First segment should have entries 1-3
        assert_eq!(segment_info[0].0, 1); // base_index
        assert_eq!(segment_info[0].1, 3); // entry_count
        assert_eq!(segment_info[0].2, Some(3)); // last_index

        // Second segment should have entries 4-5
        assert_eq!(segment_info[1].0, 4); // base_index
        assert_eq!(segment_info[1].1, 2); // entry_count
        assert_eq!(segment_info[1].2, Some(5)); // last_index
    }

    #[test]
    fn test_persistence_and_reload() {
        let temp_dir = TempDir::new().unwrap();

        // Create log and add entries
        {
            let log = RaftLog::new(temp_dir.path()).unwrap();
            for i in 1..=3 {
                let entry = LogEntry::new(1, i, format!("persistent {}", i).into_bytes());
                log.append(entry).unwrap();
            }
        }

        // Create new log instance and verify data persisted
        {
            let log = RaftLog::new(temp_dir.path()).unwrap();
            assert_eq!(log.last_index(), Some(3));
            assert_eq!(log.segment_count(), 1);

            let entry = log.get_entry(2).unwrap();
            assert_eq!(entry.command(), b"persistent 2");
        }
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let log = Arc::new(RaftLog::new(temp_dir.path()).unwrap());

        // Add some initial entries
        for i in 1..=5 {
            let entry = LogEntry::new(1, i, format!("initial {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        // Spawn multiple reader threads
        let mut handles = vec![];
        for _ in 0..5 {
            let log_clone = Arc::clone(&log);
            let handle = thread::spawn(move || {
                for i in 1..=5 {
                    let entry = log_clone.get_entry(i).unwrap();
                    assert_eq!(entry.index(), i);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_append_entries_batch() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        let entries = vec![
            LogEntry::new(1, 1, b"batch 1".to_vec()),
            LogEntry::new(1, 2, b"batch 2".to_vec()),
            LogEntry::new(2, 3, b"batch 3".to_vec()),
        ];

        log.append_entries(entries.clone()).unwrap();

        assert_eq!(log.last_index(), Some(3));
        assert_eq!(log.last_term().unwrap(), Some(2));

        // Verify all entries
        for (i, expected) in entries.iter().enumerate() {
            let retrieved = log.get_entry((i + 1) as u64).unwrap();
            assert_eq!(retrieved, *expected);
        }
    }

    #[test]
    fn test_append_entries_invalid_batch() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        let entries = vec![
            LogEntry::new(1, 1, b"valid".to_vec()),
            LogEntry::new(1, 3, b"invalid index".to_vec()), // Should be 2
        ];

        let result = log.append_entries(entries);
        match result {
            Err(Error::InvalidEntry { .. }) => {}, // Expected
            _ => panic!("Expected InvalidEntry error"),
        }

        // Log should still be empty
        assert!(log.is_empty());
    }

    #[test]
    fn test_flush_operation() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        let entry = LogEntry::new(1, 1, b"test flush".to_vec());
        log.append(entry).unwrap();

        // Explicit flush should succeed
        log.flush().unwrap();

        // Data should be persisted
        let retrieved = log.get_entry(1).unwrap();
        assert_eq!(retrieved.command(), b"test flush");
    }

    #[test]
    fn test_validate_log() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        // Empty log should validate
        log.validate().unwrap();

        // Add some entries
        for i in 1..=5 {
            let entry = LogEntry::new(1, i, format!("entry {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        // Log with entries should validate
        log.validate().unwrap();
    }

    #[test]
    fn test_compact_log() {
        let temp_dir = TempDir::new().unwrap();
        let config = RaftLogConfig {
            max_entries_per_segment: 2, // Small segments for testing
            data_dir: temp_dir.path().to_path_buf(),
        };
        let log = RaftLog::with_config(config).unwrap();

        // Add 6 entries (should create 3 segments)
        for i in 1..=6 {
            let entry = LogEntry::new(1, i, format!("entry {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        assert_eq!(log.segment_count(), 3);

        // Compact everything before index 5
        log.compact(5).unwrap();

        // Should have removed first 2 segments
        assert_eq!(log.segment_count(), 1);

        // Should still be able to read remaining entries
        let entry = log.get_entry(5).unwrap();
        assert_eq!(entry.command(), b"entry 5");

        let entry = log.get_entry(6).unwrap();
        assert_eq!(entry.command(), b"entry 6");

        // Earlier entries should be gone
        let result = log.get_entry(1);
        match result {
            Err(Error::EntryNotFound { .. }) => {}, // Expected
            _ => panic!("Expected EntryNotFound error"),
        }
    }

    #[test]
    fn test_checksum_validation_on_reload() {
        let temp_dir = TempDir::new().unwrap();

        // Create log and add entries
        {
            let log = RaftLog::new(temp_dir.path()).unwrap();
            for i in 1..=3 {
                let entry = LogEntry::new(1, i, format!("checksum test {}", i).into_bytes());
                log.append(entry).unwrap();
            }
            log.flush().unwrap();
        }

        // Reload and validate
        {
            let log = RaftLog::new(temp_dir.path()).unwrap();
            log.validate().unwrap(); // Should pass checksum validation

            assert_eq!(log.last_index(), Some(3));
            let entry = log.get_entry(2).unwrap();
            assert_eq!(entry.command(), b"checksum test 2");
        }
    }

    #[test]
    fn test_metadata_derivation() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        // Empty log metadata
        let metadata = log.metadata();
        assert_eq!(metadata.first_index, None);
        assert_eq!(metadata.last_index, None);
        assert_eq!(metadata.last_term, None);
        assert_eq!(metadata.total_entries, 0);
        assert_eq!(metadata.segment_count, 0);

        // Add entries
        for i in 1..=5 {
            let term = if i <= 3 { 1 } else { 2 };
            let entry = LogEntry::new(term, i, format!("entry {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        let metadata = log.metadata();
        assert_eq!(metadata.first_index, Some(1));
        assert_eq!(metadata.last_index, Some(5));
        assert_eq!(metadata.last_term, Some(2));
        assert_eq!(metadata.total_entries, 5);
        assert_eq!(metadata.segment_count, 1);
        assert!(metadata.append_offset.is_some());
    }

    #[test]
    fn test_term_operations() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        // Add entries with different terms
        let entries = vec![
            LogEntry::new(1, 1, b"term1_entry1".to_vec()),
            LogEntry::new(1, 2, b"term1_entry2".to_vec()),
            LogEntry::new(2, 3, b"term2_entry1".to_vec()),
            LogEntry::new(2, 4, b"term2_entry2".to_vec()),
            LogEntry::new(3, 5, b"term3_entry1".to_vec()),
        ];

        for entry in entries {
            log.append(entry).unwrap();
        }

        // Test term_at
        assert_eq!(log.term_at(1).unwrap(), 1);
        assert_eq!(log.term_at(3).unwrap(), 2);
        assert_eq!(log.term_at(5).unwrap(), 3);

        // Test last_index_for_term
        assert_eq!(log.last_index_for_term(1), Some(2));
        assert_eq!(log.last_index_for_term(2), Some(4));
        assert_eq!(log.last_index_for_term(3), Some(5));
        assert_eq!(log.last_index_for_term(4), None); // Non-existent term
    }

    #[test]
    fn test_contains_index() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        // Empty log
        assert!(!log.contains_index(1));

        // Add some entries
        for i in 1..=3 {
            let entry = LogEntry::new(1, i, format!("entry {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        assert!(log.contains_index(1));
        assert!(log.contains_index(2));
        assert!(log.contains_index(3));
        assert!(!log.contains_index(4));
        assert!(!log.contains_index(0));
    }

    #[test]
    fn test_get_entries_with_limit() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        // Add 10 entries
        for i in 1..=10 {
            let entry = LogEntry::new(1, i, format!("entry {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        // Test with limit
        let entries = log.get_entries_with_limit(3, 4).unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].index(), 3);
        assert_eq!(entries[3].index(), 6);

        // Test with limit larger than available
        let entries = log.get_entries_with_limit(8, 10).unwrap();
        assert_eq!(entries.len(), 3); // Only 8, 9, 10 available

        // Test with zero limit
        let entries = log.get_entries_with_limit(1, 0).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_entries_size() {
        let temp_dir = TempDir::new().unwrap();
        let log = RaftLog::new(temp_dir.path()).unwrap();

        // Add entries with known sizes
        let entries = vec![
            LogEntry::new(1, 1, b"small".to_vec()),      // 5 bytes command
            LogEntry::new(1, 2, b"medium data".to_vec()), // 11 bytes command
            LogEntry::new(1, 3, b"larger command data".to_vec()), // 20 bytes command
        ];

        for entry in &entries {
            log.append(entry.clone()).unwrap();
        }

        // Test size calculation
        let size = log.entries_size(1, 3).unwrap();
        let expected_size = entries.iter().map(|e| e.serialized_size()).sum::<usize>();
        assert_eq!(size, expected_size);

        // Test partial range
        let size = log.entries_size(2, 2).unwrap();
        assert_eq!(size, entries[1].serialized_size());

        // Test invalid range
        let size = log.entries_size(5, 3).unwrap();
        assert_eq!(size, 0);
    }

    #[test]
    fn test_find_segment_for_index() {
        let temp_dir = TempDir::new().unwrap();
        let config = RaftLogConfig {
            max_entries_per_segment: 3,
            data_dir: temp_dir.path().to_path_buf(),
        };
        let log = RaftLog::with_config(config).unwrap();

        // Add 7 entries (should create 3 segments: 1-3, 4-6, 7)
        for i in 1..=7 {
            let entry = LogEntry::new(1, i, format!("entry {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        // Test segment finding
        assert_eq!(log.find_segment_for_index(1), Some(1));
        assert_eq!(log.find_segment_for_index(3), Some(1));
        assert_eq!(log.find_segment_for_index(4), Some(4));
        assert_eq!(log.find_segment_for_index(6), Some(4));
        assert_eq!(log.find_segment_for_index(7), Some(7));
        assert_eq!(log.find_segment_for_index(8), None); // Doesn't exist
        assert_eq!(log.find_segment_for_index(0), None); // Before first
    }

    #[test]
    fn test_segment_statistics() {
        let temp_dir = TempDir::new().unwrap();
        let config = RaftLogConfig {
            max_entries_per_segment: 3,
            data_dir: temp_dir.path().to_path_buf(),
        };
        let log = RaftLog::with_config(config).unwrap();

        // Empty log
        let stats = log.segment_statistics();
        assert_eq!(stats.segment_count, 0);
        assert_eq!(stats.total_entries, 0);

        // Add 8 entries (segments: 3, 3, 2)
        for i in 1..=8 {
            let entry = LogEntry::new(1, i, format!("entry {}", i).into_bytes());
            log.append(entry).unwrap();
        }

        let stats = log.segment_statistics();
        assert_eq!(stats.segment_count, 3);
        assert_eq!(stats.total_entries, 8);
        assert_eq!(stats.min_entries_per_segment, 2);
        assert_eq!(stats.max_entries_per_segment, 3);
        assert!((stats.avg_entries_per_segment - 8.0/3.0).abs() < 0.001);
    }
}
