use super::segment::LogFileSegment;
use super::utils::create_memory_mapped_file;
use crate::models::{AppendResult, LogEntry, RaftLogConfig, RaftLogError};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// RaftLog manages a collection of log file segments for the Raft consensus algorithm.
/// It provides methods to append, retrieve, and truncate log entries across multiple segments.
pub struct RaftLog {
    /// Configuration for the RaftLog
    config: RaftLogConfig,
    /// Map of base_index -> LogFileSegment for efficient segment lookup
    segments: BTreeMap<u64, (PathBuf, LogFileSegment)>,
    /// Whole suffix segment files removed from the in-memory log. They are
    /// unlinked only after the replacement/truncation metadata is durable.
    pending_delete: Vec<PathBuf>,
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
            pending_delete: Vec::new(),
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
        self.segments.insert(base_index, (segment_path, segment));
        Ok(())
    }

    /// Gets the segment that contains the given index
    fn get_segment_for_index(&self, index: u64) -> Option<&LogFileSegment> {
        // Find the segment with the largest base_index <= index
        self.segments
            .range(..=index)
            .next_back()
            .map(|(_, (_, segment))| segment)
    }

    /// Gets the last (most recent) segment
    fn get_last_segment(&self) -> Option<&LogFileSegment> {
        self.segments.values().last().map(|(_, segment)| segment)
    }

    /// Gets the last (most recent) segment mutably
    fn get_last_segment_mut(&mut self) -> Option<&mut LogFileSegment> {
        self.segments
            .values_mut()
            .last()
            .map(|(_, segment)| segment)
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

        // Load each segment and get base index from file header. A failure is
        // corruption, not an excuse to silently omit part of a Raft log.
        let mut segments_with_base_index = Vec::new();
        for path in segment_files {
            let (base_index, segment) = self.load_segment_file(&path)?;
            segments_with_base_index.push((base_index, path, segment));
        }

        // Sort by base index
        segments_with_base_index.sort_by_key(|(base_index, _, _)| *base_index);

        // Duplicate bases are ambiguous recovery; segments must also cover one
        // contiguous log without holes.
        let mut expected_base = 1;
        for (base_index, path, segment) in segments_with_base_index {
            if base_index != expected_base {
                return Err(RaftLogError::CorruptedSegment(format!(
                    "expected segment base {}, found {} in {:?}",
                    expected_base, base_index, path
                )));
            }
            let next_base = segment
                .get_last_index()
                .map(|i| i + 1)
                .unwrap_or(base_index);
            if self.segments.insert(base_index, (path, segment)).is_some() {
                return Err(RaftLogError::CorruptedSegment(format!(
                    "duplicate segment base index {}",
                    base_index
                )));
            }
            expected_base = next_base;
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
        if !segment.validate_header() {
            return Err(RaftLogError::CorruptedSegment(format!(
                "invalid segment header in {:?}",
                path
            )));
        }

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

    /// Appends one entry and makes it durable before returning.
    ///
    /// Use `append_entry_unflushed` only when the caller owns a larger Raft
    /// durability boundary and will call `flush` before acknowledging it.
    pub fn append_entry(&mut self, log_entry: LogEntry) -> Result<(), RaftLogError> {
        self.append_entry_unflushed(log_entry)?;
        self.flush()
    }

    /// Appends one entry without synchronizing it to stable storage.
    ///
    /// This is intentionally public for protocol code that batches an entire
    /// AppendEntries RPC. Callers must flush before reporting success.
    pub fn append_entry_unflushed(&mut self, mut log_entry: LogEntry) -> Result<(), RaftLogError> {
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
                // Create new segment and recursively append without flushing.
                self.create_new_segment(self.next_index)?;
                self.append_entry_unflushed(log_entry)
            }
        }
    }

    /// Appends multiple entries and performs one durability barrier.
    pub fn append_entries(&mut self, log_entries: Vec<LogEntry>) -> Result<(), RaftLogError> {
        if log_entries.is_empty() {
            return Ok(());
        }
        for log_entry in log_entries {
            self.append_entry_unflushed(log_entry)?;
        }
        self.flush()
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
        self.last_index()
            .and_then(|last_index| self.get_entry(last_index))
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
        for (&base_index, (_, segment)) in self.segments.iter_mut() {
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

        // Remove whole suffix segments from memory. Keep their paths until a
        // later flush has made the replacement/truncation durable.
        for base_index in segments_to_remove {
            if let Some((path, _)) = self.segments.remove(&base_index) {
                self.pending_delete.push(path);
            }
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

    /// Flush all segments. Kept public for callers that need an explicit
    /// durability barrier around a multi-entry operation.
    pub fn flush(&mut self) -> Result<(), RaftLogError> {
        for (_, segment) in self.segments.values_mut() {
            segment.flush().map_err(|e| {
                RaftLogError::SegmentFileError(format!("Failed to flush log segment: {}", e))
            })?;
        }
        for path in self.pending_delete.drain(..) {
            fs::remove_file(&path).map_err(|e| {
                RaftLogError::SegmentFileError(format!(
                    "Failed to remove obsolete segment {:?}: {}",
                    path, e
                ))
            })?;
        }
        fs::File::open(&self.config.log_directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|e| {
                RaftLogError::DirectoryError(format!(
                    "Failed to sync log directory {:?}: {}",
                    self.config.log_directory, e
                ))
            })?;
        Ok(())
    }

    /// Gets the total number of entries in the log
    pub fn len(&self) -> u64 {
        self.segments
            .values()
            .map(|(_, s)| s.get_entry_count())
            .sum()
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
mod tests;
