use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub(crate) term: u64,
    pub(crate) index: u64,
    pub(crate) payload: Vec<u8>,
}

impl LogEntry {
    pub(crate) fn new(term: u64, index: u64, payload: Vec<u8>) -> Self {
        LogEntry {
            term,
            index,
            payload,
        }
    }

    pub fn calculate_total_size(&self) -> u64 {
        self.payload.len() as u64 + 8 + 8 + 8
    }
}

pub enum AppendResult {
    Success,
    RotationNeeded,
}

/// Configuration for RaftLog
#[derive(Debug, Clone)]
pub struct RaftLogConfig {
    /// Directory where log segment files are stored
    pub log_directory: PathBuf,
    /// Maximum size of each log segment file in bytes
    pub segment_size: u64,
    /// Maximum number of entries to return in get_entries
    pub max_entries_per_query: usize,
}

impl Default for RaftLogConfig {
    fn default() -> Self {
        RaftLogConfig {
            log_directory: PathBuf::from("./raft_logs"),
            segment_size: 64 * 1024 * 1024, // 64MB default
            max_entries_per_query: 1000,
        }
    }
}

/// Errors that can occur during RaftLog operations
#[derive(Debug, PartialEq)]
pub enum RaftLogError {
    /// Failed to create or access log directory
    DirectoryError(String),
    /// Failed to create or open a segment file
    SegmentFileError(String),
    /// Invalid log index requested
    InvalidIndex(u64),
    /// Requested too many entries (exceeds max_entries_per_query)
    TooManyEntriesRequested(usize),
    /// No log entries exist
    EmptyLog,
    /// Segment file is corrupted or has invalid format
    CorruptedSegment(String),
}
