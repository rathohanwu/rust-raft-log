use std::path::PathBuf;

/// Type of log entry for Raft consensus
#[derive(Debug, Clone, PartialEq)]
pub enum EntryType {
    /// Normal application command
    Normal = 0,
    /// No-operation entry for leader heartbeats to establish authority
    NoOp = 1,
}

impl From<u8> for EntryType {
    fn from(value: u8) -> Self {
        match value {
            0 => EntryType::Normal,
            1 => EntryType::NoOp,
            _ => EntryType::Normal, // Default to Normal for unknown values
        }
    }
}

impl From<EntryType> for u8 {
    fn from(entry_type: EntryType) -> Self {
        entry_type as u8
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub(crate) term: u64,
    pub(crate) index: u64,
    pub(crate) entry_type: EntryType,
    pub(crate) payload: Vec<u8>,
}

impl LogEntry {
    pub(crate) fn new(term: u64, index: u64, payload: Vec<u8>) -> Self {
        LogEntry {
            term,
            index,
            entry_type: EntryType::Normal,
            payload,
        }
    }

    pub(crate) fn new_with_type(term: u64, index: u64, entry_type: EntryType, payload: Vec<u8>) -> Self {
        LogEntry {
            term,
            index,
            entry_type,
            payload,
        }
    }

    pub fn calculate_total_size(&self) -> u64 {
        // term (8) + index (8) + entry_type (1) + payload_size (8) + payload
        self.payload.len() as u64 + 8 + 8 + 1 + 8
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

/// Node identifier type
pub type NodeId = u32;

/// Server state in Raft consensus
#[derive(Debug, Clone, PartialEq)]
pub enum ServerState {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

impl From<u8> for ServerState {
    fn from(value: u8) -> Self {
        match value {
            0 => ServerState::Follower,
            1 => ServerState::Candidate,
            2 => ServerState::Leader,
            _ => ServerState::Follower, // Default to Follower for unknown values
        }
    }
}

impl From<ServerState> for u8 {
    fn from(state: ServerState) -> Self {
        state as u8
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

/// Errors that can occur during RaftState operations
#[derive(Debug, PartialEq)]
pub enum RaftStateError {
    /// Failed to create or access state file
    StateFileError(String),
    /// State file is corrupted or has invalid format
    CorruptedState(String),
    /// I/O error during state operations
    IoError(String),
}
