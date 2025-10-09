//! Log entry data structure.

/// A single entry in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// Placeholder - will be implemented in Task 2
    _placeholder: (),
}

impl LogEntry {
    /// Creates a new log entry.
    pub fn new(_term: u64, _index: u64, _command: Vec<u8>) -> Self {
        Self {
            _placeholder: (),
        }
    }
}
