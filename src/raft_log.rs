//! Main Raft log coordinator.

use crate::{Error, LogEntry, Result};

/// Main Raft log interface.
#[derive(Debug)]
pub struct RaftLog {
    /// Placeholder - will be implemented in Task 5
    _placeholder: (),
}

impl RaftLog {
    /// Creates a new Raft log.
    pub fn new(_data_dir: &str) -> Result<Self> {
        Err(Error::Placeholder)
    }

    /// Appends an entry to the log.
    pub fn append(&self, _entry: LogEntry) -> Result<()> {
        Err(Error::Placeholder)
    }
}
