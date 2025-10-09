//! Log entry data structure.

/// A single entry in the Raft log.
///
/// Each log entry contains:
/// - **Term**: The Raft term when this entry was created
/// - **Index**: The position of this entry in the log (1-based)
/// - **Command**: The state machine command data
///
/// # Binary Format
///
/// When serialized to disk, each entry has this format:
/// ```text
/// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
/// │ Length (4 bytes)│ Term (8 bytes)  │ Index (8 bytes) │ Command (N bytes)│
/// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogEntry {
    /// The Raft term when this entry was created.
    term: u64,
    /// The position of this entry in the log (1-based).
    index: u64,
    /// The state machine command data.
    command: Vec<u8>,
}

impl LogEntry {
    /// Creates a new log entry.
    ///
    /// # Arguments
    ///
    /// * `term` - The Raft term when this entry was created
    /// * `index` - The position of this entry in the log (1-based)
    /// * `command` - The state machine command data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raft_log::LogEntry;
    ///
    /// let entry = LogEntry::new(1, 1, b"SET key=value".to_vec());
    /// assert_eq!(entry.term(), 1);
    /// assert_eq!(entry.index(), 1);
    /// assert_eq!(entry.command(), b"SET key=value");
    /// ```
    pub fn new(term: u64, index: u64, command: Vec<u8>) -> Self {
        Self {
            term,
            index,
            command,
        }
    }

    /// Returns the term of this entry.
    pub fn term(&self) -> u64 {
        self.term
    }

    /// Returns the index of this entry.
    pub fn index(&self) -> u64 {
        self.index
    }

    /// Returns a reference to the command data.
    pub fn command(&self) -> &[u8] {
        &self.command
    }

    /// Returns the command data, consuming the entry.
    pub fn into_command(self) -> Vec<u8> {
        self.command
    }

    /// Returns the total size of this entry when serialized.
    ///
    /// This includes the length prefix (4 bytes), term (8 bytes),
    /// index (8 bytes), and command data.
    pub fn serialized_size(&self) -> usize {
        4 + 8 + 8 + self.command.len()
    }

    /// Returns true if this entry is empty (no command data).
    pub fn is_empty(&self) -> bool {
        self.command.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_entry_creation() {
        let entry = LogEntry::new(5, 10, b"test command".to_vec());
        assert_eq!(entry.term(), 5);
        assert_eq!(entry.index(), 10);
        assert_eq!(entry.command(), b"test command");
        assert!(!entry.is_empty());
    }

    #[test]
    fn test_empty_entry() {
        let entry = LogEntry::new(1, 1, Vec::new());
        assert!(entry.is_empty());
        assert_eq!(entry.command().len(), 0);
    }

    #[test]
    fn test_serialized_size() {
        let entry = LogEntry::new(1, 1, b"hello".to_vec());
        // 4 (length) + 8 (term) + 8 (index) + 5 (command) = 25
        assert_eq!(entry.serialized_size(), 25);
    }

    #[test]
    fn test_into_command() {
        let entry = LogEntry::new(1, 1, b"test".to_vec());
        let command = entry.into_command();
        assert_eq!(command, b"test");
    }

    #[test]
    fn test_entry_equality() {
        let entry1 = LogEntry::new(1, 1, b"test".to_vec());
        let entry2 = LogEntry::new(1, 1, b"test".to_vec());
        let entry3 = LogEntry::new(1, 2, b"test".to_vec());

        assert_eq!(entry1, entry2);
        assert_ne!(entry1, entry3);
    }
}
