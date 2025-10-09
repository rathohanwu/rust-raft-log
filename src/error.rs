//! Error types for the Raft log implementation.

use std::io;

/// Result type alias for convenience.
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for Raft log operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Invalid file format or corrupted data.
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// Checksum validation failed.
    #[error("Checksum mismatch: expected {expected:08x}, got {actual:08x}")]
    ChecksumMismatch {
        /// Expected checksum value.
        expected: u32,
        /// Actual checksum value.
        actual: u32,
    },

    /// Invalid magic number in file header.
    #[error("Invalid magic number: expected 0x52414654, got {actual:08x}")]
    InvalidMagicNumber {
        /// Actual magic number found.
        actual: u32,
    },

    /// Unsupported file version.
    #[error("Unsupported version: {version}")]
    UnsupportedVersion {
        /// Version number found.
        version: u32,
    },

    /// Index out of bounds.
    #[error("Index {index} out of bounds (valid range: {min}..={max})")]
    IndexOutOfBounds {
        /// Requested index.
        index: u64,
        /// Minimum valid index.
        min: u64,
        /// Maximum valid index.
        max: u64,
    },

    /// Segment not found.
    #[error("Segment not found for index {index}")]
    SegmentNotFound {
        /// Index that was requested.
        index: u64,
    },

    /// Entry not found.
    #[error("Entry not found at index {index}")]
    EntryNotFound {
        /// Index that was requested.
        index: u64,
    },

    /// Invalid entry data.
    #[error("Invalid entry: {reason}")]
    InvalidEntry {
        /// Reason for invalidity.
        reason: String,
    },

    /// Directory operation failed.
    #[error("Directory error: {0}")]
    Directory(String),

    /// Concurrent access violation.
    #[error("Concurrent access error: {0}")]
    ConcurrentAccess(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_error_display() {
        let err = Error::InvalidFormat("bad data".to_string());
        assert_eq!(err.to_string(), "Invalid format: bad data");

        let err = Error::ChecksumMismatch {
            expected: 0x12345678,
            actual: 0x87654321,
        };
        assert_eq!(
            err.to_string(),
            "Checksum mismatch: expected 12345678, got 87654321"
        );
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let raft_err: Error = io_err.into();

        match raft_err {
            Error::Io(_) => {}, // Expected
            _ => panic!("Expected Io error variant"),
        }
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Error>();
    }
}
