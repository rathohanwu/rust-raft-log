//! # Rust Raft Log
//!
//! A high-performance, thread-safe implementation of a Raft consensus algorithm log.
//!
//! This crate provides a persistent log storage system designed specifically for
//! the Raft consensus algorithm, featuring:
//!
//! - **Segment-based storage**: Efficient log rotation and management
//! - **Thread-safe operations**: Concurrent reads with exclusive writes
//! - **Crash recovery**: Checksums and validation for data integrity
//! - **High performance**: Optimized for append-heavy workloads
//!
//! ## Basic Usage
//!
//! ```rust,no_run
//! use raft_log::{RaftLog, LogEntry};
//!
//! // Create a new Raft log
//! let log = RaftLog::new("./raft-data").expect("Failed to create log");
//!
//! // Append entries
//! let entry = LogEntry::new(1, 1, b"command data".to_vec());
//! log.append(entry).expect("Failed to append entry");
//! ```

#![deny(missing_docs)]
#![deny(unsafe_code)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

// Re-export main types for convenience
pub use error::{Error, Result};
pub use log_entry::LogEntry;
pub use raft_log::RaftLog;
pub use segment::LogSegment;
pub use segment_header::SegmentHeader;

// Module declarations - will be implemented in subsequent tasks
mod error;
mod log_entry;
mod raft_log;
mod segment;
mod segment_header;

// Internal utilities
mod utils;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_imports() {
        // This test ensures all our main types can be imported
        // More comprehensive tests will be added in later tasks
        let _: Option<Error> = None;
        let _: Option<LogEntry> = None;
        let _: Option<RaftLog> = None;
        let _: Option<LogSegment> = None;
        let _: Option<SegmentHeader> = None;
    }
}
