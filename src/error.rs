//! Error types for the Raft log implementation.

/// Result type alias for convenience.
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for Raft log operations.
#[derive(Debug)]
pub enum Error {
    /// Placeholder error - will be expanded in Task 2
    Placeholder,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Placeholder => write!(f, "Placeholder error"),
        }
    }
}

impl std::error::Error for Error {}
