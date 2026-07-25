/// Protobuf type aliases and bindings
///
/// This module contains the generated protobuf types and type aliases
/// that provide a clean interface for protobuf data structures used
/// throughout the application.

// Re-export the generated protobuf code
pub mod proto {
    tonic::include_proto!("raft");
}

// Re-export commonly used proto types for convenience
pub use proto::{
    AppendEntriesRequest as ProtoAppendEntriesRequest,
    AppendEntriesResponse as ProtoAppendEntriesResponse, ClientRequestMessage,
    ClientResponseMessage, LogEntry as ProtoLogEntry,
    RequestVoteRequest as ProtoRequestVoteRequest, RequestVoteResponse as ProtoRequestVoteResponse,
};
