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
    RequestVoteRequest as ProtoRequestVoteRequest,
    RequestVoteResponse as ProtoRequestVoteResponse,
    AppendEntriesRequest as ProtoAppendEntriesRequest,
    AppendEntriesResponse as ProtoAppendEntriesResponse,
    LogEntry as ProtoLogEntry,
    ClientRequestMessage,
    ClientResponseMessage,
};
