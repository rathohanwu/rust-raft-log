pub mod models;
pub mod consensus;
pub mod storage;
pub mod grpc;

// Re-export commonly used types for convenience
pub use models::{
    LogEntry, RaftLogConfig, RaftLogError, EntryType, NodeId,
    ServerState, RaftStateError, ClusterConfig, NodeInfo,
    YamlClusterConfig, RequestVoteRequest, RequestVoteResponse,
    AppendEntriesRequest, AppendEntriesResponse
};

pub use consensus::{
    RaftNode, RaftState, RaftStateSnapshot
};

pub use storage::{RaftLog, LogFileSegment};

pub use grpc::{RaftGrpcServer, RaftGrpcClient, RaftEventLoop};