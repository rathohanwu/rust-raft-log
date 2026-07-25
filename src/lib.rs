pub mod consensus;
pub mod grpc;
pub mod models;
pub mod storage;
#[doc(hidden)]
pub mod testkit;

// Re-export commonly used types for convenience
pub use models::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterConfig, EntryType, LogEntry, NodeId,
    NodeInfo, RaftLogConfig, RaftLogError, RaftStateError, RequestVoteRequest, RequestVoteResponse,
    ServerState, YamlClusterConfig,
};

pub use consensus::{RaftNode, RaftState, RaftStateSnapshot, StateMachine};

pub use storage::{LogFileSegment, RaftLog};

pub use grpc::{RaftGrpcClient, RaftGrpcServer, RaftNodeView};
