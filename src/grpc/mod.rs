pub mod actor;
pub mod client;
pub mod server;
pub mod service;

// Re-export the protobuf types for backward compatibility
pub use crate::models::types_proto::proto;

pub use actor::{RaftHandle, RaftNodeView};
pub use client::RaftGrpcClient;
pub use server::RaftGrpcServer;
