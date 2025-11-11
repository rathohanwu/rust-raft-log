pub mod server;
pub mod service;
pub mod client;
pub mod event_loop;

// Re-export the protobuf types for backward compatibility
pub use crate::models::types_proto::proto;

pub use server::RaftGrpcServer;
pub use client::RaftGrpcClient;
pub use event_loop::{RaftEventLoop, RaftTimingConfig};
