pub mod server;
pub mod client;
pub mod conversion;
pub mod event_loop;

// Re-export the generated protobuf code
pub mod proto {
    tonic::include_proto!("raft");
}

pub use server::RaftGrpcServer;
pub use client::RaftGrpcClient;
pub use event_loop::{RaftEventLoop, RaftTimingConfig};
