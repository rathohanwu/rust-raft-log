pub mod server;
pub mod client;
pub mod conversion;

// Re-export the generated protobuf code
pub mod proto {
    tonic::include_proto!("raft");
}

pub use server::RaftGrpcServer;
pub use client::RaftGrpcClient;
