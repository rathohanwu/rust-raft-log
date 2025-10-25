use crate::log::models::{LogEntry, EntryType, NodeId};
use crate::log::raft_rpc::{
    RequestVoteRequest, RequestVoteResponse, 
    AppendEntriesRequest, AppendEntriesResponse
};
use super::proto;

/// Convert from Rust RequestVoteRequest to protobuf
impl From<RequestVoteRequest> for proto::RequestVoteRequest {
    fn from(req: RequestVoteRequest) -> Self {
        proto::RequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        }
    }
}

/// Convert from protobuf RequestVoteRequest to Rust
impl From<proto::RequestVoteRequest> for RequestVoteRequest {
    fn from(req: proto::RequestVoteRequest) -> Self {
        RequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        }
    }
}

/// Convert from Rust RequestVoteResponse to protobuf
impl From<RequestVoteResponse> for proto::RequestVoteResponse {
    fn from(resp: RequestVoteResponse) -> Self {
        proto::RequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        }
    }
}

/// Convert from protobuf RequestVoteResponse to Rust
impl From<proto::RequestVoteResponse> for RequestVoteResponse {
    fn from(resp: proto::RequestVoteResponse) -> Self {
        RequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        }
    }
}

/// Convert from Rust LogEntry to protobuf
impl From<LogEntry> for proto::LogEntry {
    fn from(entry: LogEntry) -> Self {
        proto::LogEntry {
            term: entry.term(),
            index: entry.index(),
            entry_type: entry.entry_type().clone() as u32,
            payload: entry.payload().to_vec(),
        }
    }
}

/// Convert from protobuf LogEntry to Rust
impl From<proto::LogEntry> for LogEntry {
    fn from(entry: proto::LogEntry) -> Self {
        let entry_type = match entry.entry_type {
            0 => EntryType::Normal,
            1 => EntryType::NoOp,
            _ => EntryType::Normal, // Default to Normal for unknown values
        };
        
        LogEntry::new_with_type(
            entry.term,
            entry.index,
            entry_type,
            entry.payload,
        )
    }
}

/// Convert from Rust AppendEntriesRequest to protobuf
impl From<AppendEntriesRequest> for proto::AppendEntriesRequest {
    fn from(req: AppendEntriesRequest) -> Self {
        proto::AppendEntriesRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: req.entries.into_iter().map(|e| e.into()).collect(),
            leader_commit: req.leader_commit,
        }
    }
}

/// Convert from protobuf AppendEntriesRequest to Rust
impl From<proto::AppendEntriesRequest> for AppendEntriesRequest {
    fn from(req: proto::AppendEntriesRequest) -> Self {
        AppendEntriesRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: req.entries.into_iter().map(|e| e.into()).collect(),
            leader_commit: req.leader_commit,
        }
    }
}

/// Convert from Rust AppendEntriesResponse to protobuf
impl From<AppendEntriesResponse> for proto::AppendEntriesResponse {
    fn from(resp: AppendEntriesResponse) -> Self {
        proto::AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            last_log_index: resp.last_log_index,
        }
    }
}

/// Convert from protobuf AppendEntriesResponse to Rust
impl From<proto::AppendEntriesResponse> for AppendEntriesResponse {
    fn from(resp: proto::AppendEntriesResponse) -> Self {
        AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            last_log_index: resp.last_log_index,
        }
    }
}
