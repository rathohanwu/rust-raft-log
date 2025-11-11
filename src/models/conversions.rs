use super::types_proto::{
    ProtoAppendEntriesRequest, ProtoAppendEntriesResponse, ProtoLogEntry, ProtoRequestVoteRequest,
    ProtoRequestVoteResponse,
};
use super::{
    AppendEntriesRequest, AppendEntriesResponse, EntryType, LogEntry, RequestVoteRequest,
    RequestVoteResponse,
};

/// Convert from Rust RequestVoteRequest to protobuf
impl From<RequestVoteRequest> for ProtoRequestVoteRequest {
    fn from(req: RequestVoteRequest) -> Self {
        ProtoRequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        }
    }
}

/// Convert from protobuf RequestVoteRequest to Rust
impl From<ProtoRequestVoteRequest> for RequestVoteRequest {
    fn from(req: ProtoRequestVoteRequest) -> Self {
        RequestVoteRequest {
            term: req.term,
            candidate_id: req.candidate_id,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        }
    }
}

/// Convert from Rust RequestVoteResponse to protobuf
impl From<RequestVoteResponse> for ProtoRequestVoteResponse {
    fn from(resp: RequestVoteResponse) -> Self {
        ProtoRequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        }
    }
}

/// Convert from protobuf RequestVoteResponse to Rust
impl From<ProtoRequestVoteResponse> for RequestVoteResponse {
    fn from(resp: ProtoRequestVoteResponse) -> Self {
        RequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        }
    }
}

/// Convert from Rust LogEntry to protobuf
impl From<LogEntry> for ProtoLogEntry {
    fn from(entry: LogEntry) -> Self {
        ProtoLogEntry {
            term: entry.term(),
            index: entry.index(),
            entry_type: entry.entry_type().clone() as u32,
            payload: entry.payload().to_vec(),
        }
    }
}

/// Convert from protobuf LogEntry to Rust
impl From<ProtoLogEntry> for LogEntry {
    fn from(entry: ProtoLogEntry) -> Self {
        let entry_type = match entry.entry_type {
            0 => EntryType::Normal,
            1 => EntryType::NoOp,
            _ => EntryType::Normal, // Default to Normal for unknown values
        };

        LogEntry::new_with_type(entry.term, entry.index, entry_type, entry.payload)
    }
}

/// Convert from Rust AppendEntriesRequest to protobuf
impl From<AppendEntriesRequest> for ProtoAppendEntriesRequest {
    fn from(req: AppendEntriesRequest) -> Self {
        ProtoAppendEntriesRequest {
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
impl From<ProtoAppendEntriesRequest> for AppendEntriesRequest {
    fn from(req: ProtoAppendEntriesRequest) -> Self {
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
impl From<AppendEntriesResponse> for ProtoAppendEntriesResponse {
    fn from(resp: AppendEntriesResponse) -> Self {
        ProtoAppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            last_log_index: resp.last_log_index,
        }
    }
}

/// Convert from protobuf AppendEntriesResponse to Rust
impl From<ProtoAppendEntriesResponse> for AppendEntriesResponse {
    fn from(resp: ProtoAppendEntriesResponse) -> Self {
        AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            last_log_index: resp.last_log_index,
        }
    }
}
