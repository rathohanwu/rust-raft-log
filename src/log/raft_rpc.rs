use super::models::{NodeId, LogEntry, EntryType};

/// RequestVote RPC - Invoked by candidates to gather votes
#[derive(Debug, Clone, PartialEq)]
pub struct RequestVoteRequest {
    /// Candidate's term
    pub term: u64,
    /// Candidate requesting vote
    pub candidate_id: NodeId,
    /// Index of candidate's last log entry
    pub last_log_index: u64,
    /// Term of candidate's last log entry
    pub last_log_term: u64,
}

/// RequestVote RPC Response
#[derive(Debug, Clone, PartialEq)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself
    pub term: u64,
    /// True means candidate received vote
    pub vote_granted: bool,
}

/// AppendEntries RPC - Invoked by leader to replicate log entries; also used as heartbeat
#[derive(Debug, Clone, PartialEq)]
pub struct AppendEntriesRequest {
    /// Leader's term
    pub term: u64,
    /// So follower can redirect clients
    pub leader_id: NodeId,
    /// Index of log entry immediately preceding new ones
    pub prev_log_index: u64,
    /// Term of prev_log_index entry
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat; may send more than one for efficiency)
    pub entries: Vec<LogEntry>,
    /// Leader's commit_index
    pub leader_commit: u64,
}

/// AppendEntries RPC Response
#[derive(Debug, Clone, PartialEq)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself
    pub term: u64,
    /// True if follower contained entry matching prev_log_index and prev_log_term
    pub success: bool,
    /// For optimization: follower's last log index (to help leader find next_index faster)
    pub last_log_index: Option<u64>,
}

impl RequestVoteRequest {
    pub fn new(term: u64, candidate_id: NodeId, last_log_index: u64, last_log_term: u64) -> Self {
        RequestVoteRequest {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }
}

impl AppendEntriesRequest {
    pub fn new(
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Self {
        AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    /// Creates a heartbeat (empty AppendEntries) request
    pub fn heartbeat(term: u64, leader_id: NodeId, prev_log_index: u64, prev_log_term: u64, leader_commit: u64) -> Self {
        AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: vec![],
            leader_commit,
        }
    }

    /// Returns true if this is a heartbeat (no entries)
    pub fn is_heartbeat(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the number of entries in this request
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }
}

impl RequestVoteResponse {
    pub fn new(term: u64, vote_granted: bool) -> Self {
        RequestVoteResponse { term, vote_granted }
    }

    /// Creates a response granting the vote
    pub fn grant_vote(term: u64) -> Self {
        RequestVoteResponse {
            term,
            vote_granted: true,
        }
    }

    /// Creates a response denying the vote
    pub fn deny_vote(term: u64) -> Self {
        RequestVoteResponse {
            term,
            vote_granted: false,
        }
    }
}

impl AppendEntriesResponse {
    pub fn new(term: u64, success: bool, last_log_index: Option<u64>) -> Self {
        AppendEntriesResponse {
            term,
            success,
            last_log_index,
        }
    }

    /// Creates a successful response
    pub fn success(term: u64, last_log_index: Option<u64>) -> Self {
        AppendEntriesResponse {
            term,
            success: true,
            last_log_index,
        }
    }

    /// Creates a failure response
    pub fn failure(term: u64, last_log_index: Option<u64>) -> Self {
        AppendEntriesResponse {
            term,
            success: false,
            last_log_index,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_vote_request() {
        let request = RequestVoteRequest::new(5, 123, 10, 4);
        assert_eq!(request.term, 5);
        assert_eq!(request.candidate_id, 123);
        assert_eq!(request.last_log_index, 10);
        assert_eq!(request.last_log_term, 4);
    }

    #[test]
    fn test_request_vote_response() {
        let grant = RequestVoteResponse::grant_vote(5);
        assert_eq!(grant.term, 5);
        assert!(grant.vote_granted);

        let deny = RequestVoteResponse::deny_vote(6);
        assert_eq!(deny.term, 6);
        assert!(!deny.vote_granted);
    }

    #[test]
    fn test_append_entries_request() {
        let entries = vec![
            LogEntry::new_with_type(1, 1, EntryType::Normal, "cmd1".as_bytes().to_vec()),
            LogEntry::new_with_type(1, 2, EntryType::Normal, "cmd2".as_bytes().to_vec()),
        ];

        let request = AppendEntriesRequest::new(1, 100, 0, 0, entries.clone(), 0);
        assert_eq!(request.term, 1);
        assert_eq!(request.leader_id, 100);
        assert_eq!(request.prev_log_index, 0);
        assert_eq!(request.prev_log_term, 0);
        assert_eq!(request.entries.len(), 2);
        assert_eq!(request.leader_commit, 0);
        assert!(!request.is_heartbeat());
        assert_eq!(request.entry_count(), 2);

        // Test heartbeat
        let heartbeat = AppendEntriesRequest::heartbeat(2, 100, 5, 1, 3);
        assert_eq!(heartbeat.term, 2);
        assert_eq!(heartbeat.leader_id, 100);
        assert_eq!(heartbeat.prev_log_index, 5);
        assert_eq!(heartbeat.prev_log_term, 1);
        assert_eq!(heartbeat.leader_commit, 3);
        assert!(heartbeat.is_heartbeat());
        assert_eq!(heartbeat.entry_count(), 0);
    }

    #[test]
    fn test_append_entries_response() {
        let success = AppendEntriesResponse::success(5, Some(10));
        assert_eq!(success.term, 5);
        assert!(success.success);
        assert_eq!(success.last_log_index, Some(10));

        let failure = AppendEntriesResponse::failure(6, Some(8));
        assert_eq!(failure.term, 6);
        assert!(!failure.success);
        assert_eq!(failure.last_log_index, Some(8));
    }
}
