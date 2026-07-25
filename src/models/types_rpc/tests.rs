use super::*;
use crate::models::EntryType;

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
