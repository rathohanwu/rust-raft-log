use super::*;
use crate::models::NodeInfo;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

struct RecordingStateMachine(Arc<Mutex<Vec<Vec<u8>>>>);

impl StateMachine for RecordingStateMachine {
    fn apply(&mut self, entry: &LogEntry) {
        self.0.lock().unwrap().push(entry.payload().to_vec());
    }
}

fn create_test_node(node_id: NodeId) -> (RaftNode, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let log_dir = temp_dir.path().join("logs").to_string_lossy().to_string();
    let meta_path = temp_dir
        .path()
        .join("raft_state.meta")
        .to_string_lossy()
        .to_string();

    let nodes = vec![
        NodeInfo::new(1, "127.0.0.1".to_string(), 8001),
        NodeInfo::new(2, "127.0.0.1".to_string(), 8002),
        NodeInfo::new(3, "127.0.0.1".to_string(), 8003),
    ];

    let config = ClusterConfig::new(
        node_id,
        nodes,
        log_dir,
        meta_path,
        1024,
        100,
        (150, 300), // Election timeout range
        50,         // Heartbeat interval
    );

    let node = RaftNode::new(config).expect("Failed to create RaftNode");
    (node, temp_dir)
}

#[test]
fn test_raft_node_creation() {
    let (node, _temp_dir) = create_test_node(1);

    assert_eq!(node.get_node_id(), 1);
    assert_eq!(node.get_current_term(), 0);
    assert_eq!(node.get_server_state(), ServerState::Follower);
    assert_eq!(node.get_log_length(), 0);

    let (last_index, last_term) = node.get_last_log_info();
    assert_eq!(last_index, 0);
    assert_eq!(last_term, 0);
}

#[test]
fn test_single_node_leader_commits_without_follower_responses() {
    let temp_dir = TempDir::new().unwrap();
    let config = ClusterConfig::new(
        1,
        vec![NodeInfo::new(1, "127.0.0.1".to_string(), 8001)],
        temp_dir.path().join("logs").to_string_lossy().to_string(),
        temp_dir
            .path()
            .join("raft_state.meta")
            .to_string_lossy()
            .to_string(),
        1024,
        100,
        (150, 300),
        50,
    );
    let mut node = RaftNode::new(config).unwrap();

    assert!(node.create_vote_request().is_none());
    assert_eq!(node.get_server_state(), ServerState::Leader);
    assert_eq!(node.get_state().commit_index, 1); // leader NoOp

    let index = node.append_new_entry(b"command".to_vec()).unwrap();
    assert_eq!(index, 2);
    assert_eq!(node.get_state().commit_index, 2);
    assert_eq!(node.get_state().last_applied, 2);
}

#[test]
fn test_request_vote_handling() {
    let (mut node, _temp_dir) = create_test_node(1);

    // Test voting for a valid candidate
    let request = RequestVoteRequest::new(1, 2, 0, 0);
    let response = node.handle_request_vote(request);

    assert_eq!(response.term, 1);
    assert!(response.vote_granted);
    assert_eq!(node.get_current_term(), 1);

    // Test rejecting vote for different candidate in same term
    let request2 = RequestVoteRequest::new(1, 3, 0, 0);
    let response2 = node.handle_request_vote(request2);

    assert_eq!(response2.term, 1);
    assert!(!response2.vote_granted);

    // Test rejecting vote for older term
    let request3 = RequestVoteRequest::new(0, 3, 0, 0);
    let response3 = node.handle_request_vote(request3);

    assert_eq!(response3.term, 1);
    assert!(!response3.vote_granted);
}

#[test]
fn rejects_non_voter_ids_before_they_change_protocol_state() {
    let (mut node, _temp_dir) = create_test_node(1);

    let invalid_vote = node.handle_request_vote(RequestVoteRequest::new(7, 99, 0, 0));
    assert!(!invalid_vote.vote_granted);
    assert_eq!(node.get_current_term(), 0);

    let invalid_append =
        node.handle_append_entries(AppendEntriesRequest::heartbeat(8, 99, 0, 0, 0));
    assert!(!invalid_append.success);
    assert_eq!(node.get_current_term(), 0);

    let request = node.create_vote_request().unwrap();
    assert!(!node.handle_vote_response(99, RequestVoteResponse::grant_vote(request.term)));
    assert_eq!(node.get_server_state(), ServerState::Candidate);
    assert_eq!(node.votes_received.len(), 1);
}

#[test]
fn test_state_machine_applies_normal_entries_only() {
    let (mut node, _temp_dir) = create_test_node(1);
    let applied = Arc::new(Mutex::new(Vec::new()));
    node.state_machine = Some(Box::new(RecordingStateMachine(Arc::clone(&applied))));

    let request = AppendEntriesRequest::new(
        1,
        2,
        0,
        0,
        vec![
            LogEntry::new_with_type(1, 1, EntryType::NoOp, vec![]),
            LogEntry::new_with_type(1, 2, EntryType::Normal, b"command".to_vec()),
        ],
        2,
    );

    assert!(node.handle_append_entries(request).success);
    assert_eq!(node.get_state().last_applied, 2);
    assert_eq!(*applied.lock().unwrap(), vec![b"command".to_vec()]);
}

#[test]
fn missing_committed_entry_stops_node_and_prevents_success_reply() {
    let (mut node, _temp_dir) = create_test_node(1);
    node.state.set_commit_index(1);

    node.apply_committed_entries();

    assert!(node.is_stopped());
    assert_eq!(node.get_state().last_applied, 0);
    let response = node.handle_append_entries(AppendEntriesRequest::heartbeat(0, 2, 0, 0, 1));
    assert!(!response.success);
}

#[test]
fn higher_term_responses_clear_volatile_role_state() {
    let (mut candidate, _candidate_dir) = create_test_node(1);
    let vote_request = candidate.create_vote_request().unwrap();
    candidate.next_index.insert(2, 3);
    candidate.match_index.insert(2, 2);
    candidate.current_leader = Some(2);

    assert!(
        !candidate.handle_vote_response(2, RequestVoteResponse::deny_vote(vote_request.term + 1),)
    );
    assert_eq!(candidate.get_current_term(), vote_request.term + 1);
    assert_eq!(candidate.get_server_state(), ServerState::Follower);
    assert!(candidate.votes_received.is_empty());
    assert!(candidate.next_index.is_empty());
    assert!(candidate.match_index.is_empty());
    assert_eq!(candidate.current_leader, None);

    let (mut leader, _leader_dir) = create_test_node(1);
    let election = leader.create_vote_request().unwrap();
    assert!(leader.handle_vote_response(2, RequestVoteResponse::grant_vote(election.term)));
    leader.votes_received.insert(1);

    let request = leader
        .build_replication_requests()
        .into_iter()
        .find(|(node_id, _)| *node_id == 2)
        .unwrap()
        .1;
    assert!(!leader.handle_append_entries_response(
        2,
        &request,
        AppendEntriesResponse::failure(election.term + 1, Some(0)),
    ));
    assert_eq!(leader.get_current_term(), election.term + 1);
    assert_eq!(leader.get_server_state(), ServerState::Follower);
    assert!(leader.votes_received.is_empty());
    assert!(leader.next_index.is_empty());
    assert!(leader.match_index.is_empty());
    assert_eq!(leader.current_leader, None);
}

#[test]
fn newer_term_consistency_rejection_persists_the_new_term() {
    let (mut node, _temp_dir) = create_test_node(1);
    let state_path = node.config.meta_file_path.clone();

    let response = node.handle_append_entries(AppendEntriesRequest::heartbeat(2, 2, 1, 1, 0));

    assert!(!response.success);
    assert_eq!(response.term, 2);
    assert_eq!(node.get_current_term(), 2);

    drop(node);
    let reloaded = RaftState::from_existing(state_path).expect("reload persisted Raft state");
    assert_eq!(reloaded.get_current_term(), 2);
}

#[test]
fn test_append_entries_heartbeat() {
    let (mut node, _temp_dir) = create_test_node(1);

    // Test heartbeat from leader
    let heartbeat = AppendEntriesRequest::heartbeat(1, 2, 0, 0, 0);
    let response = node.handle_append_entries(heartbeat);

    assert_eq!(response.term, 1);
    assert!(response.success);
    assert_eq!(node.get_current_term(), 1);
    assert_eq!(node.get_server_state(), ServerState::Follower);
}

#[test]
fn test_create_vote_request() {
    let (mut node, _temp_dir) = create_test_node(1);

    let request = node.create_vote_request();

    // Should create a single vote request
    assert!(request.is_some());
    let request = request.unwrap();

    // Check node state after creating vote request
    assert_eq!(node.get_current_term(), 1);
    assert_eq!(node.get_server_state(), ServerState::Candidate);
    assert_eq!(node.get_state().voted_for, Some(1));

    // Check request content
    assert_eq!(request.term, 1);
    assert_eq!(request.candidate_id, 1);
    assert_eq!(request.last_log_index, 0);
    assert_eq!(request.last_log_term, 0);
}

#[test]
fn test_become_leader() {
    let (mut node, _temp_dir) = create_test_node(1);

    // Create vote request first (transitions to candidate)
    let vote_request = node.create_vote_request();
    assert!(vote_request.is_some());

    // A configured peer grants the quorum vote.
    assert!(node.handle_vote_response(2, RequestVoteResponse::grant_vote(1)));

    assert_eq!(node.get_server_state(), ServerState::Leader);

    // Test creating append entries requests
    let append_requests = node.build_replication_requests();
    assert_eq!(append_requests.len(), 2); // For nodes 2 and 3

    for (node_id, request) in append_requests {
        assert!(node_id == 2 || node_id == 3);
        assert_eq!(request.term, 1);
        assert_eq!(request.leader_id, 1);
        // Since we just became leader and appended NoOp, followers need that entry
        assert!(!request.is_heartbeat());
        assert_eq!(request.entries.len(), 1); // Just the NoOp entry
    }
}

#[test]
fn stopped_node_does_not_report_an_election_win() {
    let (mut node, _temp_dir) = create_test_node(1);
    let request = node.create_vote_request().unwrap();
    node.stopped = true;

    assert!(!node.handle_vote_response(2, RequestVoteResponse::grant_vote(request.term)));
    assert_ne!(node.get_server_state(), ServerState::Leader);
}

#[test]
fn test_log_consistency_check() {
    let (mut node, _temp_dir) = create_test_node(1);

    // Add some entries to the log
    let entry1 = LogEntry::new_with_type(1, 0, EntryType::Normal, "cmd1".as_bytes().to_vec());
    let entry2 = LogEntry::new_with_type(1, 0, EntryType::Normal, "cmd2".as_bytes().to_vec());

    node.log
        .append_entry(entry1)
        .expect("Failed to append entry");
    node.log
        .append_entry(entry2)
        .expect("Failed to append entry");

    // Test AppendEntries with correct previous entry
    let new_entry = LogEntry::new_with_type(1, 3, EntryType::Normal, "cmd3".as_bytes().to_vec());
    let append_request = AppendEntriesRequest::new(1, 2, 2, 1, vec![new_entry], 0);

    let response = node.handle_append_entries(append_request);
    assert!(response.success);
    assert_eq!(node.get_log_length(), 3);

    // Test AppendEntries with incorrect previous entry
    let new_entry2 = LogEntry::new_with_type(1, 4, EntryType::Normal, "cmd4".as_bytes().to_vec());
    let bad_request = AppendEntriesRequest::new(1, 2, 5, 1, vec![new_entry2], 0);

    let response2 = node.handle_append_entries(bad_request);
    assert!(!response2.success);
    assert_eq!(node.get_log_length(), 3); // Should remain unchanged
}

#[test]
fn test_append_entries_keeps_matching_overlap() {
    let (mut node, _temp_dir) = create_test_node(1);
    node.log
        .append_entry(LogEntry::new_with_type(
            1,
            0,
            EntryType::Normal,
            b"one".to_vec(),
        ))
        .unwrap();
    node.log
        .append_entry(LogEntry::new_with_type(
            1,
            0,
            EntryType::Normal,
            b"two".to_vec(),
        ))
        .unwrap();

    // This is a delayed duplicate of an already accepted AppendEntries.
    // It must not truncate and re-append the matching prefix.
    let duplicate = AppendEntriesRequest::new(
        1,
        2,
        0,
        0,
        vec![
            LogEntry::new_with_type(1, 1, EntryType::Normal, b"one".to_vec()),
            LogEntry::new_with_type(1, 2, EntryType::Normal, b"two".to_vec()),
        ],
        2,
    );
    assert!(node.handle_append_entries(duplicate).success);
    assert_eq!(node.get_log_length(), 2);
    assert_eq!(node.get_entry(1).unwrap().payload(), b"one");
    assert_eq!(node.get_entry(2).unwrap().payload(), b"two");
    assert_eq!(node.get_state().last_applied, 2);
}

#[test]
fn test_append_entries_truncates_at_first_term_conflict_only() {
    let (mut node, _temp_dir) = create_test_node(1);
    for (term, payload) in [
        (1, b"one".as_slice()),
        (1, b"old".as_slice()),
        (1, b"tail".as_slice()),
    ] {
        node.log
            .append_entry(LogEntry::new_with_type(
                term,
                0,
                EntryType::Normal,
                payload.to_vec(),
            ))
            .unwrap();
    }

    let request = AppendEntriesRequest::new(
        2,
        2,
        0,
        0,
        vec![
            LogEntry::new_with_type(1, 1, EntryType::Normal, b"one".to_vec()),
            LogEntry::new_with_type(2, 2, EntryType::Normal, b"new".to_vec()),
        ],
        0,
    );
    assert!(node.handle_append_entries(request).success);
    assert_eq!(node.get_log_length(), 2);
    assert_eq!(node.get_entry(1).unwrap().payload(), b"one");
    assert_eq!(node.get_entry(2).unwrap().term(), 2);
    assert_eq!(node.get_entry(2).unwrap().payload(), b"new");
}

#[test]
fn test_complete_raft_scenario() {
    // Create a 3-node cluster
    let (mut node1, _temp1) = create_test_node(1);
    let (mut node2, _temp2) = create_test_node(2);
    let (mut node3, _temp3) = create_test_node(3);

    // All nodes start as followers in term 0
    assert_eq!(node1.get_server_state(), ServerState::Follower);
    assert_eq!(node2.get_server_state(), ServerState::Follower);
    assert_eq!(node3.get_server_state(), ServerState::Follower);

    // Node 1 starts an election
    let vote_request = node1.create_vote_request();
    assert!(vote_request.is_some());
    let vote_request = vote_request.unwrap();
    assert_eq!(node1.get_server_state(), ServerState::Candidate);
    assert_eq!(node1.get_current_term(), 1);

    // Node 2 and Node 3 receive vote requests and grant votes
    let vote_response_2 = node2.handle_request_vote(vote_request.clone());
    let vote_response_3 = node3.handle_request_vote(vote_request.clone());

    assert!(vote_response_2.vote_granted);
    assert!(vote_response_3.vote_granted);
    assert_eq!(vote_response_2.term, 1);
    assert_eq!(vote_response_3.term, 1);

    // Node 1 receives majority votes and becomes leader
    let won_election = node1.handle_vote_response(2, vote_response_2);
    assert!(won_election);
    assert_eq!(node1.get_server_state(), ServerState::Leader);

    // Node 1 sends heartbeats to maintain leadership
    let heartbeats = node1.build_replication_requests();
    assert_eq!(heartbeats.len(), 2);

    // Followers receive heartbeats
    for (target_node_id, heartbeat) in heartbeats {
        if target_node_id == 2 {
            let response = node2.handle_append_entries(heartbeat);
            assert!(response.success);
            assert_eq!(node2.get_server_state(), ServerState::Follower);
        } else if target_node_id == 3 {
            let response = node3.handle_append_entries(heartbeat);
            assert!(response.success);
            assert_eq!(node3.get_server_state(), ServerState::Follower);
        }
    }

    // Leader appends some entries using the new API
    node1
        .append_new_entry("command1".as_bytes().to_vec())
        .expect("Failed to append entry");
    node1
        .append_new_entry("command2".as_bytes().to_vec())
        .expect("Failed to append entry");

    // Leader replicates entries to followers using the new API
    let append_requests = node1.build_replication_requests();
    assert_eq!(append_requests.len(), 2);

    // Send to followers and process responses
    for (node_id, request) in append_requests {
        if node_id == 2 {
            let response = node2.handle_append_entries(request.clone());
            node1.handle_append_entries_response(node_id, &request, response);
        } else if node_id == 3 {
            let response = node3.handle_append_entries(request.clone());
            node1.handle_append_entries_response(node_id, &request, response);
        }
    }

    // Verify all nodes have the same log entries
    // Note: Index 1 is NoOp, Index 2 is "command1", Index 3 is "command2"
    let node1_entry2 = node1.log.get_entry(2).expect("Should have entry 2");
    let node2_entry2 = node2.log.get_entry(2).expect("Should have entry 2");
    let node3_entry2 = node3.log.get_entry(2).expect("Should have entry 2");

    assert_eq!(node1_entry2.payload, "command1".as_bytes());
    assert_eq!(node2_entry2.payload, "command1".as_bytes());
    assert_eq!(node3_entry2.payload, "command1".as_bytes());
}

#[test]
fn test_election_vote_collection() {
    let (mut node1, _temp1) = create_test_node(1);
    let (mut node2, _temp2) = create_test_node(2);
    let (mut node3, _temp3) = create_test_node(3);

    // Node 1 starts election
    let vote_request = node1.create_vote_request();
    assert!(vote_request.is_some());
    let vote_request = vote_request.unwrap();
    assert_eq!(node1.votes_received.len(), 1); // Voted for self
    assert_eq!(node1.get_server_state(), ServerState::Candidate);

    // Node 2 grants vote
    let vote_response_2 = node2.handle_request_vote(vote_request.clone());
    assert!(vote_response_2.vote_granted);

    // Node 1 receives vote from node 2
    let won_election = node1.handle_vote_response(2, vote_response_2);
    assert!(won_election); // Should win with 2/3 votes
    assert_eq!(node1.get_server_state(), ServerState::Leader);
    assert_ne!(node1.get_server_state(), ServerState::Candidate);

    // Test vote from node 3 after election won (should be ignored)
    let vote_response_3 = node3.handle_request_vote(vote_request.clone());
    let late_vote = node1.handle_vote_response(3, vote_response_3);
    assert!(!late_vote); // Election already won
}

#[test]
fn test_leader_log_replication() {
    let (mut leader, _temp1) = create_test_node(1);
    let (mut follower1, _temp2) = create_test_node(2);
    let (mut follower2, _temp3) = create_test_node(3);

    // Make node 1 leader
    let vote_request = leader.create_vote_request();
    assert!(vote_request.is_some());
    assert!(leader.handle_vote_response(2, RequestVoteResponse::grant_vote(1)));

    // Leader appends some entries
    let entry_index1 = leader
        .append_new_entry("command1".as_bytes().to_vec())
        .expect("Failed to append");
    let entry_index2 = leader
        .append_new_entry("command2".as_bytes().to_vec())
        .expect("Failed to append");

    assert_eq!(entry_index1, 2); // After NoOp entry
    assert_eq!(entry_index2, 3);

    // Create replication requests
    let append_requests = leader.build_replication_requests();
    assert_eq!(append_requests.len(), 2); // For nodes 2 and 3

    // Send to followers and collect responses
    let mut responses = Vec::new();
    for (node_id, request) in &append_requests {
        if *node_id == 2 {
            let response = follower1.handle_append_entries(request.clone());
            responses.push((2, request, response));
        } else if *node_id == 3 {
            let response = follower2.handle_append_entries(request.clone());
            responses.push((3, request, response));
        }
    }

    // Leader processes responses
    for (node_id, request, response) in responses {
        let success = leader.handle_append_entries_response(node_id, request, response);
        assert!(success);
    }

    // Check replication status
    assert_eq!(leader.next_index.get(&2), Some(&4));
    assert_eq!(leader.match_index.get(&2), Some(&3));
    assert_eq!(leader.next_index.get(&3), Some(&4));
    assert_eq!(leader.match_index.get(&3), Some(&3));

    // Commit index should advance
    assert_eq!(leader.state.get_commit_index(), 3);
}

#[test]
fn test_append_entries_failure_and_retry() {
    let (mut leader, _temp1) = create_test_node(1);
    let (mut follower, _temp2) = create_test_node(2);

    // Make node 1 leader
    let vote_request = leader.create_vote_request();
    assert!(vote_request.is_some());
    assert!(leader.handle_vote_response(2, RequestVoteResponse::grant_vote(1)));

    // Leader has entries 1 (NoOp), 2, 3
    leader
        .append_new_entry("command1".as_bytes().to_vec())
        .expect("Failed to append");
    leader
        .append_new_entry("command2".as_bytes().to_vec())
        .expect("Failed to append");

    // Give the follower a known suffix end so its failure response can
    // exercise the last_log_index jump rather than the fallback path.
    follower
        .log
        .append_entry(LogEntry::new_with_type(1, 0, EntryType::NoOp, vec![]))
        .expect("seed follower log");

    // Manually set next_index to simulate follower being far behind
    leader.next_index.insert(2, 5); // Trying to send from index 5, but follower only has 0 entries

    // Leader tries to send entries starting from index 5 (which doesn't exist)
    let append_requests = leader.build_replication_requests();
    let (_, request) = &append_requests[0]; // Request for node 2

    // This should fail because follower doesn't have prev_log_index (4)
    let response = follower.handle_append_entries(request.clone());
    assert!(!response.success);

    // Leader uses the follower's last-index hint to jump to index 2.
    let success = leader.handle_append_entries_response(2, request, response);
    assert!(!success);

    // Check that next_index used the hint.
    assert_eq!(leader.next_index.get(&2), Some(&2));
    assert_eq!(leader.match_index.get(&2), Some(&0));

    // Retry from the hinted index.
    for _ in 0..1 {
        let retry_requests = leader.build_replication_requests();
        let (_, retry_request) = &retry_requests[0];
        let retry_response = follower.handle_append_entries(retry_request.clone());

        if retry_response.success {
            leader.handle_append_entries_response(2, retry_request, retry_response);
            break;
        } else {
            leader.handle_append_entries_response(2, retry_request, retry_response);
        }
    }

    // Verify follower eventually gets the correct entries
    assert_eq!(follower.get_log_length(), 3); // NoOp + command1 + command2
}

#[test]
fn stale_append_responses_do_not_regress_known_peer_progress() {
    let (mut leader, _temp_dir) = create_test_node(1);
    assert!(leader.create_vote_request().is_some());
    assert!(leader.handle_vote_response(2, RequestVoteResponse::grant_vote(1)));
    leader.append_new_entry(b"command".to_vec()).unwrap();

    let request = leader
        .build_replication_requests()
        .into_iter()
        .find(|(id, _)| *id == 2)
        .unwrap()
        .1;
    assert!(leader.handle_append_entries_response(
        2,
        &request,
        AppendEntriesResponse::success(1, Some(2)),
    ));
    assert_eq!(leader.next_index.get(&2), Some(&3));
    assert_eq!(leader.match_index.get(&2), Some(&2));

    // A duplicated response to an older, shorter request cannot undo the
    // acknowledged range; neither can its delayed failure hint.
    let old_request = AppendEntriesRequest::heartbeat(1, 1, 0, 0, 0);
    assert!(leader.handle_append_entries_response(
        2,
        &old_request,
        AppendEntriesResponse::success(1, Some(0)),
    ));
    assert!(!leader.handle_append_entries_response(
        2,
        &old_request,
        AppendEntriesResponse::failure(1, Some(0)),
    ));
    assert_eq!(leader.next_index.get(&2), Some(&3));
    assert_eq!(leader.match_index.get(&2), Some(&2));
}

#[test]
fn test_natural_append_entries_behavior() {
    let (mut leader, _temp1) = create_test_node(1);

    // Make node 1 leader
    let vote_request = leader.create_vote_request();
    assert!(vote_request.is_some());
    assert!(leader.handle_vote_response(2, RequestVoteResponse::grant_vote(1)));

    // When we become leader, a NoOp entry is automatically appended
    // So followers are immediately behind and need the NoOp entry
    let initial_requests = leader.build_replication_requests();
    assert_eq!(initial_requests.len(), 2);
    for (_, request) in &initial_requests {
        assert!(!request.is_heartbeat());
        assert_eq!(request.entries.len(), 1); // Just the NoOp entry
        assert_eq!(request.entries[0].entry_type, EntryType::NoOp);
    }

    // Add some entries to the leader's log
    leader
        .append_new_entry("command1".as_bytes().to_vec())
        .expect("Failed to append");
    leader
        .append_new_entry("command2".as_bytes().to_vec())
        .expect("Failed to append");

    // Now followers are even further behind, so requests should contain more log entries
    let replication_requests = leader.build_replication_requests();
    assert_eq!(replication_requests.len(), 2);
    for (_, request) in &replication_requests {
        assert!(!request.is_heartbeat());
        assert_eq!(request.entries.len(), 3); // NoOp + command1 + command2
    }

    // Simulate followers catching up by updating their match_index
    for node in leader.config.get_other_nodes() {
        leader.match_index.insert(node.node_id, 3); // Caught up to entry 3
        leader.next_index.insert(node.node_id, 4); // Next entry to send is 4
    }

    // Now followers are up-to-date again, so requests should be heartbeats
    let heartbeat_requests = leader.build_replication_requests();
    assert_eq!(heartbeat_requests.len(), 2);
    for (_, request) in &heartbeat_requests {
        assert!(request.is_heartbeat());
        assert_eq!(request.entries.len(), 0);
    }

    // Test that convenience methods work the same way
    let convenience_requests = leader.build_replication_requests();
    assert_eq!(heartbeat_requests, convenience_requests);
}
