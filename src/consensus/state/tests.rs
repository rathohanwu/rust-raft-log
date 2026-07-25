use super::*;
use std::fs;
use tempfile::TempDir;

fn create_test_state_file() -> (RaftState, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");
    let raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");
    (raft_state, temp_dir)
}

#[test]
fn test_new_raft_state_initialization() {
    let (raft_state, _temp_dir) = create_test_state_file();

    // Verify default values
    assert_eq!(raft_state.get_current_term(), 0);
    assert_eq!(raft_state.get_voted_for(), None);
    assert_eq!(raft_state.get_commit_index(), 0);
    assert_eq!(raft_state.get_last_applied(), 0);
    assert_eq!(raft_state.get_server_state(), ServerState::Follower);
}

#[test]
fn test_current_term_operations() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // Test setting and getting current term
    raft_state.set_current_term(42);
    assert_eq!(raft_state.get_current_term(), 42);

    raft_state.set_current_term(100);
    assert_eq!(raft_state.get_current_term(), 100);
}

#[test]
fn test_voted_for_operations() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // Initially no vote
    assert_eq!(raft_state.get_voted_for(), None);

    // Vote for candidate 123
    raft_state.set_voted_for(Some(123));
    assert_eq!(raft_state.get_voted_for(), Some(123));

    // Clear vote
    raft_state.set_voted_for(None);
    assert_eq!(raft_state.get_voted_for(), None);

    // Vote for different candidate
    raft_state.set_voted_for(Some(456));
    assert_eq!(raft_state.get_voted_for(), Some(456));
}

#[test]
fn test_commit_index_operations() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    raft_state.set_commit_index(10);
    assert_eq!(raft_state.get_commit_index(), 10);

    raft_state.set_commit_index(1000);
    assert_eq!(raft_state.get_commit_index(), 1000);
}

#[test]
fn test_last_applied_operations() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    raft_state.set_last_applied(5);
    assert_eq!(raft_state.get_last_applied(), 5);

    raft_state.set_last_applied(999);
    assert_eq!(raft_state.get_last_applied(), 999);
}

#[test]
fn test_server_state_operations() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // Test all state transitions
    raft_state.set_server_state(ServerState::Candidate);
    assert_eq!(raft_state.get_server_state(), ServerState::Candidate);

    raft_state.set_server_state(ServerState::Leader);
    assert_eq!(raft_state.get_server_state(), ServerState::Leader);

    raft_state.set_server_state(ServerState::Follower);
    assert_eq!(raft_state.get_server_state(), ServerState::Follower);
}

#[test]
fn test_volatile_server_state() {
    let (raft_state, _temp_dir) = create_test_state_file();

    // Verify that a newly created state file starts as Follower
    assert_eq!(raft_state.get_server_state(), ServerState::Follower);

    // Verify that volatile state is not persisted to disk (file is only 20 bytes)
    assert_eq!(raft_state.buffer.len(), 20);
}

#[test]
fn test_server_state_always_starts_as_follower() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    // Create a new state file (simulating server startup)
    {
        let mut raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");

        // Server should start as Follower
        assert_eq!(raft_state.get_server_state(), ServerState::Follower);

        // Change to Candidate
        raft_state.set_server_state(ServerState::Candidate);
        assert_eq!(raft_state.get_server_state(), ServerState::Candidate);

        // Change to Leader
        raft_state.set_server_state(ServerState::Leader);
        assert_eq!(raft_state.get_server_state(), ServerState::Leader);
    } // raft_state goes out of scope

    // Load the state file again (simulating server restart)
    {
        let raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");

        // Should ALWAYS start as Follower regardless of previous state
        assert_eq!(raft_state.get_server_state(), ServerState::Follower);
    }

    // Test multiple restarts
    {
        let mut raft_state =
            RaftState::from_existing(&state_path).expect("Failed to load RaftState");
        raft_state.set_server_state(ServerState::Leader);
        assert_eq!(raft_state.get_server_state(), ServerState::Leader);
    }

    {
        let raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");
        // Should still start as Follower
        assert_eq!(raft_state.get_server_state(), ServerState::Follower);
    }
}

#[test]
fn test_server_state_completely_volatile() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    // Test that server state is never written to disk
    {
        let mut raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");

        // Set some persistent state
        raft_state.set_current_term(42);
        raft_state.set_voted_for(Some(123));
        raft_state.set_commit_index(10);
        raft_state.set_last_applied(5);

        // Set volatile server state
        raft_state.set_server_state(ServerState::Leader);
        assert_eq!(raft_state.get_server_state(), ServerState::Leader);

        // Verify file size is exactly what we expect (no volatile state persisted)
        assert_eq!(raft_state.buffer.len(), 20); // Only persistent fields
    }

    // Restart and verify persistent state is preserved but server state resets
    {
        let raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");

        // Persistent state should be preserved
        assert_eq!(raft_state.get_current_term(), 42);
        assert_eq!(raft_state.get_voted_for(), Some(123));

        // Volatile state should ALWAYS reset to defaults
        assert_eq!(raft_state.get_commit_index(), 0);
        assert_eq!(raft_state.get_last_applied(), 0);
        assert_eq!(raft_state.get_server_state(), ServerState::Follower);

        // File should still be the same size
        assert_eq!(raft_state.buffer.len(), 20);
    }

    // Test that changing server state doesn't affect file size or persistent data
    {
        let mut raft_state =
            RaftState::from_existing(&state_path).expect("Failed to load RaftState");

        // Change server state multiple times
        raft_state.set_server_state(ServerState::Candidate);
        raft_state.set_server_state(ServerState::Leader);
        raft_state.set_server_state(ServerState::Follower);

        // File size should remain unchanged
        assert_eq!(raft_state.buffer.len(), 20);

        // Persistent state should be unaffected
        assert_eq!(raft_state.get_current_term(), 42);
        assert_eq!(raft_state.get_voted_for(), Some(123));
    }
}

#[test]
fn test_no_server_state_conversion_needed() {
    // This test verifies that we no longer need conversion traits
    // since server state is completely volatile

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    let mut raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");

    // Server state operations work purely in memory
    assert_eq!(raft_state.get_server_state(), ServerState::Follower);

    raft_state.set_server_state(ServerState::Candidate);
    assert_eq!(raft_state.get_server_state(), ServerState::Candidate);

    raft_state.set_server_state(ServerState::Leader);
    assert_eq!(raft_state.get_server_state(), ServerState::Leader);

    // No conversion to/from u8 is needed or used
    // The enum values are purely for in-memory comparison
    assert_ne!(raft_state.get_server_state(), ServerState::Follower);
    assert_ne!(raft_state.get_server_state(), ServerState::Candidate);
    assert_eq!(raft_state.get_server_state(), ServerState::Leader);
}

#[test]
fn test_commit_index_and_last_applied_completely_volatile() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    // Test that commit_index and last_applied are never written to disk
    {
        let mut raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");

        // All volatile state should start at default values
        assert_eq!(raft_state.get_server_state(), ServerState::Follower);
        assert_eq!(raft_state.get_commit_index(), 0);
        assert_eq!(raft_state.get_last_applied(), 0);

        // Set some persistent state
        raft_state.set_current_term(100);
        raft_state.set_voted_for(Some(456));

        // Set volatile state
        raft_state.set_server_state(ServerState::Leader);
        raft_state.set_commit_index(50);
        raft_state.set_last_applied(45);

        // Verify volatile state is set correctly
        assert_eq!(raft_state.get_server_state(), ServerState::Leader);
        assert_eq!(raft_state.get_commit_index(), 50);
        assert_eq!(raft_state.get_last_applied(), 45);

        // Verify file size is exactly what we expect (only persistent fields)
        assert_eq!(raft_state.buffer.len(), 20); // Magic(4) + Version(4) + Term(8) + VotedFor(4)
    }

    // Restart and verify persistent state is preserved but volatile state resets
    {
        let raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");

        // Persistent state should be preserved
        assert_eq!(raft_state.get_current_term(), 100);
        assert_eq!(raft_state.get_voted_for(), Some(456));

        // ALL volatile state should reset to defaults
        assert_eq!(raft_state.get_server_state(), ServerState::Follower);
        assert_eq!(raft_state.get_commit_index(), 0);
        assert_eq!(raft_state.get_last_applied(), 0);

        // File should still be the same size
        assert_eq!(raft_state.buffer.len(), 20);
    }

    // Test multiple restarts to confirm behavior is consistent
    for i in 1..=3 {
        let mut raft_state =
            RaftState::from_existing(&state_path).expect("Failed to load RaftState");

        // Volatile state should always start at defaults
        assert_eq!(raft_state.get_server_state(), ServerState::Follower);
        assert_eq!(raft_state.get_commit_index(), 0);
        assert_eq!(raft_state.get_last_applied(), 0);

        // Change volatile state
        raft_state.set_server_state(ServerState::Candidate);
        raft_state.set_commit_index(i * 10);
        raft_state.set_last_applied(i * 5);

        // Verify changes work in memory
        assert_eq!(raft_state.get_server_state(), ServerState::Candidate);
        assert_eq!(raft_state.get_commit_index(), i * 10);
        assert_eq!(raft_state.get_last_applied(), i * 5);

        // File size should never change
        assert_eq!(raft_state.buffer.len(), 20);

        // Persistent state should be unaffected
        assert_eq!(raft_state.get_current_term(), 100);
        assert_eq!(raft_state.get_voted_for(), Some(456));
    }
}

#[test]
fn test_start_new_term() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // Set initial state
    raft_state.set_current_term(5);
    raft_state.set_voted_for(Some(123));

    // Start new term
    raft_state.start_new_term(10);

    // Verify term updated and vote cleared
    assert_eq!(raft_state.get_current_term(), 10);
    assert_eq!(raft_state.get_voted_for(), None);
}

#[test]
fn test_explicit_term_transitions() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // Set initial state
    raft_state.set_current_term(5);
    raft_state.set_voted_for(Some(123));
    raft_state.set_server_state(ServerState::Leader);

    // Test start_new_term_as_candidate
    raft_state.start_new_term_as_candidate(10);
    assert_eq!(raft_state.get_current_term(), 10);
    assert_eq!(raft_state.get_voted_for(), None);
    assert_eq!(raft_state.get_server_state(), ServerState::Candidate);

    // Test start_new_term_as_follower
    raft_state.start_new_term_as_follower(15);
    assert_eq!(raft_state.get_current_term(), 15);
    assert_eq!(raft_state.get_voted_for(), None);
    assert_eq!(raft_state.get_server_state(), ServerState::Follower);
}

#[test]
fn test_vote_for_candidate() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // First vote should succeed
    assert!(raft_state.vote_for_candidate(123));
    assert_eq!(raft_state.get_voted_for(), Some(123));

    // Voting for same candidate again should succeed
    assert!(raft_state.vote_for_candidate(123));
    assert_eq!(raft_state.get_voted_for(), Some(123));

    // Voting for different candidate in same term should fail
    assert!(!raft_state.vote_for_candidate(456));
    assert_eq!(raft_state.get_voted_for(), Some(123)); // Should remain unchanged
}

#[test]
fn test_transition_to_state() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    raft_state.transition_to_state(ServerState::Candidate);
    assert_eq!(raft_state.get_server_state(), ServerState::Candidate);

    raft_state.transition_to_state(ServerState::Leader);
    assert_eq!(raft_state.get_server_state(), ServerState::Leader);
}

#[test]
fn test_state_snapshot() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // Set up some state
    raft_state.set_current_term(42);
    raft_state.set_voted_for(Some(123));
    raft_state.set_commit_index(10);
    raft_state.set_last_applied(8);
    raft_state.set_server_state(ServerState::Leader);

    // Get snapshot
    let snapshot = raft_state.get_state_snapshot();

    // Verify snapshot
    assert_eq!(snapshot.current_term, 42);
    assert_eq!(snapshot.voted_for, Some(123));
    assert_eq!(snapshot.commit_index, 10);
    assert_eq!(snapshot.last_applied, 8);
    assert_eq!(snapshot.server_state, ServerState::Leader);
}

#[test]
fn test_persistence_and_recovery() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    // Create state and set values
    {
        let mut raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");
        raft_state.set_current_term(100);
        raft_state.set_voted_for(Some(999));
        raft_state.set_commit_index(50);
        raft_state.set_last_applied(45);
        raft_state.set_server_state(ServerState::Candidate);

        // Verify in-memory state is set correctly
        assert_eq!(raft_state.get_server_state(), ServerState::Candidate);
    } // raft_state goes out of scope, persistent state should be saved

    // Load state from file and verify values
    {
        let raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");
        // Persistent state should be recovered
        assert_eq!(raft_state.get_current_term(), 100);
        assert_eq!(raft_state.get_voted_for(), Some(999));
        // Volatile state should ALWAYS reset to 0 on restart
        assert_eq!(raft_state.get_commit_index(), 0);
        assert_eq!(raft_state.get_last_applied(), 0);
        // Server state should ALWAYS start as Follower (volatile, not persisted)
        assert_eq!(raft_state.get_server_state(), ServerState::Follower);
    }
}

#[test]
fn test_multiple_persistence_cycles() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    // First cycle
    {
        let mut raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");
        raft_state.set_current_term(1);
        raft_state.set_voted_for(Some(100));
    }

    // Second cycle - load and modify
    {
        let mut raft_state =
            RaftState::from_existing(&state_path).expect("Failed to load RaftState");
        assert_eq!(raft_state.get_current_term(), 1);
        assert_eq!(raft_state.get_voted_for(), Some(100));

        raft_state.start_new_term(2);
        raft_state.set_commit_index(10);
    }

    // Third cycle - verify persistent changes persisted, volatile state reset
    {
        let raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");
        assert_eq!(raft_state.get_current_term(), 2);
        assert_eq!(raft_state.get_voted_for(), None); // Should be cleared by start_new_term
        assert_eq!(raft_state.get_commit_index(), 0); // Volatile state resets to 0
    }
}

#[test]
fn test_corrupted_magic_number() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    // Create valid state file
    {
        let _raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");
    }

    // Corrupt the magic number
    {
        let mut file_data = fs::read(&state_path).expect("Failed to read state file");
        file_data[0] = 0xFF; // Corrupt first byte of magic number
        fs::write(&state_path, file_data).expect("Failed to write corrupted file");
    }

    // Try to load corrupted file
    let result = RaftState::from_existing(&state_path);
    assert!(result.is_err());
    if let Err(RaftStateError::CorruptedState(msg)) = result {
        assert!(msg.contains("Invalid magic number"));
    } else {
        panic!("Expected CorruptedState error");
    }
}

#[test]
fn test_unsupported_version() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    // Create valid state file
    {
        let _raft_state = RaftState::new(&state_path).expect("Failed to create RaftState");
    }

    // Corrupt the version
    {
        let mut file_data = fs::read(&state_path).expect("Failed to read state file");
        file_data[4] = 0xFF; // Corrupt first byte of version
        fs::write(&state_path, file_data).expect("Failed to write corrupted file");
    }

    // Try to load corrupted file
    let result = RaftState::from_existing(&state_path);
    assert!(result.is_err());
    if let Err(RaftStateError::CorruptedState(msg)) = result {
        assert!(msg.contains("Unsupported version"));
    } else {
        panic!("Expected CorruptedState error");
    }
}

#[test]
fn test_comprehensive_state_operations() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // Simulate a complete Raft scenario

    // Start as follower in term 0
    assert_eq!(raft_state.get_server_state(), ServerState::Follower);
    assert_eq!(raft_state.get_current_term(), 0);

    // Receive vote request for term 1, vote for candidate 100
    raft_state.start_new_term_as_follower(1);
    assert!(raft_state.vote_for_candidate(100));

    // Become candidate in term 2
    raft_state.start_new_term_as_candidate(2);
    assert!(raft_state.vote_for_candidate(999)); // Vote for self

    // Become leader
    raft_state.transition_to_state(ServerState::Leader);

    // Process some log entries
    raft_state.set_commit_index(10);
    raft_state.set_last_applied(8);

    // Verify final state
    assert_eq!(raft_state.get_current_term(), 2);
    assert_eq!(raft_state.get_voted_for(), Some(999));
    assert_eq!(raft_state.get_server_state(), ServerState::Leader);
    assert_eq!(raft_state.get_commit_index(), 10);
    assert_eq!(raft_state.get_last_applied(), 8);
}

#[test]
fn test_edge_cases() {
    let (mut raft_state, _temp_dir) = create_test_state_file();

    // Test maximum values
    raft_state.set_current_term(u64::MAX);
    raft_state.set_voted_for(Some(u32::MAX));
    raft_state.set_commit_index(u64::MAX);
    raft_state.set_last_applied(u64::MAX);

    assert_eq!(raft_state.get_current_term(), u64::MAX);
    assert_eq!(raft_state.get_voted_for(), Some(u32::MAX));
    assert_eq!(raft_state.get_commit_index(), u64::MAX);
    assert_eq!(raft_state.get_last_applied(), u64::MAX);

    // Test zero values
    raft_state.set_current_term(0);
    raft_state.set_voted_for(None);
    raft_state.set_commit_index(0);
    raft_state.set_last_applied(0);

    assert_eq!(raft_state.get_current_term(), 0);
    assert_eq!(raft_state.get_voted_for(), None);
    assert_eq!(raft_state.get_commit_index(), 0);
    assert_eq!(raft_state.get_last_applied(), 0);
}

#[test]
fn test_path_handling() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let state_path = temp_dir.path().join("raft_state.meta");

    // Test normal path handling
    let raft_state = RaftState::new(&state_path);
    assert!(raft_state.is_ok());

    // Test loading existing file
    let loaded_state = RaftState::from_existing(&state_path);
    assert!(loaded_state.is_ok());

    // Test with string path
    let string_path = state_path.to_string_lossy().to_string();
    let raft_state_from_string = RaftState::new(string_path);
    assert!(raft_state_from_string.is_ok());
}
