use super::models::{NodeId, RaftStateError, ServerState};
use super::mmap_utils::MemoryMapUtil;
use super::utils::create_memory_mapped_file;
use memmap2::MmapMut;
use std::path::Path;

/// Header size for RaftState file (magic + version + data fields)
/// Note: Server state is NOT persisted - it's kept in memory only
const RAFT_STATE_HEADER_SIZE: usize = 36;

/// Magic number for RaftState files
const RAFT_STATE_MAGIC: u32 = 0x52415354; // "RAST" in ASCII

/// Current version of RaftState file format
const RAFT_STATE_VERSION: u32 = 1;

/// RaftState manages persistent Raft consensus state using memory-mapped files
///
/// File format:
/// - Magic number (4 bytes): 0x52415354 ("RAST")
/// - Version (4 bytes): File format version
/// - Current term (8 bytes): Current Raft term
/// - Voted for (4 bytes): NodeId of candidate voted for (0 = None)
/// - Commit index (8 bytes): Index of highest log entry known to be committed
/// - Last applied (8 bytes): Index of highest log entry applied to state machine
///
/// Note: Server state (Follower/Candidate/Leader) is NOT persisted to disk.
/// It is kept as a volatile in-memory field and always starts as Follower on restart.
pub struct RaftState {
    buffer: MmapMut,
    /// Volatile server state - always starts as Follower, never persisted
    server_state: ServerState,
}

impl RaftState {
    /// Creates a new RaftState with default values
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self, RaftStateError> {
        let path_str = file_path.as_ref()
            .to_str()
            .ok_or_else(|| RaftStateError::StateFileError("Invalid UTF-8 path".to_string()))?;

        let buffer = create_memory_mapped_file(
            path_str,
            RAFT_STATE_HEADER_SIZE as u64,
        )
        .map_err(|e| RaftStateError::StateFileError(format!("Failed to create state file: {}", e)))?;

        let mut raft_state = RaftState {
            buffer,
            server_state: ServerState::Follower, // Always start as Follower
        };
        raft_state.initialize_new_state();
        Ok(raft_state)
    }

    /// Loads existing RaftState from file
    pub fn from_existing<P: AsRef<Path>>(file_path: P) -> Result<Self, RaftStateError> {
        let path_str = file_path.as_ref()
            .to_str()
            .ok_or_else(|| RaftStateError::StateFileError("Invalid UTF-8 path".to_string()))?;

        let buffer = create_memory_mapped_file(
            path_str,
            RAFT_STATE_HEADER_SIZE as u64,
        )
        .map_err(|e| RaftStateError::StateFileError(format!("Failed to open state file: {}", e)))?;

        let raft_state = RaftState {
            buffer,
            server_state: ServerState::Follower, // Always start as Follower on restart
        };
        raft_state.validate_header()?;
        Ok(raft_state)
    }

    /// Initializes a new state file with default values
    fn initialize_new_state(&mut self) {
        // Write header
        MemoryMapUtil::write_u32(&mut self.buffer, 0, RAFT_STATE_MAGIC);
        MemoryMapUtil::write_u32(&mut self.buffer, 4, RAFT_STATE_VERSION);

        // Initialize persistent state with default values
        MemoryMapUtil::write_u64(&mut self.buffer, 8, 0);  // current_term = 0
        MemoryMapUtil::write_u32(&mut self.buffer, 16, 0); // voted_for = None (0)
        MemoryMapUtil::write_u64(&mut self.buffer, 20, 0); // commit_index = 0
        MemoryMapUtil::write_u64(&mut self.buffer, 28, 0); // last_applied = 0

        // Note: server_state is NOT persisted to disk - it's kept in memory only
        // and always starts as Follower on server startup/restart
    }

    /// Validates the header of an existing state file
    fn validate_header(&self) -> Result<(), RaftStateError> {
        let magic = MemoryMapUtil::read_u32(&self.buffer, 0);
        if magic != RAFT_STATE_MAGIC {
            return Err(RaftStateError::CorruptedState(format!(
                "Invalid magic number: expected {}, got {}",
                RAFT_STATE_MAGIC, magic
            )));
        }

        let version = MemoryMapUtil::read_u32(&self.buffer, 4);
        if version != RAFT_STATE_VERSION {
            return Err(RaftStateError::CorruptedState(format!(
                "Unsupported version: expected {}, got {}",
                RAFT_STATE_VERSION, version
            )));
        }

        Ok(())
    }

    /// Gets the current term
    pub fn get_current_term(&self) -> u64 {
        MemoryMapUtil::read_u64(&self.buffer, 8)
    }

    /// Sets the current term
    pub fn set_current_term(&mut self, term: u64) {
        MemoryMapUtil::write_u64(&mut self.buffer, 8, term);
    }

    /// Gets the NodeId of the candidate voted for in current term (None if no vote cast)
    pub fn get_voted_for(&self) -> Option<NodeId> {
        let voted_for = MemoryMapUtil::read_u32(&self.buffer, 16);
        if voted_for == 0 {
            None
        } else {
            Some(voted_for)
        }
    }

    /// Sets the NodeId of the candidate voted for in current term (None to clear vote)
    pub fn set_voted_for(&mut self, node_id: Option<NodeId>) {
        let value = node_id.unwrap_or(0);
        MemoryMapUtil::write_u32(&mut self.buffer, 16, value);
    }

    /// Gets the commit index
    pub fn get_commit_index(&self) -> u64 {
        MemoryMapUtil::read_u64(&self.buffer, 20)
    }

    /// Sets the commit index
    pub fn set_commit_index(&mut self, index: u64) {
        MemoryMapUtil::write_u64(&mut self.buffer, 20, index);
    }

    /// Gets the last applied index
    pub fn get_last_applied(&self) -> u64 {
        MemoryMapUtil::read_u64(&self.buffer, 28)
    }

    /// Sets the last applied index
    pub fn set_last_applied(&mut self, index: u64) {
        MemoryMapUtil::write_u64(&mut self.buffer, 28, index);
    }

    /// Gets the current server state (volatile, in-memory only)
    pub fn get_server_state(&self) -> ServerState {
        self.server_state
    }

    /// Sets the current server state (volatile, in-memory only)
    pub fn set_server_state(&mut self, state: ServerState) {
        self.server_state = state;
    }

    /// Atomically updates term and clears voted_for (used when starting new term)
    pub fn start_new_term(&mut self, new_term: u64) {
        self.set_current_term(new_term);
        self.set_voted_for(None);
    }

    /// Atomically updates term, clears voted_for, and transitions to candidate
    pub fn start_new_term_as_candidate(&mut self, new_term: u64) {
        self.set_current_term(new_term);
        self.set_voted_for(None);
        self.set_server_state(ServerState::Candidate);
    }

    /// Atomically updates term, clears voted_for, and transitions to follower
    pub fn start_new_term_as_follower(&mut self, new_term: u64) {
        self.set_current_term(new_term);
        self.set_voted_for(None);
        self.set_server_state(ServerState::Follower);
    }

    /// Atomically votes for a candidate in the current term
    /// Returns true if vote was granted, false if already voted for someone else
    pub fn vote_for_candidate(&mut self, candidate_id: NodeId) -> bool {
        match self.get_voted_for() {
            None => {
                // Haven't voted yet, grant vote
                self.set_voted_for(Some(candidate_id));
                true
            }
            Some(existing_vote) => {
                // Already voted - only grant if voting for same candidate
                existing_vote == candidate_id
            }
        }
    }

    /// Transitions to a new server state
    pub fn transition_to_state(&mut self, new_state: ServerState) {
        self.set_server_state(new_state);
    }

    /// Gets a snapshot of all current state values
    pub fn get_state_snapshot(&self) -> RaftStateSnapshot {
        RaftStateSnapshot {
            current_term: self.get_current_term(),
            voted_for: self.get_voted_for(),
            commit_index: self.get_commit_index(),
            last_applied: self.get_last_applied(),
            server_state: self.get_server_state(),
        }
    }
}

/// Snapshot of RaftState at a point in time
#[derive(Debug, Clone, PartialEq)]
pub struct RaftStateSnapshot {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub server_state: ServerState,
}

#[cfg(test)]
mod tests {
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

        // Verify that server state is not persisted to disk (file is only 36 bytes)
        assert_eq!(raft_state.buffer.len(), 36);
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
            let mut raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");
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

            // Verify file size is exactly what we expect (no server state persisted)
            assert_eq!(raft_state.buffer.len(), 36); // Only persistent fields
        }

        // Restart and verify persistent state is preserved but server state resets
        {
            let raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");

            // Persistent state should be preserved
            assert_eq!(raft_state.get_current_term(), 42);
            assert_eq!(raft_state.get_voted_for(), Some(123));
            assert_eq!(raft_state.get_commit_index(), 10);
            assert_eq!(raft_state.get_last_applied(), 5);

            // Server state should ALWAYS reset to Follower
            assert_eq!(raft_state.get_server_state(), ServerState::Follower);

            // File should still be the same size
            assert_eq!(raft_state.buffer.len(), 36);
        }

        // Test that changing server state doesn't affect file size or persistent data
        {
            let mut raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");

            // Change server state multiple times
            raft_state.set_server_state(ServerState::Candidate);
            raft_state.set_server_state(ServerState::Leader);
            raft_state.set_server_state(ServerState::Follower);

            // File size should remain unchanged
            assert_eq!(raft_state.buffer.len(), 36);

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
            assert_eq!(raft_state.get_commit_index(), 50);
            assert_eq!(raft_state.get_last_applied(), 45);
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
            let mut raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");
            assert_eq!(raft_state.get_current_term(), 1);
            assert_eq!(raft_state.get_voted_for(), Some(100));

            raft_state.start_new_term(2);
            raft_state.set_commit_index(10);
        }

        // Third cycle - verify changes persisted
        {
            let raft_state = RaftState::from_existing(&state_path).expect("Failed to load RaftState");
            assert_eq!(raft_state.get_current_term(), 2);
            assert_eq!(raft_state.get_voted_for(), None); // Should be cleared by start_new_term
            assert_eq!(raft_state.get_commit_index(), 10);
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
}
