use crate::models::{NodeId, RaftStateError, ServerState};
use crate::storage::mmap_utils::MemoryMapUtil;
use crate::storage::utils::{create_new_memory_mapped_file, open_existing_memory_mapped_file};
use memmap2::MmapMut;
use std::path::Path;

/// Header size for RaftState file (magic + version + data fields)
/// Note: Only persistent state is stored - volatile state is kept in memory only
const RAFT_STATE_HEADER_SIZE: usize = 20;

/// Magic number for RaftState files
const RAFT_STATE_MAGIC: u32 = 0x52415354; // "RAST" in ASCII

/// Current version of RaftState file format
const RAFT_STATE_VERSION: u32 = 1;

/// RaftState manages persistent Raft consensus state using memory-mapped files
///
/// Persistent state (stored on disk):
/// - Magic number (4 bytes): 0x52415354 ("RAST")
/// - Version (4 bytes): File format version
/// - Current term (8 bytes): Current Raft term
/// - Voted for (4 bytes): NodeId of candidate voted for (0 = None)
///
/// Volatile state (in-memory only, resets on restart):
/// - Server state: Always starts as Follower
/// - Commit index: Always starts at 0
/// - Last applied: Always starts at 0
pub struct RaftState {
    buffer: MmapMut,
    /// Volatile server state - always starts as Follower, never persisted
    server_state: ServerState,
    /// Volatile commit index - always starts at 0, never persisted
    commit_index: u64,
    /// Volatile last applied index - always starts at 0, never persisted
    last_applied: u64,
}

impl RaftState {
    /// Creates a new RaftState with default values
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self, RaftStateError> {
        let buffer = create_new_memory_mapped_file(file_path, RAFT_STATE_HEADER_SIZE as u64)
            .map_err(|e| {
                RaftStateError::StateFileError(format!("Failed to create state file: {}", e))
            })?;

        let mut raft_state = RaftState {
            buffer,
            server_state: ServerState::Follower, // Always start as Follower
            commit_index: 0,                     // Always start at 0
            last_applied: 0,                     // Always start at 0
        };
        raft_state.initialize_new_state();
        Ok(raft_state)
    }

    /// Loads existing RaftState from file
    pub fn from_existing<P: AsRef<Path>>(file_path: P) -> Result<Self, RaftStateError> {
        let buffer = open_existing_memory_mapped_file(file_path, RAFT_STATE_HEADER_SIZE as u64)
            .map_err(|e| {
                RaftStateError::StateFileError(format!("Failed to open state file: {}", e))
            })?;

        let raft_state = RaftState {
            buffer,
            server_state: ServerState::Follower, // Always start as Follower on restart
            commit_index: 0,                     // Always start at 0 on restart
            last_applied: 0,                     // Always start at 0 on restart
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
        MemoryMapUtil::write_u64(&mut self.buffer, 8, 0); // current_term = 0
        MemoryMapUtil::write_u32(&mut self.buffer, 16, 0); // voted_for = None (0)

        // A freshly initialized state file is also persistent state.
        self.flush().expect("failed to flush initial Raft state");

        // Note: Volatile state (server_state, commit_index, last_applied) is NOT persisted to disk
        // These fields are kept in memory only and always start with default values on server startup/restart
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

    /// Persists currentTerm and votedFor. Call this before replying to an RPC
    /// whose outcome depends on either value.
    pub fn flush(&mut self) -> Result<(), RaftStateError> {
        MemoryMapUtil::flush(&mut self.buffer)
            .map_err(|e| RaftStateError::IoError(format!("Failed to flush Raft state: {}", e)))
    }

    /// Gets the commit index (volatile, in-memory only)
    pub fn get_commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Sets the commit index (volatile, in-memory only)
    pub fn set_commit_index(&mut self, index: u64) {
        self.commit_index = index;
    }

    /// Gets the last applied index (volatile, in-memory only)
    pub fn get_last_applied(&self) -> u64 {
        self.last_applied
    }

    /// Sets the last applied index (volatile, in-memory only)
    pub fn set_last_applied(&mut self, index: u64) {
        self.last_applied = index;
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
mod tests;
