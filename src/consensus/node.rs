use super::state::{RaftState, RaftStateSnapshot};
use crate::models::{
    AppendEntriesRequest, AppendEntriesResponse, ClusterConfig, EntryType, LogEntry, NodeId,
    RequestVoteRequest, RequestVoteResponse, ServerState,
};
use crate::storage::RaftLog;
use log::{debug, info};
use std::collections::{HashMap, HashSet};

/// Deterministic application state machine driven by committed Raft commands.
///
/// The same sequence of Normal entries must produce the same result on every
/// node. Implementations run synchronously while the Raft node mutex is held,
/// so they must be fast and must not perform blocking I/O.
pub trait StateMachine: Send {
    fn apply(&mut self, entry: &LogEntry);
}

/// Core Raft node that implements the Raft consensus algorithm
pub struct RaftNode {
    /// Cluster configuration
    config: ClusterConfig,
    /// Persistent log storage
    log: RaftLog,
    /// Persistent state (term, voted_for, etc.)
    state: RaftState,
    /// Volatile leader state (only used when this node is leader)
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    /// Volatile candidate state (only used when this node is candidate)
    votes_received: HashSet<NodeId>,
    /// Current leader ID (volatile state, None if unknown)
    current_leader: Option<NodeId>,
    /// Optional application-owned state machine. None keeps this usable as a
    /// pure consensus core while still advancing last_applied.
    state_machine: Option<Box<dyn StateMachine>>,
    /// A persistence or invariant failure makes it unsafe to emit normal Raft
    /// responses. The embedding service must treat this node as unavailable.
    stopped: bool,
}

impl RaftNode {
    /// Creates a new RaftNode with the given cluster configuration
    pub fn new(config: ClusterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        Self::validate_config(&config)?;
        let log_config = config.to_raft_log_config();
        let log = RaftLog::new(log_config)?;

        // Check if state file exists and load accordingly
        let state = if std::path::Path::new(&config.meta_file_path).exists() {
            RaftState::from_existing(&config.meta_file_path)?
        } else {
            RaftState::new(&config.meta_file_path)?
        };

        Ok(RaftNode {
            config,
            log,
            state,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes_received: HashSet::new(),
            current_leader: None,
            state_machine: None,
            stopped: false,
        })
    }

    /// Creates a node which applies committed Normal entries to `state_machine`.
    pub fn new_with_state_machine(
        config: ClusterConfig,
        state_machine: Box<dyn StateMachine>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut node = Self::new(config)?;
        node.state_machine = Some(state_machine);
        Ok(node)
    }

    /// Gets the current state snapshot
    pub fn get_state(&self) -> RaftStateSnapshot {
        self.state.get_state_snapshot()
    }

    /// Gets the current node ID
    pub fn get_node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Gets the current term
    pub fn get_current_term(&self) -> u64 {
        self.state.get_current_term()
    }

    /// Gets the current leader ID (None if unknown)
    pub fn get_current_leader(&self) -> Option<NodeId> {
        self.current_leader
    }

    /// Gets the cluster configuration
    pub fn get_config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Gets the current server state
    pub fn get_server_state(&self) -> ServerState {
        self.state.get_server_state()
    }

    /// Whether this node has encountered a fatal persistence/invariant failure.
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }

    fn validate_config(config: &ClusterConfig) -> Result<(), Box<dyn std::error::Error>> {
        if config.node_id == 0 {
            return Err("Raft node ID must be nonzero".into());
        }
        let mut voters = HashSet::new();
        for node in &config.nodes {
            if node.node_id == 0 || !voters.insert(node.node_id) {
                return Err("Raft configuration must contain unique nonzero node IDs".into());
            }
        }
        if !voters.contains(&config.node_id) {
            return Err("Raft configuration must contain this node exactly once".into());
        }
        Ok(())
    }

    fn is_other_voter(&self, node_id: NodeId) -> bool {
        node_id != self.config.node_id && self.config.get_node(node_id).is_some()
    }

    fn is_voter(&self, node_id: NodeId) -> bool {
        self.config.get_node(node_id).is_some()
    }

    /// Gets the log length
    pub fn get_log_length(&self) -> u64 {
        self.log.len()
    }

    /// Gets the last log index and term
    pub fn get_last_log_info(&self) -> (u64, u64) {
        match self.log.last_index() {
            Some(index) => {
                if let Some(entry) = self.log.get_entry(index) {
                    (index, entry.term)
                } else {
                    (0, 0)
                }
            }
            None => (0, 0),
        }
    }

    /// Gets a log entry at the specified index
    pub fn get_entry(&self, index: u64) -> Option<LogEntry> {
        self.log.get_entry(index)
    }

    /// Raft cannot safely turn a persistence failure into a normal protocol
    /// response. Stop this node rather than acknowledge unpersisted state.
    fn stop(&mut self) {
        self.stopped = true;
        self.state.transition_to_state(ServerState::Follower);
        self.votes_received.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.current_leader = None;
    }

    fn persist_state_or_stop(&mut self) -> bool {
        if self.state.flush().is_err() {
            self.stop();
            return false;
        }
        true
    }

    fn finish_append_entries(
        &mut self,
        term: u64,
        term_changed: bool,
        log_changed: bool,
        leader_commit: u64,
        success: bool,
    ) -> AppendEntriesResponse {
        if self.stopped {
            return AppendEntriesResponse::failure(term, self.log.last_index());
        }
        if log_changed && self.log.flush().is_err() {
            self.stop();
            return AppendEntriesResponse::failure(term, self.log.last_index());
        }
        if term_changed && !self.persist_state_or_stop() {
            return AppendEntriesResponse::failure(term, self.log.last_index());
        }
        if success {
            self.update_commit_index(leader_commit);
            if self.stopped {
                AppendEntriesResponse::failure(term, self.log.last_index())
            } else {
                AppendEntriesResponse::success(term, self.log.last_index())
            }
        } else {
            AppendEntriesResponse::failure(term, self.log.last_index())
        }
    }

    /// Applies every newly committed entry exactly once, in index order.
    fn apply_committed_entries(&mut self) {
        while self.state.get_last_applied() < self.state.get_commit_index() {
            let index = self.state.get_last_applied() + 1;
            let Some(entry) = self.log.get_entry(index) else {
                // A committed index without a corresponding durable entry is
                // storage corruption. Continuing would let this node report
                // Raft progress while its state machine has skipped an entry.
                self.stop();
                break;
            };
            if entry.entry_type == EntryType::Normal {
                if let Some(state_machine) = self.state_machine.as_mut() {
                    state_machine.apply(&entry);
                }
            }
            self.state.set_last_applied(index);
        }
    }

    /// Handles RequestVote RPC
    pub fn handle_request_vote(&mut self, request: RequestVoteRequest) -> RequestVoteResponse {
        if self.stopped || !self.is_voter(request.candidate_id) {
            return RequestVoteResponse::deny_vote(self.state.get_current_term());
        }
        let current_term = self.state.get_current_term();

        // If request term is older, deny vote
        if request.term < current_term {
            return RequestVoteResponse::deny_vote(current_term);
        }

        // If request term is newer, update our term and become follower
        let term_changed = request.term > current_term;
        let current_term = if term_changed {
            self.state.start_new_term_as_follower(request.term);
            // Clear current leader since we're stepping down
            self.current_leader = None;
            request.term
        } else {
            current_term
        };

        // Check if candidate's log is at least as up-to-date as ours
        let (last_log_index, last_log_term) = self.get_last_log_info();
        let candidate_log_up_to_date = request.last_log_term > last_log_term
            || (request.last_log_term == last_log_term && request.last_log_index >= last_log_index);

        let vote_was_empty = self.state.get_voted_for().is_none();
        let vote_granted =
            candidate_log_up_to_date && self.state.vote_for_candidate(request.candidate_id);
        let hard_state_changed = term_changed || (vote_granted && vote_was_empty);

        // Raft requires the term/vote mutation to reach stable storage before
        // either a grant or a denial that reflects the new term is returned.
        if hard_state_changed && !self.persist_state_or_stop() {
            return RequestVoteResponse::deny_vote(current_term);
        }

        if vote_granted {
            debug!(
                "✅ Node {} granted vote to Node {} for term {}",
                self.config.node_id, request.candidate_id, current_term
            );
            RequestVoteResponse::grant_vote(current_term)
        } else if candidate_log_up_to_date {
            debug!(
                "❌ Node {} denied vote to Node {} for term {} (already voted)",
                self.config.node_id, request.candidate_id, current_term
            );
            RequestVoteResponse::deny_vote(current_term)
        } else {
            // Candidate's log is not up-to-date
            debug!(
                "❌ Node {} denied vote to Node {} for term {} (log not up-to-date)",
                self.config.node_id, request.candidate_id, current_term
            );
            RequestVoteResponse::deny_vote(current_term)
        }
    }

    /// Handles AppendEntries RPC
    pub fn handle_append_entries(
        &mut self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        let current_term = self.state.get_current_term();
        if self.stopped || !self.is_voter(request.leader_id) {
            return AppendEntriesResponse::failure(current_term, self.log.last_index());
        }

        // If request term is older, reject
        if request.term < current_term {
            return AppendEntriesResponse::failure(current_term, self.log.last_index());
        }

        // If request term is newer, update our term and become follower
        let term_changed = request.term > current_term;
        let current_term = if term_changed {
            self.state.start_new_term_as_follower(request.term);
            // Track the new leader
            let previous_leader = self.current_leader;
            self.current_leader = Some(request.leader_id);

            // Log leader change for new term
            if previous_leader != Some(request.leader_id) {
                info!(
                    "🔄 Node {} detected new LEADER: Node {} for term {}",
                    self.config.node_id, request.leader_id, request.term
                );
            }
            request.term
        } else {
            // Valid leader for current term, become follower if not already
            if self.state.get_server_state() != ServerState::Follower {
                self.state.transition_to_state(ServerState::Follower);
            }
            // Track the current leader
            let previous_leader = self.current_leader;
            self.current_leader = Some(request.leader_id);

            // Log leader detection if this is the first time we see this leader
            if previous_leader != Some(request.leader_id) {
                info!(
                    "🔄 Node {} detected LEADER: Node {} for term {}",
                    self.config.node_id, request.leader_id, current_term
                );
            }
            current_term
        };

        // Check if we have the previous log entry
        if request.prev_log_index > 0 {
            match self.log.get_entry(request.prev_log_index) {
                Some(prev_entry) => {
                    if prev_entry.term != request.prev_log_term {
                        // Previous entry term doesn't match
                        return self.finish_append_entries(
                            current_term,
                            term_changed,
                            false,
                            request.leader_commit,
                            false,
                        );
                    }
                }
                None => {
                    // Don't have the previous entry
                    return self.finish_append_entries(
                        current_term,
                        term_changed,
                        false,
                        request.leader_commit,
                        false,
                    );
                }
            }
        }

        // If this is a heartbeat (no entries), just update commit index
        if request.entries.is_empty() {
            return self.finish_append_entries(
                current_term,
                term_changed,
                false,
                request.leader_commit,
                true,
            );
        }

        // Preserve the matching prefix.  Figure 2 only permits truncation at the
        // first same-index entry with a different term.
        let first_new_index = request.prev_log_index + 1;
        let mut first_to_append = request.entries.len();
        let mut log_changed = false;
        for (offset, incoming) in request.entries.iter().enumerate() {
            let index = first_new_index + offset as u64;
            match self.log.get_entry(index) {
                Some(existing) if existing.term == incoming.term => continue,
                Some(_) => {
                    if self.log.truncate_from(index).is_err() {
                        self.stop();
                        return AppendEntriesResponse::failure(current_term, self.log.last_index());
                    }
                    log_changed = true;
                    first_to_append = offset;
                    break;
                }
                None => {
                    first_to_append = offset;
                    break;
                }
            }
        }

        for entry in request.entries.into_iter().skip(first_to_append) {
            debug!("Appending entry from {}: {:?}", self.get_node_id(), entry);
            if self.log.append_entry_unflushed(entry).is_err() {
                self.stop();
                return AppendEntriesResponse::failure(current_term, self.log.last_index());
            }
            log_changed = true;
        }

        self.finish_append_entries(
            current_term,
            term_changed,
            log_changed,
            request.leader_commit,
            true,
        )
    }

    /// Updates the commit index based on leader's commit index
    fn update_commit_index(&mut self, leader_commit: u64) {
        let current_commit = self.state.get_commit_index();
        if leader_commit > current_commit {
            let last_log_index = self.log.last_index().unwrap_or(0);
            let new_commit = std::cmp::min(leader_commit, last_log_index);
            self.state.set_commit_index(new_commit);
            self.apply_committed_entries();
        }
    }

    /// Transitions to candidate state and creates a vote request for the election
    /// Returns None if the election cannot be started or the self-vote wins a
    /// single-node election immediately.
    pub fn create_vote_request(&mut self) -> Option<RequestVoteRequest> {
        if self.stopped || self.state.get_server_state() == ServerState::Leader {
            return None;
        }
        // Increment term, clear vote, and become candidate
        let Some(new_term) = self.state.get_current_term().checked_add(1) else {
            self.stop();
            return None;
        };
        self.state.start_new_term_as_candidate(new_term);

        // Reset election state
        self.votes_received.clear();

        // Clear current leader since we're starting an election
        self.current_leader = None;

        // Vote for ourselves
        if !self.state.vote_for_candidate(self.config.node_id) {
            // This shouldn't happen since we just cleared the vote
            return None;
        }
        if !self.persist_state_or_stop() {
            return None;
        }

        // Log election start
        info!(
            "🗳️  Node {} starting election for term {} (candidate)",
            self.config.node_id, new_term
        );
        self.votes_received.insert(self.config.node_id);

        // In a single-node cluster our self-vote is already a quorum; there
        // will be no RequestVote response to trigger the normal win path.
        if self.votes_received.len() >= self.config.majority_size() {
            self.become_leader();
            return None;
        }

        // Create a single RequestVote request (identical for all other nodes)
        let (last_log_index, last_log_term) = self.get_last_log_info();
        let request =
            RequestVoteRequest::new(new_term, self.config.node_id, last_log_index, last_log_term);

        Some(request)
    }

    /// Handles a RequestVote response and returns true if election is won
    pub fn handle_vote_response(
        &mut self,
        from_node: NodeId,
        response: RequestVoteResponse,
    ) -> bool {
        if self.stopped || !self.is_other_voter(from_node) {
            return false;
        }
        if response.term > self.state.get_current_term() {
            self.state.start_new_term_as_follower(response.term);
            self.votes_received.clear();
            self.current_leader = None;
            self.persist_state_or_stop();
            return false;
        }

        // Only process votes for current election term
        if response.term != self.state.get_current_term()
            || self.state.get_server_state() != ServerState::Candidate
        {
            return false;
        }

        // If vote granted, add to our vote count
        if response.vote_granted {
            self.votes_received.insert(from_node);

            debug!(
                "✅ Node {} received vote from Node {} (votes: {}/{})",
                self.config.node_id,
                from_node,
                self.votes_received.len(),
                self.config.majority_size()
            );

            // Check if we have majority
            let majority_size = self.config.majority_size();
            if self.votes_received.len() >= majority_size {
                info!(
                    "🎉 Node {} won election with {}/{} votes for term {}",
                    self.config.node_id,
                    self.votes_received.len(),
                    self.config.cluster_size(),
                    self.state.get_current_term()
                );
                self.become_leader();
                return true;
            }
        } else {
            debug!(
                "❌ Node {} vote denied by Node {} for term {}",
                self.config.node_id, from_node, response.term
            );
        }

        false
    }

    /// Gets the current vote count for debugging/monitoring
    pub fn get_vote_count(&self) -> usize {
        self.votes_received.len()
    }

    /// Checks if currently in an election
    pub fn is_in_election(&self) -> bool {
        !self.stopped && self.state.get_server_state() == ServerState::Candidate
    }

    /// Becomes leader (initializes leader state)
    fn become_leader(&mut self) -> bool {
        let election_is_valid = self.state.get_server_state() == ServerState::Candidate
            && self.state.get_voted_for() == Some(self.config.node_id)
            && self.votes_received.contains(&self.config.node_id)
            && self.votes_received.len() >= self.config.majority_size();
        debug_assert!(
            election_is_valid,
            "leader transition requires a current-term quorum"
        );
        if self.stopped || !election_is_valid {
            return false;
        }
        let current_term = self.state.get_current_term();
        let node_id = self.config.node_id;
        // Followers must begin at the first entry of this leader term (the
        // no-op appended below), not after it.
        let next_index = self.log.last_index().unwrap_or(0) + 1;

        // A leader must not be externally visible until its current-term no-op
        // has reached stable storage.
        let noop_entry = LogEntry::new_with_type(current_term, 0, EntryType::NoOp, vec![]);
        if self.log.append_entry(noop_entry).is_err() {
            self.stop();
            return false;
        }

        self.state.transition_to_state(ServerState::Leader);

        // Set self as the current leader
        self.current_leader = Some(self.config.node_id);

        // Clear election state
        self.votes_received.clear();

        // Initialize next_index and match_index for all followers
        self.next_index.clear();
        self.match_index.clear();

        for node in self.config.get_other_nodes() {
            self.next_index.insert(node.node_id, next_index);
            self.match_index.insert(node.node_id, 0);
        }

        // A single-node cluster is itself a quorum. There are no follower
        // responses to drive `try_advance_commit_index`, so commit locally.
        if self.config.majority_size() == 1 {
            self.try_advance_commit_index();
        }

        // Log the leadership transition
        info!(
            "👑 Node {} became LEADER for term {}",
            node_id, current_term
        );
        true
    }

    /// Creates AppendEntries requests for all followers (leader only)
    /// Automatically determines whether to send log entries or heartbeats based on each follower's state:
    /// - If follower's next_index <= leader's last_log_index: sends log entries (replication)
    /// - If follower's next_index > leader's last_log_index: sends empty entries (heartbeat)
    pub fn build_replication_requests(&self) -> Vec<(NodeId, AppendEntriesRequest)> {
        if self.stopped || self.state.get_server_state() != ServerState::Leader {
            return vec![];
        }

        let current_term = self.state.get_current_term();
        let leader_commit = self.state.get_commit_index();
        let last_log_index = self.log.last_index().unwrap_or(0);
        let mut requests = Vec::new();

        for node in self.config.get_other_nodes() {
            let next_idx = self.next_index.get(&node.node_id).copied().unwrap_or(1);
            let prev_log_index = if next_idx > 1 { next_idx - 1 } else { 0 };
            let prev_log_term = if prev_log_index > 0 {
                self.log
                    .get_entry(prev_log_index)
                    .map(|e| e.term)
                    .unwrap_or(0)
            } else {
                0
            };

            // Automatically determine entries to send based on follower's next_index
            let mut entries = Vec::new();

            if next_idx <= last_log_index {
                // Follower is behind: send log entries for replication
                let batch_size = self.config.max_entries_per_query.max(1) as u64;
                let end_index = std::cmp::min(next_idx + batch_size - 1, last_log_index);
                entries = self
                    .log
                    .get_entries(next_idx, end_index)
                    .unwrap_or_default();
            }
            // If next_idx > last_log_index: follower is up-to-date, entries remains empty (heartbeat)

            let request = AppendEntriesRequest::new(
                current_term,
                self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            );

            requests.push((node.node_id, request));
        }

        requests
    }

    /// Backwards-compatible name for the mixed heartbeat/replication builder.
    pub fn create_append_entries_requests(&self) -> Vec<(NodeId, AppendEntriesRequest)> {
        self.build_replication_requests()
    }

    /// Creates heartbeat requests for all followers (leader only)
    /// This is an alias for create_append_entries_requests() - the method automatically
    /// sends heartbeats (empty entries) to followers who are up-to-date
    pub fn create_heartbeats(&self) -> Vec<(NodeId, AppendEntriesRequest)> {
        self.build_replication_requests()
    }

    /// Creates log replication requests for all followers (leader only)
    /// This is an alias for create_append_entries_requests() - the method automatically
    /// sends log entries to followers who are behind
    pub fn create_replication_requests(&self) -> Vec<(NodeId, AppendEntriesRequest)> {
        self.build_replication_requests()
    }

    /// Handles AppendEntries response from a follower (leader only)
    pub fn handle_append_entries_response(
        &mut self,
        from_node: NodeId,
        request: &AppendEntriesRequest,
        response: AppendEntriesResponse,
    ) -> bool {
        if self.stopped
            || !self.is_other_voter(from_node)
            || self.state.get_server_state() != ServerState::Leader
        {
            return false;
        }

        if response.term > self.state.get_current_term() {
            let old_term = self.state.get_current_term();
            self.state.start_new_term_as_follower(response.term);
            self.next_index.clear();
            self.match_index.clear();
            self.current_leader = None;
            self.persist_state_or_stop();

            info!("📉 Node {} stepped down from LEADER (term {} -> {}) due to higher term from Node {}",
                  self.config.node_id, old_term, response.term, from_node);
            return false;
        }

        // Ignore stale responses
        if response.term < self.state.get_current_term() {
            return false;
        }

        if response.success {
            // Success: update next_index and match_index
            let new_match_index = request.prev_log_index + request.entries.len() as u64;
            let old_match = self.match_index.get(&from_node).copied().unwrap_or(0);
            let old_next = self.next_index.get(&from_node).copied().unwrap_or(1);
            let match_index = old_match.max(new_match_index);
            self.match_index.insert(from_node, match_index);
            self.next_index
                .insert(from_node, old_next.max(match_index.saturating_add(1)));

            // Try to advance commit index
            self.try_advance_commit_index();
            true
        } else {
            // Failure: jump to the follower's reported end when available,
            // but never below an index already known replicated.
            let current_next = self.next_index.get(&from_node).copied().unwrap_or(1);
            let fallback = if current_next > 1 {
                current_next - 1
            } else {
                1
            };
            let hinted = response
                .last_log_index
                .and_then(|index| index.checked_add(1))
                .map(|index| index.min(current_next))
                .unwrap_or(fallback);
            let known_match = self.match_index.get(&from_node).copied().unwrap_or(0);
            self.next_index
                .insert(from_node, hinted.max(known_match.saturating_add(1)));
            false
        }
    }

    /// Attempts to advance the commit index based on majority replication
    fn try_advance_commit_index(&mut self) {
        let current_commit = self.state.get_commit_index();
        let last_log_index = self.log.last_index().unwrap_or(0);

        // Try to find the highest index that's replicated on a majority
        for candidate_index in (current_commit + 1)..=last_log_index {
            // Check if this entry is from current term (safety requirement)
            if let Some(entry) = self.log.get_entry(candidate_index) {
                if entry.term != self.state.get_current_term() {
                    continue;
                }
            } else {
                continue;
            }

            // Count how many nodes have this entry (including leader)
            let mut replication_count = 1; // Leader always has the entry

            for (_node_id, &match_index) in &self.match_index {
                if match_index >= candidate_index {
                    replication_count += 1;
                }
            }

            // If majority has this entry, we can commit it
            if replication_count >= self.config.majority_size() {
                self.state.set_commit_index(candidate_index);
                self.apply_committed_entries();
            } else {
                // If this index doesn't have majority, higher indices won't either
                break;
            }
        }
    }

    /// Appends a new entry to the log (leader only)
    pub fn append_new_entry(
        &mut self,
        payload: Vec<u8>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        if self.stopped || self.state.get_server_state() != ServerState::Leader {
            return Err("Only leader can append entries".into());
        }

        let entry = LogEntry::new_with_type(
            self.state.get_current_term(),
            0, // Index will be set by append_entry
            EntryType::Normal,
            payload,
        );

        if let Err(error) = self.log.append_entry(entry) {
            self.stop();
            return Err(Box::new(error));
        }
        // As above, a one-node leader needs no replication response to establish
        // majority durability.
        if self.config.majority_size() == 1 {
            self.try_advance_commit_index();
        }
        Ok(self.log.last_index().unwrap_or(0))
    }

    /// Gets replication status for all followers (leader only)
    pub fn get_replication_status(&self) -> HashMap<NodeId, (u64, u64)> {
        let mut status = HashMap::new();

        for node in self.config.get_other_nodes() {
            let next_index = self.next_index.get(&node.node_id).copied().unwrap_or(1);
            let match_index = self.match_index.get(&node.node_id).copied().unwrap_or(0);
            status.insert(node.node_id, (next_index, match_index));
        }

        status
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(node.get_vote_count(), 1);
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
        let response = node.finish_append_entries(0, false, false, 1, true);
        assert!(!response.success);
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
        let append_requests = node.create_append_entries_requests();
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
        let new_entry =
            LogEntry::new_with_type(1, 3, EntryType::Normal, "cmd3".as_bytes().to_vec());
        let append_request = AppendEntriesRequest::new(1, 2, 2, 1, vec![new_entry], 0);

        let response = node.handle_append_entries(append_request);
        assert!(response.success);
        assert_eq!(node.get_log_length(), 3);

        // Test AppendEntries with incorrect previous entry
        let new_entry2 =
            LogEntry::new_with_type(1, 4, EntryType::Normal, "cmd4".as_bytes().to_vec());
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
        let heartbeats = node1.create_heartbeats();
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
        let append_requests = node1.create_append_entries_requests();
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
        assert_eq!(node1.get_vote_count(), 1); // Voted for self
        assert!(node1.is_in_election());

        // Node 2 grants vote
        let vote_response_2 = node2.handle_request_vote(vote_request.clone());
        assert!(vote_response_2.vote_granted);

        // Node 1 receives vote from node 2
        let won_election = node1.handle_vote_response(2, vote_response_2);
        assert!(won_election); // Should win with 2/3 votes
        assert_eq!(node1.get_server_state(), ServerState::Leader);
        assert!(!node1.is_in_election());

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
        let append_requests = leader.create_append_entries_requests();
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
        let status = leader.get_replication_status();
        assert_eq!(status.get(&2), Some(&(4, 3))); // next_index=4, match_index=3
        assert_eq!(status.get(&3), Some(&(4, 3)));

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
        let append_requests = leader.create_append_entries_requests();
        let (_, request) = &append_requests[0]; // Request for node 2

        // This should fail because follower doesn't have prev_log_index (4)
        let response = follower.handle_append_entries(request.clone());
        assert!(!response.success);

        // Leader uses the follower's last-index hint to jump to index 2.
        let success = leader.handle_append_entries_response(2, request, response);
        assert!(!success);

        // Check that next_index used the hint.
        let status = leader.get_replication_status();
        assert_eq!(status.get(&2), Some(&(2, 0)));

        // Retry from the hinted index.
        for _ in 0..1 {
            let retry_requests = leader.create_append_entries_requests();
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
        assert_eq!(leader.get_replication_status().get(&2), Some(&(3, 2)));

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
        assert_eq!(leader.get_replication_status().get(&2), Some(&(3, 2)));
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
        let initial_requests = leader.create_append_entries_requests();
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
        let replication_requests = leader.create_append_entries_requests();
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
        let heartbeat_requests = leader.create_append_entries_requests();
        assert_eq!(heartbeat_requests.len(), 2);
        for (_, request) in &heartbeat_requests {
            assert!(request.is_heartbeat());
            assert_eq!(request.entries.len(), 0);
        }

        // Test that convenience methods work the same way
        let convenience_requests = leader.create_heartbeats();
        assert_eq!(heartbeat_requests, convenience_requests);
    }
}
