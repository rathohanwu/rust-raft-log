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
}

#[cfg(test)]
mod tests;
