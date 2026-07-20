# RaftNode design review

## Reviewer concurrence

This document is the product of an adversarial review between two reviewers (Codex,
authoring; Claude, verifying). After two rounds it reflects an agreed design assessment.
Claude independently verified the load-bearing claims against source:

- **Finding 1** confirmed: `truncate_from` (log.rs:329-334) drops segments from the
  in-memory map only ("File cleanup is handled separately since we don't track file paths
  by base_index") and `load_existing_segments` (log.rs:110-161) reloads every `.dat`,
  so a truncated suffix is resurrected on restart. Correctly ranked first.
- **Findings 2, 3, 4, 8, 9** confirmed as in-`node.rs` defects; the response-regression
  and `last_log_index` hint fixes are cheap and were re-prioritized to the top tier.
- **Findings 5, 6** confirmed against `get_raft_node` (server.rs:152) and the public
  `become_leader`/`create_vote_request`; the mTLS/transport-auth recommendation was
  correctly demoted to out-of-immediate-scope.

Two agreed nuances: finding 2's per-peer correlation only needs the simple monotonic
`max()` guards for the current one-in-flight design (full sequence tracking is deferred);
finding 10's apply-gap is unreachable in normal operation (commit_index is bounded by
`last_log_index`) and matters only as fail-closed handling under storage corruption.

## Summary

The normal RPC paths follow important Figure 2 rules: hard state is usually flushed before a vote grant, followers validate `prevLogIndex`/`prevLogTerm`, and leaders commit only current-term entries by majority. The most serious defect is a log-conflict/restart bug that can resurrect a discarded suffix. The next highest-value work is inexpensive and local to `node.rs`: make replication progress monotonic/correlated, use the existing failure hint, centralize persistence results, validate configured voter IDs, and close unsafe public transitions.

The ordering below reflects severity, scope, and cost—not severity alone. Each finding distinguishes an immediate, in-scope change from an intentionally larger direction.

1. **Severity: correctness — discarded log segments reappear after restart, breaking log matching and potentially state-machine safety.**

   **Location:** `RaftNode::handle_append_entries`, `src/consensus/node.rs:300-337`; `RaftLog::truncate_from`, `src/storage/log.rs:292-345`, especially `329-334`; `RaftLog::load_existing_segments`, `src/storage/log.rs:110-161`.

   **Problem:** When a conflict crosses a segment boundary, `truncate_from` removes whole suffix segments only from the in-memory map. Lines 329-334 explicitly say, “File cleanup is handled separately since we don't track file paths by base_index”; no cleanup happens. On restart, `load_existing_segments` reloads **every** `.dat` file and inserts each by base index into a map. Old and replacement files with the same base index collide, so directory iteration/insertion order determines which suffix survives.

   **Failure scenario:** A follower has entries 1..200 and an uncommitted old suffix 101..200 in a separate segment. A valid new leader replaces entries 101..120 after matching at 100. The follower replies success, then crashes. Restart reloads the old 101 segment as well as the replacement. It can advertise the abandoned suffix, lose the acknowledged replacement, vote based on the wrong last log term/index, or apply a divergent command at a reused index.

   **A. In-scope minimal fixes:** Track the path alongside each in-memory segment; after a successful, durable replacement, unlink obsolete whole-segment files and fsync the log directory. Make startup reject duplicate base indexes, non-contiguous indexes, and unreadable/corrupt segment headers rather than silently loading whichever file wins. Add a regression test: conflict/truncate across a segment boundary, append replacement, drop/reopen, and verify only the replacement suffix exists.

   **B. Larger architectural directions:** Use a manifest/generation or temporary-file/rename protocol so replacement, directory metadata, and recovery are crash-atomic at every intermediate point. This is the robust storage redesign, beyond the first node-level PR, but the first PR must at least prevent stale files from being reloaded.

2. **Severity: correctness/efficiency — reordered or duplicated AppendEntries responses can regress `match_index` and `next_index`.**

   **Location:** `RaftNode::handle_append_entries_response`, `src/consensus/node.rs:580-629`, especially `609-627`.

   **Problem:** A same-term success overwrites peer progress from the supplied request, even if it is older and shorter; a delayed failure unconditionally decrements `next_index`. Neither value is monotonic and responses have no outstanding-request correlation. This is a genuine in-struct protocol-state bug, not merely a transport concern.

   **Failure scenario:** The leader sends A through index 100 and B through 150. B succeeds first, setting progress to 150. A's duplicated success then resets it to 100; a delayed failure for A reduces it again. The leader retransmits entries and can postpone committing 150 until another successful round.

   **A. In-scope minimal fixes:** On success, set `match_index = max(old_match, request_end)` and `next_index = max(old_next, match_index + 1)`; never put `next_index` below `match_index + 1`. Carry a per-peer outstanding request range/generation in the local outbound-work bookkeeping and ignore responses that do not match it. These changes are local to leader progress handling and should be tested with success/failure responses delivered in reverse and duplicated order.

   **B. Larger architectural directions:** A pipelined replicator with multiple in-flight requests needs an explicit per-peer progress state machine and acknowledgements by sequence number. That can follow later; one-in-flight-per-peer correlation is sufficient for the immediate fix.

3. **Severity: efficiency — failed AppendEntries backs up one index per RTT despite already receiving a follower last-index hint.**

   **Location:** `RaftNode::handle_append_entries_response`, `src/consensus/node.rs:618-627`; `AppendEntriesResponse::last_log_index`, `src/models/types_rpc.rs:42-51`.

   **Problem:** Failure always decrements `next_index` by one. The response already has `last_log_index`, but it is ignored. There is no conflict-term/first-conflict-index extension either.

   **Failure scenario:** A leader is ten million entries ahead of a follower. It can require roughly ten million failed RPC round trips before reaching the follower's end, even though the response immediately tells it that end. Rejoin becomes impractically slow.

   **A. In-scope minimal fixes:** For a response correlated to the current outstanding request, set `next_index` to a bounded jump based on `response.last_log_index.map(|i| i + 1)`, while preserving `next_index >= match_index + 1` and falling back to one-step decrement when absent/untrustworthy. This is a cheap `node.rs` change because the field already exists.

   **B. Larger architectural directions:** Extend the response with `conflict_term` and that term's first index, then jump to the leader's last entry of the conflict term or directly to the first conflicting index—the fuller Raft optimization.

4. **Severity: correctness/concurrency — persistence errors do not reliably stop the node, and persistence is scattered across protocol return paths.**

   **Location:** `RaftNode::persist_state_or_stop`, `src/consensus/node.rs:130-136`; `RaftNode::handle_append_entries`, `265-335`; ignored no-op append error in `RaftNode::become_leader`, `489-494`.

   **Problem:** `expect` unwinds a task but does not set the server unavailable; it poisons the external standard mutex and later `lock().unwrap()` calls cascade. `become_leader` swallows a no-op append failure and nevertheless exposes Leader state. Multiple hand-written `term_changed` flush branches make a future reply-before-flush regression likely.

   **Failure scenario:** ENOSPC causes the leader's current-term no-op append to fail, but it remains leader and accepts client writes. Or a follower observes a higher term, its state flush fails, and the task panics while the process continues in a poisoned/uncontrolled state rather than behaving fail-stop.

   **A. In-scope minimal fixes:** Make `persist_state_or_stop` and `become_leader` return `Result`; introduce a local `stopped`/unavailable state and reject every subsequent event/RPC after a storage error. Centralize the hard-state decision in one helper that flushes before any response reflecting a changed term/vote. Do not transition/publish Leader until the current-term no-op is appended and flushed. The RPC service can map this error to `Unavailable` without changing the overall threading architecture.

   **B. Larger architectural directions:** Define a transactional WAL/hard-state durability boundary and supervise the process as explicitly fail-stop. This provides stronger guarantees across multi-file failures but is beyond a contained first PR.

5. **Severity: correctness — voter membership is not checked before IDs affect protocol state or quorum accounting.**

   **Location:** `RaftNode::handle_vote_response`, `src/consensus/node.rs:396-440`; `RaftNode::handle_request_vote`, `154-210`; `RaftNode::handle_append_entries`, `213-257`; `ClusterConfig::get_node`, `src/models/types.rs:371-374`.

   **Problem:** `handle_vote_response` inserts any caller-supplied `from_node` into `votes_received`; incoming candidate and leader IDs are also accepted without checking the configured voter set. The current code assumes all callers and claimed IDs are valid.

   **Failure scenario:** In a three-node cluster, a candidate has its self-vote. Two duplicate/misrouted grants passed as IDs 98 and 99 are counted as two distinct voters, reaching majority without a real peer vote. The candidate becomes leader in the term, violating Election Safety.

   **A. In-scope minimal fixes:** Validate configuration at construction (self exactly once, unique nonzero node IDs). In `handle_vote_response`, reject `from_node` unless it is a configured *other* voter and only count it once. Reject a RequestVote `candidate_id` or AppendEntries `leader_id` not in the configured voter set before it can change term, vote, leader hint, or election timeout. Keep an active-round set of peers actually solicited and require membership in it for vote responses.

   **B. Larger architectural directions:** Bind claimed node IDs to authenticated transport identity (for example mTLS/SPIFFE) and reject impersonation at the RPC boundary. That is a deployment/security concern outside this RaftNode-focused immediate scope, but it is needed if the network is not trusted.

6. **Severity: correctness — raw public transitions can bypass Raft election preconditions.**

   **Location:** `RaftNode::become_leader`, `src/consensus/node.rs:462-507`; `RaftNode::create_vote_request`, `354-393`; public access to the mutex at `RaftGrpcServer::get_raft_node`, `src/grpc/server.rs:151-154` (the method body is line 152).

   **Problem:** `become_leader` is public and checks only that the node is not already leader; it does not require Candidate state, the active election term, a durable self-vote, or configured-peer quorum. `create_vote_request` is likewise callable while leader. Correctness relies on every external caller following the event loop's convention.

   **Failure scenario:** An integration obtains `get_raft_node()` and calls `become_leader` on two nodes in term 7. Both append/replicate as leaders, invalidating the single-leader-per-term assumption that protects log matching.

   **A. In-scope minimal fixes:** Make `become_leader` private; expose only event-oriented entry points. Before the private transition, assert Candidate state, `current_election_term == currentTerm`, durable self-vote, and a quorum of configured voters. Reject or make private `create_vote_request` when currently Leader. Narrow/remove the mutable `get_raft_node` escape hatch where possible.

   **B. Larger architectural directions:** Replace direct node access with a command/event interface owned by the consensus runtime. This complements, but does not require waiting for, the actor model in finding 11.

7. **Severity: correctness — term increment can wrap and violate term monotonicity.**

   **Location:** `RaftNode::create_vote_request`, `src/consensus/node.rs:354-357`.

   **Problem:** `current_term + 1` panics in debug builds but wraps to zero in release builds at `u64::MAX`. Raft terms must increase monotonically.

   **Failure scenario:** A release node at `u64::MAX` times out, persists term zero, clears its vote, and can vote/accept traffic in an historical term.

   **A. In-scope minimal fixes:** Use `checked_add(1)` and return a fatal/unavailable error on exhaustion; never produce a term-zero vote request.

   **B. Larger architectural directions:** None required. This is a contained defensive correctness fix.

8. **Severity: efficiency/simplification — replication batching is hard-coded and ignores configured/request byte limits.**

   **Location:** `RaftNode::create_append_entries_requests`, `src/consensus/node.rs:513-563`, especially `540-547`; `ClusterConfig::max_entries_per_query`, `src/models/types.rs:254-257`.

   **Problem:** The literal `batch_size = 50` bypasses `max_entries_per_query`, retrieves entries one-by-one, and has no payload-byte budget.

   **Failure scenario:** Fifty multi-megabyte entries are cloned and serialized per follower during a heartbeat tick, causing memory/latency spikes. Conversely, a configuration that intentionally caps queries at one entry is ignored.

   **A. In-scope minimal fixes:** Replace the literal with a named config value (initially reuse `max_entries_per_query`) and fetch a contiguous bounded range through one log API. Add a conservative maximum RPC byte budget in `ClusterConfig` and stop before either limit, with an explicit policy for a single oversized entry.

   **B. Larger architectural directions:** Adaptive per-peer windows and byte-based congestion control can tune throughput later; they are not necessary to eliminate the current ignored configuration.

9. **Severity: simplification — role state is duplicated and heartbeat/replication aliases obscure scheduler intent.**

   **Location:** volatile fields `src/consensus/node.rs:27-37`; `RaftNode::is_in_election`, `456-460`; `create_heartbeats`/`create_replication_requests`, `566-578`.

   **Problem:** `current_election_term` mirrors `state.current_term` while candidate and must be kept coherent manually. The two public alias methods both call `create_append_entries_requests`, so a method named heartbeat may clone/send a backlog.

   **Failure scenario:** A caller schedules “heartbeats” expecting empty requests but instead transmits 50 log entries to every lagging peer, obscuring load and making response correlation harder to reason about. Future transition paths can forget to clear/update one of the duplicated election fields.

   **A. In-scope minimal fixes:** Use a role enum for the existing volatile state: `Follower { leader }`, `Candidate { term, granted_voters }`, `Leader { progress_by_peer }`. Replace aliases with one explicit `build_replication_requests`, or make a separate heartbeat builder that guarantees empty entries. This is a refactor of node-local state, not a subsystem rewrite.

   **B. Larger architectural directions:** Pair the role enum with an event-driven actor only after the local representation is stable.

10. **Severity: correctness — committed-application gaps and state-machine failures are silently converted into successful Raft progress.**

   **Location:** `RaftNode::update_commit_index`, `src/consensus/node.rs:340-349`; `RaftNode::apply_committed_entries`, `138-152`.

   **Problem:** `update_commit_index` raises `commit_index` before application, while `apply_committed_entries` simply `break`s if the next committed entry cannot be read. The AppendEntries path can return success with `last_applied < commit_index`. `StateMachine::apply` cannot return an error and may panic while holding the node mutex.

   **Failure scenario:** Storage corruption leaves `get_entry(last_applied + 1)` absent. A follower still acknowledges the leader commit and then accepts later commits although its state machine has skipped an index. An application failure after externally changing state also leaves crash/retry ambiguity.

   **A. In-scope minimal fixes:** Make a missing entry at or below commit index a fatal invariant error; do not reply success or continue normal operation. Change `StateMachine::apply` to return `Result` (or explicitly document fail-stop-only semantics) and route failure through the stopped state from finding 4. Add tests that corrupted/gapped committed logs are rejected.

   **B. Larger architectural directions:** Establish a snapshot and durable applied-checkpoint/recovery contract, including client command IDs for application-level exactly-once semantics. Raft ordering alone does not supply those facilities.

11. **Severity: concurrency — the external mutex is held across fsync and arbitrary state-machine application.**

   **Location:** `RaftNode::apply_committed_entries`, `src/consensus/node.rs:138-151`; durability in `handle_append_entries`, `328-335`, and `persist_state_or_stop`, `130-136`; mutex use in `src/grpc/service.rs:59-68`, `87-99`, and `115-155`.

   **Problem:** The documented synchronous `StateMachine::apply`, mmap/log flushes, and hard-state flush all execute while callers hold `Arc<Mutex<RaftNode>>`. Slow disk, blocking application I/O, or a callback that needs the node can block all RPCs/election timeouts or deadlock. A panic poisons the mutex and subsequent `lock().unwrap()` calls fail.

   **Failure scenario:** Applying a large commit batch performs network I/O while holding the node lock. Higher-term RPCs and heartbeats queue past election deadlines, peers elect another leader, and the original node later processes stale work; a panic poisons all later service operations.

   **A. In-scope minimal fixes:** Document the lock contract at every public mutation entry point, bound application work per event, make `apply` fallible/fail-stop as in finding 10, and replace `unwrap` on poisoned locks with a controlled unavailable response. Avoid holding the lock while constructing/sending outbound RPCs (the event loop mostly already does this).

   **B. Larger architectural directions:** Move to a single-owner Raft actor: RPC/timer handlers send events through channels, the actor serializes consensus/storage mutations, and an ordered application worker receives durable apply work with backpressure. Do not call arbitrary application code while holding consensus ownership. This is explicitly outside the immediate first PR.

## Agreed scope for the first PR

Implement the section-A items in this order:

1. Fix suffix-segment cleanup/reopen validation and add the crash/restart regression test (finding 1).
2. Make per-peer progress monotonic and correlate/ignore stale responses; then consume the existing `last_log_index` failure hint (findings 2-3).
3. Convert persistence/no-op failures into a controlled stopped state and centralize flush-before-reply logic (finding 4).
4. Validate cluster membership/uniqueness and reject non-voter IDs before counting or accepting leadership/votes (finding 5).
5. Make raw election transitions private and checked; use `checked_add` for terms (findings 6-7).
6. Replace the fixed replication batch with configured entry/byte bounds (finding 8).
7. Refactor the volatile role representation and clarify request-builder APIs (finding 9).
8. Fail closed on committed-log gaps/state-machine errors and improve mutex poison handling (findings 10-11).
