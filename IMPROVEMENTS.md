# Improvements & Findings

Findings from a full read of the implementation (`state.rs`, `log.rs`, `segment.rs`,
`mmap_utils.rs`, `node.rs`, `event_loop.rs`, `service.rs`, `main.rs`). Ordered by
severity. Line references are from the state of the code at the time of review.

The correctness bar these are measured against is defined in `VERIFICATION.md`.

---

## Critical correctness bugs

These break Raft's safety or liveness guarantees.

### 1. Persistent state is never flushed to disk
**Files:** `src/consensus/state.rs:125` (`set_current_term`), `state.rs:140`
(`set_voted_for`); `src/storage/mmap_utils.rs` (no flush fn); `src/storage/log.rs`
(appends not flushed).

`currentTerm`, `votedFor`, and log entries are written into an mmap but nothing calls
`mmap.flush()` / `msync`. Raft requires these to be on stable storage *before*
replying to a vote or append. With mmap-without-flush, writes sit in the page cache;
a crash at the wrong moment lets a node forget it already voted → **votes twice in
one term → two leaders → split brain and committed-data loss**. Highest-priority fix.

**Fix direction:** add a flush helper to `MemoryMapUtil`; flush the state file before
returning from `handle_request_vote` / `handle_append_entries`, and flush the log
before the leader counts an entry as durable.

### 2. AppendEntries truncates unconditionally — can delete committed entries
**File:** `src/consensus/node.rs:224-233`.

```rust
if first_new_index <= last_log_index {
    self.log.truncate_from(first_new_index) // truncates on ANY overlap
}
```

Figure 2 rule 3 says: delete an existing entry only when an incoming entry at the
same index has a **different term**. Here any overlapping AppendEntries — including a
routine delayed duplicate or a retransmit — truncates and re-appends, which can wipe
already-committed, already-matched entries. Violates Log Matching / State Machine
Safety.

**Fix direction:** walk incoming entries against the local log; truncate only at the
first index where terms differ; keep the matching prefix.

### 3. Runaway elections / term inflation
**File:** `src/grpc/event_loop.rs:85-98`.

Every ~10 ms loop iteration, if the timeout is expired and the node is not leader, it
sets state to `Candidate` and calls `create_vote_request()`, which does
`new_term = current_term + 1`. There is no "already campaigning" guard and
`last_heartbeat` is not reset when an election starts, so a candidate that does not
win instantly re-runs the election every ~10 ms, incrementing its term each time.
Terms explode and the cluster thrashes.

**Fix direction:** a candidate needs its own randomized election timeout; only start
a *new* election when that timer fires, not every tick.

### 4. Election timeout is fixed and computed once
**File:** `src/grpc/event_loop.rs:65, 229-246`.

`generate_election_timeout()` derives a single deterministic value from `node_id`,
evaluated one time before the loop. Raft requires a **freshly randomized** timeout
per election — that is the mechanism that breaks split votes. A static per-node
offset means recurring split-vote scenarios can never break themselves.

**Fix direction:** re-draw a randomized timeout at the start of each election.

### 5. `become_leader()` is called twice
**Files:** `src/grpc/event_loop.rs:159-165` and `src/consensus/node.rs:334`.

`handle_vote_response` already calls `self.become_leader()` and returns `true`; then
`send_vote_requests` calls `node.become_leader()` again. Result: two NoOp entries
appended and leader state re-initialized.

**Fix direction:** pick one call site.

### 6. Client writes return success before commit
**File:** `src/grpc/service.rs:117-125`.

`client_request` appends to the leader's log and immediately returns `success: true`
with the index — it never waits for the entry to replicate to a majority / commit. If
the leader crashes right after, the client believes a write succeeded that is rolled
back. Linearizability violation.

**Fix direction:** hold the client response until `commit_index` reaches the entry
(or the leader steps down, in which case return a not-leader error).

### 7. No state machine and no apply loop
**Scope:** absent from the codebase.

`commit_index` and `last_applied` exist but nothing advances `last_applied` or
applies committed entries. There is no state machine — entries are replicated but
never executed, which is the actual point of consensus.

**Fix direction:** add an apply loop that applies committed-but-unapplied entries to
a state machine and returns results to clients.

---

## Missing Raft features

- **Snapshotting / log compaction + `InstallSnapshot` RPC.** Segment rotation exists,
  but `truncate_from` only drops segments from memory — `src/storage/log.rs:322-323`
  notes file cleanup is not handled, so `.dat` files grow **unbounded** on disk
  (see the 130 leftover segments in `raft_logs_segment_rotation/`). No way to catch up
  a follower behind a compacted prefix.
- **Faster log backtracking.** On AppendEntries failure `next_index` is decremented by
  1 per round trip (`src/consensus/node.rs:505-513`) — O(n) RPCs to repair a
  far-behind follower. The paper's `conflictTerm`/`conflictIndex` hint fixes this.
- **Membership changes** (joint consensus or single-server add/remove). Config is
  static.
- **PreVote** extension — stops a partitioned node from bumping terms and disrupting a
  healthy leader on rejoin.
- **Vote-granting does not reset the election timer** (`src/consensus/node.rs:131-145`)
  — required by the paper to avoid starving a legitimately elected leader.

---

## Runtime / concurrency

- **A single dead follower stalls everything.** `send_heartbeats` and
  `send_vote_requests` (`src/grpc/event_loop.rs`) await each peer sequentially, so one
  slow/unreachable node delays heartbeats and votes to all others. Should be
  concurrent (`join_all` / per-peer tasks).

---

## What is already good

- Storage segment management, cross-segment range queries, and truncation logic.
- The RPC decision logic in `node.rs` — the handlers are pure functions of
  `(state, request)`, which makes them directly testable.
- Strong unit-test coverage on the storage and state layers.

The gap is that the existing tests pass messages synchronously by hand in a fixed
order — they never explore the interleavings (reordering, drops, crashes, partitions)
where bugs #1–#4 hide. That is what the deterministic simulation harness in
`VERIFICATION.md` is designed to cover.

---

## Suggested fix order

1. **#1 (flush)** and **#3 (runaway elections)** — highest impact, fairly contained;
   likely the main reasons it "doesn't work well" today.
2. **#2 (truncation safety)**.
3. **#6 / #7 (commit-wait + state machine)** — makes it actually useful.
4. Missing features and concurrency, as follow-ups.
