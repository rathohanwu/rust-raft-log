# Verification Plan

This document defines what "correct" means for this Raft implementation and how we
verify it. It is the reference we build tests against. The source of truth for
behavior is the **Raft paper, Figure 2**, plus the **five safety properties**.

Approach: **deterministic simulation**, **safety-first** (liveness is a noted goal,
verified more loosely). A function is only considered "good" when it satisfies both
its own contract (Layer 1) *and* the system invariants hold when it participates in
randomized, adversarial schedules (Layer 2).

---

## Why two layers

In Raft a function can be individually correct and the system can still lose
committed data. Concrete examples in this codebase:

- `truncate_from` (`src/storage/log.rs`) is correct in isolation — it truncates
  exactly what it is asked to. The safety bug is that `handle_append_entries`
  (`src/consensus/node.rs:224`) *calls* it unconditionally on any overlap. No unit
  test of `truncate_from` can catch that.
- `set_voted_for` (`src/consensus/state.rs:140`) writes the correct bytes. The
  safety bug is that nothing flushes them to disk before the RPC reply returns.

So verification is split into **function contracts** (Layer 1) and **system
invariants** (Layer 2). Both are required. The dangerous bugs live in Layer 2.

---

## The spec: Raft Figure 2 (the rules we enforce)

**Persistence rule.** `currentTerm`, `votedFor`, and `log[]` MUST be on stable
storage (flushed) *before responding to any RPC*.

**RequestVote receiver.**
1. Reply false if `term < currentTerm`.
2. Grant iff `votedFor ∈ {null, candidateId}` **and** the candidate's log is at
   least as up-to-date (`lastLogTerm` greater, or equal term with
   `lastLogIndex ≥` ours).

**AppendEntries receiver.**
1. Reply false if `term < currentTerm`.
2. Reply false if the log has no entry at `prevLogIndex` whose term equals
   `prevLogTerm`.
3. If an existing entry conflicts with a new one (same index, **different term**),
   delete the existing entry and everything after it — and *only* then.
4. Append any new entries not already present.
5. If `leaderCommit > commitIndex`, set
   `commitIndex = min(leaderCommit, index of last new entry)`.

**All servers.**
- If `commitIndex > lastApplied`: increment `lastApplied`, apply
  `log[lastApplied]` to the state machine.
- If any RPC request/response carries `T > currentTerm`: set `currentTerm = T` and
  convert to follower.

**Candidate.** On conversion: increment `currentTerm`, vote for self, **reset the
election timer**, send RequestVote to all peers. Become leader on majority. Become
follower on valid AppendEntries from a current-term leader. On election timeout,
start a *new* election.

**Leader.**
- On election, send initial empty AppendEntries; repeat on idle (heartbeats).
- On client command: append locally, then replicate.
- On success: update `nextIndex`/`matchIndex`. On failure: back up `nextIndex`, retry.
- **Commit rule:** advance `commitIndex` to `N` only if a majority have
  `matchIndex ≥ N` **and** `log[N].term == currentTerm`.

---

## Layer 1 — function contracts

"Good" per function = honors its contract in isolation, checked by unit tests.

| Function (file) | Precondition | Must guarantee |
|---|---|---|
| `set_current_term` / `set_voted_for` (`state.rs`) | — | Durable (flushed) before caller replies to an RPC; `currentTerm` monotonic non-decreasing |
| `handle_request_vote` (`node.rs`) | — | Grant ⟹ all Figure-2 grant conditions true; never grants two candidates in one term; persists before returning |
| `handle_append_entries` (`node.rs`) | — | Truncates **only** at a real term conflict; never shortens a matching prefix; commit advances only per rule 5 |
| `truncate_from` (`log.rs`) | `index ≥ 1` | Log length becomes `index-1`; entries `< index` untouched |
| `try_advance_commit_index` (`node.rs`) | leader | Never commits an entry whose `term != currentTerm`; requires majority `matchIndex` |
| `become_leader` (`node.rs`) | won majority in current term | Appends exactly one NoOp; called exactly once per election |
| `append_entry` (`log.rs`) | — | Assigns contiguous index; durable before it counts as replicated |
| `create_vote_request` (`node.rs`) | — | Called only when starting a *new* election (not every tick); increments term exactly once |

---

## Layer 2 — system invariants (the real bar)

Evaluated by an oracle after **every step** (message delivery, tick, crash,
restart), across all nodes.

1. **Election Safety** — no two distinct leaders share a term.
2. **Leader Append-Only** — a node in leader state never rewrites or deletes an
   entry already in its own log.
3. **Log Matching** — if two logs agree on `(index, term)`, their entire prefixes
   up to `index` are identical.
4. **Leader Completeness** — every committed entry is present in the log of every
   leader of a later term.
5. **State Machine Safety** — no two nodes ever apply a different entry at the same
   log index.

**Liveness (goal, not asserted every step).** Under a stable majority and bounded
message delay, a leader is elected and submitted entries eventually commit. Checked
by letting the adversary go quiet and asserting progress.

---

## The verification mechanism (deterministic simulation)

Four pieces, all driven by a seed so any failure replays exactly:

- **Mock clock** — logical time. Election/heartbeat timers fire on tick. No
  wall-clock, no `tokio::time`.
- **Mock network** — a message queue the adversary controls: reorder, duplicate,
  drop, delay, partition/heal.
- **Adversary schedule** — seeded RNG chooses which message to deliver next, when
  to crash/restart a node, when to partition.
- **Invariant oracle** — the five predicates above, evaluated on global state after
  each event; also records committed/applied entries for cross-step checks.

**Required refactor to make this testable:** `RaftNode`'s decision logic must be
separable from real gRPC and real time. The RPC handlers are already pure functions
of `(state, request)` — that is why the existing unit tests work. Only the event
loop needs time and network injected (behind a trait), so the harness can supply
mock implementations.

---

## Definition of Done

"Every function is good" is achieved when:

- **(a)** every Layer-1 contract has a passing unit test, and
- **(b)** the harness runs thousands of randomized seeds — including crash/restart
  mid-vote and network partitions — with all five invariants holding, and liveness
  achieved once the adversary goes quiet.

A failure is then always a concrete, replayable seed that can be shrunk to a minimal
counterexample.

---

## Scope note

Current scope is **safety-first**. Liveness is verified loosely (progress after
quiescence), not as a per-step assertion. Membership changes, snapshotting, and the
PreVote extension are out of scope for the initial verification pass and tracked
separately in `IMPROVEMENTS.md`.
