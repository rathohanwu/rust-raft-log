# E2E Test Plan (Tier 2: in-process, real gRPC)

Implementation-ready plan for end-to-end integration tests of the Raft log server.
Hand this to an implementer. Correctness definitions live in `VERIFICATION.md`; the
fixes under test are in `IMPROVEMENTS.md`.

**Scope:** part (a) only — in-process, real-gRPC E2E with a deterministic *reducer*
over a nondeterministic schedule (an **eventually-consistent E2E oracle**). True
network partitions and client reads are **out of scope** (see §7). Authoritative
per-step invariant checking (Election Safety, no-double-vote) belongs to the
deterministic simulation harness in `VERIFICATION.md`, **not** here — see §1.4.

> **Read §0 first.** Several scenarios depend on small implementation changes that do
> not exist yet. Building the tests without those changes will produce failures
> unrelated to the test author's work.

---

## 0. Prerequisite implementation changes

| # | Change | Why | Size | Blocks |
|---|---|---|---|---|
| P1 | **Single-node commit.** Leader must advance/apply commit locally when `majority_size() == 1`. Today `try_advance_commit_index` runs only inside `handle_append_entries_response` (node.rs:571), so a 1-node cluster never commits and client writes time out. | §4.1 can't pass otherwise | small | single-node scenarios |
| P2 | **Pre-bound listener support.** Add a server entry point that serves on an already-bound `TcpListener` (`serve_with_incoming`), so tests bind the port and hand it in. Avoids the `:0`→close→reopen race. | reliable `restart` on same port | small | all (port safety) |
| P3 | *(optional, larger)* **Durable + replayable applied state** (snapshot or persisted apply-log). Today `commit_index`/`last_applied`/`applied_entries` are volatile and there is **no boot replay** (`RaftNode::new`). After restart the node re-applies from index 1 by **re-execution** — double-applying entries; a non-idempotent state machine would corrupt. | promotes restart scenarios from "log only" to "applied-state" | large | exactly-once-across-restart |
| P4 | *(optional)* **Client request IDs + dedup** (client session). No dedup today, so a timed-out-but-committed write that is retried double-appends. | exactly-once client semantics; cleaner ack oracle | medium | strict ack accounting |
| P5 | *(optional)* **Vote/election event instrumentation.** No public API exposes vote history; E2E can only *sample* leader state. | authoritative Election-Safety / no-double-vote (else sim/unit only) | small | strong safety proof |
| P6 | *(optional)* **Structured client error codes** — add an enum field to `ClientResponseMessage` (e.g. `NOT_LEADER`, `COMMIT_TIMEOUT`, `LEADERSHIP_CHANGED`, `APPEND_FAILED`). Today it carries only a free-form `error_message` string. | distinguish `TimedOut` vs `Failed` without brittle string matching (§3.5) | small | clean ack oracle |

P1 and P2 are recommended before starting. P3–P6 are follow-ups; scenarios that need
them are tagged **blocked-on-impl** below.

---

## 1. Verification philosophy

Raft's only job is to give every node the **same ordered, durable, committed sequence
of log entries**. The payload is opaque bytes; Raft does not interpret it. The *state
machine* is the layer above the log.

### 1.1 Two different properties → two different oracles
- **Committed-prefix agreement** — the Tier-2-observable consequence of **Log
  Matching**. It is about the *log itself* (index + **term**): compare the **full
  log**, terms included, NoOp included, over the **committed** prefix at quiescence.
  This proves committed prefixes agree across nodes; it does **not** assert the
  universal Log Matching invariant over uncommitted entries (out of E2E's reach).
- **State Machine Safety** is about what gets *applied*. Prove it with the **applied
  log** across nodes.

These are **not** the same check. The earlier draft conflated them.

### 1.2 Oracle helpers (vectors, not a hash)
```text
full_log(node)    -> Vec<(index, term, entry_type, payload)>   // every entry, incl NoOp
applied_log(node) -> Vec<(index, term, entry_type, payload)>   // from applied_entries()
```
Use **vectors**, so a mismatch prints an exact per-index diff. Do **not** use a rolling
hash: it hides which entry diverged and (without term) can't distinguish
same-payload/different-term entries.

### 1.3 What E2E asserts
1. **Cross-node log agreement** — at quiescence, `full_log` committed prefixes are
   identical across live nodes (committed-prefix agreement; a consequence of Log
   Matching, not the universal invariant).
2. **Cross-node applied agreement** — `applied_log` identical across live nodes
   (State Machine Safety).
3. **Client-ack consistency** — every write that returned `success: true` appears in
   the converged log exactly once, indexes contiguous, single consistent order. A
   **timed-out write is ambiguous** (may or may not have committed) — see §3.5.
4. **Leader Completeness (failover)** — entries acked *before* a leader died are
   present after the new leader.

### 1.4 What E2E does NOT authoritatively prove
- **Election Safety** and **no-double-vote** — E2E only *samples* leader state on a
  poll interval, so a sub-interval dual-leader can be missed. Treat E2E as best-effort;
  the **authoritative** check is the deterministic sim (per-step) or unit tests. With
  P5 instrumentation, E2E can record `(term → set<leader>)` over time and assert no
  term ever had two — still upgrade, not replacement.

The reducer is deterministic; the **schedule is not** (real gRPC + randomized
wall-clock timers). Hence "eventually-consistent oracle": assert at quiescence via
polling, never after a fixed sleep.

---

## 2. In scope vs. out of scope

| In scope (this plan) | Out of scope (follow-ups) |
|---|---|
| Log agreement across nodes (`full_log` vectors) | A real KV store / business logic |
| Applied agreement + apply order (`applied_log`) | A client read/query API |
| Durable **log + term/vote** recovery | Durable/replayable **applied** state (P3) |
| Leader election, failover, rejoin/catch-up | Exactly-once client semantics (P4) |
| Write path: commit-wait, not-leader redirect | Authoritative Election-Safety (sim/P5) |
| Eventually-consistent oracle (test-only) | True network partitions (needs proxy/mock) |

---

## 3. Harness: `TestCluster`

Factor the pattern from `tests/integration_leader_election.rs` into `tests/common/`.

### 3.1 Existing APIs to reuse (don't reinvent)
- `RaftNode::new(config) -> Result<RaftNode, _>`
- `RaftGrpcServer::new(node)`; `server.get_raft_node() -> Arc<Mutex<RaftNode>>`; `server.shutdown()`
- **P2**: a new `server.start_with_listener(listener: TcpListener) -> (handles)` using
  `serve_with_incoming`. (Existing `start_with_handles()` binds internally and is the
  fallback if P2 is skipped — but then restart is racy.)
- Inspection on `RaftNode`: `get_server_state()`, `get_current_term()`,
  `get_current_leader()`, `get_state()` (snapshot: `commit_index`, `last_applied`),
  `get_log_length()`, `get_entry(index)`, `applied_entries() -> &[LogEntry]`.
- `ClusterConfig::new(node_id, nodes: Vec<NodeInfo>, log_dir, meta_path, segment_size,
  max_entries_per_query, (election_min_ms, election_max_ms), heartbeat_ms)`
- `NodeInfo::new(id, ip, port)`
- Client write: `ClientRequest` gRPC (`ClientRequestMessage { payload }` →
  `ClientResponseMessage { success, leader_id, log_index, error_message }`).

### 3.2 `TestCluster` API
```text
TestCluster::start(n) -> TestCluster
    // n NodeInfo on unique ports (bind TcpListener per node, keep it), n temp dirs
    // OWNED by TestCluster keyed by id, n ClusterConfigs with fast timers, start all.

.node_ids() / .raft(id) -> Arc<Mutex<RaftNode>>
.wait_for_leader(timeout) -> u32
.leaders() -> Vec<(id, term)>
.client_write(bytes) -> ClientOutcome            // Committed{index} | NotLeader{hint} | TimedOut | Failed
.kill(id)                                         // abort event-loop + server handles
.restart(id)                                      // rebuild RaftNode from SAME temp dir + SAME port
.wait_until(timeout, predicate)                   // generic poll helper
.stop_all()
```

### 3.3 Critical harness details
- **Temp dirs owned by `TestCluster`, keyed by id** → `restart(id)` reuses the same
  `logs/` + `raft_state.meta`. Never let a node drop its own temp dir.
- **Ports:** bind one `std::net::TcpListener` per node up front and **retain it** for
  the cluster's lifetime so the port stays reserved even across a `kill`. Each
  `start`/`restart` does `retained.try_clone()?` → `set_nonblocking(true)` →
  `tokio::net::TcpListener::from_std` and hands *that clone* to the server task (the
  clone is dropped on `kill`; the retained original keeps the port bound). Do **not**
  bind `:0`, close, and reopen — that's a TOCTOU race; and don't move the only
  listener into the task, or `restart` has nothing to rebind.
- **Fast timers:** heartbeat ~50–100ms, election ~(300, 600)ms, `election_min > ~3×
  heartbeat`. Scenarios finish in seconds.
- **Poll, never fixed-sleep** for assertions (`wait_until`).
- **`kill` fully stops the task:** abort **and await** the server + event-loop handles
  to completion before any later `restart` clones the listener again — otherwise a
  stale `accept()` loop from the old task can still be bound to the port.

### 3.4 Oracle implementation
`full_log` / `applied_log` as in §1.2 (test-only, in `tests/common`). Compare only the
**committed prefix** (`min(commit_index)` across live nodes) for Log Matching, since
uncommitted tails legitimately differ mid-flight. Write **unique payloads** (monotonic
counter or `format!("w{}", i)`).

### 3.5 Client outcome + ambiguity handling
`client_write` must distinguish:
- `Committed{index}` — `success: true`.
- `NotLeader{hint}` — follow the redirect to `hint` and retry.
- `TimedOut` — **ambiguous**: the entry may have committed. Do **not** blindly retry
  (no dedup → double-append, P4). The oracle treats a timed-out payload as
  "may-or-may-not be present"; only `Committed` payloads are asserted **must-be-present**.
- `Failed` — explicit error.

### 3.6 Asserting persistence is two-phase

Persisted state can only be asserted *before* the recovered node participates again:
starting it lets it elect, which immediately changes `current_term`/`voted_for`, and
`become_leader()` appends a fresh NoOp. So every recovery assertion splits:

1. **Offline** — construct `RaftNode::new(config)` from the existing `logs/` +
   `raft_state.meta` **without** starting the event loop/server. Assert `current_term`,
   `voted_for` (via `get_state()`), and the old log prefix (`get_entry` /
   `get_log_length`) match the pre-crash values **exactly**.
2. **Online** — start it; assert only the *invariants that survive re-election*:
   `current_term` **never regresses** and the old committed entries remain an
   **unchanged prefix** (a new term, a new vote, and a new NoOp are all permitted).

Provide `TestCluster::inspect_offline(id) -> RaftNode` (or a free helper) that builds a
node from a killed id's stored paths without registering it as running.

> **Drop the inspector before `restart`.** `inspect_offline` opens the same mmap'd
> `logs/` + `raft_state.meta` files. Copy out the values/vectors you need, then
> `drop(inspector)`, and only then start the replacement node — concurrent mappings of
> the same files race and can fail on platforms with stricter file locking. Order:
> inspect → snapshot → `drop` → `restart`.

> **Deriving these outcomes:** `Committed` and `NotLeader` are clean today
> (`success` + `leader_id != 0`). But `TimedOut` and `Failed` are **both**
> `success: false, leader_id: 0` and differ only in the free-form `error_message`
> string ("Timed out waiting for the entry to commit" vs an append error). Until
> **P6** adds a structured error code, the harness must string-match — mark that
> helper explicitly as a **temporary, brittle test-only workaround**.

---

## 4. Scenario matrix

**Status:** ✅ testable now · 🔒 blocked-on-impl (tag).

### 4.1 One node (`tests/e2e_single.rs`)
| Scenario | Actions | Assertions | Status |
|---|---|---|---|
| Elect | start 1 | becomes Leader (majority=1) | ✅ |
| Write | write 3 entries | each `Committed`; `applied_log` has all 3 in order | 🔒 P1 |
| Persistence — offline (§3.6 phase 1) | kill; construct a `RaftNode` from the same paths **without starting** | persisted `current_term`, `voted_for`, old log prefix match pre-crash **exactly** | ✅ |
| Persistence — online (§3.6 phase 2) | then start it | `current_term` never regresses; the old **committed** prefix remains unchanged (an uncommitted tail may be truncated; new term/vote/NoOp permitted) | ✅ |
| Persistence (applied) | after restart, write 1 new entry | applied re-converges *after the new commit* | 🔒 P1 (+P3 for exactly-once) |

### 4.2 Three nodes (`e2e_cluster.rs`, `e2e_failover.rs`, `e2e_recovery.rs`)
| Scenario | Actions | Key assertions | Status |
|---|---|---|---|
| Election | start 3, `wait_for_leader` | one leader; **poll** until all followers report that leader/term (not instant) | ✅ |
| Replication | write N | all `Committed`; `full_log` **and** `applied_log` converge on 3 (committed prefix) | ✅ |
| Follower redirect | write to a follower | `NotLeader{hint}`, hint = real leader | ✅ |
| Leader failover | record acked set → `kill(leader)` | new leader at higher term within timeout; cluster accepts writes again | ✅ |
| Post-failover completeness | converge after new leader | every pre-kill **acked** entry present on all live nodes (Leader Completeness) | ✅ |
| Rejoin / catch-up | `restart(killed follower)` | rejoiner's `full_log` converges; ends Follower | ✅ |
| Crash-recovery — offline (§3.6 phase 1) | `kill` all → for each, construct a `RaftNode` from its paths **without starting** | each node's persisted `current_term`, `voted_for`, committed log prefix match pre-crash **exactly** | ✅ |
| Crash-recovery — re-form (§3.6 phase 2) | `restart` all → wait for the new leader's NoOp to commit | cluster re-forms; every `current_term` never regresses; pre-crash committed log stays an unchanged prefix; applied digest re-converges once the **NoOp** commits — **no client write required** | ✅ (exactly-once across restart needs P3) |
| Post-recovery availability | after recovery, submit a new write | write reaches `Committed`; proves the write path works post-restart | ✅ |

> **Why a fresh current-term commit is required (and why the NoOp suffices):** applied
> state is volatile and there's no boot replay. A new leader only advances commit on a
> **current-term** entry — and `become_leader()` already appends a NoOp for exactly
> this. Replicating and committing that NoOp drives re-application from index 1 on
> every node, so **no client write is needed** for convergence; a post-recovery write
> is a *separate* availability assertion. Asserting applied-convergence *before* the
> NoOp commits will fail for implementation reasons, not a bug.

**Log-repair (conflict truncation): NOT an E2E scenario.** Killing a node yields a node
that is *behind* (catch-up), never one with a *conflicting* tail — that needs an
alive-but-isolated old leader appending uncommitted entries (real partition, P-tier 3,
or a test hook). Cover conflict truncation with the existing unit tests
`test_append_entries_truncates_at_first_term_conflict_only` /
`test_append_entries_keeps_matching_overlap`, and later the sim harness.

### 4.3 Two nodes (`tests/e2e_two_node.rs`) — quorum / no-split-brain
majority = 2 → tolerates 0 failures. Assertions depend on **which** node dies:
| Case | Assertions | Status |
|---|---|---|
| Both up | exactly one leader; writes `Committed` | ✅ |
| Kill the **follower** | surviving **leader stays Leader** (no CheckQuorum today); a new `client_write` **does not commit** — `TimedOut`; `commit_index` does **not** advance | ✅ |
| Kill the **leader** | lone surviving follower **cannot elect itself** (needs 2 votes) → no new leader; writes fail | ✅ |

> Do **not** assert "no leader emerges" after killing the follower — the old leader
> remains leader; it just can't commit.

---

## 5. Assertion helpers (map to the safety bar)

In `tests/common`:
- `assert_committed_logs_converge(cluster, live_ids, timeout)` — `full_log` committed
  prefixes equal (committed-prefix agreement).
- `assert_applied_converge(cluster, live_ids, timeout)` — `applied_log` equal (SM Safety).
- `assert_contains_all_acked(converged, committed_acks)` — every `Committed` write
  present once; timed-out payloads exempt (§3.5).
- `assert_contiguous_indexes(converged)` — no gaps.
- `assert_term_not_regressed(before, after)` — persisted term monotonic across restart.
- `sample_leaders_over_time(cluster, window) -> Map<term, Set<id>>` — **best-effort**
  Election-Safety sampling; note in the test that authority is sim/unit (§1.4).

---

## 6. How to run
- One file per concern.
- **Serial** (real ports + timing): `cargo test --test e2e_failover -- --test-threads=1`.
- Keep default `cargo test` fast: mark heavy scenarios `#[ignore]`, run with
  `cargo test -- --ignored`.
- Debug: `RUST_LOG=info cargo test --test e2e_failover -- --nocapture --test-threads=1`.
- Optional `run_e2e.sh` setting `RUST_LOG` + `--test-threads=1`.

---

## 7. Known limitations & follow-ups
1. **Real network partitions** aren't achievable in-process (loopback can't drop
   packets selectively). "Partition" here = fail-stop (`kill`). Isolate-and-heal needs
   separate processes behind a proxy (toxiproxy) or a mock transport. Tier 3.
2. **Client reads / linearizable queries** — no read RPC exists; adding one needs
   ReadIndex/lease and its own tests. Separate feature.
3. **Deterministic simulation** (`VERIFICATION.md`) is complementary and is the
   authority for Election Safety / no-double-vote / conflict truncation.
4. **Full linearizability checking** (Porcupine/Knossos) is a possible upgrade to the
   ack-consistency check.

---

## 8. Implementation checklist
- [ ] **P1**: single-node local commit when `majority_size() == 1`.
- [ ] **P2**: `start_with_listener(TcpListener)` (pre-bound, `serve_with_incoming`).
- [ ] `tests/common`: `TestCluster` (owned temp dirs, retained/cloned listeners, `kill`
      that awaits the task to completion, `restart`, `inspect_offline(id)`, poll).
- [ ] Oracle: `full_log` + `applied_log` (vectors, terms, NoOp incl.); converge helpers.
- [ ] `client_write` returning `ClientOutcome` with ambiguous-timeout handling (§3.5).
- [ ] Scenarios: single-node (P1), 3-node election/replication/redirect/failover/
      completeness/rejoin/crash-recovery, two-node three cases. (Log-repair stays unit.)
- [ ] `#[ignore]` heavy scenarios; `run_e2e.sh`.
- [ ] Confirm green with `--test-threads=1`; record wall-clock per file.
- [ ] *(follow-up)* P3 durable applied state, P4 request-id dedup, P5 vote instrumentation.
