# Plan: Dockerized end-to-end testing for the Raft cluster

> **Audience:** maintainers planning future Docker test infrastructure. This is a human-facing design document, not an executable runbook.

## 1. Goal

Stand up a **segregated, reproducible environment** where each Raft node runs as
its own Docker container on a dedicated network, and a test harness drives the
cluster strictly **black-box** — through gRPC and the Docker control plane — while
injecting real faults (crashes, restarts, network partitions, latency/loss) and
asserting Raft's safety and liveness properties. The environment must:

- run identically on a laptop and in CI, with Docker as the only host dependency;
- isolate every run (own network, own volumes, clean teardown) so tests never
  collide with each other or with a developer's loopback cluster;
- exercise the node exactly as it ships (same binary, same config format, real
  TCP/gRPC between separate OS processes in separate network namespaces), which
  the current in-process `e2e_*` loopback tests do **not** do;
- give a clear pass/fail signal (single exit code) and readable diagnostics.

This complements — does not replace — the existing in-process `e2e_*` suite. Those
stay for fast, deterministic transition coverage; the Docker suite adds true
black-box, multi-process, faulty-network coverage.

## 2. What exists today (baseline)

- **Node**: `rust-raft-log --node-id <N> --config <path.yaml>` (`src/main.rs`).
  Binds/serves on the address from the YAML entry for `<N>`.
- **Client**: `raft-client --config <path.yaml> --payload <str> [--node-id N]`
  (`src/bin/client.rs`) — already implements leader discovery + retry using the
  `leader_id` hint in `ClientResponseMessage`.
- **Config**: YAML (`cluster_config.yaml`) — `nodes: [{node_id, ip_address,
  port}]`, plus `cluster_settings` (`log_directory`, `log_segment_size`,
  `election_timeout_range`, `heartbeat_interval`).
- **gRPC** (`proto/raft.proto`): `RaftService { RequestVote, AppendEntries,
  ClientRequest }`. `ClientRequest → { success, leader_id, log_index,
  error_message }`.
- **Build**: `build.rs` → `tonic_prost_build`; requires `protoc` at build time.
- **Storage**: persists segments/metadata under `log_directory`.
- **CI** (`.github/workflows/ci.yml`): runs `cargo test-all` then the `#[ignore]`d
  `e2e_*` tests in-process.

## 3. Prerequisites to resolve before implementation

These are the honest gaps between "what we have" and "what black-box Docker
testing needs." Each is small but must be decided first.

### P1 — Hostname addressing + `0.0.0.0` bind — ALREADY SATISFIED (validate only)
Compose gives every service a DNS name (`node1`, `node2`, …), so the node must
dial peers by hostname and bind a non-loopback address. **Verified against the
code — this already works, so P1 is a validation step, not a code change:**
- `NodeInfo.ip_address` is a `String` (`src/models/types.rs:103`), not a parsed
  IP — a service name like `node1` is accepted verbatim.
- The client builds the endpoint as `http://{ip_address}:{port}` directly from
  that string (`src/grpc/client.rs:55`), so hostnames dial fine.
- The server already binds `0.0.0.0:{port}` (`src/grpc/server.rs:47`), reachable
  from peers and the harness.

Action: no code change; the Docker bring-up (S0) *is* the validation — a config
using service names must form a cluster. Keep this as a checkpoint, not a task.

### P2 — Add a read-only observability RPC + leadership-transition record
This is the **most important prerequisite.** Raft's correctness assertions need
to observe each node's *term, role, current leader, commit index, last-applied
index*, plus (for agreement) a committed-log range. The current gRPC surface
(`RequestVote`, `AppendEntries`, `ClientRequest` only) exposes none of that
black-box. Two parts:

- **P2a (recommended): add a `Status`/`Query` RPC** to `RaftService`, e.g.
  `GetStatus(GetStatusRequest) → { node_id, term, role, leader_id, commit_index,
  last_applied, log_len }` and a bounded `GetCommittedEntries(from, to)`,
  read-only and lock-free off the published snapshot. This makes the suite
  genuine black-box and powers healthchecks and leader-wait. Gate it behind
  config if you don't want it in production; for testing it is the clean oracle.
- **P2b: a monotonic leadership-transition record.** A single `GetStatus` sample
  can only *sample* who is leader; it cannot prove election safety (at most one
  leader per term), because a transient double-leader can appear and vanish
  between polls. Fix: each node records every `become_leader` transition as
  `(term, node_id)` and exposes the append-only list (e.g. `GetLeadershipEvents →
  [{term, node_id}]`). Election safety then becomes a **real** invariant: across
  the union of all nodes' records, no term may map to two distinct leaders. This
  needs a small hook at `become_leader` (`src/consensus/node.rs:546`), which today
  only mutates state and keeps no history.

  **The record must survive crashes — an in-memory list is not enough.** S3/S6
  kill leaders; a transition that happens *just before* a `kill` would vanish with
  the process, so a real double-leader could go undetected. Two acceptable
  designs (pick one):
  - **Durable per-node record:** append each `(term, node_id, timestamp)` to a
    small file under the node's `log_directory` volume (fsync on write) *before*
    the node acts as leader. It survives `kill` and is re-read after restart; the
    volume already persists across restarts. This is the simplest sound option.
  - **Continuous streamed capture with an acked cursor:** the harness subscribes
    to a leadership-event stream (server-streaming RPC) and the **host runner
    persists events to a ledger as they arrive**, only injecting a fault after the
    cursor is acknowledged past the pre-fault state. Heavier, but needs no on-node
    durability.

  Either way the assertion reads the *durable/persisted* record, never a
  best-effort in-memory snapshot pulled after the crash.

**Fallback (no code change): volume-mounted log inspection.** Mount each node's
`log_directory` as a named volume and parse committed entries with a read-only
`raft-inspect` bin. Works for the log-prefix checks, but couples tests to the
on-disk format and can't read in-memory role/commit state or transitions. Use
only if P2a/P2b are rejected — election safety then degrades to sampled.

The plan assumes **P2a + P2b**.

### P3 — Deterministic-enough timing knobs
Container scheduling and netem add jitter, so election/heartbeat timing must be
tunable per environment via the existing `cluster_settings` (already present).
The Docker configs will use slightly longer election timeouts than loopback to
tolerate container/network scheduling (e.g. election `600–1000ms`, heartbeat
`150ms`) and every scenario asserts against a **bounded convergence deadline**,
never a fixed sleep.

### P4 — Clean shutdown + healthcheck endpoint
Fault scenarios restart nodes, so the node must exit cleanly on `SIGTERM`
(compose `stop`) and flush durable state. Confirm `panic = "abort"` (already set)
plus a `SIGTERM` handler that stops the actor/server and flushes.

For the healthcheck, **do not use `grpc-health-probe`** as the earlier draft
implied: the image installs no such binary and the service does **not** implement
the standard gRPC health API (`proto/raft.proto` has only the three Raft RPCs).
Also, each node binds its *own* port (`node1:18001`, `node2:18002`,
`node3:18003`), so a single hard-coded probe port is wrong. **Two distinct
healthchecks, phased with the rollout:**

- **P0 healthcheck (before P2a exists) — an installed TCP-listen probe.** A bare
  `grpcurl GetStatus` cannot work yet, and `nc` is **not** in `debian-slim` by
  default, so it must be installed or avoided. Use one of: install
  `netcat-openbsd` and run `nc -z 127.0.0.1 <own-port>`; or use bash's built-in
  with no extra package — `bash -c ': > /dev/tcp/127.0.0.1/<own-port>'` (bash ≠
  present in `-slim`, so install `bash` or use the netcat route); or ship a tiny
  static `tcp-probe` binary. Whichever is chosen, **the probe binary must be in
  the image** — this is the fix for "the example needs a separate P0
  healthcheck." The §5.2 compose shows the P0 probe; the GetStatus probe is a
  later swap.
- **P2a healthcheck (after `GetStatus` lands) — `grpcurl … GetStatus`, which by
  itself will not work.** `grpcurl` needs a schema: the server exposes **no gRPC
  reflection** (verified — nothing pulls `tonic-reflection`), so a bare
  `grpcurl <addr> raft.RaftService/GetStatus` fails with "server does not support
  reflection." Fix — pick one:
  - **enable server reflection in the (test) build** (`tonic-reflection` with the
    generated `FILE_DESCRIPTOR_SET`), then `grpcurl -plaintext …` works
    unadorned; or
  - **hand `grpcurl` the schema**: copy `proto/` into the image and probe with
    `grpcurl -plaintext -import-path /proto -proto raft.proto <own-port>
    raft.RaftService/GetStatus`; or
  - **use a purpose-built probe** — a small `raft-probe` bin (reusing
    `RaftGrpcClient`) that calls `GetStatus` and exits 0/1, avoiding `grpcurl`
    entirely. Recommended, since it also removes the reflection/proto-mount
    coupling and is testable.

Each service's healthcheck must target **its own** port, so `depends_on:
{ condition: service_healthy }` and `compose --wait` actually complete.

### P5 — Application state machine for recovery/agreement assertions
`commit_index` and `last_applied` are correctly **volatile** and reset to zero
on every restart (`src/consensus/state.rs`). The durable data is the log plus
`current_term`/`voted_for`; an application state machine must therefore be
rebuilt only after Raft has re-established which prefix is committed.

**P5a is implemented for Docker E2E.** `src/testkit/arithmetic.rs` provides a
deterministic arithmetic state machine, and `raft-node-test` wires it through
`RaftNode::new_with_state_machine`. After a full restart the elected leader
appends and commits its current-term NoOp. That commit advances every node from
volatile `last_applied = 0` through the recovered durable prefix, rebuilding the
test state machine. This is not an unsafe "apply every log entry on boot": an
uncommitted suffix remains unapplied until a quorum establishes commitment.
The ignored Rust recovery test proves this with `add(9)`, a full `stop`/`start` with
volumes retained, and all three nodes reporting `value: 9`,
`applied_commands: 1`, and `last_applied: 3`.

The production `rust-raft-log` binary still constructs `RaftNode::new` without
an application state machine. Its recovery claims remain limited to durable
log-prefix agreement and post-recovery availability; applied-state assertions
are scoped to the Docker test binary.

## 4. Environment architecture

```
                 docker network: raft-e2e-net (user-defined bridge, isolated)
   ┌──────────────────────────────────────────────────────────────────────┐
   │  ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐          │
   │  │  node1  │     │  node2  │     │  node3  │ ...  │  nodeN  │          │
   │  │ :18001  │◄───►│ :18002  │◄───►│ :18003  │◄───►│         │  Raft gRPC│
   │  │ vol1    │     │ vol2    │     │ vol3    │     │ volN    │  (peers)  │
   │  └─────────┘     └─────────┘     └─────────┘     └─────────┘          │
   │       ▲              ▲               ▲                                 │
   │       └──────────────┴───────────────┴─────────────┐                  │
   │                                                     │                  │
   │                                            ┌────────────────┐          │
   │                                            │    tester      │  drives  │
   │                                            │  (harness on   │  cluster │
   │                                            │   same net)    │  + faults│
   │                                            └────────────────┘          │
   └──────────────────────────────────────────────────────────────────────┘
        faults injected via Docker control plane (compose stop/kill/restart,
        network disconnect/connect) and Pumba (netem: delay/loss/partition)
```

Design choices:

- **One container per node**, each with its **own named volume** mounted at
  `log_directory` → real per-node durable storage; survives restart for recovery
  tests; destroyed on full teardown for a clean slate.
- **User-defined bridge network** (`raft-e2e-net`) → service DNS (`node1`…),
  isolation from the host and other compose projects. No `network_mode: host`.
- **A dedicated `tester` container on the same network** runs the assertion
  harness. Running the harness *inside* the network (not from the host) is what
  makes the environment "segregated" as requested: the harness uses the same DNS
  and reachability the nodes see. Node ports are published to the host **only via
  the opt-in debug override** (§5.2) for interactive poking; CI assertions go
  through the tester container.
- **Split responsibilities cleanly (one model, no overlap):**
  - the **host runner script owns all Docker faults** — `compose kill/stop/start`,
    `network connect/disconnect`, and Pumba. A container generally should not
    sever its own network, so the tester does **not** mount the Docker socket and
    does **not** inject faults;
  - the **tester speaks only gRPC** to the nodes (writes via `client_request`,
    observations via P2a `GetStatus`/`GetCommittedEntries`/leadership events);
  - the runner sequences a scenario as: *drive/observe via tester → inject fault
    from host → drive/observe via tester again.* Because the tester is behind a
    compose `profile`, `compose up` never auto-starts it with `--suite all`; the
    runner launches it explicitly with `docker compose run --rm --no-deps tester
    --suite <id>` at each step (or runs the whole suite once, pausing for the
    runner's fault hooks — the plan uses per-step `run` for clarity).

## 5. Deliverables (file layout)

```
Dockerfile                     # multi-stage build of node + client (+ inspect)
.dockerignore                  # keep target/, .git/ out of build context
docker/
  compose.e2e.yaml             # N-node cluster + tester on raft-e2e-net
  config/
    cluster.docker.yaml        # peers addressed by service name; bind 0.0.0.0
  entrypoint.sh                # optional: template config / wait-for-peers
tests/e2e_docker/              # the black-box harness (Rust bin or crate)
  Cargo.toml / src/…           # OR a bash+grpcurl suite; see §7
  scenarios/                   # one module/script per scenario S1..S9
scripts/
  e2e-docker.sh                # build → up → wait healthy → run → collect → down
Makefile (or cargo alias)      # `make e2e-docker`
.github/workflows/e2e-docker.yml  # CI job (separate from unit CI)
```

### 5.1 Dockerfile (multi-stage, illustrative)

```dockerfile
# ---- builder ----
FROM rust:1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release --bins           # rust-raft-log, raft-client, (raft-inspect)

# ---- runtime (P0 baseline: only bins that exist today + a TCP probe) ----
FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/rust-raft-log /usr/local/bin/
COPY --from=builder /app/target/release/raft-client   /usr/local/bin/
# P0 healthcheck uses `nc -z` (netcat installed above). `raft-probe` and GetStatus
# do NOT exist yet — do not COPY them here until P1 (see the patch below).
EXPOSE 18001
ENTRYPOINT ["rust-raft-log"]
```

**P1 patch (only after `GetStatus` + `raft-probe` are built).** Add the probe bin
and switch the healthcheck to it (see §5.2). Do **not** put this in the P0 image —
copying a nonexistent `raft-probe` would fail the build:

```dockerfile
# append to the runtime stage in P1:
COPY --from=builder /app/target/release/raft-probe /usr/local/bin/
# netcat can then be dropped if nothing else uses it.
```

Notes: build all bins in one layer; use BuildKit cache mounts (`--mount=type=cache`)
or `cargo-chef` to cache dependency compilation so image rebuilds are fast in CI.
A distroless runtime is a later hardening step; `debian-slim` first for
debuggability. `panic=abort` + `lto` are already in the release profile. The
healthcheck evolves with the rollout (P4): **P0** = installed TCP-listen probe
(`nc -z`); **P2a** = `raft-probe` (or `grpcurl` *with* reflection/`-proto` — never
bare). If you keep `grpcurl` for ad-hoc debugging, copy it from
`fullstorydev/grpcurl` and always pass it a schema.

### 5.2 Compose (illustrative, 3-node + tester)

**No top-level `name:`** — the runner passes a unique `--project-name` per
invocation (see §9) so simultaneous local/CI runs never collide. Each service's
healthcheck targets **its own** port; the tester is behind a `profile` so
`up` never auto-starts it; **host ports are opt-in** (in a debug override, not
the base file) so parallel runs don't fight over `18001`.

```yaml
# docker/compose.e2e.yaml  (NO top-level `name:` — set via --project-name)
networks:
  raft-e2e-net:
    driver: bridge
volumes: { data1: {}, data2: {}, data3: {} }

x-node: &node
  image: rust-raft-log:e2e
  build: { context: .., dockerfile: ../Dockerfile }
  networks: [raft-e2e-net]
  restart: "no"                     # tests control lifecycle explicitly
  # healthcheck is per-service (own port) — see each node below.
  # NOTE (P4): P0 default is the installed TCP probe `nc -z <own-port>` (GetStatus
  # and raft-probe don't exist yet). In P1, swap each to
  # ["CMD","raft-probe","127.0.0.1:<own-port>"]. Never a bare `grpcurl … GetStatus`
  # (the server has no reflection).

services:
  node1:
    <<: *node
    command: ["--node-id", "1", "--config", "/etc/raft/cluster.docker.yaml"]
    volumes: [ "data1:/var/lib/raft", "./config:/etc/raft:ro" ]
    healthcheck:
      test: ["CMD","nc","-z","127.0.0.1","18001"]    # P1: ["CMD","raft-probe","127.0.0.1:18001"]
      interval: 1s
      timeout: 1s
      retries: 20
  node2:
    <<: *node
    command: ["--node-id", "2", "--config", "/etc/raft/cluster.docker.yaml"]
    volumes: [ "data2:/var/lib/raft", "./config:/etc/raft:ro" ]
    healthcheck:
      test: ["CMD","nc","-z","127.0.0.1","18002"]    # P1: ["CMD","raft-probe","127.0.0.1:18002"]
      interval: 1s
      timeout: 1s
      retries: 20
  node3:
    <<: *node
    command: ["--node-id", "3", "--config", "/etc/raft/cluster.docker.yaml"]
    volumes: [ "data3:/var/lib/raft", "./config:/etc/raft:ro" ]
    healthcheck:
      test: ["CMD","nc","-z","127.0.0.1","18003"]    # P1: ["CMD","raft-probe","127.0.0.1:18003"]
      interval: 1s
      timeout: 1s
      retries: 20

  # Tester only speaks gRPC to the nodes; it does NOT own the Docker socket and
  # does NOT inject faults (the host runner does — see §7). Behind a profile so
  # `compose up` never launches it; the runner starts it explicitly.
  tester:
    image: rust-raft-log:e2e-tester
    build: { context: .., dockerfile: ../tests/e2e_docker/Dockerfile }
    networks: [raft-e2e-net]
    profiles: ["tester"]
    depends_on:
      node1: { condition: service_healthy }
      node2: { condition: service_healthy }
      node3: { condition: service_healthy }
    # no docker.sock mount, no host ports.
```

Host-port publishing for interactive debugging lives in a **separate opt-in
override** so it is never active in CI or parallel runs:

```yaml
# docker/compose.debug.yaml  (used only with `-f compose.e2e.yaml -f compose.debug.yaml`)
services:
  node1: { ports: [ "18001" ] }   # ephemeral host port (docker picks it) — no fixed 18001:18001
  node2: { ports: [ "18002" ] }
  node3: { ports: [ "18003" ] }
```

And `config/cluster.docker.yaml` differs from the loopback config only in
addressing (service names, `log_directory` on the mounted volume):

```yaml
nodes:
  - { node_id: 1, ip_address: "node1", port: 18001 }
  - { node_id: 2, ip_address: "node2", port: 18002 }
  - { node_id: 3, ip_address: "node3", port: 18003 }
cluster_settings:
  log_directory: "/var/lib/raft"
  election_timeout_range: { min: 600, max: 1000 }   # roomier than loopback (P3)
  heartbeat_interval: 150
```

## 6. Correctness oracle — how we know "everything is fine"

Every scenario asserts against these invariants (via the P2a `GetStatus` /
committed-log reads), not against logs-scraping heuristics:

- **Election safety** — at most one leader per term. **Sampling `GetStatus`
  cannot prove this** (a transient double-leader can appear and vanish between
  polls), so the real invariant is asserted over the **P2b leadership-transition
  records**: across the union of all nodes' `(term, node_id)` transition lists,
  no term maps to two distinct leaders. Sampling remains a cheap secondary check
  but is explicitly *not* the safety oracle.
- **Leader liveness** — from a healthy majority, a leader is elected within a
  bounded deadline (e.g. ≤ 5× max election timeout).
- **Log matching / agreement** — for the common committed prefix
  `min(commit_index)` across reachable nodes, the committed entries are
  byte-identical and in the same order on every node.
- **State-machine safety** — *only assertable with the P5a test binary* (the
  shipped binary has no state machine): at equal `last_applied`, the applied
  state is identical across reachable nodes. Under P5b this invariant is dropped
  in favor of log-prefix agreement above.
- **Durability of acknowledged writes** — every write the harness got
  `success=true` for is present, at its returned `log_index`, after any
  failover/restart. **No acknowledged write is lost.** This is an
  **acknowledged-set durability** oracle built on **unique per-write payload IDs**
  (the harness stamps each payload with a UUID and records the ack). It
  establishes *at-least-once from the client's view* — **not exactly-once:** a
  client timeout can occur *after* the entry commits, and a retry (the payload has
  no client-supplied request id — `ClientRequestMessage` carries only `payload`)
  would append a duplicate. Duplicate-freedom / exactly-once is a **separate,
  currently-unsupported** claim (see S8 and §11) needing client session/request
  IDs + server dedup.
- **No spurious commits under partition** — a minority partition must not report
  new commits; only the majority side makes progress.
- **Convergence** — after a fault heals, all reachable nodes reconverge (equal
  committed prefix; applied state too under P5a) within a bounded deadline.

Each assertion runs to a **deadline with polling**, never a bare sleep, so tests
are robust to container jitter but still fail fast on real bugs.

## 7. Test harness: form factor

Two viable forms; the plan **recommends the Rust harness** for type-safety and
reuse of `RaftGrpcClient`, with a thin shell runner for orchestration.

- **Recommended — Rust harness bin/crate (`tests/e2e_docker`)** that:
  - talks to nodes with the existing `RaftGrpcClient` + the new `GetStatus` stub;
  - drives writes via `client_request` (reusing the leader-discovery logic);
  - **speaks gRPC only** — it does *not* shell out to Docker; the host runner
    (§9) performs all fault injection between harness steps (see §4);
  - is containerized as the `tester` service (runs on the net) and can also run
    from the host against the debug-override ports for local dev.
  Structure scenarios as ordinary `#[tokio::test]`-style cases or a `--suite`
  CLI; emit JUnit/log output for CI.
- **Alternative — bash + `grpcurl`/`raft-client`** suite. Lower barrier, no extra
  crate, but assertions on structured Raft state get awkward and brittle. Fine for
  a first smoke test; not for the full matrix.

Fault-injection toolbox (all run by the **host runner**, resolving concrete
container names/IDs — never by service name assumptions):

- **Crash / restart / rejoin**: `docker compose -p $PROJECT kill|stop|start|
  restart <svc>` (SIGKILL for hard crash, SIGTERM for graceful).
- **Network partition — concrete mechanics (do not hand-wave):**
  - *Resolve the real container AND network first — both are project-scoped:*
    ```bash
    cid=$(docker compose -p $PROJECT ps -q node1)          # container: <project>-node1-1
    net=$(docker network ls --filter "label=com.docker.compose.project=$PROJECT" \
                            --filter "name=raft-e2e-net" -q)   # network: <project>_raft-e2e-net
    ```
    A hard-coded `docker network disconnect raft-e2e-net node1` fails on **both**
    counts: the container isn't named `node1` and the network isn't named
    `raft-e2e-net` under a project. Always resolve `$net` and `$cid`.
  - *Single-node isolation:* `docker network disconnect $net $cid`; **on
    reconnect you must restore the alias** or peers lose DNS resolution —
    `docker network connect --alias node1 $net $cid`.
  - *Arbitrary group partitions* (e.g. `{node1,node2}` vs `{node3,node4,node5}`),
    which `disconnect` alone cannot express: use **Pumba netem loss/partition** or
    `iptables`/`nftables` DROP rules between the specific peer container IPs. Both
    require `NET_ADMIN` on the target container (`cap_add: [NET_ADMIN]` for the
    Pumba/tc path). Define, per S6 variant, exactly which peer→peer links are cut
    and the reheal command that restores them.
  - Prefer **Pumba** (`gaiaadm/pumba netem --duration <d> loss/delay`, or
    `pumba netem --target <ip>`) for gray failures (delay/loss/corrupt) and for
    group partitions, since it targets by container and self-heals on duration
    expiry.
- **Slow disk / CPU**: `docker update --cpus`/`--memory`, or Pumba `stress`.
- **Clock**: keep wall-clock; do not skew (Raft uses monotonic timers). Latency is
  modeled with netem, not clock changes.

## 8. Scenario matrix (the "rigorous" suite)

Ordered from smoke to chaos; CI can run a fast subset on PRs and the full set
nightly. Each is independent and starts from a clean environment (fresh volumes)
unless it explicitly tests persistence.

Rows marked **(P5a)** use the implemented state-machine test binary; production
coverage remains narrowed to log-prefix agreement + availability.

| ID | Scenario | Fault injected | Key assertions |
|----|----------|----------------|----------------|
| S0 | **Bring-up smoke** | none | all containers healthy; cluster forms |
| S1 | **Leader election** | none | exactly one leader per term (via P2b transition records); ≤ bounded time; stable |
| S2 | **Replication** | none | N writes committed; committed **log prefix** identical on all; **applied state identical (P5a)** |
| S3 | **Leader failover** | `kill` leader | new leader elected; writes resume; no ack'd write lost |
| S4 | **Follower crash + rejoin** | `kill` then `start` follower | rejoiner catches up to common committed prefix; **applied state converges (P5a)** |
| S5 | **Full-cluster restart (recovery)** | `stop` all, `start` all (volumes kept) | pre-restart committed **log prefix** survives; leader re-forms; its current-term NoOp commits the prefix; all P5a state machines rebuild equal applied state. Covered by the ignored Rust recovery test. |
| S6 | **Network partition (split-brain)** | see partition mechanics §7 | majority commits; minority makes **no new commits**; heal → converge; **≤1 leader/term throughout (P2b records)** |
| S7 | **Rolling restart** | restart nodes one-by-one, waiting for healthy | cluster stays available; no committed write lost |
| S8 | **Load / soak** | steady write load ± Pumba latency/loss | sustained commits; final convergence; **no ack'd write lost** (durability, at-least-once — *not* duplicate-free; see below) |
| S9 | **Log rotation under volume** | large payloads to force segment roll (ties to `test_log_rotation.sh`) | segments roll; after S5 restart the durable log **prefix** still reads back across segments |

**S6 concrete variants** (each names the exact cut per §7 mechanics): (a) 3-node,
isolate 1 follower → 2-node majority commits, isolated node stalls; (b) 3-node,
isolate the *leader* → remaining 2 elect a new leader, old leader cannot commit;
(c) 5-node, `{n1,n2}` vs `{n3,n4,n5}` group partition (Pumba/iptables) → only the
3-node side commits. Reheal restores links (with `--alias` on reconnect) and the
harness asserts convergence.

**S8 accounting:** the harness stamps each payload with a unique UUID, records
every `success=true` ack, and after the run asserts the acknowledged set is a
subset of the committed log. It proves **durability (no ack'd write lost)** but
**not exactly-once** — a post-commit client timeout + retry can duplicate a
payload, because there is no client request-id/dedup today (§11, S13).

Optional stretch: S10 minority-then-restore leader flapping; S11 disk-full
behavior (constrain volume size); S12 5-node vs 3-node parametrization; **S13
exactly-once** — *blocked* until client session/request IDs + server-side dedup
exist; only then can duplicate-freedom be asserted.

## 9. How to run

### 9.1 Local, one command
```bash
scripts/e2e-docker.sh            # build images → compose up → wait healthy →
                                 # run suite in tester → collect logs → compose down -v
# or:
make e2e-docker
make e2e-docker SUITE=S1,S3,S6   # subset
make e2e-docker KEEP=1           # leave the cluster up for inspection on failure
```

`scripts/e2e-docker.sh` responsibilities — note the **unique project name** that
makes concurrent runs isolated (#3):
```bash
PROJECT="raft-e2e-${RUN_ID:-$$}"          # unique per invocation; no top-level `name:`
COMPOSE="docker compose -p $PROJECT -f docker/compose.e2e.yaml"
```
1. `$COMPOSE build`
2. `$COMPOSE up -d --wait` the **nodes only** (tester stays behind its `profile`);
   `--wait` blocks on `service_healthy`, no fixed sleep.
3. For each scenario, run the **phased handshake** below.
4. On failure (or always): `$COMPOSE logs --no-color > artifacts/$PROJECT/…` per
   node, plus the harness report + ledger.
5. `$COMPOSE down -v` to destroy containers **and volumes** (unless `KEEP=1`),
   guaranteeing a clean next run. Because `$PROJECT`, the network, and the volumes
   are all project-scoped, two runs never collide.

**Host-runner ⇄ tester handshake (a scenario is not one tester call).** A fault
scenario needs observations/writes *before* and *after* a host-side fault, so a
single `tester --suite <id>` invocation cannot express it — the fault has to be
injected *between* tester steps, and state (acked writes, observed commit
indexes, leadership cursor) must persist *across* those steps. Model each
scenario as **discrete tester phases invoked by the host runner**, with the
**host runner owning the durable ledger** (a JSON file under
`artifacts/$PROJECT/<scenario>/ledger.json`) that is passed into each phase:

```bash
LEDGER=artifacts/$PROJECT/$SID/ledger.json
# Initialize the ledger FILE first. With `-v`, a missing host path is created as a
# DIRECTORY by Docker, which would break the bind-mount — so make the parent and
# write valid initial JSON before any run_phase.
mkdir -p "$(dirname "$LEDGER")"; printf '{}\n' > "$LEDGER"
run_phase () {  # $1 = phase name
  $COMPOSE run --rm --no-deps -v "$PWD/$LEDGER:/ledger.json" tester \
      --scenario "$SID" --phase "$1" --ledger /ledger.json
}
run_phase setup        # form cluster, record baseline (leader, commit index)
run_phase write-pre    # issue writes with unique UUID payloads; append acks to ledger
run_phase assert-pre   # majority committed; log/state agree; election safety so far
inject_fault "$SID"    # HOST injects (kill/partition/etc.), resolving $cid/$net (§7)
run_phase write-post   # writes under/after fault; append acks
heal_fault "$SID"      # HOST heals (reconnect + --alias, or Pumba expiry)
run_phase assert-post  # convergence; NO ack'd write lost (ledger vs committed log);
                       #   leadership-transition record shows ≤1 leader/term
```

Each phase is a short, idempotent tester subcommand that **reads and updates the
ledger** and exits 0/1; the host runner sequences phases and fault hooks and is
the single source of truth for what was acknowledged. (Equivalent alternative:
fold the whole protocol into the host runner and make the tester a thin
per-request CLI — but keep exactly one owner of the ledger.) This replaces the
earlier one-shot `--suite` description.

### 9.2 Interactive debugging
Bring up with the **debug override** so host ports exist (ephemeral mappings), and
target the port Docker assigned:
```bash
PROJECT=raft-dbg
COMPOSE="docker compose -p $PROJECT -f docker/compose.e2e.yaml -f docker/compose.debug.yaml"
$COMPOSE up -d --wait
P1=$($COMPOSE port node1 18001 | cut -d: -f2)          # resolve the ephemeral host port
grpcurl -plaintext localhost:$P1 raft.RaftService/GetStatus     # needs P2a + reflection/-proto (P4)

# NOTE: `raft-client --config cluster.docker.yaml` from the HOST will NOT work —
# that config addresses peers as node1/node2/node3, which only resolve *inside*
# the compose network. Two options:
#   (a) run the client in the network. The image ENTRYPOINT is `rust-raft-log`, so
#       override it with --entrypoint, and the config is NOT baked into the image,
#       so bind-mount it:
docker run --rm --network "${PROJECT}_raft-e2e-net" \
    -v "$PWD/docker/config:/etc/raft:ro" \
    --entrypoint raft-client rust-raft-log:e2e \
    --config /etc/raft/cluster.docker.yaml \
    --payload '{"action":"add","value":"5"}'
#   (b) or use a localhost debug config built from the ephemeral host ports above.

$COMPOSE logs -f node1
# manual partition — resolve the real container AND network (both project-scoped):
net=$(docker network ls --filter "label=com.docker.compose.project=$PROJECT" \
                        --filter "name=raft-e2e-net" -q)
docker network disconnect "$net" "$($COMPOSE ps -q node1)"
```

### 9.3 CI (`.github/workflows/e2e-docker.yml`, separate job)
- `runs-on: ubuntu-latest` (Docker + compose preinstalled).
- Build the image with BuildKit layer cache (or cargo-chef) to keep it fast.
- Run `scripts/e2e-docker.sh` with the **PR subset** (S0–S3, S5) on pull requests;
  run the **full matrix** (S0–S9) on a nightly `schedule:` trigger to keep PR
  latency low.
- Always upload `artifacts/` (per-node logs + harness report) on failure.
- Keep this **separate** from the existing fast unit/`e2e_*` job so a flaky
  chaos test never blocks unit feedback; set a generous `timeout-minutes`.

## 10. Phased rollout (each phase independently useful, keeps CI green)

1. **P0 — Buildable image + bring-up.** Dockerfile + `.dockerignore`;
   `compose.e2e.yaml` (no top-level `name:`) for 3 nodes; `cluster.docker.yaml`
   with service-name addressing. **Validate P1** (already satisfied — S0 bring-up
   *is* the check). Healthcheck is the **installed TCP-listen probe** (P4 P0
   variant), since `GetStatus` doesn't exist yet. Exit criterion: `docker compose
   -p <uniq> up --wait` forms a cluster and a network-side `raft-client` write
   commits. Ship S0.
2. **P1 — Observability + oracle.** Implement **P2a** `GetStatus` +
   `GetCommittedEntries` and **P2b** the **crash-durable** leadership-transition
   record; add the readiness probe (P4 P2a variant) — either the `raft-probe`
   bin or `grpcurl` **with reflection or `-proto`** (bare `grpcurl` won't work).
   Exit criterion: harness reads term/role/leader/commit index and a durable
   transition record black-box.
3. **P2 — Core suite.** Rust (gRPC-only) harness + host runner implementing the
   **phased handshake + ledger** (§9); scenarios **S1–S3, S5** (S5 at **P5b**
   scope: log-prefix + availability). Wire `scripts/e2e-docker.sh` (unique
   `--project-name`, project-scoped network/container resolution) + `make`. Add
   the PR CI job.
4. **P3 — State machine + chaos.** **P5a is complete:** the arithmetic machine is
   in importable `raft_log::testkit`, `raft-node-test` embeds it, and
   the ignored Rust recovery test verifies full-restart reconstruction by the elected
   leader's current-term NoOp. Add Pumba + concrete partition mechanics;
   scenarios **S4, S6, S7, S8**. Add nightly full-matrix CI.
5. **P4 — Hardening.** S9 log-rotation-under-volume; 5-node parametrization;
   distroless runtime; flake-hunt (run each scenario ×N, tune P3 timeouts);
   artifact/log-bundle polish. S13 (exactly-once) remains blocked on client
   request-id/dedup support.

## 11. Risks & mitigations

- **Flakiness from container/network jitter** → all assertions are
  deadline+poll, never fixed sleeps; roomier election timeouts (P3); run each
  chaos scenario multiple iterations in nightly to surface rare races.
- **Observability gap (no read RPC)** → P2a+P2b are hard prerequisites for
  meaningful black-box assertions; the volume-mount fallback exists but is
  discouraged and can't cover election safety.
- **Election safety can't be sampled** → asserted over P2b transition records,
  not `GetStatus` polls (§6).
- **Recovery/state-machine over-claim** → the production binary has no state
  machine, so it remains P5b-scoped. Docker E2E uses `raft-node-test`; its
  full-restart reconstruction is verified only after the elected leader commits
  a current-term NoOp, never by blindly applying an uncommitted log suffix.
- **Durability ≠ exactly-once** → the ack oracle proves no ack'd write is lost
  (at-least-once) via unique payload IDs; duplicate-freedom needs client
  request-id + dedup (S13), not yet supported — stated, not assumed.
- **Slow image builds in CI** → BuildKit cache mounts / cargo-chef; build once,
  reuse across the matrix.
- **Self-partitioning the tester** → resolved by the single orchestration model
  (§4/§7): the **host runner owns all Docker faults**; the tester speaks gRPC
  only, holds **no** Docker socket, and never severs its own connectivity.
- **Concurrent-run collisions** → unique `--project-name` per invocation, no
  top-level `name:`, host ports only in the opt-in debug override (ephemeral
  mappings); `compose down -v` per run; `KEEP=1` only for debugging.
- **Partition commands addressing the wrong target** → always resolve the real
  container via `compose ps -q <svc>` **and** the project-scoped network
  (`${PROJECT}_raft-e2e-net`, via label filter); restore `--alias` on reconnect;
  use Pumba/iptables (with `NET_ADMIN`) for group partitions (§7).
- **`grpcurl` healthcheck fails silently** → the server has no reflection, so the
  probe needs reflection enabled or `-import-path/-proto`, or use the purpose-
  built `raft-probe`; and P0 (pre-`GetStatus`) uses an installed TCP probe, not a
  binary the image lacks (P4).
- **Lost leadership event on crash** → the transition record is crash-durable
  (fsync to the node volume) or streamed to a host ledger with an acked cursor
  before faults (P2b), so S3/S6 can't miss a pre-crash transition.
- **Under-specified fault handshake** → scenarios run as host-sequenced tester
  phases (`setup/write-pre/assert-pre → fault → write-post/heal → assert-post`)
  with the **host runner owning the durable ledger**, so pre/post-fault state and
  acks survive across tester invocations (§9).
```
