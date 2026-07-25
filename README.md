# Rust Raft Log

\`rust-raft-log\` is a Rust implementation of the Raft consensus protocol. It provides a gRPC-facing Raft node, a durable segmented log, leader election, replicated client proposals, and an optional deterministic state machine for committed commands.

It is designed as a small, inspectable consensus core and an executable reference for Raft's election, replication, recovery, and failover paths. See the [Raft design guide](docs/RAFT_DESIGN.html) for the runtime model.

## What is implemented

- Randomized-timeout leader election with persistent \`current_term\` and \`voted_for\`.
- \`RequestVote\` log freshness checks and voter validation.
- \`AppendEntries\` heartbeats, batched replication, matching-prefix preservation, conflicting-suffix truncation, and follower catch-up.
- Majority-based commitment of entries from the leader's current term.
- A durable current-term \`NoOp\` when a candidate becomes leader; this establishes leadership and allows prior entries to become committed safely.
- Client proposals that succeed only after their entry is committed. Followers return the current leader ID when known so clients can retry there.
- Memory-mapped, rotating log segments and a separate memory-mapped hard-state file.
- Fail-stop behaviour after a persistence or storage-invariant failure: the node no longer returns normal successful Raft responses.
- Optional synchronous application of committed \`Normal\` entries through \`StateMachine\`.
- A single-owner Raft actor: protocol state is mutated on one dedicated thread while gRPC I/O remains asynchronous.

## Deliberate boundaries

This repository does not currently implement dynamic membership changes, log compaction/snapshots, a linearizable read protocol, or an application-state snapshot format. Client commands are raw bytes, and application-level command validation, deduplication, authorization, and state persistence belong to the embedding application.

\`current_term\` and \`voted_for\` are durable. \`commit_index\`, \`last_applied\`, and the server role are intentionally volatile and restart as follower state. An embedded state machine is applied only after Raft has established commitment; it must be deterministic, fast, and non-blocking because \`apply\` runs on the Raft actor thread.

## Architecture

\`\`\`mermaid
flowchart LR
    Client[raft-client / application] -->|ClientRequest| Service[gRPC service]
    Peer[Raft peer] -->|RequestVote / AppendEntries| Service
    Service --> Handle[RaftHandle]
    Handle --> Actor[Single-owner Raft actor]
    Actor --> Node[RaftNode]
    Node --> Log[Segmented mmap log]
    Node --> HardState[Term + vote mmap file]
    Actor -->|outbound RPCs| Peers[Peer gRPC services]
    Node -->|committed Normal entries| SM[Optional StateMachine]
\`\`\`

The gRPC service has no Raft state. It assigns request IDs and forwards work to \`RaftHandle\`; the actor serializes every state transition, RPC handler, election timeout, replication response, and client proposal. Outbound gRPC calls run on Tokio and post their responses back to that actor.

## Storage and durability

Each node owns a directory beneath \`cluster_settings.log_directory\`:

\`\`\`text
raft_logs/
└── node_1/
    ├── raft_state.meta          # magic/version, current_term, voted_for
    ├── log-segment-0000000001.dat
    └── log-segment-0000000002.dat
\`\`\`

The log is an ordered set of fixed-size memory-mapped segment files. A segment rotates when it cannot fit another entry; recovery validates segment headers and requires a contiguous sequence of segment bases. When a follower repairs a conflicting suffix, changed segment data is flushed before its \`AppendEntries\` success is reported, then obsolete segment files are removed and the directory is synced.

Hard state is flushed before a response that depends on a changed term or vote. A persistence failure stops the node rather than turning an unsafe state transition into an ordinary Raft rejection.

## Quick start

Requirements: a current stable Rust toolchain, \`protoc\` (used at build time), and Docker only for the Docker cluster workflow.

\`\`\`bash
cargo build --bins
cargo test
\`\`\`

Start three nodes using the included configuration, one terminal per node:

\`\`\`bash
cargo run --bin rust-raft-log -- --node-id 1 --config cluster_config.yaml
cargo run --bin rust-raft-log -- --node-id 2 --config cluster_config.yaml
cargo run --bin rust-raft-log -- --node-id 3 --config cluster_config.yaml
\`\`\`

Once a leader is elected, submit a command. The client follows leader hints and retries other nodes when necessary:

\`\`\`bash
cargo run --bin raft-client -- \
  --config cluster_config.yaml \
  --payload 'hello, raft'
\`\`\`

\`raft-client\` reports success only when the contacted leader has committed the entry. A request that reaches a follower returns a leader hint when that follower knows one.

## Configuration

\`cluster_config.yaml\` is the local three-node example. Every node must use the same \`nodes\` list and timing settings, while \`node_id\` selects the local node:

\`\`\`yaml
nodes:
  - node_id: 1
    ip_address: "127.0.0.1"
    port: 18001
  - node_id: 2
    ip_address: "127.0.0.1"
    port: 18002
  - node_id: 3
    ip_address: "127.0.0.1"
    port: 18003

cluster_settings:
  log_directory: "./raft_logs"
  log_segment_size: 67108864
  max_entries_per_query: 1000
  election_timeout_range:
    min: 300
    max: 450
  heartbeat_interval: 150
\`\`\`

\`log_directory\` is expanded to a per-node log directory and metadata file. \`max_entries_per_query\` also bounds the replication batch size. Election timeouts and heartbeat intervals are in milliseconds.

## Application state machines

The production binary creates a consensus-only \`RaftNode\`. Applications can supply a deterministic state machine when constructing the node:

\`\`\`rust
use raft_log::{LogEntry, RaftNode, StateMachine};

struct Counter(i64);

impl StateMachine for Counter {
    fn apply(&mut self, entry: &LogEntry) {
        // Decode entry.payload() and update the application state.
        let _ = entry;
    }
}

let node = RaftNode::new_with_state_machine(config, Box::new(Counter(0)))?;
\`\`\`

Only committed \`Normal\` entries invoke \`apply\`; Raft \`NoOp\` entries still advance \`last_applied\`. The Docker-only \`raft-node-test\` binary embeds the repository's arithmetic state machine.

## gRPC API

The service definition is [proto/raft.proto](proto/raft.proto).

| RPC | Purpose |
| --- | --- |
| \`RequestVote\` | Candidates request votes for an election term. |
| \`AppendEntries\` | Leaders send heartbeats or replicate entries; followers return success and their log end. |
| \`ClientRequest\` | Clients submit one raw command payload; success means the entry committed. |

## Tests and Docker workflow

The unit tests cover state persistence, segment encoding and recovery, log rotation/truncation, elections, replication safety, and state-machine application. In-process integration and end-to-end suites exercise leader election, replication, failover, quorum loss, and full restart recovery.

\`\`\`bash
# Default unit and integration suite
cargo test

# Ignored end-to-end suites
cargo test --test e2e_single -- --ignored
cargo test --test e2e_cluster -- --ignored
cargo test --test e2e_failover -- --ignored
cargo test --test e2e_recovery -- --ignored
cargo test --test e2e_two_node -- --ignored

# Docker checks (requires Docker)
make e2e-docker
make e2e-docker-failover
\`\`\`

For interactive Docker cluster commands and cleanup, see the scripts in `scripts/`.

## Repository map

\`\`\`text
src/
├── consensus/  # RaftNode, persistent/volatile state, StateMachine trait
├── grpc/       # actor, service, gRPC client and server
├── models/     # configuration, RPC types, protobuf conversions
├── storage/    # mmap segment and log implementation
└── testkit/    # arithmetic state machine used by tests and Docker E2E
proto/          # Raft gRPC service definition
tests/          # cross-module and end-to-end tests
docs/           # architecture diagrams and Docker playbook
\`\`\`

## Further reading

- [Raft design guide](docs/RAFT_DESIGN.html)
- [Docker cluster playbook](docs/docker_cluster_playbook.md)
- [Raft paper](https://raft.github.io/raft.pdf)
