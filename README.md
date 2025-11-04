# Rust Raft Log

A high-performance, production-ready implementation of the Raft consensus algorithm in Rust, designed for building distributed systems that require strong consistency guarantees.

## 🎯 Project Purpose & Overview

### What is Rust Raft Log?

This project implements the **Raft consensus algorithm** - a distributed consensus protocol that ensures multiple servers agree on a shared state even in the presence of failures. Raft is designed to be understandable and provides the foundation for building fault-tolerant distributed systems.

### Primary Use Cases

**Distributed Databases**: Use Raft to ensure all database replicas maintain consistent state across network partitions and server failures.

**Configuration Management**: Distribute configuration changes across a cluster of services with strong consistency guarantees.

**Leader Election**: Implement automatic leader election in distributed systems where only one node should perform certain operations.

**State Machine Replication**: Replicate any deterministic state machine across multiple servers for high availability.

**Distributed Locking**: Build distributed coordination primitives that remain consistent during failures.

### Key Benefits

- **Strong Consistency**: Guarantees linearizable reads and writes across the cluster
- **Fault Tolerance**: Continues operating as long as a majority of nodes are available
- **Automatic Recovery**: Failed nodes automatically catch up when they rejoin the cluster
- **Production Ready**: Memory-mapped storage, efficient networking, and comprehensive error handling

## 🏗️ Technical Architecture

### Raft Log Design

The core of this implementation is a **segmented log architecture** that efficiently stores and replicates commands across the cluster:

#### Consensus Algorithm Implementation
- **Leader Election**: Implements randomized timeouts to prevent split votes and ensure quick leader election
- **Log Replication**: Leaders replicate log entries to followers with automatic retry and consistency checking
- **Safety Properties**: Ensures election safety, leader append-only, log matching, leader completeness, and state machine safety
- **Membership Changes**: Supports dynamic cluster membership (planned feature)

#### Entry Types and Structure
```rust
pub struct LogEntry {
    term: u64,        // Raft term when entry was created
    index: u64,       // Position in the log
    entry_type: EntryType, // Normal command or NoOp heartbeat
    payload: Vec<u8>, // Arbitrary application data
}

pub enum EntryType {
    Normal = 0,  // Application commands
    NoOp = 1,    // Leader heartbeat entries
}
```

### Log Segments

The log is organized into **segments** for efficient storage and management:

#### Segment Organization
- **Fixed-Size Segments**: Each segment has a configurable maximum size (default: 64MB)
- **Automatic Rotation**: New segments are created when the current segment reaches capacity
- **Base Index Tracking**: Each segment knows the index of its first entry for fast lookups
- **Memory-Mapped Files**: Segments use memory-mapped I/O for high-performance access

#### Segment Lifecycle
1. **Creation**: New segments are created with a base index and empty state
2. **Appending**: Entries are appended sequentially within each segment
3. **Rotation**: When a segment fills up, a new segment is created for subsequent entries
4. **Persistence**: All data is immediately persistent due to memory-mapped storage

### Storage System

The storage system uses **memory-mapped files** for optimal performance:

#### Memory-Mapped I/O Benefits
- **Zero-Copy Operations**: Data is accessed directly from memory without copying
- **OS-Level Caching**: The operating system handles caching and write-back automatically
- **Crash Recovery**: Data is persistent immediately, enabling fast recovery after crashes
- **Efficient Random Access**: Log entries can be accessed at any position without sequential reads

#### File Organization
```
raft_logs/
├── node_1/
│   ├── raft_state.meta      # Persistent Raft state (term, voted_for, etc.)
│   ├── segment_000001.log   # Log segment starting at index 1
│   ├── segment_001000.log   # Log segment starting at index 1000
│   └── segment_002000.log   # Log segment starting at index 2000
└── node_2/
    ├── raft_state.meta
    └── segment_*.log
```

### Header Design

Each segment file begins with a **structured header** that enables fast operations:

#### Segment Header Format
```
┌─────────────────┬───────────────────┬─────────────────┬─────────────────┬─────────────────┐
│ Magic (4 bytes) │ Version (4 bytes) │ Base Index (8)  │ Entry Count (8) │ Start Pos (8)   │
└─────────────────┴───────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

#### Header Fields
- **Magic Number**: `0x52414654` ("RAFT" in ASCII) for file type validation
- **Version**: `0x00000001` for format versioning and compatibility
- **Base Index**: Index of the first log entry in this segment
- **Entry Count**: Number of entries currently stored in the segment
- **Start Append Position**: Byte offset where the next entry will be written

#### Fast Index Operations
The header design enables several optimizations:

**O(1) Latest Index Lookup**: `latest_index = base_index + entry_count - 1`

**O(log n) Segment Selection**: Use base_index in a BTreeMap for fast segment lookup

**O(1) Append Position**: No need to scan entries to find where to append

**Fast Segment Validation**: Magic number and version checks prevent corruption

#### State File Header
The persistent Raft state uses a similar header approach:
```
┌─────────────────┬───────────────────┬─────────────────┬─────────────────┬─────────────────┐
│ Magic (4 bytes) │ Version (4 bytes) │ Current Term(8) │ Voted For (4)   │ Commit Index(8) │
├─────────────────┼───────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ Last Applied(8) │ Server State (1)  │ Reserved (3)    │                 │                 │
└─────────────────┴───────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

## 📁 Code Structure

### Module Organization

The codebase is organized into two main modules with clear separation of concerns:

#### `src/log/` - Core Raft Implementation
- **`raft_node.rs`**: Main `RaftNode` struct implementing the Raft consensus algorithm
- **`raft_log.rs`**: `RaftLog` manages the collection of log segments and provides append/query operations
- **`log_file_segment.rs`**: `LogFileSegment` handles individual segment files with memory-mapped I/O
- **`raft_state.rs`**: `RaftState` manages persistent state (current term, voted for, commit index)
- **`raft_rpc.rs`**: Defines RPC message types (`RequestVote`, `AppendEntries`) for inter-node communication
- **`models.rs`**: Core data structures (`LogEntry`, `ClusterConfig`, error types)
- **`utils.rs`** & **`mmap_utils.rs`**: Utilities for memory-mapped file operations

#### `src/grpc/` - Network Communication Layer
- **`server.rs`**: `RaftGrpcServer` hosts the gRPC service and handles incoming RPCs
- **`client.rs`**: `RaftGrpcClient` manages persistent connections to other cluster nodes
- **`event_loop.rs`**: `RaftEventLoop` handles timeouts, elections, and heartbeat scheduling
- **`conversion.rs`**: Converts between internal Rust types and Protocol Buffer messages

### Key Data Structures

#### RaftNode - The Core State Machine
```rust
pub struct RaftNode {
    config: ClusterConfig,           // Cluster topology and settings
    log: RaftLog,                    // Persistent log storage
    state: RaftState,                // Persistent Raft state
    next_index: HashMap<NodeId, u64>, // Leader state: next index to send to each follower
    match_index: HashMap<NodeId, u64>, // Leader state: highest replicated index per follower
    votes_received: HashSet<NodeId>,  // Candidate state: votes received in current election
}
```

#### RaftLog - Segment Management
```rust
pub struct RaftLog {
    config: RaftLogConfig,                    // Log configuration (directory, segment size)
    segments: BTreeMap<u64, LogFileSegment>,  // Map of base_index -> segment for fast lookup
    next_index: u64,                          // Next index to assign to new entries
}
```

#### LogFileSegment - Individual Segment
```rust
pub struct LogFileSegment {
    buffer: MmapMut,  // Memory-mapped file buffer for direct I/O
}
```

### Component Interactions

#### 1. Request Processing Flow
```
gRPC Request → RaftGrpcService → RaftNode → RaftLog → LogFileSegment → Memory-Mapped File
```

#### 2. Leader Election Flow
```
RaftEventLoop (timeout) → RaftNode (become candidate) → RaftGrpcClient (send votes) → Other Nodes
```

#### 3. Log Replication Flow
```
RaftEventLoop (heartbeat) → RaftNode (create AppendEntries) → RaftGrpcClient → Followers
```

#### 4. Persistence Flow
```
LogEntry → RaftLog (append) → LogFileSegment (write) → Memory-Mapped Buffer → OS (flush to disk)
```

### Thread Safety and Concurrency

- **RaftNode**: Protected by `Arc<Mutex<RaftNode>>` for thread-safe access from gRPC handlers
- **Memory-Mapped Files**: Safe for concurrent reads, writes are serialized through the mutex
- **gRPC Client**: Uses `Arc<RwLock<HashMap>>` for connection pooling with concurrent access
- **Event Loop**: Runs in a separate async task with controlled access to shared state

## 🚀 Getting Started

### Requirements

- **Rust**: 1.70+ (2021 edition)
- **Operating System**: Linux, macOS, or Windows
- **Network**: TCP connectivity between cluster nodes

### Installation

#### Build from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/rust-raft-log.git
cd rust-raft-log

# Build the project
cargo build --release

# The binary will be available at ./target/release/rust-raft-log
```

#### Dependencies

The project uses the following key dependencies:
- `tonic` - gRPC framework for high-performance RPC communication
- `tokio` - Async runtime for handling concurrent operations
- `serde` & `serde_yaml` - Configuration parsing and serialization
- `clap` - Command-line interface with argument parsing
- `memmap2` - Memory-mapped file I/O for efficient storage
- `parking_lot` - High-performance synchronization primitives

### Configuration

#### YAML Configuration Format

Create a YAML configuration file to define your cluster topology:

```yaml
# cluster_config.yaml
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

# Optional cluster settings (with defaults)
cluster_settings:
  log_directory: "./raft_logs"
  log_segment_size: 67108864  # 64MB in bytes
  max_entries_per_query: 1000
  election_timeout_range:
    min: 150  # milliseconds
    max: 500  # milliseconds
  heartbeat_interval: 50  # milliseconds
```

#### Configuration Fields

**Required Fields**
- `nodes`: Array of node configurations
  - `node_id`: Unique identifier (u32)
  - `ip_address`: Node IP address
  - `port`: gRPC server port

**Optional Fields (cluster_settings)**
- `log_directory`: Base directory for logs (default: "./raft_logs")
- `log_segment_size`: Max segment size in bytes (default: 64MB)
- `max_entries_per_query`: Max entries per query (default: 1000)
- `election_timeout_range`: Election timeout range in ms (default: 150-500ms)
- `heartbeat_interval`: Heartbeat interval in ms (default: 50ms)

### Starting a Server

#### Command Line Interface

```bash
# Run a specific node
./target/release/rust-raft-log --node-id 1 --config cluster_config.yaml

# Or using cargo
cargo run -- --node-id 1 --config cluster_config.yaml
```

#### Command Line Arguments

- `--node-id` or `-n`: The ID of this node (must exist in config)
- `--config` or `-c`: Path to the YAML configuration file
- `--help` or `-h`: Show help information

### Running Multi-Node Clusters

#### Quick Start with Script

Use the provided convenience script to start a 3-node cluster:

```bash
# Make the script executable
chmod +x run_cluster.sh

# Start the entire cluster
./run_cluster.sh
```

#### Manual Cluster Setup

Start each node in a separate terminal:

```bash
# Terminal 1 - Node 1
./target/release/rust-raft-log --node-id 1 --config cluster_config.yaml

# Terminal 2 - Node 2
./target/release/rust-raft-log --node-id 2 --config cluster_config.yaml

# Terminal 3 - Node 3
./target/release/rust-raft-log --node-id 3 --config cluster_config.yaml
```

#### Directory Structure

The application automatically creates node-specific directories:

```
raft_logs/
├── node_1/
│   ├── raft_state.meta
│   └── segment_*.log
├── node_2/
│   ├── raft_state.meta
│   └── segment_*.log
└── node_3/
    ├── raft_state.meta
    └── segment_*.log
```

Each node maintains its own isolated log storage to prevent conflicts.

## 📖 Usage Examples & Advanced Topics

### Deployment Examples

#### Local Development (3-node cluster)

```yaml
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
```

#### Distributed Deployment

```yaml
nodes:
  - node_id: 1
    ip_address: "10.0.1.10"
    port: 8001
  - node_id: 2
    ip_address: "10.0.1.11"
    port: 8001
  - node_id: 3
    ip_address: "10.0.1.12"
    port: 8001

cluster_settings:
  log_directory: "/var/raft/logs"
  log_segment_size: 134217728  # 128MB
  election_timeout_range:
    min: 200
    max: 800
  heartbeat_interval: 75
```

#### High-Performance Configuration

```yaml
nodes:
  - node_id: 1
    ip_address: "192.168.1.10"
    port: 9001
  - node_id: 2
    ip_address: "192.168.1.11"
    port: 9001
  - node_id: 3
    ip_address: "192.168.1.12"
    port: 9001

cluster_settings:
  log_directory: "/fast-ssd/raft"
  log_segment_size: 268435456  # 256MB
  max_entries_per_query: 5000
  election_timeout_range:
    min: 100
    max: 300
  heartbeat_interval: 25
```

### Error Handling

The application provides comprehensive error handling for common issues:

#### Configuration Errors
- **Invalid node ID**: Clear error if node ID not found in config
- **Missing config file**: File not found errors with specific paths
- **YAML parsing errors**: Detailed parsing error messages

#### Runtime Errors
- **Network binding failures**: Graceful handling of port conflicts
- **Directory creation failures**: Permission and disk space issues
- **gRPC communication errors**: Connection and timeout handling

#### Example Error Messages

```bash
# Invalid node ID
Error: Node ID 99 not found in configuration file

# Missing config file
Error: Configuration file not found: missing_config.yaml

# Port already in use
Error: Failed to bind to 127.0.0.1:18001 - address already in use
```

### API Documentation

#### gRPC Service Definition

The Raft implementation exposes two main RPC endpoints:

**RequestVote RPC** - Used during leader elections for candidates to gather votes.

```protobuf
rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
```

**AppendEntries RPC** - Used by leaders for log replication and heartbeats.

```protobuf
rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
```

#### Message Types

- `RequestVoteRequest/Response`: Election voting messages
- `AppendEntriesRequest/Response`: Log replication and heartbeat messages
- `LogEntry`: Individual log entries with term, index, and payload

### Testing

#### Running Tests

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test integration_leader_election
cargo test comprehensive_raft_lifecycle

# Run with output
cargo test -- --nocapture
```

#### Test Coverage

The project includes comprehensive tests for:
- Leader election scenarios
- Log replication
- Failure recovery
- Configuration validation
- Network communication

#### Manual Testing

Test the cluster behavior:

```bash
# Start a 3-node cluster
./run_cluster.sh

# Kill the leader node to test failover
# Observe automatic leader election in logs

# Restart the killed node to test recovery
# Verify the node rejoins the cluster
```

### Monitoring

Each node outputs detailed status information:

```
🚀 Starting Raft node 1 with config: cluster_config.yaml
📋 Cluster configuration:
   Node ID: 1
   Address: 127.0.0.1:18001
   Cluster size: 3
   Log directory: ./raft_logs/node_1
✅ Raft node created successfully
🌐 Starting gRPC server on 127.0.0.1:18001
🔄 Starting Raft event loop...
👑 Won election! Becoming leader...
💓 Sending heartbeats to 2 followers...
```

#### Log Levels

- 🚀 Startup information
- 📋 Configuration details
- ✅ Success messages
- ❌ Error conditions
- 👑 Leadership changes
- 💓 Heartbeat activity
- 📤📥 Vote requests/responses

### Contributing

#### Development Setup

```bash
# Clone and setup
git clone https://github.com/yourusername/rust-raft-log.git
cd rust-raft-log

# Install development dependencies
cargo build

# Run tests
cargo test

# Format code
cargo fmt

# Run linter
cargo clippy
```

#### Code Style

- Follow Rust standard formatting (`cargo fmt`)
- Address all clippy warnings (`cargo clippy`)
- Add tests for new functionality
- Update documentation for API changes

#### Submitting Changes

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Ensure all tests pass
5. Submit a pull request

## 📄 License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.

## 🙏 Acknowledgments

- The Raft consensus algorithm by Diego Ongaro and John Ousterhout
- The Rust community for excellent async and networking libraries
- Contributors and testers who helped improve this implementation

---

**Built with ❤️ in Rust**
