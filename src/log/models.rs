use std::path::PathBuf;

/// Type of log entry for Raft consensus
#[derive(Debug, Clone, PartialEq)]
pub enum EntryType {
    /// Normal application command
    Normal = 0,
    /// No-operation entry for leader heartbeats to establish authority
    NoOp = 1,
}

impl From<u8> for EntryType {
    fn from(value: u8) -> Self {
        match value {
            0 => EntryType::Normal,
            1 => EntryType::NoOp,
            _ => EntryType::Normal, // Default to Normal for unknown values
        }
    }
}

impl From<EntryType> for u8 {
    fn from(entry_type: EntryType) -> Self {
        entry_type as u8
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub(crate) term: u64,
    pub(crate) index: u64,
    pub(crate) entry_type: EntryType,
    pub(crate) payload: Vec<u8>,
}

impl LogEntry {
    pub(crate) fn new(term: u64, index: u64, payload: Vec<u8>) -> Self {
        LogEntry {
            term,
            index,
            entry_type: EntryType::Normal,
            payload,
        }
    }

    pub(crate) fn new_with_type(term: u64, index: u64, entry_type: EntryType, payload: Vec<u8>) -> Self {
        LogEntry {
            term,
            index,
            entry_type,
            payload,
        }
    }

    pub fn calculate_total_size(&self) -> u64 {
        // term (8) + index (8) + entry_type (1) + payload_size (8) + payload
        self.payload.len() as u64 + 8 + 8 + 1 + 8
    }
}

pub enum AppendResult {
    Success,
    RotationNeeded,
}

/// Configuration for RaftLog
#[derive(Debug, Clone)]
pub struct RaftLogConfig {
    /// Directory where log segment files are stored
    pub log_directory: PathBuf,
    /// Maximum size of each log segment file in bytes
    pub segment_size: u64,
    /// Maximum number of entries to return in get_entries
    pub max_entries_per_query: usize,
}

impl Default for RaftLogConfig {
    fn default() -> Self {
        RaftLogConfig {
            log_directory: PathBuf::from("./raft_logs"),
            segment_size: 64 * 1024 * 1024, // 64MB default
            max_entries_per_query: 1000,
        }
    }
}

/// Node identifier type
pub type NodeId = u32;

/// Information about a node in the cluster
#[derive(Debug, Clone, PartialEq)]
pub struct NodeInfo {
    pub node_id: NodeId,
    pub ip_address: String,
    pub port: u16,
}

impl NodeInfo {
    pub fn new(node_id: NodeId, ip_address: String, port: u16) -> Self {
        NodeInfo {
            node_id,
            ip_address,
            port,
        }
    }

    /// Gets the node's address as "ip:port"
    pub fn get_address(&self) -> String {
        format!("{}:{}", self.ip_address, self.port)
    }
}

/// Cluster configuration for Raft nodes
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's ID
    pub node_id: NodeId,
    /// All nodes in the cluster (including this node)
    pub nodes: Vec<NodeInfo>,
    /// Directory for Raft log files
    pub log_directory: String,
    /// Path for Raft metadata file
    pub meta_file_path: String,
    /// Log segment size in bytes
    pub log_segment_size: u64,
    /// Maximum entries per query (for get_entries)
    pub max_entries_per_query: usize,
}

impl ClusterConfig {
    /// Creates a new ClusterConfig with the specified parameters
    pub fn new(
        node_id: NodeId,
        nodes: Vec<NodeInfo>,
        log_directory: String,
        meta_file_path: String,
        log_segment_size: u64,
        max_entries_per_query: usize,
    ) -> Self {
        ClusterConfig {
            node_id,
            nodes,
            log_directory,
            meta_file_path,
            log_segment_size,
            max_entries_per_query,
        }
    }

    /// Creates a default test configuration for a 3-node cluster
    pub fn test_cluster_config(node_id: NodeId) -> Self {
        let nodes = vec![
            NodeInfo::new(1, "127.0.0.1".to_string(), 8001),
            NodeInfo::new(2, "127.0.0.1".to_string(), 8002),
            NodeInfo::new(3, "127.0.0.1".to_string(), 8003),
        ];

        ClusterConfig {
            node_id,
            nodes,
            log_directory: format!("./test_logs_node_{}", node_id),
            meta_file_path: format!("./test_logs_node_{}/raft_state.meta", node_id),
            log_segment_size: 1024 * 1024, // 1MB segments
            max_entries_per_query: 100,
        }
    }

    /// Creates a single-node test configuration (for simple tests)
    pub fn test_config(node_id: NodeId) -> Self {
        let nodes = vec![
            NodeInfo::new(node_id, "127.0.0.1".to_string(), 8000 + node_id as u16),
        ];

        ClusterConfig {
            node_id,
            nodes,
            log_directory: format!("./test_logs_node_{}", node_id),
            meta_file_path: format!("./test_logs_node_{}/raft_state.meta", node_id),
            log_segment_size: 1024 * 1024, // 1MB segments
            max_entries_per_query: 100,
        }
    }

    /// Gets this node's information
    pub fn get_this_node(&self) -> Option<&NodeInfo> {
        self.nodes.iter().find(|node| node.node_id == self.node_id)
    }

    /// Gets this node's address as "ip:port"
    pub fn get_address(&self) -> String {
        if let Some(node) = self.get_this_node() {
            node.get_address()
        } else {
            "unknown".to_string()
        }
    }

    /// Gets all other nodes in the cluster (excluding this node)
    pub fn get_other_nodes(&self) -> Vec<&NodeInfo> {
        self.nodes.iter().filter(|node| node.node_id != self.node_id).collect()
    }

    /// Gets all nodes in the cluster
    pub fn get_all_nodes(&self) -> &Vec<NodeInfo> {
        &self.nodes
    }

    /// Gets a specific node by ID
    pub fn get_node(&self, node_id: NodeId) -> Option<&NodeInfo> {
        self.nodes.iter().find(|node| node.node_id == node_id)
    }

    /// Gets the cluster size
    pub fn cluster_size(&self) -> usize {
        self.nodes.len()
    }

    /// Gets the majority size (quorum) for this cluster
    pub fn majority_size(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    /// Converts to RaftLogConfig for creating RaftLog
    pub fn to_raft_log_config(&self) -> RaftLogConfig {
        RaftLogConfig {
            log_directory: self.log_directory.clone().into(),
            segment_size: self.log_segment_size,
            max_entries_per_query: self.max_entries_per_query,
        }
    }
}

/// Server state in Raft consensus
#[derive(Debug, Clone, PartialEq)]
pub enum ServerState {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}

impl From<u8> for ServerState {
    fn from(value: u8) -> Self {
        match value {
            0 => ServerState::Follower,
            1 => ServerState::Candidate,
            2 => ServerState::Leader,
            _ => ServerState::Follower, // Default to Follower for unknown values
        }
    }
}

impl From<ServerState> for u8 {
    fn from(state: ServerState) -> Self {
        state as u8
    }
}

/// Errors that can occur during RaftLog operations
#[derive(Debug, PartialEq)]
pub enum RaftLogError {
    /// Failed to create or access log directory
    DirectoryError(String),
    /// Failed to create or open a segment file
    SegmentFileError(String),
    /// Invalid log index requested
    InvalidIndex(u64),
    /// Requested too many entries (exceeds max_entries_per_query)
    TooManyEntriesRequested(usize),
    /// No log entries exist
    EmptyLog,
    /// Segment file is corrupted or has invalid format
    CorruptedSegment(String),
}

/// Errors that can occur during RaftState operations
#[derive(Debug, PartialEq)]
pub enum RaftStateError {
    /// Failed to create or access state file
    StateFileError(String),
    /// State file is corrupted or has invalid format
    CorruptedState(String),
    /// I/O error during state operations
    IoError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_info() {
        let node = NodeInfo::new(1, "192.168.1.100".to_string(), 8001);
        assert_eq!(node.node_id, 1);
        assert_eq!(node.ip_address, "192.168.1.100");
        assert_eq!(node.port, 8001);
        assert_eq!(node.get_address(), "192.168.1.100:8001");
    }

    #[test]
    fn test_cluster_config_creation() {
        let nodes = vec![
            NodeInfo::new(1, "192.168.1.100".to_string(), 8001),
            NodeInfo::new(2, "192.168.1.101".to_string(), 8002),
            NodeInfo::new(3, "192.168.1.102".to_string(), 8003),
        ];

        let config = ClusterConfig::new(
            2, // This is node 2
            nodes,
            "/var/raft/logs".to_string(),
            "/var/raft/state.meta".to_string(),
            1024 * 1024, // 1MB segments
            1000,
        );

        assert_eq!(config.node_id, 2);
        assert_eq!(config.cluster_size(), 3);
        assert_eq!(config.majority_size(), 2);
        assert_eq!(config.get_address(), "192.168.1.101:8002");

        // Test finding this node
        let this_node = config.get_this_node().unwrap();
        assert_eq!(this_node.node_id, 2);
        assert_eq!(this_node.get_address(), "192.168.1.101:8002");

        // Test finding other nodes
        let others = config.get_other_nodes();
        assert_eq!(others.len(), 2);
        assert!(others.iter().any(|n| n.node_id == 1));
        assert!(others.iter().any(|n| n.node_id == 3));

        // Test finding specific node
        let node1 = config.get_node(1).unwrap();
        assert_eq!(node1.get_address(), "192.168.1.100:8001");
    }

    #[test]
    fn test_cluster_config_helpers() {
        // Test 3-node cluster config
        let config = ClusterConfig::test_cluster_config(2);
        assert_eq!(config.node_id, 2);
        assert_eq!(config.cluster_size(), 3);
        assert_eq!(config.get_address(), "127.0.0.1:8002");

        // Test single-node config
        let single_config = ClusterConfig::test_config(5);
        assert_eq!(single_config.node_id, 5);
        assert_eq!(single_config.cluster_size(), 1);
        assert_eq!(single_config.majority_size(), 1);
        assert_eq!(single_config.get_address(), "127.0.0.1:8005");
        assert_eq!(single_config.get_other_nodes().len(), 0);
    }

    #[test]
    fn test_raft_log_config_conversion() {
        let cluster_config = ClusterConfig::test_cluster_config(1);
        let raft_log_config = cluster_config.to_raft_log_config();

        assert_eq!(raft_log_config.log_directory.to_string_lossy(), cluster_config.log_directory);
        assert_eq!(raft_log_config.segment_size, cluster_config.log_segment_size);
        assert_eq!(raft_log_config.max_entries_per_query, cluster_config.max_entries_per_query);
    }
}
