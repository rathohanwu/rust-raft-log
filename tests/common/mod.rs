#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::net::TcpListener as StdTcpListener;
use std::sync::{Arc, Mutex};

use raft_log::{
    ClusterConfig, EntryType, LogEntry, NodeId, NodeInfo, RaftGrpcClient, RaftGrpcServer, RaftNode,
    RaftNodeView, ServerState, StateMachine,
};
use serde::Deserialize;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};

pub const TEST_ELECTION_TIMEOUT: (u64, u64) = (300, 600);
pub const TEST_HEARTBEAT_MS: u64 = 75;

pub type EntryView = (u64, u64, u8, Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArithmeticState {
    pub value: i64,
    pub applied_commands: usize,
}

#[derive(Deserialize)]
struct ArithmeticCommand {
    action: String,
    value: String,
}

/// Encodes a deterministic arithmetic command used by the E2E state machine.
pub fn arithmetic_command(action: &str, value: i64) -> Vec<u8> {
    serde_json::json!({ "action": action, "value": value.to_string() })
        .to_string()
        .into_bytes()
}

/// Test-only deterministic state machine. Its shared handle lets E2E tests
/// assert replicated application state, not merely replicated log input.
struct ArithmeticStateMachine {
    state: Arc<Mutex<ArithmeticState>>,
}

impl StateMachine for ArithmeticStateMachine {
    fn apply(&mut self, entry: &LogEntry) {
        let command: ArithmeticCommand =
            serde_json::from_slice(entry.payload()).expect("E2E command payload must be JSON");
        let operand = command
            .value
            .parse::<i64>()
            .expect("E2E command value must be an i64 string");
        let mut state = self.state.lock().unwrap();
        state.value = match command.action.as_str() {
            "add" => state.value.checked_add(operand),
            "subtract" => state.value.checked_sub(operand),
            "multiply" => state.value.checked_mul(operand),
            "divide" => {
                assert_ne!(operand, 0, "E2E arithmetic command must not divide by zero");
                state.value.checked_div(operand)
            }
            action => panic!("unsupported E2E arithmetic action: {action}"),
        }
        .expect("E2E arithmetic command overflowed");
        state.applied_commands += 1;
    }
}

#[cfg(test)]
mod arithmetic_state_machine_tests {
    use super::*;

    #[test]
    fn applies_add_subtract_multiply_and_divide_in_order() {
        let state = Arc::new(Mutex::new(ArithmeticState {
            value: 0,
            applied_commands: 0,
        }));
        let mut machine = ArithmeticStateMachine {
            state: Arc::clone(&state),
        };

        for (index, (action, value)) in
            [("add", 8), ("multiply", 3), ("subtract", 4), ("divide", 2)]
                .into_iter()
                .enumerate()
        {
            machine.apply(&LogEntry::new_with_type(
                1,
                index as u64 + 1,
                EntryType::Normal,
                arithmetic_command(action, value),
            ));
        }

        assert_eq!(
            *state.lock().unwrap(),
            ArithmeticState {
                value: 10,
                applied_commands: 4,
            }
        );
    }
}

#[derive(Debug)]
pub enum ClientOutcome {
    Committed { index: u64 },
    NotLeader { hint: NodeId },
    TimedOut,
    Failed(String),
}

struct NodeRuntime {
    config: ClusterConfig,
    server: RaftGrpcServer,
    event_loop: Option<JoinHandle<()>>,
    grpc_server: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

/// In-process test cluster whose retained listener keeps every selected port reserved.
pub struct TestCluster {
    listeners: HashMap<NodeId, StdTcpListener>,
    temp_dirs: HashMap<NodeId, TempDir>,
    runtimes: HashMap<NodeId, NodeRuntime>,
    active_nodes: HashSet<NodeId>,
    state_machine_states: HashMap<NodeId, Arc<Mutex<ArithmeticState>>>,
    client: RaftGrpcClient,
}

impl TestCluster {
    pub async fn start(size: u32) -> Self {
        assert!(size > 0);
        let mut listeners = HashMap::new();
        let mut nodes = Vec::new();
        for id in 1..=size {
            let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind test listener");
            let port = listener.local_addr().unwrap().port();
            listeners.insert(id, listener);
            nodes.push(NodeInfo::new(id, "127.0.0.1".to_string(), port));
        }

        let mut temp_dirs = HashMap::new();
        let mut runtimes = HashMap::new();
        let mut state_machine_states = HashMap::new();
        for id in 1..=size {
            let temp_dir = TempDir::new().expect("create test data directory");
            let config = Self::config(id, nodes.clone(), &temp_dir);
            let state = Arc::new(Mutex::new(ArithmeticState {
                value: 0,
                applied_commands: 0,
            }));
            let server = RaftGrpcServer::new(
                RaftNode::new_with_state_machine(
                    config.clone(),
                    Box::new(ArithmeticStateMachine {
                        state: Arc::clone(&state),
                    }),
                )
                .unwrap(),
            );
            temp_dirs.insert(id, temp_dir);
            state_machine_states.insert(id, state);
            runtimes.insert(
                id,
                NodeRuntime {
                    config,
                    server,
                    event_loop: None,
                    grpc_server: None,
                },
            );
        }

        let client = RaftGrpcClient::new(runtimes.get(&1).unwrap().config.clone());
        let mut cluster = Self {
            listeners,
            temp_dirs,
            runtimes,
            active_nodes: HashSet::new(),
            state_machine_states,
            client,
        };
        for id in cluster.node_ids() {
            cluster.start_node(id).await;
        }
        cluster
    }

    fn config(id: NodeId, nodes: Vec<NodeInfo>, temp_dir: &TempDir) -> ClusterConfig {
        ClusterConfig::new(
            id,
            nodes,
            temp_dir.path().join("logs").to_string_lossy().to_string(),
            temp_dir
                .path()
                .join("raft_state.meta")
                .to_string_lossy()
                .to_string(),
            1024 * 1024,
            100,
            TEST_ELECTION_TIMEOUT,
            TEST_HEARTBEAT_MS,
        )
    }

    async fn start_node(&mut self, id: NodeId) {
        let listener = self.listeners.get(&id).unwrap().try_clone().unwrap();
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        let runtime = self.runtimes.get_mut(&id).unwrap();
        let (event_loop, grpc_server) = runtime.server.start_with_listener(listener);
        runtime.event_loop = Some(event_loop);
        runtime.grpc_server = Some(grpc_server);
        self.active_nodes.insert(id);
    }

    pub fn node_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<_> = self.runtimes.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn active_node_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<_> = self.active_nodes.iter().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn raft(&self, id: NodeId) -> Arc<Mutex<RaftNodeView>> {
        self.runtimes.get(&id).unwrap().server.get_raft_node()
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> (NodeId, u64) {
        let deadline = Instant::now() + timeout;
        loop {
            let leaders: Vec<_> = self
                .active_node_ids()
                .into_iter()
                .filter_map(|id| {
                    let node = self.raft(id);
                    let node = node.lock().unwrap();
                    (node.get_server_state() == ServerState::Leader)
                        .then_some((id, node.get_current_term()))
                })
                .collect();
            if leaders.len() == 1 {
                let (leader_id, term) = leaders[0];
                let node = self.raft(leader_id);
                let node = node.lock().unwrap();
                let commit_index = node.get_state().commit_index;
                let established = commit_index > 0
                    && node
                        .get_entry(commit_index)
                        .is_some_and(|entry| entry.term() == term);
                if established {
                    return (leader_id, term);
                }
            }
            assert!(
                Instant::now() < deadline,
                "leader election timed out: {leaders:?}"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn client_write(&self, payload: Vec<u8>) -> ClientOutcome {
        let target = self.active_node_ids()[0];
        self.client_write_from(target, payload, true).await
    }

    pub async fn client_write_to(&self, target: NodeId, payload: Vec<u8>) -> ClientOutcome {
        self.client_write_from(target, payload, false).await
    }

    async fn client_write_from(
        &self,
        mut target: NodeId,
        payload: Vec<u8>,
        follow_redirect: bool,
    ) -> ClientOutcome {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match self.client.client_request(target, payload.clone()).await {
                Ok(response) if response.success => {
                    return ClientOutcome::Committed {
                        index: response.log_index,
                    }
                }
                Ok(response) if response.leader_id != 0 && response.leader_id != target => {
                    if !follow_redirect {
                        return ClientOutcome::NotLeader {
                            hint: response.leader_id,
                        };
                    }
                    target = response.leader_id;
                }
                Ok(response)
                    if response.error_message == "Timed out waiting for the entry to commit" =>
                {
                    return ClientOutcome::TimedOut;
                }
                Ok(response)
                    if response.error_message == "Not the leader. Current leader is unknown" => {}
                Ok(response)
                    if response.error_message
                        == "Leadership changed before the entry committed" =>
                {
                    // A new leader may already be available; retry the command
                    // discovery path instead of treating this transient as final.
                    if follow_redirect {
                        target = self.active_node_ids()[0];
                    } else {
                        return ClientOutcome::Failed(response.error_message);
                    }
                }
                Ok(response) => return ClientOutcome::Failed(response.error_message),
                Err(_) => {}
            }
            if Instant::now() >= deadline {
                return ClientOutcome::Failed("leader remained unknown or unreachable".to_string());
            }
            sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn kill(&mut self, id: NodeId) {
        self.active_nodes.remove(&id);
        let runtime = self.runtimes.get_mut(&id).unwrap();
        runtime.server.shutdown();
        if let Some(handle) = runtime.event_loop.take() {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(handle) = runtime.grpc_server.take() {
            handle.abort();
            let _ = handle.await;
        }
    }

    pub async fn restart(&mut self, id: NodeId) {
        self.kill(id).await;
        let runtime = self.runtimes.get_mut(&id).unwrap();
        let state = Arc::clone(self.state_machine_states.get(&id).unwrap());
        *state.lock().unwrap() = ArithmeticState {
            value: 0,
            applied_commands: 0,
        };
        runtime.server = RaftGrpcServer::new(
            RaftNode::new_with_state_machine(
                runtime.config.clone(),
                Box::new(ArithmeticStateMachine { state }),
            )
            .unwrap(),
        );
        self.start_node(id).await;
    }

    pub fn inspect_offline(&self, id: NodeId) -> RaftNode {
        let config = self.runtimes.get(&id).unwrap().config.clone();
        RaftNode::new(config).unwrap()
    }

    pub fn committed_log(&self, id: NodeId, upper_bound: u64) -> Vec<EntryView> {
        let node = self.raft(id);
        let node = node.lock().unwrap();
        (1..=upper_bound)
            .filter_map(|index| node.get_entry(index))
            .map(entry_view)
            .collect()
    }

    pub fn arithmetic_state(&self, id: NodeId) -> ArithmeticState {
        self.state_machine_states
            .get(&id)
            .unwrap()
            .lock()
            .unwrap()
            .clone()
    }

    /// Waits until every active node has applied the same expected arithmetic
    /// result. This avoids treating a shared short committed prefix as full
    /// application-state convergence after a restart or rejoin.
    pub async fn wait_for_arithmetic_state(&self, expected: ArithmeticState, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let states: Vec<_> = self
                .active_node_ids()
                .into_iter()
                .map(|id| (id, self.arithmetic_state(id)))
                .collect();
            if states.iter().all(|(_, state)| state == &expected) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "arithmetic states did not converge to {expected:?}: {states:?}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn wait_for_committed_convergence(&self, timeout: Duration) -> Vec<EntryView> {
        let deadline = Instant::now() + timeout;
        loop {
            let ids = self.active_node_ids();
            let commit_index = ids
                .iter()
                .map(|&id| self.raft(id).lock().unwrap().get_state().commit_index)
                .min()
                .unwrap();
            let logs: Vec<_> = ids
                .iter()
                .map(|&id| self.committed_log(id, commit_index))
                .collect();
            if logs.windows(2).all(|pair| pair[0] == pair[1]) {
                return logs.into_iter().next().unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "committed logs did not converge: {logs:?}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn stop_all(&mut self) {
        for id in self.node_ids() {
            self.kill(id).await;
        }
    }
}

pub fn entry_view(entry: LogEntry) -> EntryView {
    (
        entry.index(),
        entry.term(),
        match entry.entry_type() {
            EntryType::Normal => 0,
            EntryType::NoOp => 1,
        },
        entry.payload().to_vec(),
    )
}
