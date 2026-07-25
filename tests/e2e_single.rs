mod common;

use common::{arithmetic_command, ArithmeticState, ClientOutcome, TestCluster};
use raft_log::ServerState;
use tokio::time::Duration;

#[tokio::test]
#[ignore = "real gRPC E2E; run explicitly"]
async fn e2e_single_elect_write_and_apply() {
    let mut cluster = TestCluster::start(1).await;
    let (leader, _) = cluster.wait_for_leader(Duration::from_secs(3)).await;
    assert_eq!(leader, 1);

    for payload in [
        arithmetic_command("add", 8),
        arithmetic_command("multiply", 3),
        arithmetic_command("subtract", 4),
        arithmetic_command("divide", 2),
    ] {
        assert!(matches!(
            cluster.client_write(payload).await,
            ClientOutcome::Committed { .. }
        ));
    }
    let node = cluster.node_view(1);
    let node = node.lock().unwrap();
    assert_eq!(node.get_server_state(), ServerState::Leader);
    assert_eq!(node.get_state().commit_index, node.get_state().last_applied);
    drop(node);
    assert_eq!(
        cluster.arithmetic_state(1),
        ArithmeticState {
            value: 10,
            applied_commands: 4,
        }
    );
    cluster.stop_all().await;
}

#[tokio::test]
#[ignore = "real gRPC E2E; run explicitly"]
async fn e2e_single_persistence_offline_then_online() {
    let mut cluster = TestCluster::start(1).await;
    cluster.wait_for_leader(Duration::from_secs(3)).await;
    let expected_term = cluster.node_view(1).lock().unwrap().get_current_term();
    let commit_index = cluster
        .node_view(1)
        .lock()
        .unwrap()
        .get_state()
        .commit_index;
    let expected_log = cluster.committed_log(1, commit_index);

    cluster.kill(1).await;
    let inspector = cluster.inspect_offline(1);
    assert_eq!(inspector.get_current_term(), expected_term);
    assert_eq!(
        (1..=expected_log.len() as u64)
            .filter_map(|index| inspector.get_entry(index))
            .map(common::entry_view)
            .collect::<Vec<_>>(),
        expected_log
    );
    drop(inspector);

    cluster.restart(1).await;
    cluster.wait_for_leader(Duration::from_secs(3)).await;
    let node = cluster.node_view(1);
    let node = node.lock().unwrap();
    assert!(node.get_current_term() >= expected_term);
    for expected in expected_log {
        assert_eq!(
            common::entry_view(node.get_entry(expected.0).unwrap()),
            expected
        );
    }
    drop(node);
    assert!(matches!(
        cluster.client_write(arithmetic_command("add", 7)).await,
        ClientOutcome::Committed { .. }
    ));
    assert!(
        cluster
            .node_view(1)
            .lock()
            .unwrap()
            .get_state()
            .last_applied
            > 0
    );
    assert_eq!(
        cluster.arithmetic_state(1),
        ArithmeticState {
            value: 7,
            applied_commands: 1,
        }
    );
    cluster.stop_all().await;
}
