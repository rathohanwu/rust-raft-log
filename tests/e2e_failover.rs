mod common;

use common::{arithmetic_command, ArithmeticState, ClientOutcome, TestCluster};
use raft_log::ServerState;
use tokio::time::Duration;

#[tokio::test]
#[ignore = "real gRPC E2E; run explicitly"]
async fn e2e_follower_redirect_failover_completeness_and_rejoin() {
    let mut cluster = TestCluster::start(3).await;
    let (leader, original_term) = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let follower = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != leader)
        .unwrap();
    assert!(matches!(
        cluster
            .client_write_to(follower, arithmetic_command("add", 1))
            .await,
        ClientOutcome::NotLeader { hint } if hint == leader
    ));

    let payload = arithmetic_command("add", 5);
    assert!(matches!(
        cluster.client_write(payload.clone()).await,
        ClientOutcome::Committed { .. }
    ));
    let pre_failover = cluster
        .wait_for_committed_convergence(Duration::from_secs(5))
        .await;

    cluster.kill(leader).await;
    let (new_leader, new_term) = cluster.wait_for_leader(Duration::from_secs(5)).await;
    assert_ne!(new_leader, leader);
    assert!(new_term > original_term);
    assert!(matches!(
        cluster
            .client_write(arithmetic_command("multiply", 2))
            .await,
        ClientOutcome::Committed { .. }
    ));

    let live_log = cluster
        .wait_for_committed_convergence(Duration::from_secs(5))
        .await;
    assert!(pre_failover.iter().all(|entry| live_log.contains(entry)));

    cluster.restart(leader).await;
    let converged = cluster
        .wait_for_committed_convergence(Duration::from_secs(5))
        .await;
    let restarted = cluster.raft(leader);
    let restarted = restarted.lock().unwrap();
    assert_eq!(restarted.get_server_state(), ServerState::Follower);
    let commit_index = restarted.get_state().commit_index;
    drop(restarted);
    assert_eq!(cluster.committed_log(leader, commit_index), converged);
    cluster
        .wait_for_arithmetic_state(
            ArithmeticState {
                value: 10,
                applied_commands: 2,
            },
            Duration::from_secs(5),
        )
        .await;
    cluster.stop_all().await;
}
