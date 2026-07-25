mod common;

use common::{arithmetic_command, ClientOutcome, TestCluster};
use raft_log::ServerState;
use tokio::time::{sleep, Duration};

#[tokio::test]
#[ignore = "real gRPC E2E; run explicitly"]
async fn e2e_two_node_follower_loss_preserves_leader_but_loses_quorum() {
    let mut cluster = TestCluster::start(2).await;
    let (leader, _) = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let follower = if leader == 1 { 2 } else { 1 };
    let before = cluster
        .node_view(leader)
        .lock()
        .unwrap()
        .get_state()
        .commit_index;
    cluster.kill(follower).await;
    sleep(Duration::from_millis(700)).await;
    assert_eq!(
        cluster.node_view(leader).lock().unwrap().get_server_state(),
        ServerState::Leader
    );
    assert!(matches!(
        cluster
            .client_write_to(leader, arithmetic_command("add", 1))
            .await,
        ClientOutcome::TimedOut
    ));
    assert_eq!(
        cluster
            .node_view(leader)
            .lock()
            .unwrap()
            .get_state()
            .commit_index,
        before
    );
    cluster.stop_all().await;
}

#[tokio::test]
#[ignore = "real gRPC E2E; run explicitly"]
async fn e2e_two_node_leader_loss_prevents_new_election() {
    let mut cluster = TestCluster::start(2).await;
    let (leader, _) = cluster.wait_for_leader(Duration::from_secs(5)).await;
    let follower = if leader == 1 { 2 } else { 1 };
    cluster.kill(leader).await;
    sleep(Duration::from_secs(2)).await;
    assert_ne!(
        cluster
            .node_view(follower)
            .lock()
            .unwrap()
            .get_server_state(),
        ServerState::Leader
    );
    assert!(!matches!(
        cluster
            .client_write_to(follower, arithmetic_command("subtract", 1))
            .await,
        ClientOutcome::Committed { .. }
    ));
    cluster.stop_all().await;
}
