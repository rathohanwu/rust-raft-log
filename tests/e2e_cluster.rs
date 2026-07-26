mod common;

use common::{arithmetic_command, ArithmeticState, ClientOutcome, TestCluster};
use tokio::time::Duration;

/// Real loopback-gRPC test. It is ignored by default because it uses wall-clock
/// election timing; run with `cargo test --test e2e_cluster -- --ignored`.
#[tokio::test]
#[ignore = "real gRPC E2E; run explicitly"]
async fn three_node_replication_converges() {
    let mut cluster = TestCluster::start(3).await;
    let (_leader, _term) = cluster.wait_for_leader(Duration::from_secs(5)).await;

    let mut acknowledged = Vec::new();
    for (action, value) in [("add", 8), ("multiply", 3), ("subtract", 4), ("divide", 2)] {
        let payload = arithmetic_command(action, value);
        match cluster.client_write(payload.clone()).await {
            ClientOutcome::Committed { index } => acknowledged.push((index, payload)),
            outcome => panic!("write was not committed: {outcome:?}"),
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let commit_indexes: Vec<_> = cluster
            .node_ids()
            .into_iter()
            .map(|id| cluster.node_view(id).get_state().commit_index)
            .collect();
        let committed_prefix = *commit_indexes.iter().min().unwrap();
        let logs: Vec<_> = cluster
            .node_ids()
            .into_iter()
            .map(|id| cluster.committed_log(id, committed_prefix))
            .collect();
        let applied: Vec<_> = cluster
            .node_ids()
            .into_iter()
            .map(|id| cluster.arithmetic_state(id))
            .collect();
        if logs.windows(2).all(|pair| pair[0] == pair[1])
            && applied.windows(2).all(|pair| pair[0] == pair[1])
            && applied.iter().all(|state| {
                state
                    == &ArithmeticState {
                        value: 10,
                        applied_commands: 4,
                    }
            })
            && acknowledged.iter().all(|(index, payload)| {
                logs[0]
                    .iter()
                    .filter(|(entry_index, _, _, entry_payload)| {
                        entry_index == index && entry_payload == payload
                    })
                    .count()
                    == 1
            })
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "logs did not converge: {logs:?}; applied entries: {applied:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    cluster.stop_all().await;
}
