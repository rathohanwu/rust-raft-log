mod common;

use common::{arithmetic_command, entry_view, ArithmeticState, TestCluster};
use tokio::time::{sleep, Duration, Instant};

#[tokio::test]
#[ignore = "real gRPC E2E; run explicitly"]
async fn e2e_full_crash_recovery_offline_and_noop_reapply() {
    let mut cluster = TestCluster::start(3).await;
    cluster.wait_for_leader(Duration::from_secs(5)).await;
    assert!(matches!(
        cluster.client_write(arithmetic_command("add", 9)).await,
        common::ClientOutcome::Committed { .. }
    ));
    let committed = cluster
        .wait_for_committed_convergence(Duration::from_secs(5))
        .await;
    let mut before = Vec::new();
    for id in cluster.node_ids() {
        let node = cluster.node_view(id);
        let node = node.lock().unwrap();
        before.push((
            id,
            node.get_current_term(),
            node.get_state().voted_for,
            committed.clone(),
        ));
    }

    for id in cluster.node_ids() {
        cluster.kill(id).await;
    }

    // Offline recovery proves exactly what was persisted, before elections mutate it.
    for (id, term, voted_for, expected_prefix) in &before {
        let inspector = cluster.inspect_offline(*id);
        assert_eq!(inspector.get_current_term(), *term);
        assert_eq!(inspector.get_state().voted_for, *voted_for);
        let recovered: Vec<_> = (1..=expected_prefix.len() as u64)
            .filter_map(|index| inspector.get_entry(index))
            .map(entry_view)
            .collect();
        assert_eq!(&recovered, expected_prefix);
        drop(inspector);
    }

    for id in cluster.node_ids() {
        cluster.restart(id).await;
    }
    cluster.wait_for_leader(Duration::from_secs(5)).await;

    // The new leader's current-term NoOp must commit and cause every node to apply
    // the recovered prefix again from volatile last_applied = 0.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let ready = cluster.active_node_ids().into_iter().all(|id| {
            let node = cluster.node_view(id);
            let node = node.lock().unwrap();
            node.get_state().commit_index > committed.len() as u64
                && node.get_state().last_applied == node.get_state().commit_index
        });
        if ready {
            break;
        }
        assert!(Instant::now() < deadline, "recovered NoOp did not commit");
        sleep(Duration::from_millis(25)).await;
    }

    let converged = cluster
        .wait_for_committed_convergence(Duration::from_secs(5))
        .await;
    assert!(converged.starts_with(&committed));
    assert!(matches!(
        cluster.client_write(arithmetic_command("divide", 3)).await,
        common::ClientOutcome::Committed { .. }
    ));
    cluster
        .wait_for_arithmetic_state(
            ArithmeticState {
                value: 3,
                applied_commands: 2,
            },
            Duration::from_secs(5),
        )
        .await;
    cluster.stop_all().await;
}
