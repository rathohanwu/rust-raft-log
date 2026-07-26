use raft_log::{
    ClusterConfig, EntryType, LogEntry, NodeInfo, RaftLog, RaftLogConfig, RaftState, ServerState,
};
use tempfile::TempDir;

#[test]
fn raft_log_and_state_persist_together() {
    let temp_dir = TempDir::new().unwrap();
    let mut log = RaftLog::new(RaftLogConfig {
        log_directory: temp_dir.path().join("logs"),
        segment_size: 1024,
        max_entries_per_query: 1000,
    })
    .unwrap();
    let state_path = temp_dir.path().join("raft_state.meta");
    let mut state = RaftState::new(&state_path).unwrap();

    state.start_new_term_as_candidate(2);
    assert!(state.vote_for_candidate(999));
    state.set_server_state(ServerState::Leader);
    for (entry_type, payload) in [
        (EntryType::NoOp, Vec::new()),
        (EntryType::Normal, b"command1".to_vec()),
        (EntryType::Normal, b"command2".to_vec()),
    ] {
        log.append_entry(LogEntry::new_with_type(2, 0, entry_type, payload))
            .unwrap();
    }
    state.set_commit_index(3);
    state.set_last_applied(2);

    assert_eq!(log.len(), 3);
    assert_eq!(log.last_index(), Some(3));
    assert_eq!(log.get_entry(1).unwrap().entry_type(), &EntryType::NoOp);
    assert_eq!(log.get_entry(2).unwrap().payload(), b"command1");
    assert_eq!(log.get_entry(3).unwrap().payload(), b"command2");
    assert_eq!(state.get_state_snapshot().server_state, ServerState::Leader);

    drop(state);
    let reloaded = RaftState::from_existing(state_path).unwrap();
    assert_eq!(reloaded.get_current_term(), 2);
    assert_eq!(reloaded.get_voted_for(), Some(999));
    assert_eq!(reloaded.get_server_state(), ServerState::Follower);
    assert_eq!(reloaded.get_commit_index(), 0);
    assert_eq!(reloaded.get_last_applied(), 0);
}

#[test]
fn cluster_config_constructs_compatible_log_and_state() {
    let temp_dir = TempDir::new().unwrap();
    let config = ClusterConfig::new(
        1,
        vec![
            NodeInfo::new(1, "127.0.0.1".into(), 8001),
            NodeInfo::new(2, "127.0.0.1".into(), 8002),
            NodeInfo::new(3, "127.0.0.1".into(), 8003),
        ],
        temp_dir.path().join("logs").to_string_lossy().into_owned(),
        temp_dir
            .path()
            .join("raft_state.meta")
            .to_string_lossy()
            .into_owned(),
        1024,
        1000,
        (150, 300),
        50,
    );

    assert_eq!(config.cluster_size(), 3);
    assert_eq!(config.majority_size(), 2);
    assert_eq!(config.get_node(2).unwrap().get_address(), "127.0.0.1:8002");

    let mut log = RaftLog::new(config.to_raft_log_config()).unwrap();
    let mut state = RaftState::new(&config.meta_file_path).unwrap();
    log.append_entry(LogEntry::new_with_type(
        1,
        0,
        EntryType::Normal,
        b"test command".to_vec(),
    ))
    .unwrap();
    state.set_current_term(1);
    state.set_server_state(ServerState::Leader);

    assert_eq!(log.len(), 1);
    assert_eq!(state.get_current_term(), 1);
    assert_eq!(state.get_server_state(), ServerState::Leader);
}
