use super::*;

fn entry(index: u64, payload: &str) -> LogEntry {
    LogEntry::new_with_type(1, index, EntryType::Normal, payload.as_bytes().to_vec())
}

#[test]
fn applies_arithmetic_commands_and_publishes_state() {
    let mut machine = ArithmeticStateMachine::default();

    machine.apply(&entry(1, r#"{"action":"add","value":4}"#));
    machine.apply(&entry(2, r#"{"action":"multiply","value":3}"#));

    assert_eq!(
        machine.state(),
        &ArithmeticState {
            value: 12,
            applied_commands: 2,
        }
    );
    assert_eq!(
        machine.state_snapshot().unwrap(),
        br#"{"value":12,"applied_commands":2}"#
    );
}

#[test]
fn ignores_invalid_commands_without_changing_state() {
    let mut machine = ArithmeticStateMachine::default();

    machine.apply(&entry(1, r#"{"action":"divide","value":0}"#));
    machine.apply(&entry(2, "not json"));

    assert_eq!(machine.state(), &ArithmeticState::default());
}
