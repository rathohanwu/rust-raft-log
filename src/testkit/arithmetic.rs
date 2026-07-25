use crate::{EntryType, LogEntry, StateMachine};
use log::{info, warn};
use serde::{Deserialize, Serialize};

/// Deterministic arithmetic state used exclusively by the Docker E2E node.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ArithmeticState {
    pub value: i64,
    pub applied_commands: u64,
}

#[derive(Debug, Deserialize)]
struct ArithmeticCommand {
    action: String,
    value: i64,
}

/// Applies JSON commands such as `{\"action\":\"add\",\"value\":1}`.
#[derive(Debug, Default)]
pub struct ArithmeticStateMachine {
    state: ArithmeticState,
}

impl ArithmeticStateMachine {
    pub fn state(&self) -> &ArithmeticState {
        &self.state
    }
}

impl StateMachine for ArithmeticStateMachine {
    fn apply(&mut self, entry: &LogEntry) {
        if entry.entry_type() != &EntryType::Normal {
            return;
        }

        let command: ArithmeticCommand = match serde_json::from_slice(entry.payload()) {
            Ok(command) => command,
            Err(error) => {
                warn!(
                    "Ignoring invalid arithmetic command at index {}: {}",
                    entry.index(),
                    error
                );
                return;
            }
        };

        let next_value = match command.action.as_str() {
            "add" => self.state.value.checked_add(command.value),
            "subtract" => self.state.value.checked_sub(command.value),
            "multiply" => self.state.value.checked_mul(command.value),
            "divide" if command.value != 0 => self.state.value.checked_div(command.value),
            "divide" => None,
            _ => None,
        };

        let Some(next_value) = next_value else {
            warn!(
                "Ignoring invalid arithmetic command at index {}: action={} value={}",
                entry.index(),
                command.action,
                command.value
            );
            return;
        };

        self.state.value = next_value;
        self.state.applied_commands += 1;
        info!(
            "Applied arithmetic command at index {}: action={} value={} state_value={}",
            entry.index(),
            command.action,
            command.value,
            self.state.value
        );
    }
}

#[cfg(test)]
mod tests;
