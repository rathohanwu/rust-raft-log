pub struct LogEntry {
    pub(crate) term: u64,
    pub(crate) index: u64,
    pub(crate) payload: Vec<u8>,
}

impl LogEntry {
    pub(crate) fn new(term: u64, index: u64, payload: Vec<u8>) -> Self {
        LogEntry {
            term,
            index,
            payload,
        }
    }

    pub fn calculate_total_size(&self) -> u64 {
        self.payload.len() as u64 + 8 + 8 + 8
    }
}

pub enum AppendResult {
    Success,
    RotationNeeded,
}
