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
}
