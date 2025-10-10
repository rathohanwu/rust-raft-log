use crate::log::segment::LogSegment;
use crate::log::utils::{read_u64, write_u64, BASE_INDEX_OFFSET, ENTRY_COUNT_OFFSET};

/// Header for a log segment file.
///
/// The header contains metadata about the segment and appears at the
/// beginning of each segment file.
///
/// # Binary Format
///
/// ```text
/// ┌─────────────────┬───────────────────┬─────────────────┬─────────────────┬─────────────────┐
/// │ Magic (4 bytes) │ Version (4 bytes) │ Base Index (8)  │ Entry Count (4) │ Checksum (4)    │
/// └─────────────────┴───────────────────┴─────────────────┴─────────────────┴─────────────────┘
/// ```
/// - **Magic Number**: 0x52414654 ("RAFT" in ASCII)
/// - **Version**: 0x00000001 (current version)
/// - **Base Index**: Index of the first entry in this segment
/// - **Entry Count**: Number of entries in this segment
/// - **Checksum**: CRC32 of the entire file

pub trait LogSegmentHeader {
    fn get_last_index(&self) -> Option<u64>;
    fn get_base_index(&self) -> Option<u64>;
    fn get_entry_count(&self) -> Option<u64>;
    fn set_base_index(&mut self, base_index: u64) -> bool;
    fn set_entry_count(&mut self, entry_count: u64) -> bool;
}

impl LogSegmentHeader for LogSegment {
    fn get_last_index(&self) -> Option<u64> {
        match (self.get_base_index(), self.get_entry_count()) {
            (Some(base_index), Some(entry_count)) => {
                if entry_count == 0 {
                    Some(base_index)
                } else {
                    Some(base_index + entry_count - 1)
                }
            }
            _ => None,
        }
    }
    fn get_base_index(&self) -> Option<u64> {
        read_u64(&self.buffer, BASE_INDEX_OFFSET)
    }

    fn get_entry_count(&self) -> Option<u64> {
        read_u64(&self.buffer, ENTRY_COUNT_OFFSET)
    }

    fn set_base_index(&mut self, base_index: u64) -> bool {
        write_u64(&mut self.buffer, BASE_INDEX_OFFSET, base_index)
    }

    fn set_entry_count(&mut self, entry_count: u64) -> bool {
        write_u64(&mut self.buffer, ENTRY_COUNT_OFFSET, entry_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::utils::create_memory_mapped_file;
    #[test]
    fn should_return_correct_first_index_and_entry_count() {
        let memory_map = create_memory_mapped_file("log-segment-0000001.dat", 100)
            .expect("should be opened the file");
        let mut log_segment = LogSegment::new(memory_map, 20);

        let base_index = log_segment.get_base_index().unwrap();
        assert_eq!(20, base_index);
        log_segment.set_base_index(40);
        let new_base_index = log_segment.get_base_index().unwrap();
        assert_eq!(40, new_base_index);

        let entry_count = log_segment.get_entry_count().unwrap();
        assert_eq!(0, entry_count);
        log_segment.set_entry_count(30);
        let new_entry_count = log_segment.get_entry_count().unwrap();
        assert_eq!(30, new_entry_count);

        let last_index = log_segment.get_last_index().unwrap();
        assert_eq!(69, last_index);
    }
}
