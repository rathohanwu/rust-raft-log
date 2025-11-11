use crate::models::{AppendResult, LogEntry, EntryType};
use super::utils::{
    BASE_INDEX_OFFSET, ENTRY_COUNT_OFFSET, HEADER_SIZE, MAGIC_OFFSET, START_APPEND_POSITION_OFFSET,
    VERSION_OFFSET,
};
use super::mmap_utils::MemoryMapUtil;
use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::MmapMut;
use std::io::Cursor;
use log::error;

/// Header for a log segment file.
///
/// The header contains metadata about the segment and appears at the
/// beginning of each segment file.
///
/// # Binary Format
///
/// ```text
/// ┌─────────────────┬───────────────────┬─────────────────┬─────────────────┬─────────────────┐
/// │ Magic (4 bytes) │ Version (4 bytes) │ Base Index (8)  │ Entry Count (8) │ Start Pos (8)   │
/// └─────────────────┴───────────────────┴─────────────────┴─────────────────┴─────────────────┘
/// ```
/// - **Magic Number**: 0x52414654 ("RAFT" in ASCII)
/// - **Version**: 0x00000001 (current version)
/// - **Base Index**: Index of the first entry in this segment
/// - **Entry Count**: Number of entries in this segment
/// - **Start Append Position**: Position where next entry will be written
pub struct LogFileSegment {
    buffer: MmapMut,
}

impl LogFileSegment {
    pub fn new(buffer: MmapMut, base_index: u64) -> Self {
        let mut log_segment = LogFileSegment { buffer };
        log_segment.initialize_header_for_new_log_segment(base_index);
        log_segment
    }

    /// Creates a LogFileSegment from an existing file buffer (doesn't initialize header)
    pub fn from_existing(buffer: MmapMut) -> Self {
        LogFileSegment { buffer }
    }

    fn initialize_header_for_new_log_segment(&mut self, base_index: u64) {
        let version: u32 = 1;

        self.set_magic();
        self.set_version(version);
        self.set_base_index(base_index);
        self.set_entry_count(0);
        self.set_start_append_position(HEADER_SIZE as u64);
    }

    // Header getter methods
    pub fn get_last_index(&self) -> Option<u64> {
        let base_index = self.get_base_index();
        let entry_count = self.get_entry_count();
        if entry_count == 0 {
            None
        } else {
            Some(base_index + entry_count - 1)
        }
    }

    pub fn get_base_index(&self) -> u64 {
        MemoryMapUtil::read_u64(&self.buffer, BASE_INDEX_OFFSET)
    }

    pub fn get_entry_count(&self) -> u64 {
        MemoryMapUtil::read_u64(&self.buffer, ENTRY_COUNT_OFFSET)
    }

    pub fn get_start_append_position(&self) -> u64 {
        MemoryMapUtil::read_u64(&self.buffer, START_APPEND_POSITION_OFFSET)
    }

    // Header setter methods
    fn set_magic(&mut self) {
        MemoryMapUtil::write_vec_8(&mut self.buffer, MAGIC_OFFSET, &b"RAFT".to_vec());
    }

    fn set_version(&mut self, version: u32) {
        MemoryMapUtil::write_u32(&mut self.buffer, VERSION_OFFSET, version);
    }

    fn set_base_index(&mut self, base_index: u64) {
        MemoryMapUtil::write_u64(&mut self.buffer, BASE_INDEX_OFFSET, base_index);
    }

    fn set_entry_count(&mut self, entry_count: u64) {
        MemoryMapUtil::write_u64(&mut self.buffer, ENTRY_COUNT_OFFSET, entry_count);
    }

    fn set_start_append_position(&mut self, start_append_position: u64) {
        MemoryMapUtil::write_u64(
            &mut self.buffer,
            START_APPEND_POSITION_OFFSET,
            start_append_position,
        );
    }

    // Log entry operations
    pub fn append_entry(&mut self, log_entry: LogEntry) -> AppendResult {
        let start_append_position = self.get_start_append_position();
        let total_log_entry_size = log_entry.calculate_total_size();

        if start_append_position + total_log_entry_size > self.buffer.len() as u64 {
            return AppendResult::RotationNeeded;
        }

        let next_start_append_position = self.write_payload(start_append_position, &log_entry);
        self.set_start_append_position(next_start_append_position);
        let entry_count = self.get_entry_count();
        self.set_entry_count(entry_count + 1);
        AppendResult::Success
    }

    pub fn truncate_from(&mut self, search_index: u64) -> bool {
        let base_index = self.get_base_index();
        let last_index = self.get_last_index().unwrap_or(0);
        let actual_index = search_index.checked_sub(base_index).map_or(0, |x| x + 1);
        if search_index > last_index {
            return false;
        }
        let truncate_from_position = self.find_start_append_position(actual_index);
        self.set_start_append_position(truncate_from_position);
        self.set_entry_count(search_index - base_index);
        true
    }

    // Private helper methods
    fn write_payload(&mut self, start_position: u64, log_entry: &LogEntry) -> u64 {
        let start_position = start_position as usize;
        let total_payload_size = log_entry.calculate_total_size();
        MemoryMapUtil::write_u64(&mut self.buffer, start_position, total_payload_size);
        MemoryMapUtil::write_u64(&mut self.buffer, start_position + 8, log_entry.term);
        MemoryMapUtil::write_u64(&mut self.buffer, start_position + 16, log_entry.index);
        MemoryMapUtil::write_u8(&mut self.buffer, start_position + 24, log_entry.entry_type.clone().into());
        MemoryMapUtil::write_vec_8(&mut self.buffer, start_position + 25, &log_entry.payload);
        start_position as u64 + total_payload_size
    }

    pub fn get_entry_at(&self, search_index: u64) -> Option<LogEntry> {
        let entry_count = self.get_entry_count();
        let base_index = self.get_base_index();
        let actual_index = search_index.checked_sub(base_index)? + 1;

        if actual_index <= 0 || actual_index > entry_count {
            return None;
        }

        let start_position = self.find_start_append_position(actual_index) as usize;
        let payload_size = MemoryMapUtil::read_u64(&self.buffer, start_position);
        let term = MemoryMapUtil::read_u64(&self.buffer, start_position + 8);
        let index = MemoryMapUtil::read_u64(&self.buffer, start_position + 16);
        let entry_type_byte = MemoryMapUtil::read_u8(&self.buffer, start_position + 24);
        let entry_type = EntryType::from(entry_type_byte);

        let payload = MemoryMapUtil::read_vec_8(
            &self.buffer,
            start_position + 25,
            (payload_size - 25) as usize,
        );

        Some(LogEntry::new_with_type(term, index, entry_type, payload))
    }

    fn find_start_append_position(&self, index: u64) -> u64 {
        let mut cursor = Cursor::new(&self.buffer[HEADER_SIZE..]);
        let mut total_pay_load: u64 = HEADER_SIZE as u64;
        for _i in 0..index - 1 {
            let pay_load = cursor.read_u64::<LittleEndian>();
            match pay_load {
                Err(e) => {
                    error!("Error reading payload size at index {}: {}", _i, e);
                    panic!("Error reading payload size at index {}: {}", _i, e);
                }
                Ok(entry_size) => {
                    total_pay_load += entry_size;
                    cursor.set_position(total_pay_load - HEADER_SIZE as u64);
                }
            }
        }
        total_pay_load
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::utils::create_memory_mapped_file;

    #[test]
    fn should_return_rotated_needed_result() {
        let memory_map = create_memory_mapped_file("log-segment-0000010.dat", 67)
            .expect("should be opened the file");

        let mut log_segment = LogFileSegment::new(memory_map, 1);
        let result =
            log_segment.append_entry(LogEntry::new(1, 1, "this is han1".as_bytes().to_vec()));

        match result {
            AppendResult::Success => panic!("should be rotation needed"),
            AppendResult::RotationNeeded => {}
        }
    }

    #[test]
    fn should_return_success_result() {
        let memory_map = create_memory_mapped_file("log-segment-0000011.dat", 100)
            .expect("should be opened the file");

        let mut log_segment = LogFileSegment::new(memory_map, 1);
        let result =
            log_segment.append_entry(LogEntry::new(1, 1, "this is han1".as_bytes().to_vec()));

        match result {
            AppendResult::Success => {}
            AppendResult::RotationNeeded => panic!("should be successful needed"),
        }
    }

    #[test]
    fn should_return_correct_first_index_and_entry_count() {
        let memory_map = create_memory_mapped_file("log-segment-0000001.dat", 10_000)
            .expect("should be opened the file");

        let mut log_segment = LogFileSegment::new(memory_map, 1);
        assert_eq!(0, log_segment.get_entry_count());

        log_segment.append_entry(LogEntry::new(1, 1, "this is han1".as_bytes().to_vec()));
        assert_eq!(1, log_segment.get_entry_count());

        log_segment.append_entry(LogEntry::new(1, 2, "this is han2".as_bytes().to_vec()));
        assert_eq!(2, log_segment.get_entry_count());

        let log_entry_1 = log_segment.get_entry_at(1);
        verify_log_entry(log_entry_1, 1, 1, "this is han1");

        let log_entry_2 = log_segment.get_entry_at(2);
        verify_log_entry(log_entry_2, 1, 2, "this is han2");
    }

    #[test]
    fn should_return_empty_entry_result() {
        // Given
        let memory_map = create_memory_mapped_file("log-segment-0000002.dat", 10_000)
            .expect("should be opened the file");
        let mut log_segment = LogFileSegment::new(memory_map, 8);

        // When & Then
        assert_eq!(0, log_segment.get_entry_count());
        verify_empty_log_entry(log_segment.get_entry_at(7));
        verify_empty_log_entry(log_segment.get_entry_at(8));

        // Given
        log_segment.append_entry(LogEntry::new(1, 8, "this is han8".as_bytes().to_vec()));
        log_segment.append_entry(LogEntry::new(1, 9, "this is han9".as_bytes().to_vec()));

        // When & Then
        assert_eq!(2, log_segment.get_entry_count());
        verify_log_entry(log_segment.get_entry_at(8), 1, 8, "this is han8");
        verify_log_entry(log_segment.get_entry_at(9), 1, 9, "this is han9");

        verify_empty_log_entry(log_segment.get_entry_at(10));
    }

    #[test]
    fn should_truncate_log_correctly() {
        // Given
        let memory_map = create_memory_mapped_file("log-segment-0000003.dat", 10_000)
            .expect("should be opened the file");
        let mut log_segment = LogFileSegment::new(memory_map, 11);
        log_segment.append_entry(LogEntry::new(
            1,
            11,
            "this is 11th data".as_bytes().to_vec(),
        ));
        log_segment.append_entry(LogEntry::new(
            1,
            12,
            "this is 12th data".as_bytes().to_vec(),
        ));

        assert_eq!(2, log_segment.get_entry_count());
        verify_log_entry(log_segment.get_entry_at(11), 1, 11, "this is 11th data");
        verify_log_entry(log_segment.get_entry_at(12), 1, 12, "this is 12th data");

        assert_eq!(false, log_segment.truncate_from(13));
        assert_eq!(true, log_segment.truncate_from(12));
        assert_eq!(1, log_segment.get_entry_count());
        verify_log_entry(log_segment.get_entry_at(11), 1, 11, "this is 11th data");
        verify_empty_log_entry(log_segment.get_entry_at(12));

        log_segment.append_entry(LogEntry::new(
            1,
            12,
            "this is new 12th data".as_bytes().to_vec(),
        ));

        verify_log_entry(log_segment.get_entry_at(12), 1, 12, "this is new 12th data");
    }

    fn verify_log_entry(entry: Option<LogEntry>, term: u64, index: u64, payload: &str) {
        match entry {
            None => panic!("should be some log entry"),
            Some(entry) => {
                assert_eq!(term, entry.term);
                assert_eq!(index, entry.index);
                assert_eq!(payload.as_bytes().to_vec(), entry.payload);
            }
        }
    }

    fn verify_empty_log_entry(entry: Option<LogEntry>) {
        match entry {
            None => {}
            Some(_) => panic!("should be none log entry"),
        }
    }

    #[test]
    fn test_entry_type_encoding_decoding() {
        let memory_map = create_memory_mapped_file("log-segment-entry-types.dat", 1000)
            .expect("should be opened the file");

        let mut log_segment = LogFileSegment::new(memory_map, 1);

        // Test Normal entry type
        let normal_entry = LogEntry::new_with_type(1, 1, EntryType::Normal, "normal command".as_bytes().to_vec());
        let result = log_segment.append_entry(normal_entry.clone());
        assert!(matches!(result, AppendResult::Success));

        // Test NoOp entry type
        let noop_entry = LogEntry::new_with_type(1, 2, EntryType::NoOp, vec![]);
        let result = log_segment.append_entry(noop_entry.clone());
        assert!(matches!(result, AppendResult::Success));

        // Verify entries can be retrieved with correct types
        let retrieved_normal = log_segment.get_entry_at(1).expect("Should retrieve normal entry");
        assert_eq!(retrieved_normal.entry_type, EntryType::Normal);
        assert_eq!(retrieved_normal.term, 1);
        assert_eq!(retrieved_normal.index, 1);
        assert_eq!(retrieved_normal.payload, "normal command".as_bytes());

        let retrieved_noop = log_segment.get_entry_at(2).expect("Should retrieve noop entry");
        assert_eq!(retrieved_noop.entry_type, EntryType::NoOp);
        assert_eq!(retrieved_noop.term, 1);
        assert_eq!(retrieved_noop.index, 2);
        assert_eq!(retrieved_noop.payload, vec![]);

        assert_eq!(log_segment.get_entry_count(), 2);
    }

    #[test]
    fn test_entry_type_conversion() {
        // Test EntryType to u8 conversion
        assert_eq!(u8::from(EntryType::Normal), 0);
        assert_eq!(u8::from(EntryType::NoOp), 1);

        // Test u8 to EntryType conversion
        assert_eq!(EntryType::from(0), EntryType::Normal);
        assert_eq!(EntryType::from(1), EntryType::NoOp);
        assert_eq!(EntryType::from(255), EntryType::Normal); // Unknown values default to Normal
    }

    #[test]
    fn test_calculate_total_size_with_entry_type() {
        let normal_entry = LogEntry::new_with_type(1, 1, EntryType::Normal, "test".as_bytes().to_vec());
        // term (8) + index (8) + entry_type (1) + payload_size (8) + payload (4) = 29
        assert_eq!(normal_entry.calculate_total_size(), 29);

        let noop_entry = LogEntry::new_with_type(1, 1, EntryType::NoOp, vec![]);
        // term (8) + index (8) + entry_type (1) + payload_size (8) + payload (0) = 25
        assert_eq!(noop_entry.calculate_total_size(), 25);
    }
}
