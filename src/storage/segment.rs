use super::mmap_utils::MemoryMapUtil;
use super::utils::{
    BASE_INDEX_OFFSET, ENTRY_COUNT_OFFSET, HEADER_SIZE, MAGIC_OFFSET, START_APPEND_POSITION_OFFSET,
    VERSION_OFFSET,
};
use crate::models::{AppendResult, EntryType, LogEntry};
use byteorder::{LittleEndian, ReadBytesExt};
use log::error;
use memmap2::MmapMut;
use std::io;
use std::io::Cursor;

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
    /// Validates the fixed segment header before recovery accepts this file.
    pub fn validate_header(&self) -> bool {
        if MemoryMapUtil::read_vec_8(&self.buffer, MAGIC_OFFSET, 4) != b"RAFT"
            || MemoryMapUtil::read_u32(&self.buffer, VERSION_OFFSET) != 1
            || self.get_base_index() == 0
        {
            return false;
        }

        let append_position = self.get_start_append_position() as usize;
        if append_position < HEADER_SIZE || append_position > self.buffer.len() {
            return false;
        }

        // The entry count and append position are also header fields. Validate
        // their relationship before recovery trusts either one.
        let mut position = HEADER_SIZE;
        for _ in 0..self.get_entry_count() {
            let Some(size_end) = position.checked_add(8) else {
                return false;
            };
            if size_end > append_position {
                return false;
            }
            let mut size_bytes = [0; 8];
            size_bytes.copy_from_slice(&self.buffer[position..size_end]);
            let entry_size = u64::from_le_bytes(size_bytes) as usize;
            if entry_size < 25 {
                return false;
            }
            let Some(next_position) = position.checked_add(entry_size) else {
                return false;
            };
            if next_position > append_position {
                return false;
            }
            position = next_position;
        }
        position == append_position
    }
    /// Flush this segment's mmap to stable storage.
    pub fn flush(&mut self) -> io::Result<()> {
        MemoryMapUtil::flush(&mut self.buffer)
    }
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
        MemoryMapUtil::write_u8(
            &mut self.buffer,
            start_position + 24,
            log_entry.entry_type.clone().into(),
        );
        MemoryMapUtil::write_vec_8(&mut self.buffer, start_position + 25, &log_entry.payload);
        start_position as u64 + total_payload_size
    }

    pub fn get_entry_at(&self, search_index: u64) -> Option<LogEntry> {
        let entry_count = self.get_entry_count();
        let base_index = self.get_base_index();
        let actual_index = search_index.checked_sub(base_index)? + 1;

        if actual_index == 0 || actual_index > entry_count {
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
mod tests;
