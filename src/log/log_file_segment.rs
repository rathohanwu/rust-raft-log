use super::models::LogEntry;
use super::utils::HEADER_SIZE;
use crate::log::log_file_segment_header::{LogFileSegmentHeader, LogFileSegmentHeaderImpl};
use crate::log::mmap_utils::MemoryMapUtil;
use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::MmapMut;
use std::cell::RefCell;
use std::io::Cursor;
use std::rc::Rc;

pub struct LogFileSegment {
    pub header: Box<dyn LogFileSegmentHeader>,
    pub buffer: Rc<RefCell<MmapMut>>,
}

impl LogFileSegment {
    pub fn new(buffer: MmapMut, base_index: u64) -> Self {
        let buffer = Rc::new(RefCell::new(buffer));
        let log_segment_header = LogFileSegmentHeaderImpl::new(Rc::clone(&buffer), base_index);
        let mut log_segment = LogFileSegment {
            header: Box::new(log_segment_header),
            buffer: Rc::clone(&buffer),
        };
        log_segment.initialize_header_for_new_log_segment(base_index);
        log_segment
    }

    fn initialize_header_for_new_log_segment(&mut self, base_index: u64) {
        let version: u32 = 1;

        self.header.set_magic();
        self.header.set_version(version);
        self.header.set_base_index(base_index);
        self.header.set_entry_count(0);
        self.header.set_start_append_position(HEADER_SIZE as u64);
    }

    pub fn append_try(&mut self, log_entry: LogEntry) {
        let start_append_position = self.header.get_start_append_position();
        let next_start_append_position = self.write_payload(start_append_position, &log_entry);
        self.header
            .set_start_append_position(next_start_append_position);
        let entry_count = self.header.get_entry_count();
        self.header.set_entry_count(entry_count + 1);
    }

    pub fn truncate_from(self: &mut Self, search_index: u64) -> bool {
        let base_index = self.header.get_base_index();
        let last_index = self.header.get_last_index().unwrap_or(0);
        let actual_index = search_index.checked_sub(base_index).map_or(0, |x| x + 1);
        if search_index > last_index {
            return false;
        }
        let truncate_from_position = self.find_start_append_position(actual_index);
        self.header
            .set_start_append_position(truncate_from_position);
        self.header.set_entry_count(search_index - base_index);
        true
    }
}

impl LogFileSegment {
    fn write_payload(self: &mut Self, start_position: u64, log_entry: &LogEntry) -> u64 {
        let start_position = start_position as usize;
        let total_payload_size = log_entry.payload.len() as u64 + 8 + 8 + 8;
        let mut buffer = self.buffer.borrow_mut();
        MemoryMapUtil::write_u64(&mut *buffer, start_position, total_payload_size);
        MemoryMapUtil::write_u64(&mut *buffer, start_position + 8, log_entry.term);
        MemoryMapUtil::write_u64(&mut *buffer, start_position + 16, log_entry.index);
        MemoryMapUtil::write_vec_8(&mut *buffer, start_position + 24, &log_entry.payload);
        start_position as u64 + total_payload_size
    }

    fn get_entry_at(&mut self, search_index: u64) -> Option<LogEntry> {
        let entry_count = self.header.get_entry_count();
        let base_index = self.header.get_base_index();
        let actual_index = search_index.checked_sub(base_index)? + 1;

        if actual_index <= 0 || actual_index > entry_count {
            return None;
        }

        let start_position = self.find_start_append_position(actual_index) as usize;
        let buffer = self.buffer.borrow();
        let payload_size = MemoryMapUtil::read_u64(&*buffer, start_position);
        let term = MemoryMapUtil::read_u64(&*buffer, start_position + 8);
        let index = MemoryMapUtil::read_u64(&*buffer, start_position + 16);

        let payload =
            MemoryMapUtil::read_vec_8(&*buffer, start_position + 24, (payload_size - 24) as usize);

        Some(LogEntry::new(term, index, payload))
    }

    fn find_start_append_position(&self, index: u64) -> u64 {
        let buffer = self.buffer.borrow();
        let mut cursor = Cursor::new(&buffer[HEADER_SIZE..]);
        let mut total_pay_load: u64 = HEADER_SIZE as u64;
        for index in 0..index - 1 {
            let pay_load = cursor.read_u32::<LittleEndian>();
            match pay_load {
                Err(e) => {
                    eprintln!("Error reading payload size at index {}: {}", index, e);
                    panic!("Error reading payload size at index {}: {}", index, e);
                }
                Ok(entry_size) => {
                    total_pay_load += entry_size as u64;
                    cursor.set_position(total_pay_load);
                }
            }
        }
        total_pay_load
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::utils::create_memory_mapped_file;
    #[test]
    fn should_return_correct_first_index_and_entry_count() {
        let memory_map = create_memory_mapped_file("log-segment-0000001.dat", 10_000)
            .expect("should be opened the file");

        let mut log_segment = LogFileSegment::new(memory_map, 1);
        assert_eq!(0, log_segment.header.get_entry_count());

        log_segment.append_try(LogEntry::new(1, 1, "this is han1".as_bytes().to_vec()));
        assert_eq!(1, log_segment.header.get_entry_count());

        log_segment.append_try(LogEntry::new(1, 2, "this is han2".as_bytes().to_vec()));
        assert_eq!(2, log_segment.header.get_entry_count());

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
        assert_eq!(0, log_segment.header.get_entry_count());
        verify_empty_log_entry(log_segment.get_entry_at(7));
        verify_empty_log_entry(log_segment.get_entry_at(8));

        // Given
        log_segment.append_try(LogEntry::new(1, 8, "this is han8".as_bytes().to_vec()));
        log_segment.append_try(LogEntry::new(1, 9, "this is han9".as_bytes().to_vec()));

        // When & Then
        assert_eq!(2, log_segment.header.get_entry_count());
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
        log_segment.append_try(LogEntry::new(
            1,
            11,
            "this is 11th data".as_bytes().to_vec(),
        ));
        log_segment.append_try(LogEntry::new(
            1,
            12,
            "this is 12th data".as_bytes().to_vec(),
        ));

        assert_eq!(2, log_segment.header.get_entry_count());
        verify_log_entry(log_segment.get_entry_at(11), 1, 11, "this is 11th data");
        verify_log_entry(log_segment.get_entry_at(12), 1, 12, "this is 12th data");

        assert_eq!(false, log_segment.truncate_from(13));
        assert_eq!(true, log_segment.truncate_from(12));
        assert_eq!(1, log_segment.header.get_entry_count());
        verify_log_entry(log_segment.get_entry_at(11), 1, 11, "this is 11th data");
        verify_empty_log_entry(log_segment.get_entry_at(12));

        log_segment.append_try(LogEntry::new(
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
}
