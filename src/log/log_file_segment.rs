use super::models::LogEntry;
use super::utils::HEADER_SIZE;
use crate::log::mmap_utils::MemoryMapUtil;
use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::MmapMut;
use std::io;
use std::io::{Cursor, Read};

pub struct LogFileSegment {
    pub buffer: MmapMut,
}

impl LogFileSegment {
    pub fn new(memory_map: MmapMut, base_index: u64) -> Self {
        let mut log_segment = LogFileSegment { buffer: memory_map };
        log_segment.initialize_header_for_new_log_segment(base_index);
        log_segment
    }

    fn initialize_header_for_new_log_segment(&mut self, base_index: u64) {
        let version: u32 = 1;
        self.set_magic();
        self.set_version(version);
        self.set_base_index(base_index);
        self.set_entry_count(0);
        self.set_start_append_position(HEADER_SIZE as u64);
    }
}

impl LogFileSegment {
    fn append_try(&mut self, log_entry: LogEntry) {
        let start_append_position = self.get_start_append_position();
        let next_start_append_position = self.write_payload(start_append_position, &log_entry);
        self.set_start_append_position(next_start_append_position);
        let entry_count = self.get_entry_count();
        self.set_entry_count(entry_count + 1);
    }

    fn write_payload(self: &mut Self, start_position: u64, log_entry: &LogEntry) -> u64 {
        let start_position = start_position as usize;
        let buffer = &mut self.buffer;
        let total_payload_size = log_entry.payload.len() as u64 + 8 + 8 + 8;
        MemoryMapUtil::write_u64(buffer, start_position, total_payload_size);
        MemoryMapUtil::write_u64(buffer, start_position + 8, log_entry.term);
        MemoryMapUtil::write_u64(buffer, start_position + 16, log_entry.index);
        MemoryMapUtil::write_vec_8(buffer, start_position + 24, &log_entry.payload);
        start_position as u64 + total_payload_size
    }

    fn get_entry_at(&mut self, index: u64) -> Option<LogEntry> {
        let entry_count = self.get_entry_count();
        let base_index = self.get_base_index();
        let actual_index = base_index + index - 1;

        if index == 0 || index > entry_count {
            return None;
        }
        let mut cursor = io::Cursor::new(&self.buffer[HEADER_SIZE..]);
        let start_position = find_start_append_position(&mut cursor, actual_index);
        cursor.set_position(start_position);

        let payload_size = cursor.read_u64::<LittleEndian>().ok()?;
        let term = cursor.read_u64::<LittleEndian>().ok()?;
        let idx = cursor.read_u64::<LittleEndian>().ok()?;
        let mut payload = vec![0u8; (payload_size - 24) as usize]; // subtracting size of term and index
        cursor.read_exact(&mut payload).ok()?;

        Some(LogEntry::new(term, idx, payload))
    }
}

fn find_start_append_position(cursor: &mut Cursor<&[u8]>, index: u64) -> u64 {
    let mut total_pay_load: u64 = 0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::utils::create_memory_mapped_file;
    #[test]
    fn should_return_correct_first_index_and_entry_count() {
        let memory_map = create_memory_mapped_file("log-segment-0000001.dat", 10_000)
            .expect("should be opened the file");

        let mut log_segment = LogFileSegment::new(memory_map, 1);
        assert_eq!(0, log_segment.get_entry_count());

        log_segment.append_try(LogEntry::new(1, 1, "this is han1".as_bytes().to_vec()));
        assert_eq!(1, log_segment.get_entry_count());

        log_segment.append_try(LogEntry::new(1, 2, "this is han2".as_bytes().to_vec()));
        assert_eq!(2, log_segment.get_entry_count());

        let log_entry_1 = log_segment.get_entry_at(1);

        match log_entry_1 {
            None => panic!("should be some log entry"),
            Some(entry) => {
                assert_eq!(1, entry.term);
                assert_eq!(1, entry.index);
                assert_eq!("this is han1".as_bytes().to_vec(), entry.payload);
            }
        }

        let log_entry_2 = log_segment.get_entry_at(2);

        match log_entry_2 {
            None => panic!("should be some log entry"),
            Some(entry) => {
                assert_eq!(1, entry.term);
                assert_eq!(2, entry.index);
                assert_eq!("this is han2".as_bytes().to_vec(), entry.payload);
            }
        }
    }
}
