use crate::log::header::LogSegmentHeader;
use crate::log::utils::HEADER_SIZE;
use memmap2::MmapMut;
use std::io;
use std::io::Write;

pub struct LogSegment {
    pub buffer: MmapMut,
}

impl LogSegment {
    pub fn new(memory_map: MmapMut, base_index: u64) -> Self {
        let mut log_segment = LogSegment { buffer: memory_map };
        let _ = log_segment.initialize_header_for_new_log_segment(base_index);
        log_segment
    }

    fn initialize_header_for_new_log_segment(&mut self, base_index: u64) -> io::Result<()> {
        let version: i32 = 1;
        let mut cursor = io::Cursor::new(&mut self.buffer[..HEADER_SIZE]);
        cursor.write_all("RAFT".as_bytes())?;
        cursor.write_all(&version.to_le_bytes())?;
        self.set_base_index(base_index);
        self.set_entry_count(0);

        Ok(())
    }
}
