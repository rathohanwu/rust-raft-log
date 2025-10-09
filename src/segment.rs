//! Log segment implementation.
//!
//! A log segment represents a single file containing a contiguous range of log entries.
//! Each segment has a header and a series of entries stored in binary format.

use crate::{
    io::{read_entry, read_header, write_entry, write_header},
    utils::{calculate_file_checksum, segment_filename},
    Error, LogEntry, Result, SegmentHeader,
};
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// A single log segment file.
///
/// Each segment contains:
/// - A 24-byte header with metadata
/// - Zero or more variable-size log entries
/// - CRC32 checksum validation
#[derive(Debug)]
pub struct LogSegment {
    /// Path to the segment file.
    file_path: PathBuf,
    /// Segment header with metadata.
    header: SegmentHeader,
    /// Current append offset in the file.
    append_offset: u64,
    /// File handle for writing (opened lazily).
    writer: Option<BufWriter<File>>,
}

impl LogSegment {
    /// Creates a new empty log segment.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Directory where the segment file will be created
    /// * `base_index` - Index of the first entry that will be stored in this segment
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use raft_log::LogSegment;
    ///
    /// let segment = LogSegment::create("./data", 1).unwrap();
    /// ```
    pub fn create<P: AsRef<Path>>(data_dir: P, base_index: u64) -> Result<Self> {
        let data_dir = data_dir.as_ref();

        // Create data directory if it doesn't exist
        std::fs::create_dir_all(data_dir)?;

        let filename = segment_filename(base_index);
        let file_path = data_dir.join(filename);

        // Create the file and write initial header
        let mut file = File::create(&file_path)?;
        let header = SegmentHeader::new(base_index, 0, 0);

        write_header(&mut file, &header)?;
        file.flush()?;

        Ok(Self {
            file_path,
            header,
            append_offset: 24, // Header size
            writer: None,
        })
    }

    /// Opens an existing log segment.
    ///
    /// # Arguments
    ///
    /// * `file_path` - Path to the existing segment file
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use raft_log::LogSegment;
    ///
    /// let segment = LogSegment::open("./data/log-0000000001.segment").unwrap();
    /// ```
    pub fn open<P: AsRef<Path>>(file_path: P) -> Result<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        let mut file = File::open(&file_path)?;
        let header = read_header(&mut file)?;

        // Calculate append offset by reading through all entries
        let append_offset = Self::calculate_append_offset(&mut file, &header)?;

        Ok(Self {
            file_path,
            header,
            append_offset,
            writer: None,
        })
    }

    /// Validates the segment file integrity.
    pub fn validate(&self) -> Result<()> {
        let mut file = File::open(&self.file_path)?;

        // Read and validate header
        let header = read_header(&mut file)?;
        if header != self.header {
            return Err(Error::InvalidFormat("Header mismatch".to_string()));
        }

        // Calculate and verify checksum
        let calculated_checksum = calculate_file_checksum(&mut file)
            .map_err(|e| Error::Io(e))?;

        if calculated_checksum != self.header.checksum() {
            return Err(Error::ChecksumMismatch {
                expected: self.header.checksum(),
                actual: calculated_checksum,
            });
        }

        Ok(())
    }

    /// Returns the segment header.
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Returns the base index of this segment.
    pub fn base_index(&self) -> u64 {
        self.header.base_index()
    }

    /// Returns the number of entries in this segment.
    pub fn entry_count(&self) -> u32 {
        self.header.entry_count()
    }

    /// Returns the last index in this segment, if any.
    pub fn last_index(&self) -> Option<u64> {
        self.header.last_index()
    }

    /// Returns true if this segment is empty.
    pub fn is_empty(&self) -> bool {
        self.header.is_empty()
    }

    /// Returns the file path of this segment.
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    /// Flushes any pending writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.flush().map_err(Error::Io)?;
        }
        Ok(())
    }

    /// Appends an entry to this segment.
    ///
    /// # Arguments
    ///
    /// * `entry` - The log entry to append
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The entry index doesn't match the expected next index
    /// - File I/O operations fail
    pub fn append(&mut self, entry: LogEntry) -> Result<()> {
        // Validate entry index
        let expected_index = self.base_index() + u64::from(self.entry_count());
        if entry.index() != expected_index {
            return Err(Error::InvalidEntry {
                reason: format!(
                    "Entry index {} doesn't match expected index {}",
                    entry.index(),
                    expected_index
                ),
            });
        }

        // Get or create writer
        if self.writer.is_none() {
            let file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(&self.file_path)?;
            self.writer = Some(BufWriter::new(file));
        }

        let writer = self.writer.as_mut().unwrap();

        // Seek to append position
        writer.get_mut().seek(SeekFrom::Start(self.append_offset))?;

        // Write the entry
        write_entry(writer, &entry)?;
        writer.flush()?;

        // Update metadata
        self.append_offset += entry.serialized_size() as u64;
        self.header.set_entry_count(self.header.entry_count() + 1);

        // Update header in file
        self.update_header()?;

        Ok(())
    }

    /// Reads an entry at the specified index.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the entry to read
    ///
    /// # Returns
    ///
    /// The log entry at the specified index.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The index is out of bounds for this segment
    /// - File I/O operations fail
    pub fn read_entry(&self, index: u64) -> Result<LogEntry> {
        // Check if index is in this segment
        if index < self.base_index() {
            return Err(Error::IndexOutOfBounds {
                index,
                min: self.base_index(),
                max: self.last_index().unwrap_or(self.base_index()),
            });
        }

        if let Some(last_index) = self.last_index() {
            if index > last_index {
                return Err(Error::IndexOutOfBounds {
                    index,
                    min: self.base_index(),
                    max: last_index,
                });
            }
        } else {
            return Err(Error::EntryNotFound { index });
        }

        // Calculate which entry number this is (0-based)
        let entry_number = (index - self.base_index()) as u32;

        // Read through entries to find the target
        let mut file = File::open(&self.file_path)?;
        file.seek(SeekFrom::Start(24))?; // Skip header

        let mut reader = BufReader::new(file);

        for i in 0..=entry_number {
            let entry = read_entry(&mut reader)?;
            if i == entry_number {
                return Ok(entry);
            }
        }

        Err(Error::EntryNotFound { index })
    }

    /// Reads all entries in this segment.
    pub fn read_all_entries(&self) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::with_capacity(self.entry_count() as usize);

        let mut file = File::open(&self.file_path)?;
        file.seek(SeekFrom::Start(24))?; // Skip header

        let mut reader = BufReader::new(file);

        for _ in 0..self.entry_count() {
            let entry = read_entry(&mut reader)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Updates the header in the file with current metadata.
    fn update_header(&mut self) -> Result<()> {
        // First write the header with a temporary checksum of 0
        self.header.set_checksum(0);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.file_path)?;
        file.seek(SeekFrom::Start(0))?;
        write_header(&mut file, &self.header)?;
        file.flush()?;
        drop(file);

        // Now calculate the actual checksum
        let mut file = File::open(&self.file_path)?;
        let checksum = calculate_file_checksum(&mut file)
            .map_err(|e| Error::Io(e))?;
        drop(file);

        // Update header with correct checksum
        self.header.set_checksum(checksum);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.file_path)?;
        file.seek(SeekFrom::Start(0))?;
        write_header(&mut file, &self.header)?;
        file.flush()?;

        Ok(())
    }

    /// Calculates the append offset by reading through all entries.
    fn calculate_append_offset(file: &mut File, header: &SegmentHeader) -> Result<u64> {
        file.seek(SeekFrom::Start(24))?; // Skip header

        let mut offset = 24u64;
        let mut reader = BufReader::new(file);

        for _ in 0..header.entry_count() {
            let entry = read_entry(&mut reader)?;
            offset += entry.serialized_size() as u64;
        }

        Ok(offset)
    }
}

impl Drop for LogSegment {
    fn drop(&mut self) {
        // Ensure any buffered writes are flushed
        if let Some(ref mut writer) = self.writer {
            let _ = writer.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_segment() {
        let temp_dir = TempDir::new().unwrap();
        let segment = LogSegment::create(temp_dir.path(), 1).unwrap();

        assert_eq!(segment.base_index(), 1);
        assert_eq!(segment.entry_count(), 0);
        assert!(segment.is_empty());
        assert_eq!(segment.last_index(), None);

        // File should exist
        assert!(segment.file_path().exists());
    }

    #[test]
    fn test_open_segment() {
        let temp_dir = TempDir::new().unwrap();

        // Create a segment
        let segment = LogSegment::create(temp_dir.path(), 10).unwrap();
        let file_path = segment.file_path().to_path_buf();
        drop(segment);

        // Open the same segment
        let segment = LogSegment::open(&file_path).unwrap();
        assert_eq!(segment.base_index(), 10);
        assert_eq!(segment.entry_count(), 0);
    }

    #[test]
    fn test_append_single_entry() {
        let temp_dir = TempDir::new().unwrap();
        let mut segment = LogSegment::create(temp_dir.path(), 1).unwrap();

        let entry = LogEntry::new(1, 1, b"test command".to_vec());
        segment.append(entry.clone()).unwrap();

        assert_eq!(segment.entry_count(), 1);
        assert_eq!(segment.last_index(), Some(1));
        assert!(!segment.is_empty());

        // Read the entry back
        let read_entry = segment.read_entry(1).unwrap();
        assert_eq!(read_entry, entry);
    }

    #[test]
    fn test_append_multiple_entries() {
        let temp_dir = TempDir::new().unwrap();
        let mut segment = LogSegment::create(temp_dir.path(), 5).unwrap();

        let entries = vec![
            LogEntry::new(1, 5, b"command 1".to_vec()),
            LogEntry::new(1, 6, b"command 2".to_vec()),
            LogEntry::new(2, 7, b"command 3".to_vec()),
        ];

        for entry in &entries {
            segment.append(entry.clone()).unwrap();
        }

        assert_eq!(segment.entry_count(), 3);
        assert_eq!(segment.last_index(), Some(7));

        // Read all entries back
        for (i, expected) in entries.iter().enumerate() {
            let read_entry = segment.read_entry(5 + i as u64).unwrap();
            assert_eq!(read_entry, *expected);
        }
    }

    #[test]
    fn test_read_all_entries() {
        let temp_dir = TempDir::new().unwrap();
        let mut segment = LogSegment::create(temp_dir.path(), 1).unwrap();

        let entries = vec![
            LogEntry::new(1, 1, b"entry 1".to_vec()),
            LogEntry::new(1, 2, b"entry 2".to_vec()),
            LogEntry::new(1, 3, b"entry 3".to_vec()),
        ];

        for entry in &entries {
            segment.append(entry.clone()).unwrap();
        }

        let all_entries = segment.read_all_entries().unwrap();
        assert_eq!(all_entries, entries);
    }

    #[test]
    fn test_invalid_entry_index() {
        let temp_dir = TempDir::new().unwrap();
        let mut segment = LogSegment::create(temp_dir.path(), 10).unwrap();

        // Try to append entry with wrong index
        let entry = LogEntry::new(1, 5, b"wrong index".to_vec()); // Should be 10
        let result = segment.append(entry);

        match result {
            Err(Error::InvalidEntry { .. }) => {}, // Expected
            _ => panic!("Expected InvalidEntry error"),
        }
    }

    #[test]
    fn test_read_out_of_bounds() {
        let temp_dir = TempDir::new().unwrap();
        let segment = LogSegment::create(temp_dir.path(), 5).unwrap();

        // Try to read from empty segment
        let result = segment.read_entry(5);
        match result {
            Err(Error::EntryNotFound { index: 5 }) => {}, // Expected
            _ => panic!("Expected EntryNotFound error"),
        }

        // Try to read below base index
        let result = segment.read_entry(3);
        match result {
            Err(Error::IndexOutOfBounds { index: 3, min: 5, max: 5 }) => {}, // Expected
            _ => panic!("Expected IndexOutOfBounds error"),
        }
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("log-0000000001.segment");

        // Create segment and add entries
        {
            let mut segment = LogSegment::create(temp_dir.path(), 1).unwrap();
            segment.append(LogEntry::new(1, 1, b"persistent data".to_vec())).unwrap();
        }

        // Open segment and verify data persisted
        {
            let segment = LogSegment::open(&file_path).unwrap();
            assert_eq!(segment.entry_count(), 1);

            let entry = segment.read_entry(1).unwrap();
            assert_eq!(entry.command(), b"persistent data");
        }
    }

    #[test]
    fn test_validate_segment() {
        let temp_dir = TempDir::new().unwrap();
        let mut segment = LogSegment::create(temp_dir.path(), 1).unwrap();

        // Add some entries
        segment.append(LogEntry::new(1, 1, b"test".to_vec())).unwrap();

        // Validation should pass
        segment.validate().unwrap();
    }
}
