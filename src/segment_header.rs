//! Segment header data structure.

/// Magic number for Raft log segment files: "RAFT" in ASCII.
pub const MAGIC_NUMBER: u32 = 0x52414654;

/// Current file format version.
pub const VERSION: u32 = 0x00000001;

/// Size of the segment header in bytes.
pub const HEADER_SIZE: usize = 24;

/// Header for a log segment file.
///
/// The header contains metadata about the segment and appears at the
/// beginning of each segment file.
///
/// # Binary Format
///
/// ```text
/// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐
/// │ Magic (4 bytes) │ Version (4 bytes)│ Base Index (8)  │ Entry Count (4) │ Checksum (4)    │
/// └─────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘
/// ```
///
/// - **Magic Number**: 0x52414654 ("RAFT" in ASCII)
/// - **Version**: 0x00000001 (current version)
/// - **Base Index**: Index of the first entry in this segment
/// - **Entry Count**: Number of entries in this segment
/// - **Checksum**: CRC32 of the entire file
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentHeader {
    /// Magic number for file format validation.
    magic: u32,
    /// File format version.
    version: u32,
    /// Index of the first entry in this segment.
    base_index: u64,
    /// Number of entries in this segment.
    entry_count: u32,
    /// CRC32 checksum of the entire file.
    checksum: u32,
}

impl SegmentHeader {
    /// Creates a new segment header.
    ///
    /// # Arguments
    ///
    /// * `base_index` - Index of the first entry in this segment
    /// * `entry_count` - Number of entries in this segment
    /// * `checksum` - CRC32 checksum of the entire file
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raft_log::SegmentHeader;
    ///
    /// let header = SegmentHeader::new(1, 100, 0x12345678);
    /// assert_eq!(header.base_index(), 1);
    /// assert_eq!(header.entry_count(), 100);
    /// assert_eq!(header.checksum(), 0x12345678);
    /// ```
    pub fn new(base_index: u64, entry_count: u32, checksum: u32) -> Self {
        Self {
            magic: MAGIC_NUMBER,
            version: VERSION,
            base_index,
            entry_count,
            checksum,
        }
    }

    /// Returns the magic number.
    pub fn magic(&self) -> u32 {
        self.magic
    }

    /// Returns the file format version.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Returns the base index (index of first entry).
    pub fn base_index(&self) -> u64 {
        self.base_index
    }

    /// Returns the number of entries in this segment.
    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    /// Returns the checksum.
    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    /// Sets the entry count.
    pub fn set_entry_count(&mut self, count: u32) {
        self.entry_count = count;
    }

    /// Sets the checksum.
    pub fn set_checksum(&mut self, checksum: u32) {
        self.checksum = checksum;
    }

    /// Returns the last index in this segment.
    ///
    /// This is calculated as `base_index + entry_count - 1`.
    /// Returns `None` if the segment is empty.
    pub fn last_index(&self) -> Option<u64> {
        if self.entry_count == 0 {
            None
        } else {
            Some(self.base_index + u64::from(self.entry_count) - 1)
        }
    }

    /// Checks if this header is valid.
    ///
    /// Validates the magic number and version.
    pub fn is_valid(&self) -> bool {
        self.magic == MAGIC_NUMBER && self.version == VERSION
    }

    /// Returns true if this segment is empty.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_header_creation() {
        let header = SegmentHeader::new(1, 100, 0x12345678);
        assert_eq!(header.magic(), MAGIC_NUMBER);
        assert_eq!(header.version(), VERSION);
        assert_eq!(header.base_index(), 1);
        assert_eq!(header.entry_count(), 100);
        assert_eq!(header.checksum(), 0x12345678);
        assert!(header.is_valid());
        assert!(!header.is_empty());
    }

    #[test]
    fn test_last_index() {
        let header = SegmentHeader::new(10, 5, 0);
        assert_eq!(header.last_index(), Some(14)); // 10 + 5 - 1

        let empty_header = SegmentHeader::new(1, 0, 0);
        assert_eq!(empty_header.last_index(), None);
    }

    #[test]
    fn test_empty_segment() {
        let header = SegmentHeader::new(1, 0, 0);
        assert!(header.is_empty());
        assert_eq!(header.last_index(), None);
    }

    #[test]
    fn test_header_mutation() {
        let mut header = SegmentHeader::new(1, 0, 0);
        header.set_entry_count(50);
        header.set_checksum(0xABCDEF);

        assert_eq!(header.entry_count(), 50);
        assert_eq!(header.checksum(), 0xABCDEF);
        assert_eq!(header.last_index(), Some(50)); // 1 + 50 - 1
    }

    #[test]
    fn test_constants() {
        assert_eq!(MAGIC_NUMBER, 0x52414654); // "RAFT"
        assert_eq!(VERSION, 0x00000001);
        assert_eq!(HEADER_SIZE, 24);
    }
}
