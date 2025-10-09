//! Internal utility functions.
//!
//! This module contains utility functions for the Raft log implementation.

use crc32fast::Hasher;
use std::io::{Read, Seek, SeekFrom};

/// Calculates CRC32 checksum of data.
pub fn calculate_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Calculates CRC32 checksum of a file, excluding the checksum field itself.
///
/// This function reads the entire file and calculates the checksum,
/// but skips the 4-byte checksum field at offset 20 in the header.
pub fn calculate_file_checksum<R: Read + Seek>(reader: &mut R) -> std::io::Result<u32> {
    reader.seek(SeekFrom::Start(0))?;

    let mut hasher = Hasher::new();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer)?;

    // Calculate checksum of everything except the checksum field (bytes 20-23)
    if buffer.len() >= 24 {
        // Hash header up to checksum field (bytes 0-19)
        hasher.update(&buffer[0..20]);
        // Hash everything after checksum field (bytes 24+)
        if buffer.len() > 24 {
            hasher.update(&buffer[24..]);
        }
    } else {
        // File too small, hash everything
        hasher.update(&buffer);
    }

    Ok(hasher.finalize())
}

/// Generates a filename for a log segment.
pub fn segment_filename(base_index: u64) -> String {
    format!("log-{:010}.segment", base_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_calculate_checksum() {
        let data = b"hello world";
        let checksum = calculate_checksum(data);

        // CRC32 of "hello world" should be consistent
        assert_eq!(checksum, calculate_checksum(data));

        // Different data should have different checksum
        let checksum2 = calculate_checksum(b"hello world!");
        assert_ne!(checksum, checksum2);
    }

    #[test]
    fn test_segment_filename() {
        assert_eq!(segment_filename(1), "log-0000000001.segment");
        assert_eq!(segment_filename(123), "log-0000000123.segment");
        assert_eq!(segment_filename(9999999999), "log-9999999999.segment");
    }

    #[test]
    fn test_file_checksum_small_file() {
        let data = b"small";
        let mut cursor = Cursor::new(data);
        let checksum = calculate_file_checksum(&mut cursor).unwrap();

        // Should hash the entire small file
        assert_eq!(checksum, calculate_checksum(data));
    }

    #[test]
    fn test_file_checksum_with_header() {
        // Create a mock file with 24+ bytes
        let mut data = vec![0u8; 30];
        data[20] = 0xFF; // Checksum field that should be skipped
        data[21] = 0xFF;
        data[22] = 0xFF;
        data[23] = 0xFF;

        let mut cursor = Cursor::new(&data);
        let checksum = calculate_file_checksum(&mut cursor).unwrap();

        // Should be same as hashing bytes 0-19 + 24-29
        let mut expected_data = Vec::new();
        expected_data.extend_from_slice(&data[0..20]);
        expected_data.extend_from_slice(&data[24..]);
        let expected = calculate_checksum(&expected_data);

        assert_eq!(checksum, expected);
    }
}
