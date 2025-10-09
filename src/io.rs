//! Binary I/O operations for the Raft log file format.
//!
//! This module provides serialization and deserialization functions for:
//! - Segment headers (24 bytes)
//! - Log entries (variable size)
//!
//! All binary data is stored in little-endian format for consistency.

use crate::{Error, LogEntry, Result, SegmentHeader};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

/// Writes a segment header to the given writer.
///
/// # Binary Format
/// ```text
/// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐
/// │ Magic (4 bytes) │ Version (4 bytes)│ Base Index (8)  │ Entry Count (4) │ Checksum (4)    │
/// └─────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘
/// ```
pub fn write_header<W: Write>(writer: &mut W, header: &SegmentHeader) -> Result<()> {
    writer.write_u32::<LittleEndian>(header.magic())?;
    writer.write_u32::<LittleEndian>(header.version())?;
    writer.write_u64::<LittleEndian>(header.base_index())?;
    writer.write_u32::<LittleEndian>(header.entry_count())?;
    writer.write_u32::<LittleEndian>(header.checksum())?;
    Ok(())
}

/// Reads a segment header from the given reader.
pub fn read_header<R: Read>(reader: &mut R) -> Result<SegmentHeader> {
    let magic = reader.read_u32::<LittleEndian>()?;
    let version = reader.read_u32::<LittleEndian>()?;
    let base_index = reader.read_u64::<LittleEndian>()?;
    let entry_count = reader.read_u32::<LittleEndian>()?;
    let checksum = reader.read_u32::<LittleEndian>()?;

    let header = SegmentHeader::new(base_index, entry_count, checksum);
    
    // Validate magic number
    if magic != header.magic() {
        return Err(Error::InvalidMagicNumber { actual: magic });
    }
    
    // Validate version
    if version != header.version() {
        return Err(Error::UnsupportedVersion { version });
    }

    Ok(header)
}

/// Writes a log entry to the given writer.
///
/// # Binary Format
/// ```text
/// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
/// │ Length (4 bytes)│ Term (8 bytes)  │ Index (8 bytes) │ Command (N bytes)│
/// └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
/// ```
pub fn write_entry<W: Write>(writer: &mut W, entry: &LogEntry) -> Result<()> {
    // Calculate entry size (term + index + command)
    let entry_size = 8 + 8 + entry.command().len();
    
    // Write length prefix
    writer.write_u32::<LittleEndian>(entry_size as u32)?;
    
    // Write term and index
    writer.write_u64::<LittleEndian>(entry.term())?;
    writer.write_u64::<LittleEndian>(entry.index())?;
    
    // Write command data
    writer.write_all(entry.command())?;
    
    Ok(())
}

/// Reads a log entry from the given reader.
pub fn read_entry<R: Read>(reader: &mut R) -> Result<LogEntry> {
    // Read length prefix
    let entry_size = reader.read_u32::<LittleEndian>()?;
    
    // Validate minimum size (term + index = 16 bytes)
    if entry_size < 16 {
        return Err(Error::InvalidEntry {
            reason: format!("Entry size {} is too small (minimum 16 bytes)", entry_size),
        });
    }
    
    // Read term and index
    let term = reader.read_u64::<LittleEndian>()?;
    let index = reader.read_u64::<LittleEndian>()?;
    
    // Calculate command size
    let command_size = entry_size - 16;
    
    // Read command data
    let mut command = vec![0u8; command_size as usize];
    reader.read_exact(&mut command)?;
    
    Ok(LogEntry::new(term, index, command))
}

/// Calculates the size of an entry when serialized.
pub fn entry_serialized_size(entry: &LogEntry) -> usize {
    4 + 8 + 8 + entry.command().len() // length + term + index + command
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_header::{MAGIC_NUMBER, VERSION};
    use std::io::Cursor;

    #[test]
    fn test_header_round_trip() {
        let original = SegmentHeader::new(100, 50, 0x12345678);
        let mut buffer = Vec::new();
        
        // Write header
        write_header(&mut buffer, &original).unwrap();
        assert_eq!(buffer.len(), 24); // Header should be exactly 24 bytes
        
        // Read header back
        let mut cursor = Cursor::new(buffer);
        let restored = read_header(&mut cursor).unwrap();
        
        assert_eq!(original, restored);
    }

    #[test]
    fn test_entry_round_trip() {
        let original = LogEntry::new(5, 10, b"test command data".to_vec());
        let mut buffer = Vec::new();
        
        // Write entry
        write_entry(&mut buffer, &original).unwrap();
        
        // Expected size: 4 (length) + 8 (term) + 8 (index) + 17 (command) = 37
        assert_eq!(buffer.len(), 37);
        
        // Read entry back
        let mut cursor = Cursor::new(buffer);
        let restored = read_entry(&mut cursor).unwrap();
        
        assert_eq!(original, restored);
    }

    #[test]
    fn test_empty_command_entry() {
        let original = LogEntry::new(1, 1, Vec::new());
        let mut buffer = Vec::new();
        
        write_entry(&mut buffer, &original).unwrap();
        
        let mut cursor = Cursor::new(buffer);
        let restored = read_entry(&mut cursor).unwrap();
        
        assert_eq!(original, restored);
        assert!(restored.is_empty());
    }

    #[test]
    fn test_invalid_magic_number() {
        let mut buffer = Vec::new();
        buffer.write_u32::<LittleEndian>(0xDEADBEEF).unwrap(); // Wrong magic
        buffer.write_u32::<LittleEndian>(VERSION).unwrap();
        buffer.write_u64::<LittleEndian>(1).unwrap();
        buffer.write_u32::<LittleEndian>(0).unwrap();
        buffer.write_u32::<LittleEndian>(0).unwrap();
        
        let mut cursor = Cursor::new(buffer);
        let result = read_header(&mut cursor);
        
        match result {
            Err(Error::InvalidMagicNumber { actual }) => assert_eq!(actual, 0xDEADBEEF),
            _ => panic!("Expected InvalidMagicNumber error"),
        }
    }

    #[test]
    fn test_unsupported_version() {
        let mut buffer = Vec::new();
        buffer.write_u32::<LittleEndian>(MAGIC_NUMBER).unwrap();
        buffer.write_u32::<LittleEndian>(999).unwrap(); // Wrong version
        buffer.write_u64::<LittleEndian>(1).unwrap();
        buffer.write_u32::<LittleEndian>(0).unwrap();
        buffer.write_u32::<LittleEndian>(0).unwrap();
        
        let mut cursor = Cursor::new(buffer);
        let result = read_header(&mut cursor);
        
        match result {
            Err(Error::UnsupportedVersion { version }) => assert_eq!(version, 999),
            _ => panic!("Expected UnsupportedVersion error"),
        }
    }

    #[test]
    fn test_invalid_entry_size() {
        let mut buffer = Vec::new();
        buffer.write_u32::<LittleEndian>(8).unwrap(); // Too small (< 16)
        
        let mut cursor = Cursor::new(buffer);
        let result = read_entry(&mut cursor);
        
        match result {
            Err(Error::InvalidEntry { .. }) => {}, // Expected
            _ => panic!("Expected InvalidEntry error"),
        }
    }

    #[test]
    fn test_entry_serialized_size() {
        let entry = LogEntry::new(1, 1, b"hello".to_vec());
        assert_eq!(entry_serialized_size(&entry), 25); // 4 + 8 + 8 + 5
        assert_eq!(entry_serialized_size(&entry), entry.serialized_size());
    }
}
