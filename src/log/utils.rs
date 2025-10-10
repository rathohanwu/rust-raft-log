use byteorder::{LittleEndian, ReadBytesExt};
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::io;
use std::io::{Error, Write};

pub fn create_memory_mapped_file(file_path: &str, size: u64) -> Result<MmapMut, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)?;
    file.set_len(size)?;
    let memory_map = unsafe { MmapMut::map_mut(&file)? };
    Ok(memory_map)
}

pub fn write_u64(buffer: &mut MmapMut, offset: u64, value: u64) -> bool {
    let mut cursor = io::Cursor::new(&mut buffer[..HEADER_SIZE]);
    cursor.set_position(offset);
    let result = cursor.write_all(&value.to_le_bytes());
    match result {
        Err(e) => {
            eprintln!("Error writing u64 at offset {}: {}", offset, e);
            false
        }
        Ok(_) => true,
    }
}

pub fn read_u64(buffer: &MmapMut, offset: u64) -> Option<u64> {
    let mut cursor = io::Cursor::new(&buffer[..HEADER_SIZE]);
    cursor.set_position(offset);
    let result = cursor.read_u64::<LittleEndian>();
    match result {
        Err(e) => {
            eprintln!("Error reading u64 at offset {}: {}", offset, e);
            None
        }
        Ok(value) => Some(value),
    }
}

pub const HEADER_SIZE: usize = 24;
pub const BASE_INDEX_OFFSET: u64 = 8;
pub const ENTRY_COUNT_OFFSET: u64 = 16;
