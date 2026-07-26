use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind};
use std::path::Path;

pub fn create_new_memory_mapped_file(
    file_path: impl AsRef<Path>,
    size: u64,
) -> Result<MmapMut, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(file_path)?;
    file.set_len(size)?;
    unsafe { MmapMut::map_mut(&file) }
}

pub fn open_existing_memory_mapped_file(
    file_path: impl AsRef<Path>,
    expected_size: u64,
) -> Result<MmapMut, Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(false)
        .open(file_path.as_ref())?;
    let actual_size = file.metadata()?.len();
    if actual_size != expected_size {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "memory-mapped file {:?} has size {actual_size}, expected {expected_size}",
                file_path.as_ref()
            ),
        ));
    }
    unsafe { MmapMut::map_mut(&file) }
}

pub const HEADER_SIZE: usize = 32;

pub const MAGIC_OFFSET: usize = 0;
pub const VERSION_OFFSET: usize = 4;
pub const BASE_INDEX_OFFSET: usize = 8;
pub const ENTRY_COUNT_OFFSET: usize = 16;
pub const START_APPEND_POSITION_OFFSET: usize = 24;
