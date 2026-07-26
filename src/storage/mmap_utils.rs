use memmap2::MmapMut;
use std::io;

pub struct MemoryMapUtil;

impl MemoryMapUtil {
    /// Makes all changes to a mapping durable before an RPC response is sent.
    pub fn flush(memory_map: &mut MmapMut) -> io::Result<()> {
        memory_map.flush()
    }
    pub fn write_vec_8(memory_map: &mut MmapMut, offset: usize, value: &[u8]) {
        memory_map[offset..offset + value.len()].copy_from_slice(value);
    }

    pub fn read_vec_8(memory_map: &MmapMut, offset: usize, length: usize) -> Vec<u8> {
        memory_map[offset..offset + length].to_vec()
    }

    pub fn write_u32(memory_map: &mut MmapMut, offset: usize, value: u32) {
        memory_map[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn read_u32(memory_map: &MmapMut, offset: usize) -> u32 {
        let byte_slice = &memory_map[offset..offset + 4];
        let mut u32_bytes: [u8; 4] = [0; 4];
        u32_bytes.copy_from_slice(byte_slice);
        u32::from_le_bytes(u32_bytes)
    }

    pub fn write_u64(memory_map: &mut MmapMut, offset: usize, value: u64) {
        memory_map[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn read_u64(memory_map: &MmapMut, offset: usize) -> u64 {
        let byte_slice = &memory_map[offset..offset + 8];
        let mut u64_bytes: [u8; 8] = [0; 8];
        u64_bytes.copy_from_slice(byte_slice);
        u64::from_le_bytes(u64_bytes)
    }

    pub fn write_u8(memory_map: &mut MmapMut, offset: usize, value: u8) {
        memory_map[offset] = value;
    }

    pub fn read_u8(memory_map: &MmapMut, offset: usize) -> u8 {
        memory_map[offset]
    }
}
