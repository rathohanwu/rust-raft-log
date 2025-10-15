use memmap2::MmapMut;

pub struct MemoryMapUtil;

impl MemoryMapUtil {
    pub fn write_u64(memory_map: &mut MmapMut, offset: usize, value: u64) {
        memory_map[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn read_u64(memory_map: &MmapMut, offset: usize) -> u64 {
        let byte_slice = &memory_map[offset..offset + 8];
        let mut u64_bytes: [u8; 8] = [0; 8];
        u64_bytes.copy_from_slice(byte_slice);
        u64::from_le_bytes(u64_bytes)
    }
}
