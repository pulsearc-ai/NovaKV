const POLY: u32 = 0x82f63b78;
const MASK_DELTA: u32 = 0xa282ead8;
const TABLE0: [u32; 256] = make_table0();
const TABLE1: [u32; 256] = make_table_next(TABLE0, TABLE0);
const TABLE2: [u32; 256] = make_table_next(TABLE1, TABLE0);
const TABLE3: [u32; 256] = make_table_next(TABLE2, TABLE0);

/// CRC32C of `data`, continued from `init_crc`. Uses a hardware
/// CRC32C instruction when the running CPU has one (ARM `crc`,
/// x86-64 SSE4.2), else a portable slicing-by-4 table. Every
/// path is bit-identical: the hardware `crc32c` instruction
/// computes the same Castagnoli CRC32C as the table.
pub fn extend(init_crc: u32, data: &[u8]) -> u32 {
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: gated by runtime detection of the `crc`
            // feature, which `extend_hw_aarch64` requires.
            return unsafe { extend_hw_aarch64(init_crc, data) };
        }
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("sse4.2") {
            // SAFETY: gated by runtime detection of SSE4.2,
            // which `extend_hw_x86_64` requires.
            return unsafe { extend_hw_x86_64(init_crc, data) };
        }
    }
    extend_software(init_crc, data)
}

/// Portable slicing-by-4 table fallback. Used when no hardware
/// CRC32C instruction is available.
fn extend_software(init_crc: u32, data: &[u8]) -> u32 {
    let mut crc = init_crc ^ 0xffff_ffff;
    let mut offset = 0usize;
    while offset + 16 <= data.len() {
        crc = step4(crc, data, offset);
        crc = step4(crc, data, offset + 4);
        crc = step4(crc, data, offset + 8);
        crc = step4(crc, data, offset + 12);
        offset += 16;
    }
    while offset + 4 <= data.len() {
        crc = step4(crc, data, offset);
        offset += 4;
    }
    while offset < data.len() {
        let index = ((crc as u8) ^ data[offset]) as usize;
        crc = TABLE0[index] ^ (crc >> 8);
        offset += 1;
    }
    crc ^ 0xffff_ffff
}

/// ARM CRC32C path. The `crc` target feature provides the
/// `crc32c{b,h,w,d}` instructions used here.
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn extend_hw_aarch64(init_crc: u32, data: &[u8]) -> u32 {
    use std::arch::aarch64::{__crc32cb, __crc32cd, __crc32ch, __crc32cw};
    let mut crc = init_crc ^ 0xffff_ffff;
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        crc = __crc32cd(crc, u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    let mut rem = chunks.remainder();
    if rem.len() >= 4 {
        crc = __crc32cw(crc, u32::from_le_bytes(rem[..4].try_into().unwrap()));
        rem = &rem[4..];
    }
    if rem.len() >= 2 {
        crc = __crc32ch(crc, u16::from_le_bytes(rem[..2].try_into().unwrap()));
        rem = &rem[2..];
    }
    if let Some(&byte) = rem.first() {
        crc = __crc32cb(crc, byte);
    }
    crc ^ 0xffff_ffff
}

/// x86-64 CRC32C path (SSE4.2 `crc32` instruction).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn extend_hw_x86_64(init_crc: u32, data: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u16, _mm_crc32_u32, _mm_crc32_u64, _mm_crc32_u8};
    let mut crc64 = u64::from(init_crc ^ 0xffff_ffff);
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        crc64 = _mm_crc32_u64(crc64, u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    let mut crc = crc64 as u32;
    let mut rem = chunks.remainder();
    if rem.len() >= 4 {
        crc = _mm_crc32_u32(crc, u32::from_le_bytes(rem[..4].try_into().unwrap()));
        rem = &rem[4..];
    }
    if rem.len() >= 2 {
        crc = _mm_crc32_u16(crc, u16::from_le_bytes(rem[..2].try_into().unwrap()));
        rem = &rem[2..];
    }
    if let Some(&byte) = rem.first() {
        crc = _mm_crc32_u8(crc, byte);
    }
    crc ^ 0xffff_ffff
}

#[inline]
fn step4(crc: u32, data: &[u8], offset: usize) -> u32 {
    let word = u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]);
    let c = crc ^ word;
    TABLE3[(c & 0xff) as usize]
        ^ TABLE2[((c >> 8) & 0xff) as usize]
        ^ TABLE1[((c >> 16) & 0xff) as usize]
        ^ TABLE0[(c >> 24) as usize]
}

pub fn value(data: &[u8]) -> u32 {
    extend(0, data)
}

pub fn mask(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(MASK_DELTA)
}

pub fn unmask(masked_crc: u32) -> u32 {
    masked_crc.wrapping_sub(MASK_DELTA).rotate_right(17)
}

const fn make_table0() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

const fn make_table_next(previous: [u32; 256], base: [u32; 256]) -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let crc = previous[i];
        table[i] = (crc >> 8) ^ base[(crc & 0xff) as usize];
        i += 1;
    }
    table
}
