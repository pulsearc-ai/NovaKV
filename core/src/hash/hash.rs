pub fn hash(data: &[u8], seed: u32) -> u32 {
    const M: u32 = 0xc6a4a793;
    const R: u32 = 24;

    let mut h = seed ^ ((data.len() as u32).wrapping_mul(M));
    let mut chunks = data.chunks_exact(4);

    for chunk in &mut chunks {
        let word = crate::coding::decode_fixed32(chunk);
        h = h.wrapping_add(word);
        h = h.wrapping_mul(M);
        h ^= h >> 16;
    }

    let remainder = chunks.remainder();
    match remainder.len() {
        3 => {
            h = h.wrapping_add(u32::from(remainder[2]) << 16);
            h = h.wrapping_add(u32::from(remainder[1]) << 8);
            h = h.wrapping_add(u32::from(remainder[0]));
            h = h.wrapping_mul(M);
            h ^= h >> R;
        }
        2 => {
            h = h.wrapping_add(u32::from(remainder[1]) << 8);
            h = h.wrapping_add(u32::from(remainder[0]));
            h = h.wrapping_mul(M);
            h ^= h >> R;
        }
        1 => {
            h = h.wrapping_add(u32::from(remainder[0]));
            h = h.wrapping_mul(M);
            h ^= h >> R;
        }
        _ => {}
    }

    h
}
