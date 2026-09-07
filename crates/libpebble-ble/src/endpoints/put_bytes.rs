//! PutBytes endpoint (0xBEEF) packet encoding for app installation.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PutBytesObjectType {
    AppResource = 0x04,
    AppExecutable = 0x05,
    Worker = 0x07,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PutBytesResponse {
    pub acknowledged: bool,
    pub cookie: u32,
}

pub fn build_app_init(size: u32, object_type: PutBytesObjectType, app_id: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(10);
    out.push(0x01);
    out.extend_from_slice(&size.to_be_bytes());
    out.push(object_type as u8 | 0x80);
    out.extend_from_slice(&app_id.to_le_bytes());
    out
}

pub fn build_put(cookie: u32, data: &[u8]) -> Result<Vec<u8>, &'static str> {
    let len = u32::try_from(data.len()).map_err(|_| "PutBytes chunk exceeds u32")?;
    let mut out = Vec::with_capacity(9 + data.len());
    out.push(0x02);
    out.extend_from_slice(&cookie.to_be_bytes());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(data);
    Ok(out)
}

pub fn build_commit(cookie: u32, crc32: u32) -> [u8; 9] {
    let mut out = [0; 9];
    out[0] = 0x03;
    out[1..5].copy_from_slice(&cookie.to_be_bytes());
    out[5..9].copy_from_slice(&crc32.to_be_bytes());
    out
}

pub fn build_abort(cookie: u32) -> [u8; 5] {
    command_with_cookie(0x04, cookie)
}

pub fn build_install(cookie: u32) -> [u8; 5] {
    command_with_cookie(0x05, cookie)
}

fn command_with_cookie(command: u8, cookie: u32) -> [u8; 5] {
    let mut out = [0; 5];
    out[0] = command;
    out[1..5].copy_from_slice(&cookie.to_be_bytes());
    out
}

pub fn parse_response(payload: &[u8]) -> Option<PutBytesResponse> {
    if payload.len() != 5 || !matches!(payload[0], 0x01 | 0x02) {
        return None;
    }
    Some(PutBytesResponse {
        acknowledged: payload[0] == 0x01,
        cookie: u32::from_be_bytes(payload[1..5].try_into().ok()?),
    })
}

/// STM32-compatible CRC used by Pebble's PutBytes COMMIT command.
///
/// Input is consumed as little-endian 32-bit words. A final partial word is
/// interpreted in big-endian byte order, matching libpebble3's historical
/// zero-padding behavior.
pub fn calculate_crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        crc = crc_word(crc, u32::from_le_bytes(chunk.try_into().unwrap()));
    }
    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut word = [0; 4];
        word[4 - remainder.len()..].copy_from_slice(remainder);
        crc = crc_word(crc, u32::from_be_bytes(word));
    }
    crc
}

fn crc_word(mut crc: u32, word: u32) -> u32 {
    crc ^= word;
    for _ in 0..32 {
        crc = if crc & 0x8000_0000 != 0 {
            (crc << 1) ^ 0x04c1_1db7
        } else {
            crc << 1
        };
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_init_matches_libpebble() {
        assert_eq!(
            build_app_init(1000, PutBytesObjectType::AppExecutable, 0x1234_5678),
            [0x01, 0, 0, 3, 0xe8, 0x85, 0x78, 0x56, 0x34, 0x12]
        );
    }

    #[test]
    fn put_and_commit_are_big_endian() {
        assert_eq!(
            build_put(0x1234_5678, &[0xaa, 0xbb]).unwrap(),
            [0x02, 0x12, 0x34, 0x56, 0x78, 0, 0, 0, 2, 0xaa, 0xbb]
        );
        assert_eq!(
            build_commit(0x1234_5678, 0xaabb_ccdd),
            [0x03, 0x12, 0x34, 0x56, 0x78, 0xaa, 0xbb, 0xcc, 0xdd]
        );
    }

    #[test]
    fn crc_matches_libpebble_canonical_vectors() {
        assert_eq!(calculate_crc32(&[]), 0xffff_ffff);
        assert_eq!(calculate_crc32(&[0xab]), 0x1d60_4014);
        assert_eq!(calculate_crc32(&[1, 2, 3, 4]), 0x1dab_e74f);
        assert_eq!(calculate_crc32(&[1, 2, 3, 4, 5, 6]), 0x205d_bd4f);
        assert_eq!(
            calculate_crc32(&[1, 2, 3, 4, 0x50, 6, 0x70, 8]),
            0x99f9_e573
        );
    }
}
