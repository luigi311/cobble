//! AppFetch endpoint (6001) used by the watch to request an app binary.

use uuid::Uuid;

const FETCH_APP: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppFetchRequest {
    pub uuid: Uuid,
    /// Watch-assigned application bank id, little-endian on the wire.
    pub app_id: u32,
}

pub fn parse_app_fetch_request(payload: &[u8]) -> Option<AppFetchRequest> {
    if payload.len() != 21 || payload[0] != FETCH_APP {
        return None;
    }
    Some(AppFetchRequest {
        uuid: Uuid::from_slice(&payload[1..17]).ok()?,
        app_id: u32::from_le_bytes(payload[17..21].try_into().ok()?),
    })
}

/// Tell the watch that the requested app is available and transfer will start.
pub fn build_app_fetch_start() -> [u8; 2] {
    [FETCH_APP, 0x01]
}

/// Tell the watch that another PutBytes session is currently active.
pub fn build_app_fetch_busy() -> [u8; 2] {
    [FETCH_APP, 0x02]
}

/// Tell the watch that this companion does not have the requested UUID.
pub fn build_app_fetch_invalid_uuid() -> [u8; 2] {
    [FETCH_APP, 0x03]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fetch_request() {
        let uuid = Uuid::parse_str("5bfacb04-9449-461e-b3e6-7637d490ed53").unwrap();
        let mut payload = vec![FETCH_APP];
        payload.extend_from_slice(uuid.as_bytes());
        payload.extend_from_slice(&0x7856_3412_u32.to_le_bytes());
        assert_eq!(
            parse_app_fetch_request(&payload),
            Some(AppFetchRequest {
                uuid,
                app_id: 0x7856_3412
            })
        );
    }

    #[test]
    fn response_statuses_match_the_protocol() {
        assert_eq!(build_app_fetch_start(), [FETCH_APP, 0x01]);
        assert_eq!(build_app_fetch_busy(), [FETCH_APP, 0x02]);
        assert_eq!(build_app_fetch_invalid_uuid(), [FETCH_APP, 0x03]);
    }
}
