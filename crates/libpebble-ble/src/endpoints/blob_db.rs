//! BlobDB endpoint (0xb1db) — key/value database writes, including notifications.

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobDBCommand {
    Insert = 0x01,
    Delete = 0x04,
    Clear = 0x05,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobDBId {
    Pin = 1,
    App = 2,
    Reminder = 3,
    Notification = 4,
    Weather = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BlobDBStatus {
    Success = 1,
    GeneralFailure = 2,
    InvalidOperation = 3,
    InvalidDatabaseId = 4,
    InvalidData = 5,
    KeyDoesNotExist = 6,
    DatabaseFull = 7,
    DataStale = 8,
}

impl BlobDBStatus {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Success),
            2 => Some(Self::GeneralFailure),
            3 => Some(Self::InvalidOperation),
            4 => Some(Self::InvalidDatabaseId),
            5 => Some(Self::InvalidData),
            6 => Some(Self::KeyDoesNotExist),
            7 => Some(Self::DatabaseFull),
            8 => Some(Self::DataStale),
            _ => None,
        }
    }
}

pub fn build_blobdb_insert(db: BlobDBId, key: &[u8; 16], blob: &[u8], token: u16) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(BlobDBCommand::Insert as u8);
    out.extend_from_slice(&token.to_le_bytes());
    out.push(db as u8);
    out.push(16u8); // key length
    out.extend_from_slice(key);
    out.extend_from_slice(&(blob.len() as u16).to_le_bytes());
    out.extend_from_slice(blob);
    out
}

pub fn parse_blobdb_response(payload: &[u8]) -> Option<(u16, u8)> {
    if payload.len() < 3 {
        return None;
    }
    let token = u16::from_le_bytes([payload[0], payload[1]]);
    let status = payload[2];
    Some((token, status))
}

// ---------------------------------------------------------------------------
// Notifications (built on top of BlobDB inserts)
// ---------------------------------------------------------------------------

const NOTIFICATION_ICON_GENERIC: u32 = 0x80000037;
const NOTIFICATIONS_APP_UUID: &str = "b2cae818-10f8-46df-ad2b-98ad2254a3c1";

pub fn build_notification_blob(
    title: &str,
    body: &str,
    subtitle: &str,
    timestamp: u32,
    icon: u32,
) -> Vec<u8> {
    let parent_uuid = Uuid::parse_str(NOTIFICATIONS_APP_UUID).unwrap().into_bytes();
    let item_uuid = Uuid::new_v4().into_bytes();

    let mut attrs = Vec::new();
    let mut attr_count = 0u8;
    for (attr_id, value) in [(1u8, title), (2u8, subtitle), (3u8, body)] {
        if !value.is_empty() {
            let raw = value.as_bytes();
            attrs.push(attr_id);
            attrs.extend_from_slice(&(raw.len() as u16).to_le_bytes());
            attrs.extend_from_slice(raw);
            attr_count += 1;
        }
    }
    // Icon attribute (u32)
    attrs.push(4u8);
    attrs.extend_from_slice(&4u16.to_le_bytes());
    attrs.extend_from_slice(&icon.to_le_bytes());
    attr_count += 1;

    let mut out = Vec::new();
    out.extend_from_slice(&item_uuid);
    out.extend_from_slice(&parent_uuid);
    out.extend_from_slice(&timestamp.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes()); // duration
    out.push(0x01); // type = notification
    out.extend_from_slice(&0x0001u16.to_le_bytes()); // flags
    out.push(0x04); // layout = notification
    out.extend_from_slice(&(attrs.len() as u16).to_le_bytes());
    out.push(attr_count);
    out.push(0); // action count
    out.extend_from_slice(&attrs);
    out
}

pub fn build_notification(
    title: &str,
    body: &str,
    subtitle: &str,
    timestamp: u32,
    token: u16,
) -> Vec<u8> {
    let blob = build_notification_blob(title, body, subtitle, timestamp, NOTIFICATION_ICON_GENERIC);
    let key: [u8; 16] = Uuid::new_v4().into_bytes();
    build_blobdb_insert(BlobDBId::Notification, &key, &blob, token)
}
