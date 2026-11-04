//! Installed language-pack queries and selection on SystemMessage (endpoint 18).
//! Mirrors libpebble3's token, slot, version and locale checks.

pub const MULTIPLE_LANGUAGE_PACKS_CAPABILITY: u64 = 1 << 28;
pub const ENGLISH_SLOT: u8 = 0xff;
pub const MAX_LANGUAGE_PACKS: usize = 4;
pub const PACKS_RESPONSE: u8 = 0x0e;
pub const SELECT_RESPONSE: u8 = 0x13;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanguagePack {
    pub slot: u8,
    pub version: u16,
    pub locale: String,
    pub name: String,
    pub readable: bool,
    pub has_translation: bool,
}

impl LanguagePack {
    /// ShellLanguage value. Slot zero retains the legacy installed-pack value.
    pub fn preference_value(&self) -> u32 {
        slot_preference_value(self.slot)
    }
}

pub fn slot_preference_value(slot: u8) -> u32 {
    match slot {
        ENGLISH_SLOT => 1,
        0 => 0,
        _ => u32::from(slot) + 1,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanguagePacks {
    pub active_slot: u8,
    pub packs: Vec<LanguagePack>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LanguageResponse {
    Packs(LanguagePacks),
    Selected { slot: u8, success: bool },
}

pub fn build_packs_request(token: u8) -> Vec<u8> {
    vec![0, 0x0d, token]
}

pub fn build_select_request(token: u8, pack: Option<&LanguagePack>) -> Vec<u8> {
    let mut out = vec![0, 0x11, token, pack.map_or(ENGLISH_SLOT, |pack| pack.slot)];
    out.extend_from_slice(&pack.map_or(0, |pack| pack.version).to_le_bytes());
    let mut locale = [0; 6];
    if let Some(pack) = pack {
        let bytes = pack.locale.as_bytes();
        let len = bytes.len().min(locale.len());
        locale[..len].copy_from_slice(&bytes[..len]);
    }
    out.extend_from_slice(&locale);
    out
}

pub(crate) fn parse_response(payload: &[u8]) -> Option<(u8, u8, LanguageResponse)> {
    if *payload.first()? != 0 {
        return None;
    }
    let kind = *payload.get(1)?;
    let token = *payload.get(2)?;
    let response = match kind {
        PACKS_RESPONSE => {
            let active_slot = *payload.get(3)?;
            if active_slot != ENGLISH_SLOT && usize::from(active_slot) >= MAX_LANGUAGE_PACKS {
                return None;
            }
            let records = payload.get(4..4 + MAX_LANGUAGE_PACKS * 40)?;
            let mut packs = Vec::new();
            for record in records.chunks_exact(40) {
                let flags = record[1];
                if flags & 1 == 0 {
                    continue;
                }
                let slot = record[0];
                if usize::from(slot) >= MAX_LANGUAGE_PACKS
                    || packs.iter().any(|pack: &LanguagePack| pack.slot == slot)
                {
                    return None;
                }
                let text = |bytes: &[u8]| {
                    let end = bytes
                        .iter()
                        .position(|byte| *byte == 0)
                        .unwrap_or(bytes.len());
                    String::from_utf8_lossy(&bytes[..end]).into_owned()
                };
                packs.push(LanguagePack {
                    slot,
                    version: u16::from_le_bytes([record[2], record[3]]),
                    locale: text(&record[4..10]),
                    name: text(&record[10..40]),
                    readable: flags & 2 != 0,
                    has_translation: flags & 4 != 0,
                });
            }
            LanguageResponse::Packs(LanguagePacks { active_slot, packs })
        }
        SELECT_RESPONSE => LanguageResponse::Selected {
            slot: *payload.get(3)?,
            success: *payload.get(4)? == 1,
        },
        _ => return None,
    };
    Some((kind, token, response))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn packs_reply() -> Vec<u8> {
        let mut reply = vec![0, PACKS_RESPONSE, 7, 2];
        reply.resize(164, 0);
        reply[4..8].copy_from_slice(&[2, 7, 0x34, 0x12]);
        reply[8..13].copy_from_slice(b"de_DE");
        reply[14..21].copy_from_slice(b"Deutsch");
        reply
    }

    #[test]
    fn language_packets_match_the_official_layout() {
        let (_, token, LanguageResponse::Packs(packs)) = parse_response(&packs_reply()).unwrap()
        else {
            panic!("expected language packs");
        };
        assert_eq!(token, 7);
        assert_eq!(packs.active_slot, 2);
        assert_eq!(packs.packs.len(), 1);
        let pack = &packs.packs[0];
        assert_eq!(pack.name, "Deutsch");
        assert!(pack.readable && pack.has_translation);
        assert_eq!(pack.preference_value(), 3);
        assert_eq!(build_packs_request(7), [0, 0x0d, 7]);
        assert_eq!(
            build_select_request(8, Some(pack)),
            [0, 0x11, 8, 2, 0x34, 0x12, b'd', b'e', b'_', b'D', b'E', 0]
        );
        assert_eq!(
            build_select_request(9, None),
            [0, 0x11, 9, 0xff, 0, 0, 0, 0, 0, 0, 0, 0]
        );
        assert_eq!(
            parse_response(&[0, SELECT_RESPONSE, 8, 2, 1]),
            Some((
                SELECT_RESPONSE,
                8,
                LanguageResponse::Selected {
                    slot: 2,
                    success: true
                }
            ))
        );
    }

    #[test]
    fn malformed_pack_lists_are_rejected() {
        let reply = packs_reply();
        for len in 0..reply.len() {
            assert!(parse_response(&reply[..len]).is_none());
        }
        let mut invalid = reply.clone();
        invalid[4] = 4;
        assert!(parse_response(&invalid).is_none());
        let mut duplicate = reply.clone();
        duplicate[44..84].copy_from_slice(&reply[4..44]);
        assert!(parse_response(&duplicate).is_none());
    }
}
