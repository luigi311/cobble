use super::*;
use libpebble_ble::endpoints::language::{
    ENGLISH_SLOT, MULTIPLE_LANGUAGE_PACKS_CAPABILITY, slot_preference_value,
};
use libpebble_ble::{LanguagePack, LanguagePacks};

pub(super) fn supports_language_packs(watch: Option<&WatchVersionInfo>) -> bool {
    watch.is_some_and(|watch| watch.capabilities & MULTIPLE_LANGUAGE_PACKS_CAPABILITY != 0)
}

pub(super) fn reported_language_slot(settings: &HashMap<String, WatchPrefValue>) -> Option<u8> {
    if settings.get("langEnglish") == Some(&WatchPrefValue::Bool(true)) {
        return Some(ENGLISH_SLOT);
    }
    match settings.get("language")? {
        WatchPrefValue::Number(0) => Some(0),
        WatchPrefValue::Number(1) => Some(ENGLISH_SLOT),
        WatchPrefValue::Number(value @ 2..=4) => Some((*value - 1) as u8),
        _ => None,
    }
}

pub(super) fn validate_hrm_interval(
    code: u8,
    firmware: (u8, u8, u16),
) -> Result<HrMonitoringInterval, DaemonError> {
    let interval = HrMonitoringInterval::from_u8(code);
    if !interval.is_supported_by(firmware) {
        return Err(DaemonError::Failed(format!(
            "HRM measurement interval {code} is unsupported by this firmware"
        )));
    }
    Ok(interval)
}

pub(super) fn language_options(packs: &LanguagePacks) -> Vec<(u32, String)> {
    let mut options = vec![(1, "English".into())];
    for pack in &packs.packs {
        if pack.readable && pack.has_translation {
            options.push((
                pack.preference_value(),
                if pack.name.is_empty() {
                    pack.locale.clone()
                } else {
                    pack.name.clone()
                },
            ));
        }
    }
    let active = slot_preference_value(packs.active_slot);
    if !options.iter().any(|(value, _)| *value == active) {
        options.push((
            active,
            format!("Current language (slot {})", packs.active_slot),
        ));
    }
    options
}

pub(super) fn language_selection(
    packs: Option<&LanguagePacks>,
    value: u32,
) -> Result<Option<LanguagePack>, DaemonError> {
    let packs = packs.ok_or_else(|| {
        DaemonError::Failed(
            "installed language packs are unavailable; refresh before editing".into(),
        )
    })?;
    if value == slot_preference_value(ENGLISH_SLOT) {
        return Ok(None);
    }
    packs
        .packs
        .iter()
        .find(|pack| pack.preference_value() == value && pack.readable && pack.has_translation)
        .cloned()
        .map(Some)
        .ok_or_else(|| {
            DaemonError::Failed(
                "the selected language pack is unavailable or has no translation".into(),
            )
        })
}

pub(super) fn language_readback_matches(
    selected: Option<&LanguagePack>,
    actual: Option<&LanguagePacks>,
) -> bool {
    let Some(actual) = actual else { return false };
    match selected {
        None => actual.active_slot == ENGLISH_SLOT,
        Some(selected) => {
            actual.active_slot == selected.slot
                && actual.packs.iter().any(|pack| {
                    pack.slot == selected.slot
                        && pack.version == selected.version
                        && pack.locale == selected.locale
                        && pack.readable
                        && pack.has_translation
                })
        }
    }
}

/// Single-pack firmware can select English or its installed pack. Preserve a
/// reported legacy value without inventing a language name or changing it.
pub(super) fn legacy_language_options(current: u32) -> Vec<(u32, String)> {
    let mut options = vec![(1, "English".into()), (0, "Installed language pack".into())];
    if current > 1 {
        options.push((current, format!("Current language (value {current})")));
    }
    options
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_uses_current_text_keys_and_observed_language_pack_names() {
        let (event_tx, _) = mpsc::unbounded_channel();
        let (music_tx, _) = mpsc::unbounded_channel();
        let (phone_tx, _) = mpsc::unbounded_channel();
        let daemon = CobbleDaemon::new(
            Config::default(),
            watch::channel(0).0,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
            PathBuf::new(),
            PathBuf::new(),
            false,
            event_tx,
            None,
            music_tx,
            phone_tx,
            PkjsManager::unavailable(),
        );
        // A synthetic version response: firmware tag starts at byte 5,
        // hardware platform at 46, and the 64-bit capabilities at 142.
        let mut version = vec![0; 150];
        version[0] = 1;
        version[5..12].copy_from_slice(b"v4.44.0");
        version[46] = 18; // Emery
        version[142..150].copy_from_slice(&MULTIPLE_LANGUAGE_PACKS_CAPABILITY.to_le_bytes());
        {
            let mut state = daemon.state.lock().unwrap();
            state.device_config_watch = Some(
                libpebble_ble::endpoints::system::parse_watch_version_response(&version).unwrap(),
            );
            state.watch_settings = HashMap::from([
                ("textStyle".into(), WatchPrefValue::Number(0)),
                ("systemTextSize".into(), WatchPrefValue::Number(2)),
                ("notifTextSize".into(), WatchPrefValue::Number(4)),
                ("langEnglish".into(), WatchPrefValue::Bool(true)),
                ("language".into(), WatchPrefValue::Number(1)),
            ]);
            state.watch_setting_raw = HashMap::from([
                ("systemTextSize".into(), vec![3]),
                ("notifTextSize".into(), vec![4]),
            ]);
            state.language_packs = Some(LanguagePacks {
                active_slot: 2,
                packs: vec![LanguagePack {
                    slot: 2,
                    version: 42,
                    locale: "de_DE".into(),
                    name: "Deutsch".into(),
                    readable: true,
                    has_translation: true,
                }],
            });
        }
        let map = daemon.get_device_config();
        assert!(bool::try_from(map.get("capability.hrm.five_minute_sampling").unwrap()).unwrap());
        assert!(!map.contains_key("preference.textStyle.value"));
        assert!(!map.contains_key("preference.langEnglish.value"));
        assert_eq!(
            u32::try_from(map.get("preference.systemTextSize.value").unwrap()).unwrap(),
            2
        );
        assert_eq!(
            u32::try_from(map.get("preference.notifTextSize.value").unwrap()).unwrap(),
            4
        );
        assert_eq!(
            u32::try_from(map.get("preference.language.value").unwrap()).unwrap(),
            3
        );
        let options =
            Vec::<(u32, String)>::try_from(map["preference.language.options"].try_clone().unwrap())
                .unwrap();
        assert_eq!(options, [(1, "English".into()), (3, "Deutsch".into())]);

        daemon.cache_watch_setting("langEnglish".into(), WatchPrefValue::Bool(false), vec![0]);
        daemon.cache_watch_setting("language".into(), WatchPrefValue::Number(3), vec![3]);
        assert_eq!(
            u32::try_from(
                daemon
                    .get_device_config()
                    .get("preference.language.value")
                    .unwrap()
            )
            .unwrap(),
            3
        );
        daemon.cache_watch_setting("langEnglish".into(), WatchPrefValue::Bool(true), vec![1]);
        assert_eq!(
            u32::try_from(
                daemon
                    .get_device_config()
                    .get("preference.language.value")
                    .unwrap()
            )
            .unwrap(),
            1
        );

        // Single-pack firmware keeps the enum control; the older English
        // toggle is a fallback only when the enum was not reported.
        daemon
            .state
            .lock()
            .unwrap()
            .device_config_watch
            .as_mut()
            .unwrap()
            .capabilities = 0;
        let legacy = daemon.get_device_config();
        assert_eq!(
            u32::try_from(legacy.get("preference.language.value").unwrap()).unwrap(),
            3
        );
        assert!(!legacy.contains_key("preference.langEnglish.value"));
        daemon
            .state
            .lock()
            .unwrap()
            .watch_settings
            .remove("language");
        assert!(
            bool::try_from(
                daemon
                    .get_device_config()
                    .get("preference.langEnglish.value")
                    .unwrap()
            )
            .unwrap()
        );

        daemon.reset_device_config(DeviceConfigState::Loading);
        assert!(daemon.state.lock().unwrap().language_packs.is_none());
        assert!(
            !daemon
                .get_device_config()
                .contains_key("preference.language.value")
        );
    }

    #[test]
    fn interval_edits_require_supported_firmware_but_known_old_intervals_still_work() {
        assert!(validate_hrm_interval(4, (4, 38, 99)).is_err());
        assert_eq!(
            validate_hrm_interval(4, (4, 39, 0)).unwrap(),
            HrMonitoringInterval::FiveMin
        );
        assert!(validate_hrm_interval(17, (4, 44, 0)).is_err());
        assert_eq!(
            validate_hrm_interval(0, (4, 9, 146)).unwrap(),
            HrMonitoringInterval::TenMin
        );
        assert!(validate_hrm_interval(0, (4, 9, 145)).is_err());
    }

    #[test]
    fn language_choices_require_readable_translations_and_keep_pack_identity() {
        let translation = LanguagePack {
            slot: 2,
            version: 42,
            locale: "de_DE".into(),
            name: "Deutsch".into(),
            readable: true,
            has_translation: true,
        };
        let packs = LanguagePacks {
            active_slot: 2,
            packs: vec![
                translation.clone(),
                LanguagePack {
                    slot: 0,
                    name: "Font only".into(),
                    has_translation: false,
                    ..translation.clone()
                },
                LanguagePack {
                    slot: 3,
                    name: "Unreadable".into(),
                    readable: false,
                    ..translation.clone()
                },
            ],
        };
        assert_eq!(
            language_options(&packs),
            [(1, "English".into()), (3, "Deutsch".into())]
        );
        assert_eq!(
            language_selection(Some(&packs), 3).unwrap(),
            Some(translation)
        );
        assert_eq!(language_selection(Some(&packs), 1).unwrap(), None);
        assert!(language_selection(Some(&packs), 0).is_err());
        assert!(language_selection(Some(&packs), 4).is_err());
        assert!(language_selection(None, 1).is_err());
        assert!(language_readback_matches(
            Some(&packs.packs[0]),
            Some(&packs)
        ));
        assert!(!language_readback_matches(None, Some(&packs)));
        let mut replaced = packs.clone();
        replaced.packs[0].version += 1;
        assert!(!language_readback_matches(
            Some(&packs.packs[0]),
            Some(&replaced)
        ));
        assert!(!language_readback_matches(Some(&packs.packs[0]), None));
        assert_eq!(
            legacy_language_options(3).last().unwrap(),
            &(3, "Current language (value 3)".into())
        );
    }
}
