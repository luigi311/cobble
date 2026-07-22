use std::cell::Cell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cobble_client::{
    ApplyDisposition, CobbleClient, DaemonConfigPatch, DaemonConfigSnapshot, DeviceConfigPatch,
    DeviceConfigSnapshot, DeviceConfigState, DistanceUnits, FieldAvailability, HealthConfigPatch,
    HrmMeasurementInterval, IntervalsIcuPatch, PreferenceValue, SecretPatch, StatusEvent, VarDict,
};
use slint::{ModelRc, VecModel};
use tracing::warn;

slint::include_modules!();

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let initial_snapshot = rt.block_on(async {
        let client = CobbleClient::new().await.ok()?;
        client.get_daemon_config().await.ok()
    });

    let window = AppWindow::new()?;

    // AppWindow::new() initializes Slint's platform, but does not show its
    // window yet. Set the desktop ID in between so Phosh can associate them.
    slint::set_xdg_app_id("cobble")?;

    let config_baseline: Arc<Mutex<Option<DaemonConfigSnapshot>>> =
        Arc::new(Mutex::new(initial_snapshot.clone()));
    let device_revision = Arc::new(Mutex::new(0u64));
    let device_baseline: Arc<Mutex<Option<DeviceConfigSnapshot>>> = Arc::new(Mutex::new(None));
    if let Some(snapshot) = &initial_snapshot {
        apply_daemon_config(&window, snapshot);
    } else {
        window.set_save_status("Daemon unavailable; configuration could not be loaded.".into());
    }
    window.set_cfg_intervals_api_key("".into());
    window.set_cfg_intervals_applied_api_key("".into());
    window.set_cfg_intervals_status("Loading sync status…".into());

    let effective_db_path = initial_snapshot
        .as_ref()
        .map(|snapshot| PathBuf::from(&snapshot.active_database_path))
        .or_else(|| match cobble_client::offline_database_path() {
            Ok(path) => Some(path),
            Err(error) => {
                warn!("offline database path unavailable: {error}");
                None
            }
        })
        .unwrap_or_default();

    // Derive the watch timezone offset from synced data so all times/labels
    // render in the watch's local zone, independent of the host's system tz.
    if let Ok(conn) = cobble_db::connect_readonly(&effective_db_path) {
        cobble_db::set_watch_offset(cobble_db::watch_tz_offset(&conn));
    }

    // ── Shared filter state (main-thread only) ───────────────────────────────
    let period_workout = Rc::new(Cell::new(1i32));
    let offset_workout = Rc::new(Cell::new(0i32));
    let bar_range_w = Rc::new(Cell::new((-1i64, -1i64)));
    let period_sleep = Rc::new(Cell::new(1i32));
    let offset_sleep = Rc::new(Cell::new(0i32));
    let period_heart = Rc::new(Cell::new(1i32));
    let offset_heart = Rc::new(Cell::new(0i32));

    // ── Set initial period labels ────────────────────────────────────────────
    window.set_workout_period_label(cobble_db::period_label(1, 0).into());
    window.set_workout_can_forward(false);
    window.set_sleep_period_label(cobble_db::period_label(1, 0).into());
    window.set_sleep_can_forward(false);
    window.set_heart_period_label(cobble_db::period_label(1, 0).into());
    window.set_heart_can_forward(false);

    // ── Initial data load ────────────────────────────────────────────────────
    reload_workout_chart(&window, &effective_db_path, 1, 0);
    reload_workout_sessions(&window, &effective_db_path, 1, 0, (-1, -1));
    reload_sleep_chart(&window, &effective_db_path, 1, 0);
    reload_sleep_stats(&window, &effective_db_path, 1, 0);
    reload_heart_stats(&window, &effective_db_path, 1, 0);

    // ── Background tokio runtime ─────────────────────────────────────────────
    // Enter the runtime context on the main thread. zbus (via cobble-client)
    // needs an ambient Tokio runtime when it creates its connection/executor
    // tasks; without this guard those code paths panic with "there is no reactor
    // running". Load-bearing — do not remove.
    let _rt_guard = rt.enter();

    refresh_wellness_status(window.as_weak(), rt.handle());

    {
        let weak = window.as_weak();
        let config_baseline = config_baseline.clone();
        let device_revision = device_revision.clone();
        let device_baseline = device_baseline.clone();
        rt.spawn(async move {
            loop {
                // Stream daemon/watch status via D-Bus signals (no polling).
                if let Ok(client) = CobbleClient::new().await {
                    let weak2 = weak.clone();
                    let baseline = config_baseline.clone();
                    let device_revision = device_revision.clone();
                    let device_baseline = device_baseline.clone();
                    let _ = client
                        .watch_status(move |ev| {
                            if matches!(ev, StatusEvent::DaemonConfigChanged(_)) {
                                let weak_config = weak2.clone();
                                let baseline = baseline.clone();
                                tokio::spawn(async move {
                                    let snapshot = match CobbleClient::new().await {
                                        Ok(client) => client.get_daemon_config().await.ok(),
                                        Err(_) => None,
                                    };
                                    slint::invoke_from_event_loop(move || {
                                        let (Some(window), Some(snapshot)) =
                                            (weak_config.upgrade(), snapshot)
                                        else {
                                            return;
                                        };
                                        apply_daemon_config(&window, &snapshot);
                                        *baseline.lock().unwrap() = Some(snapshot);
                                    })
                                    .ok();
                                });
                            }
                            if matches!(ev, StatusEvent::DeviceConfigChanged { .. }) {
                                let weak_config = weak2.clone();
                                let device_revision = device_revision.clone();
                                let device_baseline = device_baseline.clone();
                                tokio::spawn(async move {
                                    let snapshot = match CobbleClient::new().await {
                                        Ok(client) => client.get_device_config().await.ok(),
                                        Err(_) => None,
                                    };
                                    slint::invoke_from_event_loop(move || {
                                        let (Some(window), Some(snapshot)) =
                                            (weak_config.upgrade(), snapshot)
                                        else {
                                            return;
                                        };
                                        if !window.get_dc_dirty() && !window.get_dc_applying() {
                                            *device_revision.lock().unwrap() = snapshot.revision;
                                            apply_device_config(&window, &snapshot);
                                            *device_baseline.lock().unwrap() = Some(snapshot);
                                        }
                                    })
                                    .ok();
                                });
                            }
                            let weak3 = weak2.clone();
                            slint::invoke_from_event_loop(move || {
                                if let Some(w) = weak3.upgrade() {
                                    apply_status(&w, ev);
                                }
                            })
                            .ok();
                        })
                        .await;
                }
                // watch_status only returns if the bus connection drops; retry.
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    // ── Device Config refresh ────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let rt_handle = rt.handle().clone();
        let device_revision = device_revision.clone();
        let device_baseline = device_baseline.clone();
        window.on_refresh_device_config(move || {
            let Some(window) = weak.upgrade() else { return };
            if !window.get_watch_connected() {
                window.set_dc_state("disconnected".into());
                window.set_dc_status_error(true);
                window.set_dc_status("Connect a watch before refreshing Device Config.".into());
                return;
            }
            window.set_dc_loading(true);
            window.set_dc_status_error(false);
            window.set_dc_status("Reading settings from the watch…".into());
            let weak2 = weak.clone();
            let device_revision = device_revision.clone();
            let device_baseline = device_baseline.clone();
            rt_handle.spawn(async move {
                let result = async {
                    let client = CobbleClient::new().await?;
                    client.refresh_device_config().await
                }
                .await;
                slint::invoke_from_event_loop(move || {
                    let Some(window) = weak2.upgrade() else {
                        return;
                    };
                    window.set_dc_loading(false);
                    match result {
                        Ok(snapshot) => {
                            *device_revision.lock().unwrap() = snapshot.revision;
                            apply_device_config(&window, &snapshot);
                            *device_baseline.lock().unwrap() = Some(snapshot);
                        }
                        Err(error) => {
                            window.set_dc_state("error".into());
                            window.set_dc_status_error(true);
                            window.set_dc_status(format!("Refresh failed: {error}").into());
                        }
                    }
                })
                .ok();
            });
        });
    }

    {
        let weak = window.as_weak();
        window.on_device_health_changed(move || {
            if let Some(window) = weak.upgrade() {
                update_health_display(&window);
            }
        });
    }

    {
        let weak = window.as_weak();
        let revision = device_revision.clone();
        let baseline = device_baseline.clone();
        let rt_handle = rt.handle().clone();
        window.on_apply_device_config(move || {
            let Some(window) = weak.upgrade() else { return };
            if !window.get_watch_connected() || window.get_dc_applying() { return; }
            let Some(current) = baseline.lock().unwrap().clone() else { return };
            let staged_units = if window.get_dc_units_index() == 0 { DistanceUnits::Metric } else { DistanceUnits::Imperial };
            let staged_interval = match window.get_dc_hrm_interval_index() {
                    0 => HrmMeasurementInterval::TenMinutes,
                    1 => HrmMeasurementInterval::ThirtyMinutes,
                    2 => HrmMeasurementInterval::OneHour,
                    _ => HrmMeasurementInterval::Off,
                };
            let mut preferences = std::collections::BTreeMap::new();
            let original_preferences = current.preferences;
            let mut stage_bool = |key: &str, available: bool, value: bool| {
                if available && original_preferences.get(key).and_then(|field| field.value.as_ref()) != Some(&PreferenceValue::Bool(value)) {
                    preferences.insert(key.to_owned(), PreferenceValue::Bool(value));
                }
            };
            stage_bool("clock24h", window.get_dc_clock_24h_available(), window.get_dc_clock_24h_value());
            stage_bool("timezoneSource", window.get_dc_timezone_manual_available(), window.get_dc_timezone_manual_value());
            stage_bool("stationaryMode", window.get_dc_stationary_mode_available(), window.get_dc_stationary_mode_value());
            stage_bool("displayOrientationLeftHanded", window.get_dc_left_handed_available(), window.get_dc_left_handed_value());
            stage_bool("lightEnabled", window.get_dc_light_enabled_available(), window.get_dc_light_enabled_value());
            stage_bool("lightAmbientSensorEnabled", window.get_dc_light_ambient_available(), window.get_dc_light_ambient_value());
            stage_bool("lightMotion", window.get_dc_light_motion_available(), window.get_dc_light_motion_value());
            stage_bool("lightDynamicIntensity", window.get_dc_light_dynamic_legacy_available(), window.get_dc_light_dynamic_legacy_value());
            stage_bool("menuScrollWrapAround", window.get_dc_menu_wrap_available(), window.get_dc_menu_wrap_value());
            stage_bool("notifDesignStyle", window.get_dc_notif_design_available(), window.get_dc_notif_design_value());
            stage_bool("notifVibeDelay", window.get_dc_notif_delay_available(), window.get_dc_notif_delay_value());
            stage_bool("notifBacklight", window.get_dc_notif_backlight_available(), window.get_dc_notif_backlight_value());
            stage_bool("dndManuallyEnabled", window.get_dc_dnd_manual_available(), window.get_dc_dnd_manual_value());
            stage_bool("dndSmartEnabled", window.get_dc_dnd_smart_available(), window.get_dc_dnd_smart_value());
            stage_bool("dndMotionBacklight", window.get_dc_dnd_motion_backlight_available(), window.get_dc_dnd_motion_backlight_value());
            stage_bool("dndAutoDismiss", window.get_dc_dnd_auto_dismiss_available(), window.get_dc_dnd_auto_dismiss_value());
            stage_bool("timelineQuickViewEnabled", window.get_dc_timeline_quick_view_available(), window.get_dc_timeline_quick_view_value());
            stage_bool("musicShowVolumeControls", window.get_dc_music_volume_available(), window.get_dc_music_volume_value());
            stage_bool("musicShowProgressBar", window.get_dc_music_progress_available(), window.get_dc_music_progress_value());
            stage_bool("langEnglish", window.get_dc_language_english_available(), window.get_dc_language_english_value());
            let mut stage_number = |key: &str, available: bool, value: u32| {
                if available && original_preferences.get(key).and_then(|field| field.value.as_ref()) != Some(&PreferenceValue::Unsigned(value)) {
                    preferences.insert(key.to_owned(), PreferenceValue::Unsigned(value));
                }
            };
            stage_number("lightTimeoutMs", window.get_dc_light_timeout_available(), window.get_dc_light_timeout_ms() as u32);
            stage_number("lightTouch", window.get_dc_light_touch_available(), window.get_dc_light_touch_index() as u32);
            let intensity_codes = [10, 25, 50, 100];
            stage_number("lightIntensity", window.get_dc_light_intensity_available(), intensity_codes[window.get_dc_light_intensity_index().clamp(0, 3) as usize]);
            stage_number("lightPreset", window.get_dc_light_preset_available(), window.get_dc_light_preset_index() as u32);
            stage_number("lightDynamicMode", window.get_dc_light_dynamic_mode_available(), window.get_dc_light_dynamic_mode_index() as u32);
            stage_number("menuScrollVibeBehavior", window.get_dc_menu_vibe_available(), window.get_dc_menu_vibe_index() as u32);
            stage_number("mask", window.get_dc_notif_filter_available(), [0, 2, 15][window.get_dc_notif_filter_index().clamp(0, 2) as usize]);
            stage_number("notifWindowTimeout", window.get_dc_notif_timeout_available(), window.get_dc_notif_timeout_ms() as u32);
            stage_number("vibeIntensity", window.get_dc_vibe_intensity_available(), window.get_dc_vibe_intensity_index() as u32);
            stage_number("vibeScoreNotifications", window.get_dc_vibe_notifications_available(), [1, 2, 4, 8, 9, 10, 12][window.get_dc_vibe_notifications_index().clamp(0, 6) as usize]);
            stage_number("vibeScoreIncomingCalls", window.get_dc_vibe_calls_available(), [1, 3, 5, 8, 9, 10, 12][window.get_dc_vibe_calls_index().clamp(0, 6) as usize]);
            stage_number("vibeScoreAlarms", window.get_dc_vibe_alarms_available(), [3, 5, 8, 9, 10, 11, 12, 14][window.get_dc_vibe_alarms_index().clamp(0, 7) as usize]);
            stage_number("dndInterruptionsMask", window.get_dc_dnd_interruptions_available(), [0, 2][window.get_dc_dnd_interruptions_index().clamp(0, 1) as usize]);
            stage_number("dndShowNotifications", window.get_dc_dnd_show_notifications_available(), window.get_dc_dnd_show_notifications_index().clamp(0, 1) as u32);
            stage_number("timelineQuickViewBeforeTimeMin", window.get_dc_timeline_minutes_available(), window.get_dc_timeline_minutes() as u32);
            stage_number("motionSensitivity", window.get_dc_motion_sensitivity_available(), [10, 25, 40, 55, 70, 85, 100][window.get_dc_motion_sensitivity_index().clamp(0, 6) as usize]);
            stage_number("lightAmbientThreshold", window.get_dc_light_ambient_threshold_available(), window.get_dc_light_ambient_threshold() as u32);
            stage_number("dynBacklightMinThreshold", window.get_dc_dynamic_backlight_threshold_available(), window.get_dc_dynamic_backlight_threshold() as u32);
            let backlight_colors = [
                0xff0000, 0xff7f00, 0xffff00, 0x7fff00, 0x00ff00, 0x00ffff,
                0x0000ff, 0x7f00ff, 0xff00ff, 0xff66cc, 0xffbfa2, 0xffffff,
            ];
            if window.get_dc_light_color_available() {
                let original = original_preferences.get("lightColor").and_then(|field| match field.value.as_ref() {
                    Some(PreferenceValue::Unsigned(value)) | Some(PreferenceValue::Color(value)) => Some(*value), _ => None,
                });
                let selected = backlight_colors.get(window.get_dc_light_color_index() as usize).copied().or(original);
                if let (Some(selected), Some(original)) = (selected, original) {
                    if selected != original { preferences.insert("lightColor".into(), PreferenceValue::Color(selected)); }
                }
            }
            let quick_launch_values = [
                "off",
                "2220d805-cf9a-4e12-92b9-5ca778aff6bb",
                "d0f12e6c-97eb-2287-a2f5-115dfaa1d168",
                "d4f7be63-97e6-4952-b265-dd4bce11c155",
                "88c28c12-7f81-42db-aaa6-14ccef6f27e5",
                "daae3686-bff6-4ba5-921b-262f847bb6e8",
                "79c76b48-6111-4e80-8deb-3119eebef33e",
                "36d8c6ed-4c83-4fa1-a9e2-8f12dc941f8c",
            ];
            let mut stage_quick_launch = |key: &str, available: bool, index: i32| {
                if !available { return; }
                let original = original_preferences.get(key).and_then(|field| match field.value.as_ref() {
                    Some(PreferenceValue::Text(value)) => Some(value.as_str()), _ => None,
                });
                let value = quick_launch_values.get(index as usize).copied().or(original);
                if let (Some(value), Some(original)) = (value, original) {
                    if value != original { preferences.insert(key.to_owned(), PreferenceValue::Text(value.to_owned())); }
                }
            };
            stage_quick_launch("qlUp", window.get_dc_ql_up_available(), window.get_dc_ql_up_index());
            stage_quick_launch("qlDown", window.get_dc_ql_down_available(), window.get_dc_ql_down_index());
            stage_quick_launch("qlSelect", window.get_dc_ql_select_available(), window.get_dc_ql_select_index());
            stage_quick_launch("qlBack", window.get_dc_ql_back_available(), window.get_dc_ql_back_index());
            stage_quick_launch("qlComboBackUp", window.get_dc_ql_combo_back_up_available(), window.get_dc_ql_combo_back_up_index());
            stage_quick_launch("qlComboUpDown", window.get_dc_ql_combo_up_down_available(), window.get_dc_ql_combo_up_down_index());
            stage_quick_launch("qlSingleClickUp", window.get_dc_ql_tap_up_available(), window.get_dc_ql_tap_up_index());
            stage_quick_launch("qlSingleClickDown", window.get_dc_ql_tap_down_available(), window.get_dc_ql_tap_down_index());
            if window.get_dc_language_available() {
                let original = original_preferences.get("language").and_then(|field| match field.value.as_ref() {
                    Some(PreferenceValue::Unsigned(value)) => Some(*value), _ => None,
                });
                let selected = match window.get_dc_language_index() {
                    index @ 0..=8 => Some(index as u32 + 1),
                    9 => Some(0),
                    _ => original,
                };
                if let (Some(selected), Some(original)) = (selected, original) {
                    if selected != original { preferences.insert("language".into(), PreferenceValue::Unsigned(selected)); }
                }
            }
            if window.get_dc_text_size_available() {
                let code = window.get_dc_text_size_index() as u32;
                let unchanged = original_preferences.get("textStyle").and_then(|field| field.value.as_ref())
                    .is_some_and(|value| matches!(value, PreferenceValue::Unsigned(old) if *old == code));
                if !unchanged { preferences.insert("textStyle".into(), PreferenceValue::Unsigned(code)); }
            }
            let patch = current.health.value.as_ref().map(|original| {
                let original_hrm = original.hrm.value.as_ref();
                HealthConfigPatch {
                height_mm: (original.height_mm != window.get_dc_height_mm() as u16).then_some(window.get_dc_height_mm() as u16),
                weight_dag: (original.weight_dag != window.get_dc_weight_dag() as u16).then_some(window.get_dc_weight_dag() as u16),
                tracking_enabled: (original.tracking_enabled != window.get_dc_tracking_value()).then_some(window.get_dc_tracking_value()),
                activity_insights_enabled: (original.activity_insights_enabled != window.get_dc_activity_insights_value()).then_some(window.get_dc_activity_insights_value()),
                sleep_insights_enabled: (original.sleep_insights_enabled != window.get_dc_sleep_insights_value()).then_some(window.get_dc_sleep_insights_value()),
                age: (original.age != window.get_dc_age_value() as u8).then_some(window.get_dc_age_value() as u8),
                gender: (original.gender != window.get_dc_gender_index() as u8).then_some(window.get_dc_gender_index() as u8),
                distance_units: (window.get_dc_units_available() && original.distance_units.value != Some(staged_units)).then_some(staged_units),
                hrm_enabled: original_hrm.filter(|value| value.enabled != window.get_dc_hrm_enabled_value()).map(|_| window.get_dc_hrm_enabled_value()),
                hrm_measurement_interval: original_hrm.filter(|value| window.get_dc_hrm_interval_available() && value.measurement_interval != Some(staged_interval)).map(|_| staged_interval),
                hrm_during_activity: original_hrm.filter(|value| window.get_dc_hrm_during_activity_available() && value.during_activity != Some(window.get_dc_hrm_during_activity_value())).map(|_| window.get_dc_hrm_during_activity_value()),
                heart_rate_thresholds: None,
                }
            });
            window.set_dc_applying(true);
            window.set_dc_status_error(false);
            window.set_dc_status("Applying device settings…".into());
            let expected_revision = *revision.lock().unwrap();
            let weak2 = weak.clone();
            let revision2 = revision.clone();
            let baseline2 = baseline.clone();
            rt_handle.spawn(async move {
                let result = async {
                    let client = CobbleClient::new().await?;
                    client.update_device_config(DeviceConfigPatch {
                        expected_revision,
                        health: patch,
                        preferences,
                    }).await
                }.await;
                slint::invoke_from_event_loop(move || {
                    let Some(window) = weak2.upgrade() else { return };
                    window.set_dc_applying(false);
                    match result {
                        Ok(snapshot) => {
                            *revision2.lock().unwrap() = snapshot.revision;
                            *baseline2.lock().unwrap() = Some(snapshot.clone());
                            if snapshot.state == DeviceConfigState::Error {
                                window.set_dc_status_error(true);
                                window.set_dc_status(snapshot.error.as_ref().map_or_else(
                                    || "Apply failed; the watch state was refreshed.".to_string(),
                                    |error| format!("Apply failed: {}", error.message),
                                ).into());
                            } else {
                                let read_back = snapshot.capabilities.blob_db_version >= 1;
                                apply_device_config(&window, &snapshot);
                                window.set_dc_status(if read_back {
                                    "Device settings applied and read back from the watch."
                                } else {
                                    "Device settings accepted; this watch cannot confirm complete readback."
                                }.into());
                            }
                        }
                        Err(error) => {
                            window.set_dc_status_error(true);
                            window.set_dc_status(format!("Apply failed: {error}").into());
                        }
                    }
                }).ok();
            });
        });
    }

    {
        let weak = window.as_weak();
        let baseline = device_baseline.clone();
        window.on_discard_device_config(move || {
            if let (Some(window), Some(snapshot)) =
                (weak.upgrade(), baseline.lock().unwrap().clone())
            {
                apply_device_config(&window, &snapshot);
            }
        });
    }

    {
        let weak = window.as_weak();
        let revision = device_revision.clone();
        let baseline = device_baseline.clone();
        let rt_handle = rt.handle().clone();
        window.on_reset_device_config_defaults(move || {
            let Some(window) = weak.upgrade() else { return };
            if !window.get_watch_connected() || window.get_dc_applying() { return; }
            window.set_dc_applying(true);
            window.set_dc_status_error(false);
            window.set_dc_status("Resetting observed general settings…".into());
            let expected_revision = *revision.lock().unwrap();
            let weak2 = weak.clone();
            let revision2 = revision.clone();
            let baseline2 = baseline.clone();
            rt_handle.spawn(async move {
                let result = async {
                    let client = CobbleClient::new().await?;
                    client.reset_device_config_defaults(expected_revision).await
                }.await;
                slint::invoke_from_event_loop(move || {
                    let Some(window) = weak2.upgrade() else { return };
                    window.set_dc_applying(false);
                    match result {
                        Ok(snapshot) => {
                            *revision2.lock().unwrap() = snapshot.revision;
                            *baseline2.lock().unwrap() = Some(snapshot.clone());
                            apply_device_config(&window, &snapshot);
                            if snapshot.state == DeviceConfigState::Error {
                                window.set_dc_status_error(true);
                                window.set_dc_status(snapshot.error.as_ref().map_or_else(
                                    || "Reset failed; the watch state was refreshed.".to_string(),
                                    |error| format!("Reset failed: {}", error.message),
                                ).into());
                            } else {
                                window.set_dc_status("Observed general settings reset to defaults; health settings were unchanged.".into());
                            }
                        }
                        Err(error) => {
                            window.set_dc_status_error(true);
                            window.set_dc_status(format!("Reset failed: {error}").into());
                        }
                    }
                }).ok();
            });
        });
    }

    // ── Refresh ──────────────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone();
        let ow = offset_workout.clone();
        let brw = bar_range_w.clone();
        let ps = period_sleep.clone();
        let os = offset_sleep.clone();
        let ph = period_heart.clone();
        let oh = offset_heart.clone();
        window.on_refresh_data(move || {
            if let Ok(conn) = cobble_db::connect_readonly(&db2) {
                cobble_db::set_watch_offset(cobble_db::watch_tz_offset(&conn));
            }
            if let Some(w) = weak.upgrade() {
                reload_workout_chart(&w, &db2, pw.get(), ow.get());
                reload_workout_sessions(&w, &db2, pw.get(), ow.get(), brw.get());
                reload_sleep_chart(&w, &db2, ps.get(), os.get());
                reload_sleep_stats(&w, &db2, ps.get(), os.get());
                reload_heart_stats(&w, &db2, ph.get(), oh.get());
            }
        });
    }

    // ── Workout: period changed ───────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone();
        let ow = offset_workout.clone();
        let brw = bar_range_w.clone();
        window.on_workout_period_changed(move |p| {
            pw.set(p);
            ow.set(0);
            brw.set((-1, -1));
            if let Some(w) = weak.upgrade() {
                update_workout_nav(&w, p, 0);
                reload_workout_chart(&w, &db2, p, 0);
                reload_workout_sessions(&w, &db2, p, 0, (-1, -1));
            }
        });
    }

    // ── Workout: go back ────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone();
        let ow = offset_workout.clone();
        let brw = bar_range_w.clone();
        window.on_workout_go_back(move || {
            let new_off = ow.get() + 1;
            ow.set(new_off);
            brw.set((-1, -1));
            let p = pw.get();
            if let Some(w) = weak.upgrade() {
                update_workout_nav(&w, p, new_off);
                reload_workout_chart(&w, &db2, p, new_off);
                reload_workout_sessions(&w, &db2, p, new_off, (-1, -1));
            }
        });
    }

    // ── Workout: go forward ──────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone();
        let ow = offset_workout.clone();
        let brw = bar_range_w.clone();
        window.on_workout_go_forward(move || {
            let new_off = (ow.get() - 1).max(0);
            ow.set(new_off);
            brw.set((-1, -1));
            let p = pw.get();
            if let Some(w) = weak.upgrade() {
                update_workout_nav(&w, p, new_off);
                reload_workout_chart(&w, &db2, p, new_off);
                reload_workout_sessions(&w, &db2, p, new_off, (-1, -1));
            }
        });
    }

    // ── Workout: bar tapped ──────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let pw = period_workout.clone();
        let ow = offset_workout.clone();
        let brw = bar_range_w.clone();
        window.on_bar_tapped(move |s, e| {
            let range = if s < 0 {
                (-1i64, -1i64)
            } else {
                (s as i64, e as i64)
            };
            brw.set(range);
            if let Some(w) = weak.upgrade() {
                reload_workout_sessions(&w, &db2, pw.get(), ow.get(), range);
                if s < 0 {
                    reload_workout_avg_label(&w, &db2, pw.get(), ow.get());
                } else if pw.get() > 0 {
                    let Some(range) = bar_date_range(s as i64, e as i64) else {
                        warn!("cannot determine date range for workout bar {s}–{e}");
                        return;
                    };
                    reload_workout_avg_label_for_range(&w, &db2, range);
                }
            }
        });
    }

    // ── Sleep: period changed ────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ps = period_sleep.clone();
        let os = offset_sleep.clone();
        window.on_sleep_period_changed(move |p| {
            ps.set(p);
            os.set(0);
            if let Some(w) = weak.upgrade() {
                update_sleep_nav(&w, p, 0);
                reload_sleep_chart(&w, &db2, p, 0);
                reload_sleep_stats(&w, &db2, p, 0);
            }
        });
    }

    // ── Sleep: bar tapped ────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ps = period_sleep.clone();
        let os = offset_sleep.clone();
        window.on_sleep_bar_tapped(move |s, e| {
            if let Some(w) = weak.upgrade() {
                if s < 0 {
                    // Tapping the selected bar again clears the selection and
                    // restores stats for the whole period.
                    reload_sleep_avg_label(&w, &db2, ps.get(), os.get());
                    reload_sleep_stats(&w, &db2, ps.get(), os.get());
                } else {
                    let Some(range) = bar_date_range(s as i64, e as i64) else {
                        warn!("cannot determine date range for sleep bar {s}–{e}");
                        return;
                    };
                    if ps.get() > 0 {
                        reload_sleep_avg_label_for_range(&w, &db2, range);
                    }
                    reload_sleep_stats_for_range(&w, &db2, range);
                }
            }
        });
    }

    // ── Sleep: go back ───────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ps = period_sleep.clone();
        let os = offset_sleep.clone();
        window.on_sleep_go_back(move || {
            let new_off = os.get() + 1;
            os.set(new_off);
            let p = ps.get();
            if let Some(w) = weak.upgrade() {
                update_sleep_nav(&w, p, new_off);
                reload_sleep_chart(&w, &db2, p, new_off);
                reload_sleep_stats(&w, &db2, p, new_off);
            }
        });
    }

    // ── Sleep: go forward ────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ps = period_sleep.clone();
        let os = offset_sleep.clone();
        window.on_sleep_go_forward(move || {
            let new_off = (os.get() - 1).max(0);
            os.set(new_off);
            let p = ps.get();
            if let Some(w) = weak.upgrade() {
                update_sleep_nav(&w, p, new_off);
                reload_sleep_chart(&w, &db2, p, new_off);
                reload_sleep_stats(&w, &db2, p, new_off);
            }
        });
    }

    // ── Heart: period changed ───────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ph = period_heart.clone();
        let oh = offset_heart.clone();
        window.on_heart_period_changed(move |p| {
            ph.set(p);
            oh.set(0);
            if let Some(w) = weak.upgrade() {
                update_heart_nav(&w, p, 0);
                reload_heart_stats(&w, &db2, p, 0);
            }
        });
    }

    // ── Heart: go back ───────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ph = period_heart.clone();
        let oh = offset_heart.clone();
        window.on_heart_go_back(move || {
            let new_off = oh.get() + 1;
            oh.set(new_off);
            let p = ph.get();
            if let Some(w) = weak.upgrade() {
                update_heart_nav(&w, p, new_off);
                reload_heart_stats(&w, &db2, p, new_off);
            }
        });
    }

    // ── Heart: go forward ────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2 = effective_db_path.clone();
        let ph = period_heart.clone();
        let oh = offset_heart.clone();
        window.on_heart_go_forward(move || {
            let new_off = (oh.get() - 1).max(0);
            oh.set(new_off);
            let p = ph.get();
            if let Some(w) = weak.upgrade() {
                update_heart_nav(&w, p, new_off);
                reload_heart_stats(&w, &db2, p, new_off);
            }
        });
    }

    // ── Save config ──────────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let rt_handle = rt.handle().clone();
        let config_baseline = config_baseline.clone();
        window.on_save_config(move || {
            let Some(w) = weak.upgrade() else { return };
            let Some(baseline) = config_baseline.lock().unwrap().clone() else {
                w.set_save_status("Error: daemon configuration is unavailable".into());
                return;
            };
            let edited_database = w.get_cfg_db().to_string();
            let edited_api_key = w.get_cfg_intervals_api_key().to_string();
            let patch = DaemonConfigPatch {
                expected_revision: baseline.revision,
                address: (w.get_cfg_address().as_str() != baseline.config.address)
                    .then(|| w.get_cfg_address().to_string()),
                adapter: (w.get_cfg_adapter().as_str() != baseline.config.adapter)
                    .then(|| w.get_cfg_adapter().to_string()),
                verbose: (w.get_cfg_verbose() != baseline.config.verbose)
                    .then(|| w.get_cfg_verbose()),
                database_path: (edited_database != baseline.config.database_path.clone().unwrap_or_default())
                    .then(|| if edited_database.is_empty() { None } else { Some(edited_database) }),
                intervals_icu: Some(IntervalsIcuPatch {
                    enabled: (w.get_cfg_intervals_enabled() != baseline.config.intervals_icu.enabled)
                        .then(|| w.get_cfg_intervals_enabled()),
                    athlete_id: (w.get_cfg_intervals_athlete_id().as_str()
                        != baseline.config.intervals_icu.athlete_id)
                        .then(|| w.get_cfg_intervals_athlete_id().to_string()),
                    api_key: (!edited_api_key.is_empty())
                        .then_some(SecretPatch::Replace(edited_api_key)),
                }),
            };
            w.set_save_status("Saving…".into());
            let weak2 = weak.clone();
            let baseline2 = config_baseline.clone();
            rt_handle.spawn(async move {
                let result = async {
                    let client = CobbleClient::new().await?;
                    client.update_daemon_config(patch).await
                }.await;
                slint::invoke_from_event_loop(move || {
                    let Some(window) = weak2.upgrade() else { return };
                    match result {
                        Ok(update) => {
                            apply_daemon_config(&window, &update.snapshot);
                            window.set_cfg_intervals_api_key("".into());
                            window.set_cfg_intervals_applied_api_key("".into());
                            *baseline2.lock().unwrap() = Some(update.snapshot);
                            let database_restart = update.fields.values().any(|value| {
                                *value == ApplyDisposition::DaemonAndGuiRestartRequired
                            });
                            let daemon_restart = update.fields.values().any(|value| {
                                *value == ApplyDisposition::DaemonRestartRequired
                            });
                            let reconnecting = update.fields.values().any(|value| {
                                *value == ApplyDisposition::Reconnecting
                            });
                            let status = if database_restart && daemon_restart {
                                "Saved. Restart cobbled and Cobble to use the new database and logging setting."
                            } else if database_restart {
                                "Saved. Restart cobbled and Cobble to use the new database."
                            } else if daemon_restart {
                                "Saved. Restart cobbled to apply the logging setting."
                            } else if reconnecting {
                                "Saved. Reconnecting with the new watch connection settings."
                            } else {
                                "Saved and applied by cobbled."
                            };
                            window.set_save_status_error(false);
                            window.set_save_status(status.into());
                        }
                        Err(error) => {
                            window.set_save_status_error(true);
                            window.set_save_status(format!("Error: {error}").into());
                        }
                    }
                }).ok();
            });
        });
    }

    // ── Wellness sync control ───────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let rt_handle = rt.handle().clone();
        window.on_sync_wellness(move || {
            let Some(w) = weak.upgrade() else { return };
            w.set_cfg_intervals_status("Sync in progress…".into());
            w.set_cfg_intervals_status_error(false);
            let weak2 = weak.clone();
            rt_handle.spawn(async move {
                let result = match CobbleClient::new().await {
                    Err(error) => Err(error.to_string()),
                    Ok(client) => match client.sync_wellness().await {
                        Err(error) => Err(error.to_string()),
                        Ok(()) => wait_for_wellness_sync(&client).await,
                    },
                };
                slint::invoke_from_event_loop(move || {
                    if let Some(w) = weak2.upgrade() {
                        match result {
                            Ok(status) => apply_wellness_status(&w, &status),
                            Err(error) => {
                                w.set_cfg_intervals_status(format!("Error: {error}").into());
                                w.set_cfg_intervals_status_error(true);
                            }
                        }
                    }
                })
                .ok();
            });
        });
    }

    // ── Scan for Pebble watches ─────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let rt_handle = rt.handle().clone();
        window.on_scan_watches(move || {
            let weak_for_scan = weak.clone();
            let weak_for_ui = weak.clone();
            let rt = rt_handle.clone();
            // Show scanning state immediately on the UI thread.
            slint::invoke_from_event_loop(move || {
                if let Some(w) = weak_for_ui.upgrade() {
                    w.set_scan_in_progress(true);
                }
            })
            .ok();
            rt.spawn(async move {
                let results = match CobbleClient::new().await {
                    Err(e) => {
                        warn!("Scan: {e}");
                        Vec::new()
                    }
                    Ok(client) => match client.scan(5.0).await {
                        Err(e) => {
                            warn!("Scan: {e}");
                            Vec::new()
                        }
                        Ok(results) => results,
                    },
                };
                slint::invoke_from_event_loop(move || {
                    if let Some(w) = weak_for_scan.upgrade() {
                        let model: VecModel<WatchDevice> = VecModel::default();
                        for (addr, name) in results {
                            model.push(WatchDevice {
                                address: addr.into(),
                                name: name.into(),
                            });
                        }
                        w.set_scan_results(ModelRc::new(model));
                        w.set_scan_in_progress(false);
                    }
                })
                .ok();
            });
        });
    }

    // ── Device: manual refresh ───────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let rt_handle = rt.handle().clone();
        window.on_refresh_device(move || {
            let weak2 = weak.clone();
            rt_handle.spawn(async move {
                let Ok(client) = CobbleClient::new().await else {
                    return;
                };
                if !client.connected().await {
                    return;
                }
                let info = client.get_watch_info().await.ok();
                let battery = client.battery_level().await.unwrap_or(-1);
                slint::invoke_from_event_loop(move || {
                    if let Some(w) = weak2.upgrade() {
                        w.set_battery_level(battery as i32);
                        if let Some(info) = info {
                            apply_status(&w, StatusEvent::WatchInfo(info));
                        }
                    }
                })
                .ok();
            });
        });
    }

    // ── Device actions (Settings ▸ Device Actions) ───────────────────────────
    {
        let rt_handle = rt.handle().clone();
        let w = window.as_weak();
        window.on_reboot_watch({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || {
                spawn_action(
                    &rt,
                    w.clone(),
                    "Rebooting watch…",
                    "Reboot command accepted.",
                    |c| async move { c.reboot_watch().await },
                )
            }
        });
        window.on_reset_into_recovery({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || {
                spawn_action(
                    &rt,
                    w.clone(),
                    "Rebooting into recovery…",
                    "Recovery command accepted.",
                    |c| async move { c.reset_into_recovery().await },
                )
            }
        });
        window.on_create_core_dump({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || {
                spawn_action(
                    &rt,
                    w.clone(),
                    "Requesting core dump…",
                    "Core dump request accepted.",
                    |c| async move { c.create_core_dump().await },
                )
            }
        });
        window.on_forget_watch({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || {
                spawn_action(
                    &rt,
                    w.clone(),
                    "Removing Bluetooth bond…",
                    "Bluetooth bond removed.",
                    |c| async move { c.forget().await },
                )
            }
        });
        window.on_factory_reset({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || {
                spawn_action(
                    &rt,
                    w.clone(),
                    "Factory resetting watch…",
                    "Factory reset command accepted.",
                    |c| async move { c.factory_reset(true).await },
                )
            }
        });
    }

    window.run()?;
    drop(rt);
    Ok(())
}

/// Run one device action at a time and report only completed outcomes.
fn spawn_action<F, Fut>(
    rt: &tokio::runtime::Handle,
    weak: slint::Weak<AppWindow>,
    pending: &'static str,
    success: &'static str,
    f: F,
) where
    F: FnOnce(CobbleClient) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = cobble_client::Result<()>> + Send + 'static,
{
    let Some(window) = weak.upgrade() else { return };
    if window.get_action_busy() {
        return;
    }
    window.set_action_busy(true);
    window.set_action_error(false);
    window.set_action_status(pending.into());
    drop(window);
    rt.spawn(async move {
        let result = async { f(CobbleClient::new().await?).await }.await;
        slint::invoke_from_event_loop(move || {
            if let Some(w) = weak.upgrade() {
                w.set_action_busy(false);
                match result {
                    Ok(()) => {
                        w.set_action_error(false);
                        w.set_action_status(success.into());
                    }
                    Err(error) => {
                        w.set_action_error(true);
                        w.set_action_status(action_error_message(&error.to_string()).into());
                    }
                }
            }
        })
        .ok();
    });
}

fn action_error_message(error: &str) -> String {
    let lower = error.to_ascii_lowercase();
    if lower.contains("not connected") || lower.contains("disconnected") {
        "The watch disconnected before the action completed.".into()
    } else if lower.contains("timeout") || lower.contains("timed out") {
        "The watch did not respond in time. Please reconnect and try again.".into()
    } else if lower.contains("rejected") || lower.contains("nack") {
        "The watch rejected this action.".into()
    } else if lower.contains("serviceunknown") || lower.contains("name has no owner") {
        "The Cobble daemon is not running.".into()
    } else {
        "The action could not be completed. Check the daemon log for details.".into()
    }
}

fn refresh_wellness_status(weak: slint::Weak<AppWindow>, rt: &tokio::runtime::Handle) {
    let rt = rt.clone();
    rt.spawn(async move {
        loop {
            let result = match CobbleClient::new().await {
                Err(error) => Err(error.to_string()),
                Ok(client) => client
                    .get_wellness_sync_status()
                    .await
                    .map_err(|error| error.to_string()),
            };
            let delay = match &result {
                Ok(status) if wellness_status_bool(status, "running") => Duration::from_secs(2),
                Ok(_) => Duration::from_secs(60),
                Err(_) => Duration::from_secs(10),
            };
            let weak2 = weak.clone();
            slint::invoke_from_event_loop(move || {
                if let Some(w) = weak2.upgrade() {
                    match result {
                        Ok(status) => apply_wellness_status(&w, &status),
                        Err(error) => {
                            w.set_cfg_intervals_status(format!("Error: {error}").into());
                            w.set_cfg_intervals_status_error(true);
                        }
                    }
                }
            })
            .ok();
            tokio::time::sleep(delay).await;
        }
    });
}

async fn wait_for_wellness_sync(client: &CobbleClient) -> Result<VarDict, String> {
    loop {
        let status = client
            .get_wellness_sync_status()
            .await
            .map_err(|error| error.to_string())?;
        if !wellness_status_bool(&status, "running") {
            return Ok(status);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn apply_wellness_status(w: &AppWindow, status: &VarDict) {
    let enabled = wellness_status_bool(status, "enabled");
    let valid = wellness_status_bool(status, "valid");
    let running = wellness_status_bool(status, "running");
    let last_error = wellness_status_string(status, "last_error");
    let message = if !enabled {
        "Wellness sync disabled.".to_string()
    } else if !valid {
        "Invalid Intervals.icu configuration.".to_string()
    } else if running {
        "Sync in progress…".to_string()
    } else {
        if !last_error.is_empty() {
            format!("Last error: {last_error}")
        } else {
            let last_success = wellness_status_string(status, "last_success");
            if last_success.is_empty() {
                "No successful sync yet.".to_string()
            } else {
                let pending = wellness_status_u64(status, "pending_dates");
                if pending == 0 {
                    format!("Last sync: {last_success}")
                } else {
                    format!("Last sync: {last_success}; {pending} pending")
                }
            }
        }
    };
    w.set_cfg_intervals_status(message.into());
    w.set_cfg_intervals_status_error(enabled && (!valid || (!running && !last_error.is_empty())));
}

fn wellness_status_string(status: &VarDict, key: &str) -> String {
    status
        .get(key)
        .and_then(|value| <&str>::try_from(value).ok())
        .unwrap_or_default()
        .to_string()
}

fn wellness_status_bool(status: &VarDict, key: &str) -> bool {
    status
        .get(key)
        .and_then(|value| bool::try_from(value).ok())
        .unwrap_or(false)
}

fn wellness_status_u64(status: &VarDict, key: &str) -> u64 {
    status
        .get(key)
        .and_then(|value| u64::try_from(value).ok())
        .unwrap_or(0)
}

/// Apply a status event to the window's properties (runs on the UI thread).
fn apply_status(w: &AppWindow, ev: StatusEvent) {
    match ev {
        StatusEvent::DaemonRunning(r) => w.set_daemon_running(r),
        StatusEvent::Connected(c) => {
            w.set_watch_connected(c);
            if !c {
                w.set_battery_level(-1);
                clear_watch_info(w);
                clear_device_config(w);
            }
        }
        StatusEvent::Battery(b) => w.set_battery_level(b as i32),
        StatusEvent::WatchInfo(info) => {
            w.set_wi_model(info.model.into());
            w.set_wi_firmware(info.firmware_version.into());
            w.set_wi_color(info.color.into());
            w.set_wi_board(info.board.into());
            w.set_wi_serial(info.serial.into());
            w.set_wi_bt(info.bt_address.into());
            w.set_wi_language(info.language.into());
        }
        StatusEvent::DaemonConfigChanged(_) => {}
        StatusEvent::DeviceConfigChanged { .. } => {}
    }
}

fn apply_daemon_config(w: &AppWindow, snapshot: &DaemonConfigSnapshot) {
    let config = &snapshot.config;
    w.set_cfg_address(config.address.clone().into());
    w.set_cfg_adapter(config.adapter.clone().into());
    w.set_cfg_verbose(config.verbose);
    w.set_cfg_db(config.database_path.clone().unwrap_or_default().into());
    w.set_cfg_intervals_enabled(config.intervals_icu.enabled);
    w.set_cfg_intervals_athlete_id(config.intervals_icu.athlete_id.clone().into());
    w.set_cfg_intervals_applied_enabled(config.intervals_icu.enabled);
    w.set_cfg_intervals_applied_athlete_id(config.intervals_icu.athlete_id.clone().into());
    if let Some(error) = &snapshot.error {
        w.set_save_status_error(true);
        w.set_save_status(format!("Config file error: {}", error.message).into());
    } else if snapshot.active_database_path != snapshot.resolved_database_path
        && snapshot.active_verbose != snapshot.config.verbose
    {
        w.set_save_status_error(false);
        w.set_save_status(
            "Configuration changed. Restart cobbled and Cobble to use the new database and logging setting."
                .into(),
        );
    } else if snapshot.active_database_path != snapshot.resolved_database_path {
        w.set_save_status_error(false);
        w.set_save_status(
            "Configuration changed. Restart cobbled and Cobble to use the new database.".into(),
        );
    } else if snapshot.active_verbose != snapshot.config.verbose {
        w.set_save_status_error(false);
        w.set_save_status("Configuration changed. Restart cobbled to apply logging.".into());
    }
}

fn apply_device_config(w: &AppWindow, snapshot: &DeviceConfigSnapshot) {
    w.set_confirm_device_defaults(false);
    let state = match snapshot.state {
        DeviceConfigState::Disconnected => "disconnected",
        DeviceConfigState::Loading => "loading",
        DeviceConfigState::Ready => "ready",
        DeviceConfigState::Partial => "partial",
        DeviceConfigState::Error => "error",
        DeviceConfigState::Unsupported => "unsupported",
    };
    w.set_dc_state(state.into());
    w.set_dc_loading(snapshot.state == DeviceConfigState::Loading);
    let status = match snapshot.state {
        DeviceConfigState::Disconnected => {
            "Watch disconnected; no device settings are available.".to_string()
        }
        DeviceConfigState::Loading => "Reading settings from the watch…".to_string(),
        DeviceConfigState::Ready => {
            "Settings read successfully from the connected watch.".to_string()
        }
        DeviceConfigState::Partial => snapshot.error.as_ref().map_or_else(
            || "Only part of the watch configuration was received.".to_string(),
            |error| format!("Partial settings: {}", error.message),
        ),
        DeviceConfigState::Error => snapshot.error.as_ref().map_or_else(
            || "The watch configuration could not be read.".to_string(),
            |error| format!("Read failed: {}", error.message),
        ),
        DeviceConfigState::Unsupported => snapshot.error.as_ref().map_or_else(
            || "This watch cannot provide a complete settings snapshot.".to_string(),
            |error| error.message.clone(),
        ),
    };
    w.set_dc_status_error(matches!(snapshot.state, DeviceConfigState::Error));
    w.set_dc_status(status.into());

    if let Some(watch) = &snapshot.watch {
        w.set_dc_watch_id(watch.watch_id.clone().into());
        w.set_dc_platform(watch.platform.clone().unwrap_or_default().into());
        w.set_dc_firmware(watch.firmware.clone().unwrap_or_default().into());
    } else {
        w.set_dc_watch_id("".into());
        w.set_dc_platform("".into());
        w.set_dc_firmware("".into());
    }
    w.set_dc_last_read(
        snapshot
            .last_read_at_ms
            .map(|value| value.to_string())
            .unwrap_or_default()
            .into(),
    );

    if let Some(health) = &snapshot.health.value {
        w.set_dc_health_available(true);
        w.set_dc_height_mm(health.height_mm as i32);
        w.set_dc_weight_dag(health.weight_dag as i32);
        w.set_dc_age_value(health.age as i32);
        w.set_dc_gender_index(health.gender.min(2) as i32);
        w.set_dc_tracking_value(health.tracking_enabled);
        w.set_dc_activity_insights_value(health.activity_insights_enabled);
        w.set_dc_sleep_insights_value(health.sleep_insights_enabled);
        w.set_dc_units_available(
            health.distance_units.availability == FieldAvailability::Available,
        );
        w.set_dc_units_index(i32::from(matches!(
            health.distance_units.value,
            Some(DistanceUnits::Imperial)
        )));
        let hrm = health.hrm.value.as_ref();
        w.set_dc_hrm_available(hrm.is_some());
        w.set_dc_hrm_enabled_value(hrm.is_some_and(|value| value.enabled));
        w.set_dc_hrm_interval_available(hrm.and_then(|value| value.measurement_interval).is_some());
        w.set_dc_hrm_interval_index(hrm.and_then(|value| value.measurement_interval).map_or(
            0,
            |value| match value {
                HrmMeasurementInterval::TenMinutes => 0,
                HrmMeasurementInterval::ThirtyMinutes => 1,
                HrmMeasurementInterval::OneHour => 2,
                HrmMeasurementInterval::Off => 3,
                HrmMeasurementInterval::Unknown(_) => 0,
            },
        ));
        w.set_dc_hrm_during_activity_available(
            hrm.and_then(|value| value.during_activity).is_some(),
        );
        w.set_dc_hrm_during_activity_value(
            hrm.and_then(|value| value.during_activity).unwrap_or(false),
        );
        w.set_dc_height(format!("{} mm", health.height_mm).into());
        w.set_dc_weight(format!("{} dag", health.weight_dag).into());
        w.set_dc_age(health.age.to_string().into());
        w.set_dc_sex(
            match health.gender {
                0 => "Female",
                1 => "Male",
                2 => "Other",
                _ => "Unknown",
            }
            .into(),
        );
        w.set_dc_tracking(
            if health.tracking_enabled {
                "Enabled"
            } else {
                "Disabled"
            }
            .into(),
        );
        let insights = match (
            health.activity_insights_enabled,
            health.sleep_insights_enabled,
        ) {
            (true, true) => "Activity and sleep",
            (true, false) => "Activity only",
            (false, true) => "Sleep only",
            (false, false) => "Disabled",
        };
        w.set_dc_insights(insights.into());
        w.set_dc_units(
            match health.distance_units.value {
                Some(DistanceUnits::Metric) => "Metric",
                Some(DistanceUnits::Imperial) => "Imperial",
                Some(DistanceUnits::Unknown(_)) => "Unknown",
                None => availability_label(health.distance_units.availability),
            }
            .into(),
        );
        w.set_dc_hrm(
            health
                .hrm
                .value
                .as_ref()
                .map_or_else(
                    || availability_label(health.hrm.availability),
                    |hrm| if hrm.enabled { "Enabled" } else { "Disabled" },
                )
                .into(),
        );
        update_health_display(w);
    } else {
        w.set_dc_health_available(false);
        w.set_dc_units_available(false);
        w.set_dc_hrm_available(false);
        w.set_dc_hrm_interval_available(false);
        w.set_dc_hrm_during_activity_available(false);
        w.set_dc_height("".into());
        w.set_dc_weight("".into());
        w.set_dc_age("".into());
        w.set_dc_sex("".into());
        w.set_dc_tracking("".into());
        w.set_dc_insights("".into());
        w.set_dc_units("".into());
        w.set_dc_hrm("".into());
    }
    w.set_dc_dirty(false);
    let pref_bool = |key: &str| {
        snapshot
            .preferences
            .get(key)
            .and_then(|field| match field.value {
                Some(PreferenceValue::Bool(value)) => Some(value),
                _ => None,
            })
    };
    let clock = pref_bool("clock24h");
    w.set_dc_clock_24h_available(clock.is_some());
    w.set_dc_clock_24h_value(clock.unwrap_or(false));
    let timezone_manual = pref_bool("timezoneSource");
    w.set_dc_timezone_manual_available(timezone_manual.is_some());
    w.set_dc_timezone_manual_value(timezone_manual.unwrap_or(false));
    let stationary = pref_bool("stationaryMode");
    w.set_dc_stationary_mode_available(stationary.is_some());
    w.set_dc_stationary_mode_value(stationary.unwrap_or(false));
    let left = pref_bool("displayOrientationLeftHanded");
    w.set_dc_left_handed_available(left.is_some());
    w.set_dc_left_handed_value(left.unwrap_or(false));
    let text_size = snapshot
        .preferences
        .get("textStyle")
        .and_then(|field| match field.value {
            Some(PreferenceValue::Unsigned(value @ 0..=2)) => Some(value as i32),
            _ => None,
        });
    w.set_dc_text_size_available(text_size.is_some());
    w.set_dc_text_size_index(text_size.unwrap_or(1));
    let set_bool = |key: &str, available: fn(&AppWindow, bool), value: fn(&AppWindow, bool)| {
        let current = pref_bool(key);
        available(w, current.is_some());
        value(w, current.unwrap_or(false));
    };
    set_bool(
        "lightEnabled",
        AppWindow::set_dc_light_enabled_available,
        AppWindow::set_dc_light_enabled_value,
    );
    set_bool(
        "lightAmbientSensorEnabled",
        AppWindow::set_dc_light_ambient_available,
        AppWindow::set_dc_light_ambient_value,
    );
    set_bool(
        "lightMotion",
        AppWindow::set_dc_light_motion_available,
        AppWindow::set_dc_light_motion_value,
    );
    set_bool(
        "lightDynamicIntensity",
        AppWindow::set_dc_light_dynamic_legacy_available,
        AppWindow::set_dc_light_dynamic_legacy_value,
    );
    set_bool(
        "menuScrollWrapAround",
        AppWindow::set_dc_menu_wrap_available,
        AppWindow::set_dc_menu_wrap_value,
    );
    set_bool(
        "notifDesignStyle",
        AppWindow::set_dc_notif_design_available,
        AppWindow::set_dc_notif_design_value,
    );
    set_bool(
        "notifVibeDelay",
        AppWindow::set_dc_notif_delay_available,
        AppWindow::set_dc_notif_delay_value,
    );
    set_bool(
        "notifBacklight",
        AppWindow::set_dc_notif_backlight_available,
        AppWindow::set_dc_notif_backlight_value,
    );
    set_bool(
        "dndManuallyEnabled",
        AppWindow::set_dc_dnd_manual_available,
        AppWindow::set_dc_dnd_manual_value,
    );
    set_bool(
        "dndSmartEnabled",
        AppWindow::set_dc_dnd_smart_available,
        AppWindow::set_dc_dnd_smart_value,
    );
    set_bool(
        "dndMotionBacklight",
        AppWindow::set_dc_dnd_motion_backlight_available,
        AppWindow::set_dc_dnd_motion_backlight_value,
    );
    set_bool(
        "dndAutoDismiss",
        AppWindow::set_dc_dnd_auto_dismiss_available,
        AppWindow::set_dc_dnd_auto_dismiss_value,
    );
    set_bool(
        "timelineQuickViewEnabled",
        AppWindow::set_dc_timeline_quick_view_available,
        AppWindow::set_dc_timeline_quick_view_value,
    );
    set_bool(
        "musicShowVolumeControls",
        AppWindow::set_dc_music_volume_available,
        AppWindow::set_dc_music_volume_value,
    );
    set_bool(
        "musicShowProgressBar",
        AppWindow::set_dc_music_progress_available,
        AppWindow::set_dc_music_progress_value,
    );
    let pref_number = |key: &str| {
        snapshot
            .preferences
            .get(key)
            .and_then(|field| match field.value {
                Some(PreferenceValue::Unsigned(value)) => Some(value),
                _ => None,
            })
    };
    let timeout = pref_number("lightTimeoutMs").filter(|value| (1000..=10000).contains(value));
    w.set_dc_light_timeout_available(timeout.is_some());
    w.set_dc_light_timeout_ms(timeout.unwrap_or(3000) as i32);
    let touch = pref_number("lightTouch").filter(|value| *value <= 2);
    w.set_dc_light_touch_available(touch.is_some());
    w.set_dc_light_touch_index(touch.unwrap_or(0) as i32);
    let intensity = pref_number("lightIntensity")
        .and_then(|value| [10, 25, 50, 100].iter().position(|code| *code == value));
    w.set_dc_light_intensity_available(intensity.is_some());
    w.set_dc_light_intensity_index(intensity.unwrap_or(1) as i32);
    let preset = pref_number("lightPreset").filter(|value| *value <= 3);
    w.set_dc_light_preset_available(preset.is_some());
    w.set_dc_light_preset_index(preset.unwrap_or(1) as i32);
    let dynamic = pref_number("lightDynamicMode").filter(|value| *value <= 3);
    w.set_dc_light_dynamic_mode_available(dynamic.is_some());
    w.set_dc_light_dynamic_mode_index(dynamic.unwrap_or(2) as i32);
    let menu_vibe = pref_number("menuScrollVibeBehavior").filter(|value| *value <= 2);
    w.set_dc_menu_vibe_available(menu_vibe.is_some());
    w.set_dc_menu_vibe_index(menu_vibe.unwrap_or(0) as i32);
    let filter =
        pref_number("mask").and_then(|value| [0, 2, 15].iter().position(|code| *code == value));
    w.set_dc_notif_filter_available(filter.is_some());
    w.set_dc_notif_filter_index(filter.unwrap_or(2) as i32);
    let notif_timeout = pref_number("notifWindowTimeout").filter(|value| *value <= 600000);
    w.set_dc_notif_timeout_available(notif_timeout.is_some());
    w.set_dc_notif_timeout_ms(notif_timeout.unwrap_or(180000) as i32);
    let vibe_intensity = pref_number("vibeIntensity").filter(|value| *value <= 2);
    w.set_dc_vibe_intensity_available(vibe_intensity.is_some());
    w.set_dc_vibe_intensity_index(vibe_intensity.unwrap_or(2) as i32);
    let option_index = |key: &str, codes: &[u32]| {
        pref_number(key).and_then(|value| codes.iter().position(|code| *code == value))
    };
    let vibe_notifications = option_index("vibeScoreNotifications", &[1, 2, 4, 8, 9, 10, 12]);
    w.set_dc_vibe_notifications_available(vibe_notifications.is_some());
    w.set_dc_vibe_notifications_index(vibe_notifications.unwrap_or(4) as i32);
    let vibe_calls = option_index("vibeScoreIncomingCalls", &[1, 3, 5, 8, 9, 10, 12]);
    w.set_dc_vibe_calls_available(vibe_calls.is_some());
    w.set_dc_vibe_calls_index(vibe_calls.unwrap_or(3) as i32);
    let vibe_alarms = option_index("vibeScoreAlarms", &[3, 5, 8, 9, 10, 11, 12, 14]);
    w.set_dc_vibe_alarms_available(vibe_alarms.is_some());
    w.set_dc_vibe_alarms_index(vibe_alarms.unwrap_or(5) as i32);
    let dnd_interruptions = option_index("dndInterruptionsMask", &[0, 2]);
    w.set_dc_dnd_interruptions_available(dnd_interruptions.is_some());
    w.set_dc_dnd_interruptions_index(dnd_interruptions.unwrap_or(0) as i32);
    let dnd_show = pref_number("dndShowNotifications").filter(|value| *value <= 1);
    w.set_dc_dnd_show_notifications_available(dnd_show.is_some());
    w.set_dc_dnd_show_notifications_index(dnd_show.unwrap_or(1) as i32);
    let timeline_minutes =
        pref_number("timelineQuickViewBeforeTimeMin").filter(|value| *value <= 30);
    w.set_dc_timeline_minutes_available(timeline_minutes.is_some());
    w.set_dc_timeline_minutes(timeline_minutes.unwrap_or(10) as i32);
    let backlight_colors = [
        0xff0000, 0xff7f00, 0xffff00, 0x7fff00, 0x00ff00, 0x00ffff, 0x0000ff, 0x7f00ff, 0xff00ff,
        0xff66cc, 0xffbfa2, 0xffffff,
    ];
    let light_color = pref_number("lightColor");
    w.set_dc_light_color_available(light_color.is_some());
    w.set_dc_light_color_index(
        light_color
            .and_then(|value| backlight_colors.iter().position(|known| *known == value))
            .unwrap_or(12) as i32,
    );
    let motion_sensitivity = pref_number("motionSensitivity").and_then(|value| {
        [10, 25, 40, 55, 70, 85, 100]
            .iter()
            .position(|known| *known == value)
    });
    w.set_dc_motion_sensitivity_available(motion_sensitivity.is_some());
    w.set_dc_motion_sensitivity_index(motion_sensitivity.unwrap_or(3) as i32);
    let ambient_threshold =
        pref_number("lightAmbientThreshold").filter(|value| (1..=4096).contains(value));
    w.set_dc_light_ambient_threshold_available(ambient_threshold.is_some());
    w.set_dc_light_ambient_threshold(ambient_threshold.unwrap_or(150) as i32);
    let dynamic_threshold = pref_number("dynBacklightMinThreshold").filter(|value| *value <= 4096);
    w.set_dc_dynamic_backlight_threshold_available(dynamic_threshold.is_some());
    w.set_dc_dynamic_backlight_threshold(dynamic_threshold.unwrap_or(5) as i32);
    let quick_launch_values = [
        "off",
        "2220d805-cf9a-4e12-92b9-5ca778aff6bb",
        "d0f12e6c-97eb-2287-a2f5-115dfaa1d168",
        "d4f7be63-97e6-4952-b265-dd4bce11c155",
        "88c28c12-7f81-42db-aaa6-14ccef6f27e5",
        "daae3686-bff6-4ba5-921b-262f847bb6e8",
        "79c76b48-6111-4e80-8deb-3119eebef33e",
        "36d8c6ed-4c83-4fa1-a9e2-8f12dc941f8c",
    ];
    let set_quick_launch = |key: &str,
                            available: fn(&AppWindow, bool),
                            index: fn(&AppWindow, i32)| {
        let current = snapshot
            .preferences
            .get(key)
            .and_then(|field| match field.value.as_ref() {
                Some(PreferenceValue::Text(value)) => Some(value.as_str()),
                _ => None,
            });
        available(w, current.is_some());
        index(
            w,
            current
                .and_then(|value| quick_launch_values.iter().position(|known| *known == value))
                .unwrap_or(8) as i32,
        );
    };
    set_quick_launch(
        "qlUp",
        AppWindow::set_dc_ql_up_available,
        AppWindow::set_dc_ql_up_index,
    );
    set_quick_launch(
        "qlDown",
        AppWindow::set_dc_ql_down_available,
        AppWindow::set_dc_ql_down_index,
    );
    set_quick_launch(
        "qlSelect",
        AppWindow::set_dc_ql_select_available,
        AppWindow::set_dc_ql_select_index,
    );
    set_quick_launch(
        "qlBack",
        AppWindow::set_dc_ql_back_available,
        AppWindow::set_dc_ql_back_index,
    );
    set_quick_launch(
        "qlComboBackUp",
        AppWindow::set_dc_ql_combo_back_up_available,
        AppWindow::set_dc_ql_combo_back_up_index,
    );
    set_quick_launch(
        "qlComboUpDown",
        AppWindow::set_dc_ql_combo_up_down_available,
        AppWindow::set_dc_ql_combo_up_down_index,
    );
    set_quick_launch(
        "qlSingleClickUp",
        AppWindow::set_dc_ql_tap_up_available,
        AppWindow::set_dc_ql_tap_up_index,
    );
    set_quick_launch(
        "qlSingleClickDown",
        AppWindow::set_dc_ql_tap_down_available,
        AppWindow::set_dc_ql_tap_down_index,
    );
    let language = pref_number("language");
    w.set_dc_language_available(language.is_some());
    w.set_dc_language_index(
        language
            .filter(|value| (1..=9).contains(value))
            .map_or(9, |value| value as i32 - 1),
    );
    let language_english = pref_bool("langEnglish");
    w.set_dc_language_english_available(language_english.is_some());
    w.set_dc_language_english_value(language_english.unwrap_or(false));

    let mut entries = Vec::new();
    if let Some(watch) = &snapshot.watch {
        entries.push(DeviceConfigEntry {
            group: "Capabilities & Diagnostics".into(),
            label: "Platform".into(),
            value: watch
                .platform
                .clone()
                .unwrap_or_else(|| "Unknown".into())
                .into(),
            status: "available".into(),
        });
        entries.push(DeviceConfigEntry {
            group: "".into(),
            label: "Firmware".into(),
            value: watch
                .firmware
                .clone()
                .unwrap_or_else(|| "Unknown".into())
                .into(),
            status: "available".into(),
        });
    }
    entries.push(DeviceConfigEntry {
        group: if entries.is_empty() {
            "Capabilities & Diagnostics".into()
        } else {
            "".into()
        },
        label: "BlobDB version".into(),
        value: snapshot.capabilities.blob_db_version.to_string().into(),
        status: "available".into(),
    });
    entries.push(DeviceConfigEntry {
        group: "".into(),
        label: "Complete readback".into(),
        value: if snapshot
            .capabilities
            .supported
            .iter()
            .any(|capability| capability == "complete_refresh")
        {
            "Yes"
        } else {
            "No"
        }
        .into(),
        status: "available".into(),
    });
    let mut previous_group = "Capabilities & Diagnostics".to_owned();
    for (key, field) in &snapshot.preferences {
        if matches!(
            key.as_str(),
            "clock24h"
                | "displayOrientationLeftHanded"
                | "textStyle"
                | "lightEnabled"
                | "lightAmbientSensorEnabled"
                | "lightMotion"
                | "lightTimeoutMs"
                | "lightTouch"
                | "lightIntensity"
                | "lightDynamicIntensity"
                | "lightPreset"
                | "lightDynamicMode"
                | "menuScrollWrapAround"
                | "menuScrollVibeBehavior"
                | "mask"
                | "notifDesignStyle"
                | "notifVibeDelay"
                | "notifBacklight"
                | "notifWindowTimeout"
                | "vibeIntensity"
                | "vibeScoreNotifications"
                | "vibeScoreIncomingCalls"
                | "vibeScoreAlarms"
                | "dndManuallyEnabled"
                | "dndSmartEnabled"
                | "dndInterruptionsMask"
                | "dndShowNotifications"
                | "dndMotionBacklight"
                | "dndAutoDismiss"
                | "timelineQuickViewEnabled"
                | "timelineQuickViewBeforeTimeMin"
                | "musicShowVolumeControls"
                | "musicShowProgressBar"
                | "qlUp"
                | "qlDown"
                | "qlSelect"
                | "qlBack"
                | "qlComboBackUp"
                | "qlComboUpDown"
                | "qlSingleClickUp"
                | "qlSingleClickDown"
                | "language"
                | "lightColor"
                | "motionSensitivity"
                | "lightAmbientThreshold"
                | "dynBacklightMinThreshold"
        ) {
            continue;
        }
        let group = preference_group(key);
        let heading = if group == previous_group {
            String::new()
        } else {
            group.to_owned()
        };
        previous_group = group.to_owned();
        let decoded = match &field.value {
            Some(PreferenceValue::Bool(value)) => {
                if *value {
                    "On".to_string()
                } else {
                    "Off".to_string()
                }
            }
            Some(PreferenceValue::Unsigned(value)) | Some(PreferenceValue::Color(value)) => {
                value.to_string()
            }
            Some(PreferenceValue::Text(value)) => value.clone(),
            Some(PreferenceValue::Enum { code, label }) => {
                label.clone().unwrap_or_else(|| format!("Unknown ({code})"))
            }
            Some(PreferenceValue::Unknown) | None => "—".to_string(),
        };
        let raw = field
            .raw
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        let value = if raw.is_empty() {
            decoded
        } else {
            format!("{decoded} · raw {raw}")
        };
        entries.push(DeviceConfigEntry {
            group: heading.into(),
            label: humanize_preference_key(key).into(),
            value: value.into(),
            status: availability_wire(field.availability).into(),
        });
    }
    w.set_dc_preferences(ModelRc::new(VecModel::from(entries)));
}

fn update_health_display(w: &AppWindow) {
    let height_mm = w.get_dc_height_mm() as f64;
    let weight_dag = w.get_dc_weight_dag() as f64;
    if w.get_dc_units_index() == 0 {
        w.set_dc_height_display(format!("{:.1} cm", height_mm / 10.0).into());
        w.set_dc_weight_display(format!("{:.2} kg", weight_dag / 100.0).into());
    } else {
        w.set_dc_height_display(format!("{:.1} in", height_mm / 25.4).into());
        w.set_dc_weight_display(format!("{:.1} lb", weight_dag / 45.359_237).into());
    }
}

fn clear_device_config(w: &AppWindow) {
    w.set_dc_state("disconnected".into());
    w.set_dc_loading(false);
    w.set_dc_status_error(false);
    w.set_dc_status("Connect a watch to inspect its settings.".into());
    w.set_dc_watch_id("".into());
    w.set_dc_height("".into());
    w.set_dc_health_available(false);
    w.set_dc_units_available(false);
    w.set_dc_hrm_available(false);
    w.set_dc_hrm_interval_available(false);
    w.set_dc_hrm_during_activity_available(false);
    w.set_dc_dirty(false);
    w.set_dc_applying(false);
    w.set_dc_clock_24h_available(false);
    w.set_dc_timezone_manual_available(false);
    w.set_dc_stationary_mode_available(false);
    w.set_dc_left_handed_available(false);
    w.set_dc_text_size_available(false);
    w.set_dc_light_enabled_available(false);
    w.set_dc_light_ambient_available(false);
    w.set_dc_light_motion_available(false);
    w.set_dc_light_timeout_available(false);
    w.set_dc_light_touch_available(false);
    w.set_dc_light_intensity_available(false);
    w.set_dc_light_dynamic_legacy_available(false);
    w.set_dc_light_preset_available(false);
    w.set_dc_light_dynamic_mode_available(false);
    w.set_dc_menu_wrap_available(false);
    w.set_dc_menu_vibe_available(false);
    w.set_dc_notif_filter_available(false);
    w.set_dc_notif_design_available(false);
    w.set_dc_notif_delay_available(false);
    w.set_dc_notif_backlight_available(false);
    w.set_dc_notif_timeout_available(false);
    w.set_dc_vibe_intensity_available(false);
    w.set_dc_vibe_notifications_available(false);
    w.set_dc_vibe_calls_available(false);
    w.set_dc_vibe_alarms_available(false);
    w.set_dc_dnd_manual_available(false);
    w.set_dc_dnd_smart_available(false);
    w.set_dc_dnd_interruptions_available(false);
    w.set_dc_dnd_show_notifications_available(false);
    w.set_dc_dnd_motion_backlight_available(false);
    w.set_dc_dnd_auto_dismiss_available(false);
    w.set_dc_timeline_quick_view_available(false);
    w.set_dc_timeline_minutes_available(false);
    w.set_dc_music_volume_available(false);
    w.set_dc_music_progress_available(false);
    w.set_dc_ql_up_available(false);
    w.set_dc_ql_down_available(false);
    w.set_dc_ql_select_available(false);
    w.set_dc_ql_back_available(false);
    w.set_dc_ql_combo_back_up_available(false);
    w.set_dc_ql_combo_up_down_available(false);
    w.set_dc_ql_tap_up_available(false);
    w.set_dc_ql_tap_down_available(false);
    w.set_dc_language_available(false);
    w.set_dc_language_english_available(false);
    w.set_dc_light_color_available(false);
    w.set_dc_motion_sensitivity_available(false);
    w.set_dc_light_ambient_threshold_available(false);
    w.set_dc_dynamic_backlight_threshold_available(false);
    w.set_dc_backlight_advanced_expanded(false);
    w.set_dc_preferences(ModelRc::new(
        VecModel::from(Vec::<DeviceConfigEntry>::new()),
    ));
}

fn availability_label(value: FieldAvailability) -> &'static str {
    match value {
        FieldAvailability::Available => "Available",
        FieldAvailability::NotReceived => "Not received",
        FieldAvailability::Unsupported => "Unsupported",
        FieldAvailability::Invalid => "Invalid",
    }
}

fn availability_wire(value: FieldAvailability) -> &'static str {
    match value {
        FieldAvailability::Available => "available",
        FieldAvailability::NotReceived => "not received",
        FieldAvailability::Unsupported => "unsupported",
        FieldAvailability::Invalid => "invalid",
    }
}

fn preference_group(key: &str) -> &'static str {
    if key.contains("quiet") || key.contains("dnd") {
        "Quiet Time"
    } else if key.contains("light") || key.contains("backlight") {
        "Backlight & Input"
    } else if key.contains("notif") || key.contains("vibr") {
        "Notifications"
    } else if key.contains("timeline") {
        "Timeline"
    } else if key.contains("music") {
        "Music"
    } else if key.contains("clock") || key.contains("time") {
        "Time & Display"
    } else {
        "Other Preferences"
    }
}

fn humanize_preference_key(key: &str) -> String {
    let mut result = String::new();
    for character in key.chars() {
        if character.is_ascii_uppercase() && !result.is_empty() {
            result.push(' ');
        }
        result.push(character);
    }
    let mut characters = result.chars();
    characters
        .next()
        .map(|first| first.to_uppercase().collect::<String>() + characters.as_str())
        .unwrap_or(result)
}

fn clear_watch_info(w: &AppWindow) {
    w.set_wi_model("".into());
    w.set_wi_firmware("".into());
    w.set_wi_color("".into());
    w.set_wi_board("".into());
    w.set_wi_serial("".into());
    w.set_wi_bt("".into());
    w.set_wi_language("".into());
}

// ─── Navigation label helpers ─────────────────────────────────────────────────

fn update_workout_nav(w: &AppWindow, period: i32, offset: i32) {
    w.set_workout_period_label(cobble_db::period_label(period, offset).into());
    w.set_workout_can_forward(offset > 0);
}

fn update_sleep_nav(w: &AppWindow, period: i32, offset: i32) {
    w.set_sleep_period_label(cobble_db::period_label(period, offset).into());
    w.set_sleep_can_forward(offset > 0);
}

fn update_heart_nav(w: &AppWindow, period: i32, offset: i32) {
    w.set_heart_period_label(cobble_db::period_label(period, offset).into());
    w.set_heart_can_forward(offset > 0);
}

// ─── Workout helpers ──────────────────────────────────────────────────────────

fn reload_workout_chart(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    match cobble_db::connect_readonly(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match cobble_db::load_steps_chart(&conn, period, offset) {
            Err(e) => warn!("load steps chart failed: {e}"),
            Ok(chart) => {
                window.set_today_steps_label(chart.summary.into());
                window.set_steps_avg_label(chart.avg_label.into());
                window.set_steps_delta_positive(chart.delta_positive);
                window.set_steps_delta_label(chart.delta_label.into());
                let slint_steps: Vec<DaySteps> = chart
                    .bars
                    .into_iter()
                    .map(|s| DaySteps {
                        label: s.label.into(),
                        steps_label: s.steps_label.into(),
                        fraction: s.fraction,
                        bar_start: s.bar_start as i32,
                        bar_end: s.bar_end as i32,
                    })
                    .collect();
                window.set_daily_steps(ModelRc::new(VecModel::from(slint_steps)));
            }
        },
    }
}

fn reload_workout_avg_label(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    match cobble_db::connect_readonly(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match cobble_db::load_steps_chart(&conn, period, offset) {
            Err(e) => warn!("load steps chart failed: {e}"),
            Ok(chart) => window.set_steps_avg_label(chart.avg_label.into()),
        },
    }
}

fn reload_workout_avg_label_for_range(
    window: &AppWindow,
    db_path: &PathBuf,
    range: cobble_db::DateRange,
) {
    match cobble_db::connect_readonly(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match cobble_db::load_steps_avg_label_for_range(&conn, range) {
            Err(e) => warn!("load selected steps average failed: {e}"),
            Ok(label) => window.set_steps_avg_label(label.into()),
        },
    }
}

fn reload_workout_sessions(
    window: &AppWindow,
    db_path: &PathBuf,
    period: i32,
    offset: i32,
    bar_range: (i64, i64),
) {
    let (start, end) = if bar_range.0 < 0 {
        cobble_db::period_range_offset(period, offset)
    } else {
        bar_range
    };
    match cobble_db::connect_readonly(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match cobble_db::load_sessions_filtered(&conn, 1, start, end) {
            Err(e) => warn!("load workout sessions failed: {e}"),
            Ok(sessions) => {
                window.set_sessions(ModelRc::new(VecModel::from(to_slint_sessions(sessions))));
            }
        },
    }
}

// ─── Sleep helpers ────────────────────────────────────────────────────────────

fn reload_sleep_chart(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    match cobble_db::connect_readonly(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match cobble_db::load_sleep_chart(&conn, period, offset) {
            Err(e) => warn!("load sleep chart failed: {e}"),
            Ok(chart) => {
                window.set_sleep_label(chart.summary.into());
                window.set_sleep_avg_label(chart.avg_label.into());
                window.set_sleep_delta_positive(chart.delta_positive);
                window.set_sleep_delta_label(chart.delta_label.into());
                let slint_bars: Vec<SleepBar> = chart
                    .bars
                    .into_iter()
                    .map(|b| SleepBar {
                        label: b.label.into(),
                        bar_start: b.bar_start as i32,
                        bar_end: b.bar_end as i32,
                        light_fraction: b.light_fraction,
                        deep_fraction: b.deep_fraction,
                        total_label: b.total_label.into(),
                        deep_label: b.deep_label.into(),
                    })
                    .collect();
                window.set_sleep_bars(ModelRc::new(VecModel::from(slint_bars)));
            }
        },
    }
}

fn reload_sleep_avg_label(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    match cobble_db::connect_readonly(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match cobble_db::load_sleep_chart(&conn, period, offset) {
            Err(e) => warn!("load sleep chart failed: {e}"),
            Ok(chart) => window.set_sleep_avg_label(chart.avg_label.into()),
        },
    }
}

fn reload_sleep_avg_label_for_range(
    window: &AppWindow,
    db_path: &PathBuf,
    range: cobble_db::DateRange,
) {
    match cobble_db::connect_readonly(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match cobble_db::load_sleep_avg_label_for_range(&conn, range) {
            Err(e) => warn!("load selected sleep average failed: {e}"),
            Ok(label) => window.set_sleep_avg_label(label.into()),
        },
    }
}

fn reload_sleep_stats(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    reload_sleep_stats_for_range(window, db_path, cobble_db::range_for(period, offset));
}

fn reload_sleep_stats_for_range(
    window: &AppWindow,
    db_path: &PathBuf,
    range: cobble_db::DateRange,
) {
    match cobble_db::connect_readonly(db_path) {
        Err(e) => warn!("cannot open DB: {e}"),
        Ok(conn) => match cobble_db::load_sleep_stats_for_range(&conn, range) {
            Err(e) => warn!("load sleep stats failed: {e}"),
            Ok(stats) => {
                window.set_sleep_stats(SleepStats {
                    deep_avg_label: stats.deep_avg_label.into(),
                    light_pct: stats.light_pct,
                    deep_pct: stats.deep_pct,
                    awake_pct: stats.awake_pct,
                    avg_bedtime: stats.avg_bedtime.into(),
                    avg_wakeup: stats.avg_wakeup.into(),
                    highest_dur: stats.highest_dur.into(),
                    lowest_dur: stats.lowest_dur.into(),
                });
            }
        },
    }
}

/// Convert the UTC bounds carried by a chart bar back into its watch-local
/// calendar range. Day bars cover one date; month-view week bars cover the
/// (possibly partial) ISO week span represented by that bar.
fn bar_date_range(start: i64, end: i64) -> Option<cobble_db::DateRange> {
    let start = cobble_db::watch_local_date(start)?;
    let end = cobble_db::watch_local_date(end)?;
    (start <= end).then_some(cobble_db::DateRange { start, end })
}

fn clear_heart_data(window: &AppWindow) {
    window.set_heart_stats(HeartStats {
        average_label: "—".into(),
        resting_label: "—".into(),
        sleeping_label: "—".into(),
        lowest_label: "—".into(),
        highest_label: "—".into(),
        samples_label: "—".into(),
    });
    window.set_heart_average_path("".into());
    window.set_heart_resting_path("".into());
    window.set_heart_trend_max_label("".into());
    window.set_heart_trend_mid_label("".into());
    window.set_heart_trend_min_label("".into());
    window.set_heart_trend_has_data(false);
    window.set_heart_trend_points(ModelRc::new(VecModel::from(Vec::<HeartTrendPoint>::new())));
}

fn reload_heart_stats(window: &AppWindow, db_path: &PathBuf, period: i32, offset: i32) {
    match cobble_db::connect_readonly(db_path) {
        Err(e) => {
            clear_heart_data(window);
            warn!("cannot open DB: {e}");
        }
        Ok(conn) => {
            let stats = match cobble_db::load_heart_stats(&conn, period, offset) {
                Err(e) => {
                    clear_heart_data(window);
                    warn!("load heart stats failed: {e}");
                    return;
                }
                Ok(stats) => stats,
            };
            let trend = match cobble_db::load_heart_trend(&conn, period, offset) {
                Err(e) => {
                    clear_heart_data(window);
                    warn!("load heart trend failed: {e}");
                    return;
                }
                Ok(trend) => trend,
            };

            window.set_heart_stats(HeartStats {
                average_label: stats.average_label.into(),
                resting_label: stats.resting_label.into(),
                sleeping_label: stats.sleeping_label.into(),
                lowest_label: stats.lowest_label.into(),
                highest_label: stats.highest_label.into(),
                samples_label: stats.samples_label.into(),
            });
            apply_heart_trend(window, trend);
        }
    }
}

fn apply_heart_trend(window: &AppWindow, trend: cobble_db::HeartTrend) {
    let min_bpm = trend.min_bpm;
    let max_bpm = trend.max_bpm;

    let points: Vec<HeartTrendPoint> = trend
        .points
        .iter()
        .map(|point| HeartTrendPoint {
            label: point.label.clone().into(),
        })
        .collect();

    window.set_heart_average_path(heart_trend_path(&trend.points, min_bpm, max_bpm, false).into());
    window.set_heart_resting_path(heart_trend_path(&trend.points, min_bpm, max_bpm, true).into());
    window.set_heart_trend_max_label(format!("{} bpm", max_bpm.round() as i32).into());
    window.set_heart_trend_mid_label(
        format!("{} bpm", ((min_bpm + max_bpm) / 2.0).round() as i32).into(),
    );
    window.set_heart_trend_min_label(format!("{} bpm", min_bpm.round() as i32).into());
    window.set_heart_trend_has_data(
        trend
            .points
            .iter()
            .any(|point| point.average_bpm.is_some() || point.resting_bpm.is_some()),
    );
    window.set_heart_trend_points(ModelRc::new(VecModel::from(points)));
}

fn heart_trend_path(
    points: &[cobble_db::HeartTrendPointData],
    min_bpm: f32,
    max_bpm: f32,
    resting: bool,
) -> String {
    let point_count = points.len();
    let span = (max_bpm - min_bpm).max(1.0);
    let mut path = String::new();
    let mut previous: Option<(f32, f32)> = None;
    const MARKER_HALF_WIDTH: f32 = 0.8;

    for (index, point) in points.iter().enumerate() {
        let value = if resting {
            point.resting_bpm
        } else {
            point.average_bpm
        };
        let Some(value) = value else {
            previous = None;
            continue;
        };
        let x = if point_count <= 1 {
            50.0
        } else {
            (index as f32 + 0.5) * 100.0 / point_count as f32
        };
        let y = ((max_bpm - value) / span * 100.0).clamp(0.0, 100.0);
        if let Some((previous_x, previous_y)) = previous {
            path.push_str(&format!(
                "M {previous_x:.2} {previous_y:.2} L {x:.2} {y:.2}"
            ));
        }
        let marker_start = (x - MARKER_HALF_WIDTH).max(0.0);
        let marker_end = (x + MARKER_HALF_WIDTH).min(100.0);
        if path.is_empty() {
            path.push_str(&format!(
                "M {marker_start:.2} {y:.2} L {marker_end:.2} {y:.2}"
            ));
        } else {
            path.push_str(&format!(
                " M {marker_start:.2} {y:.2} L {marker_end:.2} {y:.2}"
            ));
        }
        previous = Some((x, y));
    }
    path
}

// ─── Conversion ───────────────────────────────────────────────────────────────

fn to_slint_sessions(sessions: Vec<cobble_db::HealthSessionData>) -> Vec<HealthSession> {
    sessions
        .into_iter()
        .map(|s| HealthSession {
            type_name: s.type_name.into(),
            start_label: s.start_label.into(),
            duration_label: s.duration_label.into(),
            has_metrics: s.has_metrics,
            metrics_label: s.metrics_label.into(),
        })
        .collect()
}
