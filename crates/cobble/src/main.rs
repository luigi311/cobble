use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cobble_client::{
    ApplyDisposition, CobbleClient, DaemonConfigPatch, DaemonConfigSnapshot, IntervalsIcuPatch,
    SecretPatch, StatusEvent, VarDict,
};
use slint::{ModelRc, VecModel};
use tracing::warn;

slint::include_modules!();

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
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
        .unwrap_or_default();

    // Derive the watch timezone offset from synced data so all times/labels
    // render in the watch's local zone, independent of the host's system tz.
    if let Ok(conn) = cobble_db::connect_readonly(&effective_db_path) {
        cobble_db::set_watch_offset(cobble_db::watch_tz_offset(&conn));
    }

    // ── Shared filter state (main-thread only) ───────────────────────────────
    let period_workout  = Rc::new(Cell::new(1i32));
    let offset_workout  = Rc::new(Cell::new(0i32));
    let bar_range_w     = Rc::new(Cell::new((-1i64, -1i64)));
    let period_sleep    = Rc::new(Cell::new(1i32));
    let offset_sleep    = Rc::new(Cell::new(0i32));
    let period_heart    = Rc::new(Cell::new(1i32));
    let offset_heart    = Rc::new(Cell::new(0i32));

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
        rt.spawn(async move {
            loop {
                // Stream daemon/watch status via D-Bus signals (no polling).
                if let Ok(client) = CobbleClient::new().await {
                    let weak2 = weak.clone();
                    let baseline = config_baseline.clone();
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

    // ── Refresh ──────────────────────────────────────────────────────────────
    {
        let weak = window.as_weak();
        let db2  = effective_db_path.clone();
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        let ps = period_sleep.clone();  let os = offset_sleep.clone();
        let ph = period_heart.clone();  let oh = offset_heart.clone();
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
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        window.on_workout_period_changed(move |p| {
            pw.set(p); ow.set(0); brw.set((-1, -1));
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
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        window.on_workout_go_back(move || {
            let new_off = ow.get() + 1;
            ow.set(new_off); brw.set((-1, -1));
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
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        window.on_workout_go_forward(move || {
            let new_off = (ow.get() - 1).max(0);
            ow.set(new_off); brw.set((-1, -1));
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
        let pw = period_workout.clone(); let ow = offset_workout.clone(); let brw = bar_range_w.clone();
        window.on_bar_tapped(move |s, e| {
            let range = if s < 0 { (-1i64, -1i64) } else { (s as i64, e as i64) };
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
        let ps = period_sleep.clone(); let os = offset_sleep.clone();
        window.on_sleep_period_changed(move |p| {
            ps.set(p); os.set(0);
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
        let ps = period_sleep.clone(); let os = offset_sleep.clone();
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
        let ps = period_sleep.clone(); let os = offset_sleep.clone();
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
        let ps = period_sleep.clone(); let os = offset_sleep.clone();
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
        let ph = period_heart.clone(); let oh = offset_heart.clone();
        window.on_heart_period_changed(move |p| {
            ph.set(p); oh.set(0);
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
        let ph = period_heart.clone(); let oh = offset_heart.clone();
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
        let ph = period_heart.clone(); let oh = offset_heart.clone();
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
            }).ok();
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
                }).ok();
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
                let Ok(client) = CobbleClient::new().await else { return };
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
            move || spawn_action(&rt, w.clone(), |c| async move { c.reboot_watch().await })
        });
        window.on_reset_into_recovery({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.reset_into_recovery().await })
        });
        window.on_create_core_dump({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.create_core_dump().await })
        });
        window.on_forget_watch({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.forget().await })
        });
        window.on_factory_reset({
            let rt = rt_handle.clone();
            let w = w.clone();
            move || spawn_action(&rt, w.clone(), |c| async move { c.factory_reset(true).await })
        });
    }

    window.run()?;
    drop(rt);
    Ok(())
}

/// Run a device action on the runtime; the UI sets an optimistic status before
/// calling, so only failures are reported back.
fn spawn_action<F, Fut>(rt: &tokio::runtime::Handle, weak: slint::Weak<AppWindow>, f: F)
where
    F: FnOnce(CobbleClient) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = cobble_client::Result<()>> + Send + 'static,
{
    rt.spawn(async move {
        if let Err(e) = async { f(CobbleClient::new().await?).await }.await {
            let msg = format!("Error: {e}");
            slint::invoke_from_event_loop(move || {
                if let Some(w) = weak.upgrade() {
                    w.set_action_error(true);
                    w.set_action_status(msg.into());
                }
            })
            .ok();
        }
    });
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
    w.set_cfg_intervals_status_error(
        enabled && (!valid || (!running && !last_error.is_empty())),
    );
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
                let slint_steps: Vec<DaySteps> = chart.bars.into_iter().map(|s| DaySteps {
                    label: s.label.into(),
                    steps_label: s.steps_label.into(),
                    fraction: s.fraction,
                    bar_start: s.bar_start as i32,
                    bar_end: s.bar_end as i32,
                }).collect();
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
                let slint_bars: Vec<SleepBar> = chart.bars.into_iter().map(|b| SleepBar {
                    label: b.label.into(),
                    bar_start: b.bar_start as i32,
                    bar_end: b.bar_end as i32,
                    light_fraction: b.light_fraction,
                    deep_fraction: b.deep_fraction,
                    total_label: b.total_label.into(),
                    deep_label: b.deep_label.into(),
                }).collect();
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
    window.set_heart_trend_points(ModelRc::new(VecModel::from(
        Vec::<HeartTrendPoint>::new(),
    )));
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
        .map(|point| HeartTrendPoint { label: point.label.clone().into() })
        .collect();

    window.set_heart_average_path(
        heart_trend_path(&trend.points, min_bpm, max_bpm, false).into(),
    );
    window.set_heart_resting_path(
        heart_trend_path(&trend.points, min_bpm, max_bpm, true).into(),
    );
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
            path.push_str(&format!("M {previous_x:.2} {previous_y:.2} L {x:.2} {y:.2}"));
        }
        let marker_start = (x - MARKER_HALF_WIDTH).max(0.0);
        let marker_end = (x + MARKER_HALF_WIDTH).min(100.0);
        if path.is_empty() {
            path.push_str(&format!("M {marker_start:.2} {y:.2} L {marker_end:.2} {y:.2}"));
        } else {
            path.push_str(&format!(" M {marker_start:.2} {y:.2} L {marker_end:.2} {y:.2}"));
        }
        previous = Some((x, y));
    }
    path
}

// ─── Conversion ───────────────────────────────────────────────────────────────

fn to_slint_sessions(sessions: Vec<cobble_db::HealthSessionData>) -> Vec<HealthSession> {
    sessions.into_iter().map(|s| HealthSession {
        type_name: s.type_name.into(),
        start_label: s.start_label.into(),
        duration_label: s.duration_label.into(),
        has_metrics: s.has_metrics,
        metrics_label: s.metrics_label.into(),
    }).collect()
}
