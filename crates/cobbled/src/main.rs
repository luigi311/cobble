//! Daemon entry point.
//!
//! Reads `$XDG_CONFIG_HOME/cobbled/config.toml` (or the path given by
//! `--config`), acquires the session D-Bus, exports the CobbleDaemon interface,
//! requests the well-known name (org.cobble.Daemon), opens the watch
//! connection, and runs until signalled.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

use clap::Parser;
use tokio::{
    signal,
    signal::unix::SignalKind,
    sync::{mpsc, watch},
};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const PBW_METADATA_BACKFILL: &str = "pbw_metadata";
const PBW_METADATA_BACKFILL_VERSION: i64 = 1;

mod call_monitor;
mod codec;
mod config;
mod config_watcher;
mod http;
mod integrations;
mod location;
mod mpris_monitor;
mod notification;
mod notify_monitor;
mod pkjs;
mod service;
mod supervisor;
mod weather;

use cobble_db::AppDb;
use integrations::worker;
use libpebble_ble::PbwBundle;
use notify_monitor::NotificationMonitor;
use pkjs::PkjsManager;
use service::{BUS_NAME, CobbleDaemon, OBJECT_PATH, run_signal_emitter};
use supervisor::run_supervisor;

#[derive(Parser)]
#[command(
    name = "cobbled",
    about = "Long-lived daemon owning the Pebble BLE connection."
)]
struct Cli {
    /// Path to config file (default: $XDG_CONFIG_HOME/cobbled/config.toml)
    #[arg(long)]
    config: Option<PathBuf>,
    /// Increase log verbosity: -v = debug, -vv = trace. Overrides config/RUST_LOG.
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config_path = match cli.config {
        Some(p) => p,
        None => config::default_config_path()?,
    };
    let cfg = config::load(&config_path)?;

    // Verbosity: CLI -v count wins; legacy `verbose = true` in config maps to
    // the deepest level (trace). Our crates follow the chosen level while noisy
    // dependencies (zbus, bluer) are kept one notch quieter so shared logs stay
    // readable. Level 0 still honours RUST_LOG for surgical control.
    let level = if cli.verbose > 0 {
        cli.verbose
    } else if cfg.verbose {
        2
    } else {
        0
    };
    let filter = match level {
        0 => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        1 => EnvFilter::new("info,cobbled=debug,libpebble_ble=debug"),
        _ => EnvFilter::new("debug,cobbled=trace,libpebble_ble=trace"),
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();

    config::warn_if_invalid(&config_path, &cfg);
    info!(
        "loaded config from {} (intervals_icu={:?})",
        config_path.display(),
        cfg.redacted_intervals_icu()
    );

    let db_path = config::resolved_db_path(&cfg)?;
    let app_db: Option<Arc<Mutex<AppDb>>> = match AppDb::open(&db_path) {
        Ok(db) => {
            if let Err(error) = db.recover_interrupted_pbw_installs() {
                warn!("could not recover interrupted PBW installs: {error}");
            }
            if let Err(error) = refresh_pbw_metadata(&db) {
                warn!("could not refresh retained PBW metadata: {error:#}");
            }
            info!("app DB opened at {}", db_path.display());
            Some(Arc::new(Mutex::new(db)))
        }
        Err(e) => {
            warn!("could not open app DB at {}: {e}", db_path.display());
            None
        }
    };

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let pkjs = PkjsManager::start(
        app_db.clone(),
        db_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("pkjs"),
    );
    let (wellness_revision_tx, wellness_revision_rx) = watch::channel(0_u64);
    let (wellness_sync_tx, wellness_sync_rx) = watch::channel(0_u64);
    let (wellness_shutdown_tx, wellness_shutdown_rx) = watch::channel(false);
    let wellness_running = Arc::new(AtomicBool::new(false));
    let wellness_status_revision = Arc::new(AtomicU64::new(0));

    // Channel for forwarding watch music-control actions to the MPRIS monitor.
    let (music_action_tx, music_action_rx) = mpsc::unbounded_channel();

    // Channel for forwarding watch phone actions to the call monitor.
    let (phone_action_tx, phone_action_rx) = mpsc::unbounded_channel();

    let daemon = CobbleDaemon::new(
        cfg.clone(),
        wellness_sync_tx,
        wellness_running.clone(),
        wellness_status_revision.clone(),
        config_path.clone(),
        db_path.clone(),
        cfg.verbose,
        event_tx,
        app_db.clone(),
        music_action_tx,
        phone_action_tx,
        pkjs.clone(),
    );

    // Build the session D-Bus connection.
    let conn = zbus::connection::Builder::session()?
        .name(BUS_NAME)?
        .serve_at(OBJECT_PATH, daemon.clone())?
        .build()
        .await?;

    info!("owning {BUS_NAME} at {OBJECT_PATH}");

    // Start the signal emission task.
    let conn_for_signals = conn.clone();
    let daemon_for_signals = daemon.clone();
    let app_db_for_signals = app_db.clone();
    tokio::spawn(async move {
        run_signal_emitter(
            conn_for_signals,
            daemon_for_signals,
            event_rx,
            app_db_for_signals,
            wellness_revision_tx,
        )
        .await;
    });

    let wellness_worker = app_db.clone().map(|db| {
        tokio::spawn(worker::run(
            db,
            daemon.integration_config_changed(),
            wellness_revision_rx,
            wellness_sync_rx,
            wellness_shutdown_rx,
            wellness_running,
            wellness_status_revision,
        ))
    });

    // Start the desktop notification monitor.
    let mut notify_monitor = NotificationMonitor::new();
    let daemon_for_notif = daemon.clone();
    let notif_cb = Arc::new(move |app: String, summary: String, body: String| {
        daemon_for_notif.on_desktop_notification(app, summary, body);
    });
    if let Err(e) = notify_monitor.start(notif_cb).await {
        warn!("could not start notification monitor: {e}");
    }

    // Start the reconnect supervisor in the background.
    let daemon_for_super = daemon.clone();
    let pkjs_for_super = pkjs.clone();
    tokio::spawn(async move {
        run_supervisor(daemon_for_super, pkjs_for_super).await;
    });

    // Watch the config file for external changes (manual edits, GUI saves)
    // and auto-reload whenever it is written.
    config_watcher::watch_config(config_path.clone(), daemon.clone());

    // Start the MPRIS media-player monitor — discovers desktop players,
    // pushes metadata/playback to the watch, and forwards watch actions back.
    {
        let daemon2 = daemon.clone();
        tokio::spawn(async move {
            let monitor = match mpris_monitor::MprisMonitor::new(daemon2).await {
                Ok(m) => m,
                Err(e) => {
                    warn!("mpris: {e}");
                    return;
                }
            };
            let monitor = std::sync::Arc::new(monitor);
            let monitor2 = monitor.clone();
            // Spawn the action-forwarder: receive from the channel and
            // dispatch to the active MPRIS player.
            tokio::spawn(async move {
                let mut rx = music_action_rx;
                while let Some(action) = rx.recv().await {
                    monitor2.handle_action(&action).await;
                }
            });
            monitor.run().await;
        });
    }

    // Start the call monitor: ModemManager / oFono → watch + watch → modem.
    {
        let daemon5 = daemon.clone();
        let rx = phone_action_rx;
        tokio::spawn(async move {
            call_monitor::run_call_monitor(daemon5, rx).await;
        });
    }

    // Start the weather provider: Location portal → Open-Meteo → watch.
    {
        let daemon6 = daemon.clone();
        tokio::spawn(async move {
            weather::run_weather(daemon6).await;
        });
    }

    // Run until SIGINT or SIGTERM.
    let mut sigterm = signal::unix::signal(SignalKind::terminate())?;
    tokio::select! {
        _ = signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }

    info!("shutting down ...");
    let _ = wellness_shutdown_tx.send(true);
    if let Some(worker) = wellness_worker {
        if let Err(error) = worker.await {
            warn!("wellness exporter shutdown failed: {error}");
        }
    }
    daemon.set_stopping();
    pkjs.shutdown().await;
    notify_monitor.stop().await;

    Ok(())
}

fn refresh_pbw_metadata(db: &AppDb) -> anyhow::Result<()> {
    if db.maintenance_version(PBW_METADATA_BACKFILL)? >= PBW_METADATA_BACKFILL_VERSION {
        return Ok(());
    }

    let mut parse_failures = Vec::new();
    for app in db.list_pbw_apps()? {
        let cached = db
            .load_cached_pbw_app(&app.uuid)?
            .ok_or_else(|| anyhow::anyhow!("retained PBW {} disappeared", app.uuid))?;
        let configurable = match PbwBundle::is_configurable(&cached.pbw) {
            Ok(configurable) => configurable,
            Err(error) => {
                parse_failures.push(format!("{}: {error}", app.uuid));
                continue;
            }
        };
        if configurable != app.configurable
            && !db.set_pbw_app_configurable(&app.uuid, configurable)?
        {
            anyhow::bail!(
                "retained PBW {} disappeared during metadata update",
                app.uuid
            );
        }
    }

    if !parse_failures.is_empty() {
        anyhow::bail!(
            "could not parse retained PBW metadata: {}",
            parse_failures.join("; ")
        );
    }

    db.set_maintenance_version(PBW_METADATA_BACKFILL, PBW_METADATA_BACKFILL_VERSION)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cobble_db::PbwAppRecord;

    #[test]
    fn pbw_metadata_backfill_stays_incomplete_after_parse_failure() {
        let directory = tempfile::tempdir().unwrap();
        let db = AppDb::open(&directory.path().join("app.db")).unwrap();
        let app = |uuid: &str, updated_at| PbwAppRecord {
            uuid: uuid.into(),
            name: "Invalid".into(),
            version: "1.0".into(),
            watchface: false,
            configurable: false,
            platform: "basalt".into(),
            state: "installed".into(),
            installed_at: Some(1),
            updated_at,
        };
        let first_uuid = "01234567-89ab-cdef-0123-456789abcdef";
        let second_uuid = "fedcba98-7654-3210-fedc-ba9876543210";
        db.stage_pbw_app(&app(first_uuid, 1), b"not a PBW").unwrap();
        db.stage_pbw_app(&app(second_uuid, 2), b"also not a PBW")
            .unwrap();

        let error = refresh_pbw_metadata(&db).unwrap_err().to_string();
        assert!(error.contains(first_uuid), "{error}");
        assert!(error.contains(second_uuid), "{error}");
        assert_eq!(db.maintenance_version(PBW_METADATA_BACKFILL).unwrap(), 0);
    }

    #[test]
    fn completed_pbw_metadata_backfill_skips_future_scans() {
        let directory = tempfile::tempdir().unwrap();
        let db = AppDb::open(&directory.path().join("app.db")).unwrap();
        refresh_pbw_metadata(&db).unwrap();
        assert_eq!(
            db.maintenance_version(PBW_METADATA_BACKFILL).unwrap(),
            PBW_METADATA_BACKFILL_VERSION
        );
        db.stage_pbw_app(
            &PbwAppRecord {
                uuid: "01234567-89ab-cdef-0123-456789abcdef".into(),
                name: "Invalid".into(),
                version: "1.0".into(),
                watchface: false,
                configurable: false,
                platform: "basalt".into(),
                state: "installed".into(),
                installed_at: Some(1),
                updated_at: 1,
            },
            b"not a PBW",
        )
        .unwrap();

        refresh_pbw_metadata(&db).unwrap();
    }
}
