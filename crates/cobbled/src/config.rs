use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use tracing::{debug, warn};

pub use cobble_config::Config;

/// Returns `$XDG_CONFIG_HOME/cobbled/config.toml` or
/// `~/.config/cobbled/config.toml` as a fallback.
pub fn default_config_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(p) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p)
    } else if let Some(p) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p).join(".config")
    } else {
        anyhow::bail!(
            "neither XDG_CONFIG_HOME nor HOME is set; \
             use --config to specify the config file path explicitly"
        );
    };
    Ok(base.join("cobbled/config.toml"))
}

pub fn default_db_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(p) = std::env::var_os("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p)
    } else if let Some(p) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p).join(".local/share")
    } else {
        anyhow::bail!("neither XDG_DATA_HOME nor HOME is set; set db explicitly")
    };
    Ok(base.join("cobbled/cobbled.db"))
}

pub fn resolved_db_path(config: &Config) -> anyhow::Result<PathBuf> {
    config
        .db
        .as_deref()
        .map(PathBuf::from)
        .map(Ok)
        .unwrap_or_else(default_db_path)
}

pub fn load(path: &Path) -> anyhow::Result<Config> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            toml::from_str(&text).with_context(|| format!("parse config file {}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Return a default config so the daemon can start without a
            // pre-existing config file. The GUI (or manual editing) can
            // supply a watch address later; reload_config will pick it up.
            debug!("config file {} not found; using defaults", path.display());
            Ok(Config::default())
        }
        Err(e) => Err(e).with_context(|| format!("read config file {}", path.display())),
    }
}

pub fn save(path: &Path, config: &Config) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create config directory {}", parent.display()))?;
    }
    let text = toml::to_string_pretty(config).context("serialize config")?;
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("config path has no filename"))?;
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temporary = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!(
            ".{}.tmp-{}-{unique}",
            file_name.to_string_lossy(),
            std::process::id()
        ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let result = (|| {
        let mut file = options
            .open(&temporary)
            .with_context(|| format!("open temporary config {}", temporary.display()))?;
        #[cfg(unix)]
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        file.write_all(text.as_bytes())?;
        file.flush()?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)
            .with_context(|| format!("replace config {}", path.display()))?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::File::open(parent)
            .with_context(|| format!("open config directory {}", parent.display()))?
            .sync_all()
            .with_context(|| format!("sync config directory {}", parent.display()))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

/// Log invalid integration settings after the caller has initialized tracing.
pub fn warn_if_invalid(path: &Path, config: &Config) {
    if let Err(error) = config.validate() {
        warn!(
            "config file {} has invalid integration settings: {error}",
            path.display()
        );
    }
}
