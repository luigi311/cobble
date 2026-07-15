use std::path::{Path, PathBuf};

use anyhow::Context;
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

/// Log invalid integration settings after the caller has initialized tracing.
pub fn warn_if_invalid(path: &Path, config: &Config) {
    if let Err(error) = config.validate() {
        warn!(
            "config file {} has invalid integration settings: {error}",
            path.display()
        );
    }
}
