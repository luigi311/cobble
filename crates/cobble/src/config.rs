use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

pub use cobble_config::{Config, IntervalsIcuConfig, merge_intervals_edits};

pub fn default_config_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(p) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p)
    } else if let Some(p) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p).join(".config")
    } else {
        anyhow::bail!("neither XDG_CONFIG_HOME nor HOME is set");
    };
    Ok(base.join("cobbled/config.toml"))
}

pub fn default_db_path() -> anyhow::Result<PathBuf> {
    let base = if let Some(p) = std::env::var_os("XDG_DATA_HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p)
    } else if let Some(p) = std::env::var_os("HOME").filter(|v| !v.is_empty()) {
        PathBuf::from(p).join(".local/share")
    } else {
        anyhow::bail!("neither XDG_DATA_HOME nor HOME is set");
    };
    Ok(base.join("cobbled/cobbled.db"))
}

pub fn load(path: &Path) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read config {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parse config {}", path.display()))
}

pub fn save(path: &Path, cfg: &Config) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create config dir {}", parent.display()))?;
    }
    let text = toml::to_string_pretty(cfg).context("serialise config")?;
    let file_name = path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("config path {} has no file name", path.display()))?;
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let temp_path = path
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
    let mut temp_created = false;
    let result = (|| {
        let mut file = options
            .open(&temp_path)
            .with_context(|| format!("open config {}", path.display()))?;
        temp_created = true;
        #[cfg(unix)]
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("set permissions on config {}", path.display()))?;
        file.write_all(text.as_bytes())
            .with_context(|| format!("write config {}", path.display()))?;
        file.flush()
            .with_context(|| format!("flush config {}", path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync config {}", path.display()))?;
        drop(file);
        std::fs::rename(&temp_path, path)
            .with_context(|| format!("replace config {}", path.display()))?;
        Ok(())
    })();
    if temp_created && result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn save_round_trips_integration_config_and_restricts_permissions() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cobble-config-test-{unique}"));
        let path = dir.join("cobbled/config.toml");
        let config = Config {
            address: "E6:94:0A:D4:D5:DC".into(),
            adapter: "hci0".into(),
            verbose: false,
            db: None,
            integrations: cobble_config::Integrations {
                intervals_icu: cobble_config::IntervalsIcuConfig {
                    enabled: true,
                    athlete_id: "i123456".into(),
                    api_key: "secret-api-key".into(),
                },
            },
        };

        save(&path, &config).unwrap();
        let decoded = load(&path).unwrap();
        assert_eq!(decoded.integrations, config.integrations);
        #[cfg(unix)]
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );

        std::fs::remove_dir_all(dir).unwrap();
    }
}
