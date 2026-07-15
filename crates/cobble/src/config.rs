use std::path::{Path, PathBuf};

use anyhow::Context;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub use cobble_config::Config;

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
    std::fs::write(path, text).with_context(|| format!("write config {}", path.display()))?;
    #[cfg(unix)]
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("set permissions on config {}", path.display()))?;
    Ok(())
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
