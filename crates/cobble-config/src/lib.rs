use std::fmt;

use serde::{Deserialize, Serialize};

/// Complete configuration shared by the daemon and GUI.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    /// Watch Bluetooth address, e.g. E6:94:0A:D4:D5:DC.
    #[serde(default)]
    pub address: String,
    /// HCI adapter name.
    #[serde(default = "default_adapter")]
    pub adapter: String,
    /// Enable verbose (TRACE-level) logging.
    #[serde(default)]
    pub verbose: bool,
    /// Path to the SQLite database, if overridden.
    #[serde(default)]
    pub db: Option<String>,
    /// Optional outbound integrations.
    #[serde(default)]
    pub integrations: Integrations,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            address: String::new(),
            adapter: default_adapter(),
            verbose: false,
            db: None,
            integrations: Integrations::default(),
        }
    }
}

/// Configured outbound integrations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Integrations {
    #[serde(default)]
    pub intervals_icu: IntervalsIcuConfig,
}

/// Intervals.icu credentials and enablement state.
///
/// Validation and redacted diagnostics belong to the integration/configuration
/// workflow; the model intentionally preserves the exact configured strings.
#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IntervalsIcuConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub athlete_id: String,
    #[serde(default)]
    pub api_key: String,
}

impl IntervalsIcuConfig {
    /// Whether both credentials are present after trimming for validation.
    pub fn is_configured(&self) -> bool {
        !self.athlete_id.trim().is_empty() && !self.api_key.trim().is_empty()
    }

    /// Validate settings that are required before the integration can run.
    /// Disabled integrations intentionally accept empty credentials.
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.athlete_id.trim().is_empty() {
            anyhow::bail!("Intervals.icu athlete_id is required when the integration is enabled");
        }
        if self.api_key.trim().is_empty() {
            anyhow::bail!("Intervals.icu api_key is required when the integration is enabled");
        }
        if self.athlete_id.contains('/') || self.athlete_id.contains('\\') {
            anyhow::bail!("Intervals.icu athlete_id must not contain path separators");
        }
        Ok(())
    }
}

impl fmt::Debug for IntervalsIcuConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntervalsIcuConfig")
            .field("enabled", &self.enabled)
            .field("athlete_id", &self.athlete_id)
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

impl Config {
    /// Validate all integration settings without exposing credentials.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.integrations.intervals_icu.validate()
    }

    /// A safe-to-log snapshot of the Intervals.icu configuration.
    pub fn redacted_intervals_icu(&self) -> RedactedIntervalsIcuConfig {
        let intervals = &self.integrations.intervals_icu;
        RedactedIntervalsIcuConfig {
            enabled: intervals.enabled,
            athlete_id: intervals.athlete_id.clone(),
            api_key_configured: !intervals.api_key.trim().is_empty(),
        }
    }
}

/// Intervals.icu settings suitable for logs and status diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactedIntervalsIcuConfig {
    pub enabled: bool,
    pub athlete_id: String,
    pub api_key_configured: bool,
}

fn default_adapter() -> String {
    "hci0".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_sections_use_safe_defaults() {
        let config: Config = toml::from_str("").unwrap();
        assert_eq!(config, Config::default());
        assert_eq!(config.adapter, "hci0");
        assert!(!config.integrations.intervals_icu.enabled);
    }

    #[test]
    fn nested_integration_round_trips() {
        let config = Config {
            address: "E6:94:0A:D4:D5:DC".into(),
            adapter: "hci1".into(),
            verbose: true,
            db: Some("/tmp/cobbled.db".into()),
            integrations: Integrations {
                intervals_icu: IntervalsIcuConfig {
                    enabled: true,
                    athlete_id: "i123456".into(),
                    api_key: "secret-api-key".into(),
                },
            },
        };
        let text = toml::to_string_pretty(&config).unwrap();
        let decoded: Config = toml::from_str(&text).unwrap();
        assert_eq!(decoded, config);
        assert!(text.contains("[integrations.intervals_icu]"));
    }

    #[test]
    fn validation_is_disabled_by_default_and_rejects_unsafe_enabled_values() {
        assert!(Config::default().validate().is_ok());
        let mut config = Config::default();
        config.integrations.intervals_icu.enabled = true;
        assert!(config.validate().is_err());

        config.integrations.intervals_icu.athlete_id = "i123/unsafe".into();
        config.integrations.intervals_icu.api_key = "secret-api-key".into();
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("path separators"));
        assert!(!error.contains("secret-api-key"));
    }

    #[test]
    fn debug_output_redacts_api_key() {
        let mut config = Config::default();
        config.integrations.intervals_icu.api_key = "secret-api-key".into();
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret-api-key"));
        assert!(debug.contains("[REDACTED]"));
        assert_eq!(
            config.redacted_intervals_icu(),
            RedactedIntervalsIcuConfig {
                enabled: false,
                athlete_id: String::new(),
                api_key_configured: true,
            }
        );
    }
}
