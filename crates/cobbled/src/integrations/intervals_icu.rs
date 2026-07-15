//! Intervals.icu request types and client implementation.

use std::time::{Duration, SystemTime};

use anyhow::Context;
use cobble_config::IntervalsIcuConfig;
use cobble_db::DailyWellness;
use serde::Serialize;
use sha2::{Digest, Sha256};

/// One partial wellness update in the Intervals.icu bulk request.
///
/// Optional fields are omitted so a missing local observation cannot clear a
/// value that already exists on the remote wellness record.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct WellnessRecord {
    pub(crate) id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) steps: Option<u32>,
    #[serde(rename = "sleepSecs", skip_serializing_if = "Option::is_none")]
    pub(crate) sleep_secs: Option<u32>,
    #[serde(rename = "avgSleepingHR", skip_serializing_if = "Option::is_none")]
    pub(crate) avg_sleeping_hr: Option<f32>,
    #[serde(rename = "restingHR", skip_serializing_if = "Option::is_none")]
    pub(crate) resting_hr: Option<u16>,
}

impl From<&DailyWellness> for WellnessRecord {
    fn from(wellness: &DailyWellness) -> Self {
        Self {
            id: wellness.date.format("%Y-%m-%d").to_string(),
            steps: wellness.steps,
            sleep_secs: wellness.sleep_secs,
            avg_sleeping_hr: wellness.avg_sleeping_hr,
            resting_hr: wellness.resting_hr,
        }
    }
}

impl WellnessRecord {
    /// Hash the compact JSON representation that is sent to Intervals.icu.
    /// Struct field order and omission rules make this a stable per-date
    /// representation of exactly the fields owned by Cobble.
    pub(crate) fn payload_hash(&self) -> anyhow::Result<String> {
        let canonical = serde_json::to_vec(self).context("serialize wellness payload")?;
        let digest = Sha256::digest(canonical);
        Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
    }
}

const API_BASE_URL: &str = "https://intervals.icu/api/v1";
const API_KEY_USERNAME: &str = "API_KEY";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseClass {
    Success,
    Retryable,
    Authentication,
    ClientError,
    Unexpected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResponseClassification {
    pub(crate) status: u16,
    pub(crate) class: ResponseClass,
    pub(crate) retry_after: Option<Duration>,
}

impl ResponseClassification {
    pub(crate) fn should_retry(self) -> bool {
        matches!(
            self.class,
            ResponseClass::Retryable | ResponseClass::Unexpected
        )
    }

    /// A status-only description safe to retain in logs or export state.
    pub(crate) fn summary(self) -> String {
        let description = match self.class {
            ResponseClass::Success => "success",
            ResponseClass::Retryable => "transient failure",
            ResponseClass::Authentication => "authentication failure",
            ResponseClass::ClientError => "invalid request",
            ResponseClass::Unexpected => "unexpected response",
        };
        format!("HTTP {}: {description}", self.status)
    }
}

pub(crate) fn classify_response(response: &reqwest::Response) -> ResponseClassification {
    let status = response.status();
    let class = match status.as_u16() {
        200..=299 => ResponseClass::Success,
        401 | 403 => ResponseClass::Authentication,
        400 | 404 | 422 => ResponseClass::ClientError,
        408 | 429 | 500..=599 => ResponseClass::Retryable,
        _ => ResponseClass::Unexpected,
    };
    let retry_after = if matches!(class, ResponseClass::Retryable | ResponseClass::Unexpected) {
        parse_retry_after(response.headers().get(reqwest::header::RETRY_AFTER))
    } else {
        None
    };
    ResponseClassification {
        status: status.as_u16(),
        class,
        retry_after,
    }
}

fn parse_retry_after(header: Option<&reqwest::header::HeaderValue>) -> Option<Duration> {
    let value = header?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    let date = httpdate::parse_http_date(value).ok()?;
    Some(
        date.duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}

/// Small provider-owned HTTP client. Response classification is kept separate
/// so retry and authentication policy can be applied by the next client piece.
pub(crate) struct IntervalsIcuClient {
    http: reqwest::Client,
    base_url: reqwest::Url,
}

impl IntervalsIcuClient {
    pub(crate) fn new() -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("cobbled/0.6")
            .timeout(Duration::from_secs(15))
            .build()
            .context("build Intervals.icu HTTP client")?;
        let base_url =
            reqwest::Url::parse(API_BASE_URL).context("parse Intervals.icu API base URL")?;
        Ok(Self { http, base_url })
    }

    fn endpoint(&self, athlete_id: &str) -> anyhow::Result<reqwest::Url> {
        let mut endpoint = self.base_url.clone();
        {
            let mut segments = endpoint
                .path_segments_mut()
                .map_err(|()| anyhow::anyhow!("Intervals.icu API URL cannot contain a host"))?;
            segments
                .pop_if_empty()
                .push("athlete")
                .push(athlete_id)
                .push("wellness-bulk");
        }
        Ok(endpoint)
    }

    /// Send a bulk wellness update using Intervals.icu's API-key Basic auth.
    /// Status handling and response-body sanitization are handled separately.
    pub(crate) async fn send_wellness(
        &self,
        config: &IntervalsIcuConfig,
        records: &[WellnessRecord],
    ) -> anyhow::Result<reqwest::Response> {
        if !config.enabled {
            anyhow::bail!("Intervals.icu integration is disabled");
        }
        config
            .validate()
            .context("invalid Intervals.icu configuration")?;

        let endpoint = self.endpoint(&config.athlete_id)?;
        self.http
            .put(endpoint)
            .basic_auth(API_KEY_USERNAME, Some(&config.api_key))
            .json(records)
            .send()
            .await
            .context("send Intervals.icu wellness request")
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn resting_hr_is_optional_and_part_of_the_exact_payload_hash() {
        let wellness = DailyWellness {
            date: NaiveDate::from_ymd_opt(2026, 7, 15).unwrap(),
            steps: Some(8_000),
            sleep_secs: Some(8 * 3600),
            avg_sleeping_hr: Some(64.5),
            resting_hr: Some(58),
        };
        let record = WellnessRecord::from(&wellness);
        let json = serde_json::to_value(&record).unwrap();
        assert_eq!(json.get("restingHR").and_then(|value| value.as_u64()), Some(58));

        let without_resting_hr = WellnessRecord::from(&DailyWellness {
            resting_hr: None,
            ..wellness
        });
        let json_without = serde_json::to_value(&without_resting_hr).unwrap();
        assert!(json_without.get("restingHR").is_none());
        assert_ne!(
            record.payload_hash().unwrap(),
            without_resting_hr.payload_hash().unwrap()
        );
    }
}
