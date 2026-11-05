//! Weather writes, capability discovery, and solar timeline pins.

use super::{Pebble, rand_u16};
use crate::{
    BlobDBId, BlobDBStatus, PebbleError, WeatherForecast,
    endpoints::{
        blob_db::{
            build_blobdb_delete, build_blobdb_insert, build_blobdb_insert_with_timestamp,
            build_blobdb_raw_str_insert, build_weather_prefs_blob,
        },
        weather::{
            WEATHER_DB_V4_CAPABILITY, build_weather_pins, build_weather_record, hourly_count,
            weather_in_fahrenheit,
        },
    },
};
use tracing::{debug, info, warn};

impl Pebble {
    /// Observe effective temperature-unit changes, coalescing repeated identical preferences.
    pub fn watch_weather_units(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.lock().unwrap().weather_units_tx.subscribe()
    }

    /// Send a Celsius forecast as v4.5 when advertised, otherwise v3, awaiting watch acceptance.
    /// Resolve temperature units from synced preferences; solar pins are an explicit opt-in.
    pub async fn push_weather_forecast(
        &self,
        location_key: &[u8; 16],
        forecast: &WeatherForecast,
    ) -> Result<(), PebbleError> {
        let _operation = self.weather_operation.lock().await;
        if !self.is_connected() {
            return Err(PebbleError::NotConnected);
        }
        let (generation, cached) = {
            let inner = self.inner.lock().unwrap();
            (inner.connection_id, inner.watch_capabilities)
        };
        let capabilities = match cached {
            Some(caps) => caps,
            None => match self.get_watch_version().await {
                Ok(info) => info.capabilities,
                Err(error) => {
                    warn!("weather capabilities unavailable ({error}); using v3");
                    0
                }
            },
        };
        let needs_preferences = {
            let inner = self.inner.lock().unwrap();
            if inner.connection_id != generation {
                return Err(PebbleError::NotConnected);
            }
            inner.blob_db_version >= 1 && observed_units(&inner).is_none()
        };
        if needs_preferences && let Err(error) = self.refresh_watch_preferences().await {
            warn!("weather temperature preference refresh failed: {error}");
        }
        let (blob_db_version, fahrenheit) = {
            let inner = self.inner.lock().unwrap();
            if inner.connection_id != generation {
                return Err(PebbleError::NotConnected);
            }
            let units = observed_units(&inner);
            if units.is_none() {
                warn!("weather temperature preferences unavailable; defaulting to Celsius");
            }
            (inner.blob_db_version, units.unwrap_or(false))
        };
        let now = chrono::Local::now().timestamp() as u32;
        let mut blob = build_weather_record(forecast, now, capabilities, fahrenheit)
            .map_err(|e| PebbleError::Other(e.into()))?;
        let mut status = self
            .write_weather_record(BlobDBId::Weather, location_key, &blob, now, blob_db_version)
            .await?;
        if blob[0] == 4
            && matches!(
                status,
                BlobDBStatus::InvalidData | BlobDBStatus::NotSupported
            )
        {
            warn!("watch rejected weather v4.5 ({status:?}); retrying v3");
            blob = build_weather_record(
                forecast,
                now,
                capabilities & !WEATHER_DB_V4_CAPABILITY,
                fahrenheit,
            )
            .map_err(|e| PebbleError::Other(e.into()))?;
            status = self
                .write_weather_record(BlobDBId::Weather, location_key, &blob, now, blob_db_version)
                .await?;
        }
        accepted(status, "weather record")?;
        info!(
            "Weather format selected: {} (temperature_unit={}, daily={}, hourly_today={}, hourly_tomorrow={})",
            if blob[0] == 4 { "v4.5" } else { "v3" },
            if fahrenheit { "fahrenheit" } else { "celsius" },
            if blob[0] == 4 {
                forecast.daily.len().min(7)
            } else {
                0
            },
            if blob[0] == 4 {
                hourly_count(&forecast.today_hourly)
            } else {
                0
            },
            if blob[0] == 4 {
                hourly_count(&forecast.tomorrow_hourly)
            } else {
                0
            }
        );
        let prefs_token = rand_u16();
        let prefs = build_blobdb_raw_str_insert(
            BlobDBId::AppConfigs,
            "weatherApp",
            &build_weather_prefs_blob(&[*location_key]),
            prefs_token,
        )
        .map_err(|e| PebbleError::Other(e.into()))?;
        accepted(
            self.send_blobdb_confirmed(prefs_token, "weatherApp", &prefs)
                .await?,
            "weather locations",
        )?;
        for pin in build_weather_pins(forecast, location_key, capabilities, fahrenheit, now) {
            let result = match pin.blob {
                Some(blob) => {
                    self.write_weather_record(BlobDBId::Pin, &pin.key, &blob, now, blob_db_version)
                        .await
                }
                None => {
                    let token = rand_u16();
                    let payload = build_blobdb_delete(BlobDBId::Pin, &pin.key, token);
                    self.send_blobdb_confirmed(token, "weather solar pin deletion", &payload)
                        .await
                }
            };
            match result {
                Ok(BlobDBStatus::Success | BlobDBStatus::KeyDoesNotExist) => {}
                Ok(status) => warn!("weather solar pin rejected: {status:?}"),
                Err(error) => {
                    if !self.is_connected() {
                        return Err(error);
                    }
                    warn!("weather solar pin update failed: {error}");
                }
            }
        }
        debug!("weather forecast update accepted");
        Ok(())
    }

    /// Use the negotiated BlobDB command and wait for the matching token's status.
    async fn write_weather_record(
        &self,
        db: BlobDBId,
        key: &[u8; 16],
        blob: &[u8],
        now: u32,
        blob_db_version: u8,
    ) -> Result<BlobDBStatus, PebbleError> {
        let token = rand_u16();
        let payload = if blob_db_version >= 1 {
            build_blobdb_insert_with_timestamp(db, key, blob, now, token)
        } else {
            build_blobdb_insert(db, key, blob, token)
        }
        .map_err(|e| PebbleError::Other(e.into()))?;
        self.send_blobdb_confirmed(token, "weather", &payload).await
    }
}

/// Distinguish unknown units from metric; an explicit temperature choice overrides distance.
pub(super) fn observed_units(inner: &super::PebbleInner) -> Option<bool> {
    let raw = |key: &str| {
        inner
            .observed_preferences
            .get(&(12, key.into()))
            .filter(|value| value.len() == 1)
            .map(|value| value[0])
    };
    let temperature = raw("unitsTemperature");
    match temperature {
        Some(1) => Some(false),
        Some(2) => Some(true),
        _ => raw("unitsDistance")
            .filter(|value| *value <= 1)
            .map(|imperial| weather_in_fahrenheit(temperature, imperial != 0)),
    }
}

/// Surface watch rejection instead of reporting a queued weather update as successful.
fn accepted(status: BlobDBStatus, item: &str) -> Result<(), PebbleError> {
    if status == BlobDBStatus::Success {
        Ok(())
    } else {
        Err(PebbleError::Other(format!(
            "watch rejected {item}: {status:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Endpoint,
        endpoints::pebble_pack,
        transport::{TransportMode, test_transport},
    };
    use std::sync::Arc;
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };

    fn connected(
        caps: Option<u64>,
        blob_version: u8,
    ) -> (Arc<Pebble>, mpsc::UnboundedReceiver<Vec<u8>>) {
        let pebble = Arc::new(Pebble::new("00:00:00:00:00:40", "weather-test"));
        let (mut transport, rx) = test_transport(TransportMode::ReverseV2);
        transport.activate();
        {
            let mut inner = pebble.inner.lock().unwrap();
            inner.transport = Some(transport);
            inner.watch_capabilities = caps;
            inner.blob_db_version = blob_version;
            inner
                .observed_preferences
                .insert((12, "unitsDistance".into()), vec![0]);
        }
        pebble.connected_tx.send_replace(true);
        (pebble, rx)
    }

    async fn packet(rx: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Vec<u8> {
        timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap()
    }

    fn reply(pebble: &Pebble, packet: &[u8], status: BlobDBStatus) {
        let payload = &packet[4..];
        super::super::dispatch::on_pebble_message(
            pebble_pack(Endpoint::BlobDb, &[payload[1], payload[2], status as u8]).unwrap(),
            &pebble.inner,
        );
    }

    /// Exercise the production DB-12 writeback decoder and its notification path.
    fn preference(pebble: &Pebble, db: u8, key: &str, value: &[u8]) {
        let mut payload = vec![0x09, 0x70, 0, db, 0, 0, 0, 0, key.len() as u8];
        payload.extend_from_slice(key.as_bytes());
        payload.extend_from_slice(&(value.len() as u16).to_le_bytes());
        payload.extend_from_slice(value);
        super::super::dispatch::on_pebble_message(
            pebble_pack(Endpoint::BlobDbV2, &payload).unwrap(),
            &pebble.inner,
        );
    }

    /// Retry missing preferences before writing, and decode the synchronized Fahrenheit choice.
    #[tokio::test]
    async fn missing_units_are_refreshed_before_weather_encoding() {
        let (pebble, mut rx) = connected(Some(WEATHER_DB_V4_CAPABILITY), 1);
        pebble.inner.lock().unwrap().observed_preferences.clear();
        let task = {
            let p = pebble.clone();
            tokio::spawn(async move {
                p.push_weather_forecast(
                    &[7; 16],
                    &WeatherForecast {
                        current_temp: 18,
                        ..Default::default()
                    },
                )
                .await
            })
        };
        let mark = packet(&mut rx).await;
        assert_eq!(
            u16::from_be_bytes(mark[2..4].try_into().unwrap()),
            Endpoint::BlobDbV2 as u16
        );
        assert_eq!(mark[4], 0x0c);
        assert_eq!(mark[7], 12);
        preference(&pebble, 12, "unitsTemperature", &[2]);
        super::super::dispatch::on_pebble_message(
            pebble_pack(
                Endpoint::BlobDbV2,
                &[0x8c, mark[5], mark[6], BlobDBStatus::Success as u8],
            )
            .unwrap(),
            &pebble.inner,
        );
        super::super::dispatch::on_pebble_message(
            pebble_pack(Endpoint::BlobDbV2, &[0x0a, 0x71, 0, 12]).unwrap(),
            &pebble.inner,
        );
        assert_eq!(packet(&mut rx).await[4], 0x89);
        assert_eq!(packet(&mut rx).await[4], 0x8a);
        let weather = packet(&mut rx).await;
        assert_eq!(i16::from_le_bytes(weather[32..34].try_into().unwrap()), 64);
        reply(&pebble, &weather, BlobDBStatus::Success);
        let prefs = packet(&mut rx).await;
        reply(&pebble, &prefs, BlobDBStatus::Success);
        assert!(task.await.unwrap().is_ok());
        pebble.disconnect().await;
    }

    /// A rejected preference refresh logs the documented Celsius fallback and still sends weather.
    #[tokio::test]
    async fn unavailable_units_fall_back_after_failed_refresh() {
        let (pebble, mut rx) = connected(Some(WEATHER_DB_V4_CAPABILITY), 1);
        pebble.inner.lock().unwrap().observed_preferences.clear();
        let task = {
            let p = pebble.clone();
            tokio::spawn(async move {
                p.push_weather_forecast(
                    &[7; 16],
                    &WeatherForecast {
                        current_temp: 18,
                        ..Default::default()
                    },
                )
                .await
            })
        };
        let mark = packet(&mut rx).await;
        assert_eq!(mark[4], 0x0c);
        super::super::dispatch::on_pebble_message(
            pebble_pack(
                Endpoint::BlobDbV2,
                &[0x8c, mark[5], mark[6], BlobDBStatus::NotSupported as u8],
            )
            .unwrap(),
            &pebble.inner,
        );
        let weather = packet(&mut rx).await;
        assert_eq!(i16::from_le_bytes(weather[32..34].try_into().unwrap()), 18);
        reply(&pebble, &weather, BlobDBStatus::Success);
        let prefs = packet(&mut rx).await;
        reply(&pebble, &prefs, BlobDBStatus::Success);
        assert!(task.await.unwrap().is_ok());
        pebble.disconnect().await;
    }

    /// Explicit unit preferences override distance units, with automatic mode inheriting distance.
    #[tokio::test]
    async fn observed_preferences_select_the_encoded_temperature_unit() {
        for (temp, imperial, expected) in [(1, 1, 18), (2, 0, 64), (0, 1, 64)] {
            let (pebble, mut rx) = connected(Some(WEATHER_DB_V4_CAPABILITY), 0);
            preference(&pebble, 12, "unitsDistance", &[imperial]);
            preference(&pebble, 12, "unitsTemperature", &[temp]);
            let _ = packet(&mut rx).await;
            let _ = packet(&mut rx).await;
            let task = {
                let p = pebble.clone();
                tokio::spawn(async move {
                    p.push_weather_forecast(
                        &[7; 16],
                        &WeatherForecast {
                            current_temp: 18,
                            ..Default::default()
                        },
                    )
                    .await
                })
            };
            let weather = packet(&mut rx).await;
            assert_eq!(
                i16::from_le_bytes(weather[28..30].try_into().unwrap()),
                expected
            );
            reply(&pebble, &weather, BlobDBStatus::Success);
            let prefs = packet(&mut rx).await;
            reply(&pebble, &prefs, BlobDBStatus::Success);
            assert!(task.await.unwrap().is_ok());
            pebble.disconnect().await;
        }
    }

    /// Initial synchronization and duplicate writebacks do not queue an extra weather refresh.
    #[tokio::test]
    async fn only_effective_unit_changes_wake_the_weather_provider() {
        let (pebble, _rx) = connected(Some(WEATHER_DB_V4_CAPABILITY), 0);
        preference(&pebble, 12, "unitsTemperature", &[2]);
        let mut units = pebble.watch_weather_units();
        assert!(!units.has_changed().unwrap());
        preference(&pebble, 12, "unitsTemperature", &[2]);
        assert!(!units.has_changed().unwrap());
        preference(&pebble, 12, "unitsTemperature", &[1]);
        assert!(units.has_changed().unwrap());
        units.borrow_and_update();
        preference(&pebble, 12, "unitsTemperature", &[1]);
        preference(&pebble, 12, "unitsDistance", &[1]); // explicit Celsius wins
        preference(&pebble, 7, "unitsTemperature", &[2]); // wrong DB
        preference(&pebble, 12, "clock24h", &[1]);
        assert!(!units.has_changed().unwrap());
        preference(&pebble, 12, "unitsTemperature", &[0]); // automatic now inherits imperial
        assert!(units.has_changed().unwrap());
        units.borrow_and_update();
        preference(&pebble, 12, "unitsTemperature", &[0]);
        assert!(!units.has_changed().unwrap());
        pebble.disconnect().await;
    }

    /// Drive the acknowledged solar pin insert, deletion, and missing-key paths end to end.
    #[tokio::test]
    async fn weather_updates_and_deletes_solar_pins_after_locations_are_accepted() {
        use crate::{WeatherDailyForecast, endpoints::weather::WEATHER_APP_CAPABILITY};
        let (pebble, mut rx) =
            connected(Some(WEATHER_DB_V4_CAPABILITY | WEATHER_APP_CAPABILITY), 1);
        let task = {
            let p = pebble.clone();
            tokio::spawn(async move {
                p.push_weather_forecast(
                    &[7; 16],
                    &WeatherForecast {
                        is_current_location: true,
                        sunrise_sunset_pins: true,
                        daily: vec![WeatherDailyForecast {
                            sunrise_utc: Some(1750001000),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                )
                .await
            })
        };
        let weather = packet(&mut rx).await;
        reply(&pebble, &weather, BlobDBStatus::Success);
        let prefs = packet(&mut rx).await;
        reply(&pebble, &prefs, BlobDBStatus::Success);
        for slot in 0..6 {
            let pin = packet(&mut rx).await;
            assert_eq!(pin[7], BlobDBId::Pin as u8);
            if slot == 0 {
                assert_eq!(pin[4], 0x0d);
                assert_eq!(pin[31 + 38], 2);
                reply(&pebble, &pin, BlobDBStatus::Success);
            } else {
                assert_eq!(pin[4], 0x04);
                reply(&pebble, &pin, BlobDBStatus::KeyDoesNotExist);
            }
        }
        assert!(task.await.unwrap().is_ok());
        assert!(pebble.inner.lock().unwrap().blobdb_pending.is_empty());
        pebble.disconnect().await;
    }

    /// Run real weather writes through both BlobDB command formats and the v4 rejection fallback.
    #[tokio::test]
    async fn weather_waits_for_acceptance_and_retries_rejected_v4_as_v3() {
        for blob_version in [0, 1] {
            let (pebble, mut rx) = connected(Some(WEATHER_DB_V4_CAPABILITY), blob_version);
            let task = {
                let p = pebble.clone();
                tokio::spawn(async move {
                    p.push_weather_forecast(
                        &[7; 16],
                        &WeatherForecast {
                            location_name: "Test".into(),
                            ..Default::default()
                        },
                    )
                    .await
                })
            };
            let first = packet(&mut rx).await;
            let blob_offset = 4 + if blob_version == 0 { 23 } else { 27 };
            assert_eq!(first[4], if blob_version == 0 { 1 } else { 13 });
            assert_eq!(first[7], BlobDBId::Weather as u8);
            assert_eq!(first[blob_offset], 4);
            assert!(!task.is_finished());
            reply(&pebble, &first, BlobDBStatus::InvalidData);
            let fallback = packet(&mut rx).await;
            assert_eq!(fallback[blob_offset], 3);
            reply(&pebble, &fallback, BlobDBStatus::Success);
            let prefs = packet(&mut rx).await;
            assert_eq!(prefs[7], BlobDBId::AppConfigs as u8);
            assert!(!task.is_finished());
            reply(&pebble, &prefs, BlobDBStatus::Success);
            assert!(
                timeout(Duration::from_secs(1), task)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_ok()
            );
            pebble.disconnect().await;
        }
    }

    /// A failed capability query safely uses v3; a rejected v3 write reaches the caller.
    #[tokio::test]
    async fn unavailable_capabilities_use_v3_and_report_watch_rejection() {
        let (pebble, mut rx) = connected(None, 0);
        let task = {
            let p = pebble.clone();
            tokio::spawn(async move {
                p.push_weather_forecast(&[7; 16], &WeatherForecast::default())
                    .await
            })
        };
        let version = packet(&mut rx).await;
        assert_eq!(
            u16::from_be_bytes(version[2..4].try_into().unwrap()),
            Endpoint::WatchVersion as u16
        );
        super::super::dispatch::on_pebble_message(
            pebble_pack(Endpoint::WatchVersion, &[1]).unwrap(),
            &pebble.inner,
        );
        let weather = packet(&mut rx).await;
        assert_eq!(weather[4 + 23], 3);
        reply(&pebble, &weather, BlobDBStatus::GeneralFailure);
        assert!(
            task.await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("rejected weather record")
        );
        assert!(pebble.inner.lock().unwrap().blobdb_pending.is_empty());
        pebble.disconnect().await;
    }

    /// Link loss fails pending weather immediately and forgets the old watch's capabilities.
    #[tokio::test]
    async fn disconnect_cancels_weather_wait_and_clears_capabilities() {
        let (pebble, mut rx) = connected(Some(WEATHER_DB_V4_CAPABILITY), 0);
        let task = {
            let p = pebble.clone();
            tokio::spawn(async move {
                p.push_weather_forecast(&[7; 16], &WeatherForecast::default())
                    .await
            })
        };
        let _ = packet(&mut rx).await;
        pebble.disconnect().await;
        assert!(
            timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        let inner = pebble.inner.lock().unwrap();
        assert!(inner.watch_capabilities.is_none());
        assert!(inner.blobdb_pending.is_empty());
    }
}
