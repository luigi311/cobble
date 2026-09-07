//! Location acquisition: desktop Location portal → IP geolocation with DB cache.
//!
//! * **Location portal**: asks the desktop for a city-level location with the
//!   user's permission.
//! * **ifconfig.me**: gets the current public IP address.
//! * **ipapi.co** (free HTTPS API): city-level IP geolocation, cached in the
//!   database to avoid rate limits.
//! * **Nominatim** (OpenStreetMap): reverse geocodes coordinates to a city name.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ashpd::desktop::location::{Accuracy, CreateSessionOptions, LocationProxy};
use futures::StreamExt;
use tracing::debug;

use crate::http;
use cobble_db::{AppDb, IpLocation};

/// Get coordinates and a human-readable city name.
///
/// Tries the desktop Location portal first. Falls back to IP-based geolocation
/// (cached in the database, fetched from ipapi.co only if not already cached
/// for the current IP).
pub async fn get_location(db: Option<Arc<Mutex<AppDb>>>) -> anyhow::Result<(f64, f64, String)> {
    match try_location_portal().await {
        Ok(location) => return Ok(location),
        Err(e) => debug!("Location portal unavailable ({e}); falling back to IP geolocation"),
    }
    try_ip_geolocation(db).await
}

async fn try_location_portal() -> anyhow::Result<(f64, f64, String)> {
    let proxy = LocationProxy::new()
        .await
        .map_err(|e| anyhow::anyhow!("Location portal: {e}"))?;
    let session = proxy
        .create_session(CreateSessionOptions::default().set_accuracy(Accuracy::City))
        .await
        .map_err(|e| anyhow::anyhow!("Location portal CreateSession: {e}"))?;
    let location = async {
        let mut updates = proxy
            .receive_location_updated()
            .await
            .map_err(|e| anyhow::anyhow!("Location portal LocationUpdated: {e}"))?;

        // Poll Start and LocationUpdated together so an immediate update cannot race
        // with signal subscription. A generous timeout leaves time for a first-run
        // permission prompt without holding up the IP fallback indefinitely.
        tokio::time::timeout(Duration::from_secs(30), async {
            let (_, location) = tokio::try_join!(
                async {
                    proxy
                        .start(&session, None, Default::default())
                        .await
                        .map_err(|e| anyhow::anyhow!("Location portal Start: {e}"))?
                        .response()
                        .map_err(|e| anyhow::anyhow!("Location portal permission: {e}"))?;
                    Ok::<(), anyhow::Error>(())
                },
                async {
                    updates
                        .next()
                        .await
                        .ok_or_else(|| anyhow::anyhow!("Location portal update stream ended"))
                }
            )?;
            Ok::<_, anyhow::Error>(location)
        })
        .await
        .map_err(|_| anyhow::anyhow!("Location portal update timed out"))?
    }
    .await;

    if let Err(e) = session.close().await {
        debug!("Location portal session close failed: {e}");
    }

    let location = location?;
    let lat = location.latitude();
    let lon = location.longitude();
    let name = reverse_geocode(lat, lon).await?;
    Ok((lat, lon, name))
}

// ── IP geolocation (with DB cache) ──────────────────────────────────────

async fn try_ip_geolocation(db: Option<Arc<Mutex<AppDb>>>) -> anyhow::Result<(f64, f64, String)> {
    // 1. Get current public IP.  ifconfig.me/ip returns bare-IP plaintext;
    // the root path now returns an HTML page since mid-2026.
    let ip = match http::http_get_text("https://ifconfig.me/ip").await {
        Ok(ip) if looks_like_ip(&ip) => ip,
        Ok(raw) => {
            debug!("ifconfig.me returned non-IP response ({raw:?}); using ipapi directly");
            return fetch_ipapi_and_build().await;
        }
        Err(_) => {
            // Can't get IP — fall back to uncached ipapi if DB is available,
            // or just call ipapi directly.
            return fetch_ipapi_and_build().await;
        }
    };

    // 2. Check the database cache.
    if let Some(ref db) = db
        && let Some(loc) = db.lock().unwrap().lookup_ip_location(&ip)
    {
        let name = location_name(&loc.city);
        debug!("weather: cached IP location ({name})");
        return Ok((loc.latitude, loc.longitude, name));
    }

    // 3. Not cached — fetch from ipapi.co.
    debug!("weather: IP location not cached; querying ipapi.co");
    let (lat, lon, city, region) = fetch_ipapi_raw().await?;
    let name = location_name(&city);

    // 4. Store in cache if DB is available.
    if let Some(ref db) = db {
        let loc = IpLocation {
            latitude: lat,
            longitude: lon,
            city,
            region,
        };
        if let Err(e) = db.lock().unwrap().store_ip_location(&ip, &loc) {
            tracing::warn!("weather: failed to cache IP location: {e}");
        }
    }

    Ok((lat, lon, name))
}

/// Fetch raw data from ipapi.co.  Returns (lat, lon, city, region).
async fn fetch_ipapi_raw() -> anyhow::Result<(f64, f64, String, String)> {
    let body = http::http_get("https://ipapi.co/json/").await?;
    let json: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| anyhow::anyhow!("ipapi parse: {e}"))?;

    let lat = json["latitude"]
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("ipapi: missing latitude"))?;
    let lon = json["longitude"]
        .as_f64()
        .ok_or_else(|| anyhow::anyhow!("ipapi: missing longitude"))?;
    let city = json["city"].as_str().unwrap_or("").to_string();
    let region = json["region"].as_str().unwrap_or("").to_string();

    Ok((lat, lon, city, region))
}

/// Fetch from ipapi and build the location result directly (fallback when
/// we can't get our current IP).
async fn fetch_ipapi_and_build() -> anyhow::Result<(f64, f64, String)> {
    let (lat, lon, city, _) = fetch_ipapi_raw().await?;
    Ok((lat, lon, location_name(&city)))
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn location_name(city: &str) -> String {
    if city.is_empty() {
        "Current Location".into()
    } else {
        city.to_string()
    }
}

/// Quick validation: the response should look like an IP address, not an HTML page.
fn looks_like_ip(s: &str) -> bool {
    !s.is_empty()
        && !s.contains('<')
        && s.chars()
            .all(|c| c.is_ascii_hexdigit() || c == '.' || c == ':')
}

// ── Nominatim reverse geocoding ─────────────────────────────────────────

async fn reverse_geocode(lat: f64, lon: f64) -> anyhow::Result<String> {
    let url = format!(
        "https://nominatim.openstreetmap.org/reverse?lat={lat:.6}&lon={lon:.6}&format=json&zoom=10"
    );
    let body = http::http_get(&url).await?;
    let json: serde_json::Value = serde_json::from_str(&body)?;
    let address = &json["address"];

    let city = address["city"]
        .as_str()
        .or_else(|| address["town"].as_str())
        .or_else(|| address["village"].as_str())
        .unwrap_or("");

    if !city.is_empty() {
        Ok(city.to_string())
    } else {
        Ok("Current Location".to_string())
    }
}
