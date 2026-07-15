# cobble

Talk to a Pebble smartwatch over Bluetooth Low Energy from Linux — as a Rust
library, a long-lived Rust daemon, and a Python client other apps use.

The watch gives you exactly **one** BLE link, so exactly one process can own
it. That process is the daemon (`cobbled`); everything else talks to the daemon over D-Bus.

## Components

```
crates/libpebble-ble   Rust BLE/protocol library. Owns BlueZ (via bluer),
                       the PPoGATT GATT server, pairing, AppMessage, and all
                       endpoint codecs. Knows nothing about D-Bus or the daemon.
          ↑
crates/cobbled         Rust daemon. Wraps one libpebble-ble Pebble instance,
                       exports org.cobble.Daemon on the session bus, handles
                       reconnection, forwards desktop notifications to the watch.
          ↑
packages/              Python client. cobble-client is the only Python
cobble-client          package; it wraps the D-Bus proxy behind the same API
                       libpebble-ble exposes (same decorators, same AppMessage
                       dict, same u8/u16/u32/i8/i16/i32 width wrappers).
```

The library never learns the daemon exists. The client never opens a BLE link.

## Rust library structure

```
crates/libpebble-ble/src/
  pebble.rs            High-level Pebble struct: connect lifecycle, endpoint
                       dispatch, AppMessage API, scan.

  transport/           BLE transport layer.
    agent.rs           BlueZ auto-accept pairing agent (registered during
                       first-time bonding; only accepts the configured address).
    gatt_server.rs     Phone-hosted PPoGATT GATT server (BlueZ peripheral).
                       The watch connects back to this as a GATT client.
    ppogatt.rs         PPoGATT framing, windowed sequence numbers, reassembly.

  endpoints/           One file per Pebble Protocol endpoint.
    mod.rs             Endpoint enum, pebble_pack/pebble_unpack framing.
    app_message.rs     AppMessage PUSH/ACK/NACK encode and decode (endpoint 48).
    app_run_state.rs   Launch/stop watchapps (endpoint 52).
    blob_db.rs         BlobDB inserts, notification builder, weather blobs,
                       and BlobDB2 bidirectional sync protocol (endpoints
                       0xb1db / 0xb2db).
    datalog.rs         DataLog session protocol — used by health data sync
                       (endpoint 0x11).
    health.rs          Health/settings blob decode + encode: activityPreferences
                       (height/weight/age/gender), hrmPreferences, heartRate
                       zones, unitsDistance, and the HealthSync request.
    watch_pref.rs      General watch-settings (WatchPrefs) typed registry —
                       decode db-12 keys (backlight, clock, vibration, …).
    music.rs           Music control (32): push now-playing/playback/volume to
                       the watch; parse inbound media-key actions.
    phone_version.rs   Phone capability advertisement (endpoint 17).
    ping.rs            Ping/Pong (endpoint 2001).
    system.rs          WatchVersion (16), SystemMessage (18), and factory
                       registry / watch color (5001): firmware version, board,
                       serial, platform, capabilities, color.
    reset.rs           Reboot / recovery / factory reset / core dump (2003).
    screenshot.rs      Capture + decode the watch framebuffer (8000): 1-bit B/W
                       or 8-bit Pebble color -> RGBA.
    time.rs            UTC clock sync (endpoint 11).

  error.rs             PebbleError.
  uuids.rs             All Pebble and PPoGATT GATT UUIDs, plus system app
                       UUIDs (weather, health, notifications, etc.).
```

Adding a new endpoint: create `endpoints/<name>.rs`, add it to
`endpoints/mod.rs`, and add a match arm in `pebble.rs::on_pebble_message`.

## Liveness — two independent questions

* **Is the daemon process alive?** Its well-known bus name (`org.cobble.Daemon`)
  has an owner. `CobbleClient.is_daemon_running()` checks this with
  `NameHasOwner` — no socket connect, no timeout, no stale pidfile.
* **Is the watch reachable?** The daemon's `Connected` property +
  `ConnectionChanged` signal. `CobbleClient.connected` / `is_connected()`.

A daemon can be alive while the watch is out of range; apps need to check both.

## Quick start

The config file is optional — the daemon starts with defaults if it doesn't exist.
The path follows the XDG Base Directory spec: `$XDG_CONFIG_HOME/cobbled/config.toml`,
which defaults to `~/.config/cobbled/config.toml` when `XDG_CONFIG_HOME` is not set.

You can create it with just the watch address (everything else has sane defaults):

```sh
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/cobbled"
config_path="${XDG_CONFIG_HOME:-$HOME/.config}/cobbled/config.toml"
install -m 600 /dev/null "$config_path"
cat > "$config_path" << 'EOF'
# address = "E6:94:0A:D4:D5:DC"   # your watch Bluetooth address
# adapter = "hci0"                # optional, default is hci0
# verbose = false                  # optional, or use -v at runtime
# db = "/custom/path/cobbled.db"   # optional, default is XDG_DATA_HOME/cobbled/cobbled.db

# Intervals.icu wellness export is disabled by default. Uncomment and fill in
# both credentials to enable the integration.
# [integrations.intervals_icu]
# enabled = true
# athlete_id = "i123456"
# api_key = "replace-with-personal-api-key"
EOF
```

Run the daemon (owns the BLE link, syncs time, forwards desktop notifications):

```sh
cobbled                          # reads ~/.config/cobbled/config.toml
cobbled --verbose                # TRACE-level logging (overrides config)
cobbled --config /other/path.toml  # use a different config file
```

Any Python app talks to it without touching BLE or D-Bus:

```python
import asyncio
from cobble_client import CobbleClient, u16

async def main():
    async with CobbleClient() as cobble:
        @cobble.on_app_message
        def handler(app_uuid, data):
            print("from watch:", app_uuid, data)

        await cobble.send_app_message(
            "00000000-0000-0000-0000-000000000000",
            {0: "hello", 1: u16(150)},
        )
        await asyncio.sleep(60)

asyncio.run(main())
```

## Building

```sh
# Build everything (Rust)
cargo build --release

# Run tests
cargo test

# Build and run the daemon directly (config file must exist first)
cargo run --bin cobbled

# Python client: install dependencies and run tests
uv sync --all-packages
uv run pytest
```

## Installing the daemon

```sh
# Build the release binary
cargo build --release

# Copy the binary somewhere on your PATH
sudo install -m755 target/release/cobbled /usr/local/bin/

# Or build the .deb (requires cargo-deb or debhelper setup)
dpkg-buildpackage -us -uc -b
sudo apt install ./cobbled_*_*.deb
```

### Configure the daemon

Create `$XDG_CONFIG_HOME/cobbled/config.toml` (defaults to
`~/.config/cobbled/config.toml` when `XDG_CONFIG_HOME` is not set).
The daemon starts without it — it will wait for a config change or
D-Bus `ReloadConfig` before attempting a watch connection.

The GUI (`cobble`) can scan for Pebble devices and write the config for you.

When configuring the file manually, create it with mode `0600` before adding
credentials (for example, `install -m 600 /dev/null "${XDG_CONFIG_HOME:-$HOME/.config}/cobbled/config.toml"`),
or run `chmod 600` on an existing file first. Keep the file owner-only readable
and writable so the stored API key is not exposed to other local users.

```toml
# $XDG_CONFIG_HOME/cobbled/config.toml
# address = "E6:94:0A:D4:D5:DC"   # your watch Bluetooth address (optional — daemon starts without it)
# adapter = "hci0"               # optional, default hci0
# verbose = false                 # optional
# db = "/custom/path/cobbled.db"  # optional, default XDG_DATA_HOME/cobbled/cobbled.db

# Optional Intervals.icu wellness export. The GUI exposes these same fields.
# [integrations.intervals_icu]
# enabled = true
# athlete_id = "i123456"
# api_key = "replace-with-personal-api-key"
```

Intervals.icu export is disabled unless `enabled = true` and both credentials
are present. The daemon stores the API key in the TOML file for now; the GUI
writes that file with mode `0600` on Unix and the daemon never logs or stores
the key in its SQLite export ledger. Enabling a new athlete account performs a
full local-history backfill. Disabling the integration stops outbound requests
without deleting local data or export state.

Start as a user service (must be a user service — the notification monitor
connects to your session D-Bus, which only exists inside your login):

```sh
systemctl --user daemon-reload
systemctl --user enable --now cobbled.service
```

### Platform notes

* **dbus-broker systems**: The notification monitor uses `BecomeMonitor`
  (the dbus-broker-compatible API) and falls back to `eavesdrop=true`
  AddMatch on older `dbus-daemon` installs.

* **BlueZ `AccessDenied`**: add yourself to the `bluetooth` group and start a
  fresh session: `sudo usermod -aG bluetooth "$USER"`, then log out and back in.

## D-Bus interface (`org.cobble.Daemon`)

Object path: `/org/cobble/Daemon` — session bus.

| Kind | Name | Signature | Notes |
|------|------|-----------|-------|
| Property | `Connected` | `b` | watch BLE link is up |
| Property | `WatchAddress` | `s` | configured watch address |
| Property | `BatteryLevel` | `n` | watch battery percentage (0–100), or -1 if unknown |
| Method | `SendAppMessage` | `(s, a{i(sv)}, b) → u` | uuid, data, wait_ack → txn |
| Method | `LaunchApp` | `(s)` | uuid |
| Method | `StopApp` | `(s)` | uuid |
| Method | `UpdateTime` | `()` | sync watch clock to system time |
| Method | `Notify` | `(s, s, s) → u` | title, body, subtitle → token |
| Method | `Ping` | `() → b` | daemon liveness probe |
| Method | `Scan` | `(d) → a(ss)` | timeout\_secs → [(address, name)] |
| Method | `ActivateHealth` | `(q, q, y, y, b)` | height\_cm, weight\_kg, age, gender (0=female 1=male 2=other), hrm\_enabled |
| Method | `FetchHealthData` | `()` | flush pending health records from watch |
| Method | `FetchHealthParams` | `()` | re-sync watch settings (health + general) from watch |
| Method | `GetHealthProfile` | `() → a{sv}` | watch health profile: height/weight/age/gender, HRM, HR zones, units |
| Method | `GetWatchSettings` | `() → a{sv}` | general watch settings (backlight, clock, vibration, quiet time, …) |
| Method | `GetWatchVersion` | `() → a{sv}` | firmware version, board, serial, BT address, language, capabilities, platform |
| Method | `GetWatchColor` | `() → a{sv}` | watch color/variant (protocol\_number, js\_name, description, watch\_type, supports\_hrm) |
| Method | `Screenshot` | `() → ay` | capture the watch screen as PNG bytes |
| Method | `SetMusicPlayerInfo` | `(s, s)` | pkg, name — which media app is playing |
| Method | `SetMusicTrack` | `(s, s, s, u, u, u)` | artist, album, title, track\_length\_ms, track\_count, track\_number |
| Method | `SetMusicPlaybackState` | `(y, u, u, y, y)` | state (0=paused 1=playing 2=rewind 3=ffwd 4=unknown), track\_position\_ms, play\_rate\_pct, shuffle (0=unknown 1=off 2=on), repeat (0=unknown 1=off 2=one 3=all) |
| Method | `SetMusicVolume` | `(y)` | volume\_percent (0–100) |
| Method | `RebootWatch` | `()` | reboot the watch |
| Method | `ResetIntoRecovery` | `()` | reboot into recovery (PRF) firmware |
| Method | `CreateCoreDump` | `()` | trigger a watch core dump |
| Method | `FactoryReset` | `(b)` | DESTRUCTIVE — wipe + unpair; requires `confirm = true` |
| Method | `Forget` | `()` | remove the Bluetooth bond (unpair); re-pairs on next reconnect |
| Method | `PushIncomingCall` | `(u, s, s)` | cookie, caller_number, caller_name — show incoming call screen on watch |
| Method | `PushMissedCall` | `(u, s, s)` | cookie, caller_number, caller_name — missed call notification |
| Method | `PushCallStart` | `(u)` | cookie — transition watch to in-call screen |
| Method | `PushCallEnd` | `(u)` | cookie — end call on watch |
| Method | `ReprocessHealthData` | `()` | rebuild derived health tables from raw blobs |
| Method | `PushWeather` | `(ay, s, s, n, y, n, n, y, n, n, b)` | location\_key (16 bytes), location\_name, forecast\_short, current\_temp\_c, current\_weather, today\_high\_c, today\_low\_c, tomorrow\_weather, tomorrow\_high\_c, tomorrow\_low\_c, is\_current\_location. Weather types: 0=PartlyCloudy 1=CloudyDay 2=LightSnow 3=LightRain 4=HeavyRain 5=HeavySnow 6=Generic 7=Sun 8=RainAndSnow |
| Method | `ReloadConfig` | `()` | re-read config; disconnects if address/adapter changed. Also called automatically by the filesystem watcher. |
| Method | `SyncWellness` | `()` | request one serialized Intervals.icu reconciliation; fails when the integration is disabled/invalid or the app database is unavailable |
| Method | `GetWellnessSyncStatus` | `() → a{sv}` | live and durable account-scoped status; includes `enabled`, `configured`, `valid`, `running`, `athlete_id`, hash-aware `exported_dates`/`pending_dates`, `last_success`, `last_error`, and `last_error_at` |
| Signal | `AppMessageReceived` | `(s, a{i(sv)})` | uuid, data |
| Signal | `AckReceived` | `(u)` | txn |
| Signal | `NackReceived` | `(u)` | txn |
| Signal | `ConnectionChanged` | `(b)` | connected |
| Signal | `HealthDataReceived` | `(u, ay, u, u, u, y, q, ay)` | tag, app\_uuid, session\_timestamp, items\_left, crc, item\_type, item\_size, data |
| Signal | `HealthProfileReceived` | `(a{sv})` | watch health profile, emitted on connect and on change |
| Signal | `WatchSettingReceived` | `(s, v)` | key, value — emitted per general watch setting as it syncs |
| Signal | `BatteryChanged` | `(n)` | watch battery percentage (-1 = unknown) |
| Signal | `AppRunStateChanged` | `(s, b)` | app uuid, running — emitted when an app opens/closes on the watch |
| Signal | `MusicActionReceived` | `(s)` | media-control action from the watch (play, pause, play\_pause, next\_track, previous\_track, volume\_up, volume\_down, get\_current\_track) |
| Signal | `PhoneActionReceived` | `(s, u)` | action (answer/hangup), cookie — emitted when watch sends phone action |

AppMessage values cross D-Bus as `(tag, variant)` pairs where tag is one of
`u8 u16 u32 i8 i16 i32 uint int str bytes`. The Python client handles all
marshalling transparently.

Health data is stored automatically in SQLite at
`$XDG_DATA_HOME/cobbled/cobbled.db` (or the path set in `config.toml`).
The `HealthDataReceived` signal fires for each batch so external tools can
consume raw records without reading the database directly.

### Intervals.icu wellness export

The exporter owns only the fields it sends: `steps`, `sleepSecs`, and
`avgSleepingHR`. Missing local observations are omitted from a request, so
they do not clear unrelated remote wellness fields. The worker scans local
history on startup and configuration changes, reconciles changed dates using a
durable payload-hash ledger, and performs bounded bulk uploads. Successful
health-data persistence wakes it early; an hourly reconciliation remains as a
safety net.

Use the GUI’s **Sync Now** control or call `SyncWellness` to request an
immediate run. The GUI polls until the serialized reconciliation finishes.
`GetWellnessSyncStatus` reports whether work is currently running, the current
account, and the latest durable success/error summary. Exported and pending
counts compare current local payload hashes with the successful ledger, so
newly discovered and locally changed dates are pending even before an upload
attempt. Error text is sanitized and contains no response body, authorization
header, or API key.

#### Manual verification checklist

Run this checklist with a dedicated test athlete/account and a dedicated test
date. Do not use production credentials in a repository, issue, log, or screen
capture.

1. Record the remote wellness document for the test date before syncing,
   including fields that Cobble does not own (for example, mood, calories, or
   notes).
2. Configure the test account through the GUI or the TOML example above.
   Confirm that the API key field is masked in the GUI, the configuration file
   is readable only by its owner on Unix, and daemon logs contain no key.
3. Trigger `SyncWellness` or the GUI’s **Sync Now** control. Confirm that the
   request updates only `steps`, `sleepSecs`, and `avgSleepingHR`; missing local
   observations are omitted rather than sent as zeroes or nulls.
4. Compare the remote document with the baseline. Confirm that unrelated
   fields are unchanged, and query `GetWellnessSyncStatus` to confirm the
   account, exported-date count, and successful-sync timestamp.
5. Trigger the same sync again and restart the daemon. Confirm that the
   durable ledger prevents an unchanged date from being treated as new work.
6. Add or correct a local health record for the test date, trigger another
   sync, and confirm that only the corresponding owned field changes remotely.
7. Temporarily use an invalid key. Confirm that the status and logs expose
   only a sanitized error, then restore the key and reload the configuration
   without losing the local health data.
8. Change the athlete ID and confirm that the new account is backfilled. Disable
   the integration and confirm that outbound requests stop while local state
   remains available; re-enable it only after cleanup.
9. Remove the dedicated test data or reset the test account according to the
   provider’s policy.

This checklist requires a reachable Intervals.icu test account and is not part
of the automated build; no live provider test is run by default.

#### Field ownership and conflict policy

Cobble is authoritative only for the fields it emits: `steps`, `sleepSecs`,
and `avgSleepingHR`. A successful reconciliation can replace those three
remote values with the current local aggregates. It does not merge, clear, or
otherwise modify fields outside that set. When a local observation is missing,
the corresponding field is omitted from the partial update so the existing
remote value is preserved.

There is no conflict detection for another service writing one of Cobble’s
owned fields: the last successful writer wins. Configure a single owner for
those fields, or disable the Cobble integration if another service should be
authoritative. The per-account, per-date ledger prevents re-sending an
unchanged Cobble payload, but it is not a remote conflict-resolution system.

## Supported features

### libpebble-ble
- [x] Connect via BLE (pairing, reconnect, MTU/connectivity handshake)
- [x] Pings
- [x] App launch / stop (+ inbound run-state events)
- [x] AppMessage
- [x] Time sync
- [ ] Notifications
  - [x] Send
  - [ ] Actions
  - [x] Categorization (Text/Call/Other)
- [x] Phone calls
  - [x] Actions
- [x] Weather (Open-Meteo auto-fetch, GeoClue2/ipapi.co location, 3h refresh, connection-gated)
- [x] Health
  - [x] Steps
  - [x] Sleep
  - [x] Heartrate
- [x] Watch settings
  - [x] Health profile read (height/weight/age/gender/HRM/HR zones/units)
  - [x] General settings read (backlight, clock, vibration, quiet time, …)
- [x] Watch info (firmware version, board, serial, BT address, capabilities, platform, color)
- [x] Battery level (read + change notifications)
- [x] Screenshot (capture watch screen, decoded to RGBA pixels; PNG encoding lives in cobbled)
- [x] Device management (reboot, recovery, factory reset, core dump, forget/unpair)
- [x] Music
  - [x] Push now-playing / playback state / volume to the watch
  - [x] Parse inbound control actions (play/pause/next/volume)
- [ ] PBW install

### cobbled (Daemon)
- [x] Pings
- [x] Reconnects
- [x] Time Sync
- [ ] Notifications
  - [x] Forwarding
  - [ ] Actions (Dismiss)
  - [x] Categorizations
- [x] Phonecalls (ModemManager / oFono bridge, incoming call → watch + watch answer/hangup → modem)
  - [x] Actions
- [x] AppMessages
  - [x] External applications
- [x] Health (data sync + profile/settings read)
- [x] Intervals.icu wellness export (durable backfill, retries, GUI status/control)
- [x] Watch info + device management (version, color, battery, screenshot, reboot/reset/forget)
- [x] Music push + MPRIS auto-discovery (desktop players → watch metadata + watch controls → desktop player, system volume via pactl/wpctl)
- [x] Weather (Open-Meteo auto-fetch, GeoClue2/ipapi.co location, 3h refresh, connection-gated)

Every libpebble-ble capability is exposed over D-Bus and supported by the
Python client — see the [D-Bus interface](#d-bus-interface-orgcobbledaemon) table.


## Why one repo

The daemon and Python client must agree on the D-Bus wire contract (bus name,
object path, interface name, AppMessage value encoding). A monorepo makes a
contract change one atomic commit that covers both ends at once.
