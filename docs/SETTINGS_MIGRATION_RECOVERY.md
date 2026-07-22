# Settings migration and recovery

This document covers the versioned Daemon Config and Device Config APIs introduced by the unreleased Cobble settings work.

## Compatibility contract

Applications should depend on `cobble-client`, not on `libpebble-ble` or daemon implementation crates. Both public settings dictionaries currently report API version `1`.

- Daemon Config uses `GetDaemonConfig` and revision-guarded `UpdateDaemonConfig`.
- Device Config uses `GetDeviceConfig`, `RefreshDeviceConfig`, revision-guarded `UpdateDeviceConfig`, and `ResetDeviceConfigDefaults`.
- Rust and Python clients decode those dictionaries into matching typed snapshots.
- Unknown fields must be ignored by clients. An unsupported API version must be rejected rather than decoded as the current version.
- Writes include the revision that was read. A revision conflict means another writer or refresh changed the snapshot; refresh, reapply the intended edits, and retry.

No on-disk database migration is required for these APIs. Existing health data remains in the configured SQLite database. Existing `config.toml` keys remain compatible.

## Configuration paths and application state

The daemon is authoritative for its effective config file. Use `GetDaemonConfig.config_path` instead of assuming the default location. By default it is `${XDG_CONFIG_HOME:-~/.config}/cobbled/config.toml`; `cobbled --config PATH` selects another file.

The database path is resolved by the daemon. An omitted `db` setting uses `${XDG_DATA_HOME:-~/.local/share}/cobbled/cobbled.db`. Cobble reopens its read-side database when the daemon reports a different resolved path.

Apply dispositions explain what happened to each changed field:

- `applied_live`: active immediately.
- `reconnecting`: saved and applied through a watch reconnect.
- `gui_data_source_reopen_required`: the GUI must reopen its database source.
- `daemon_restart_required`: saved, but the running daemon still uses the previous value.
- `daemon_and_gui_restart_required`: both processes must restart or reopen state.

## Recovering from common failures

### The daemon will not start

Run `cobbled --config PATH -v` in a terminal and inspect the first configuration or database error. Check that the parent directories are writable and that the configured database is a regular SQLite file. Cobble does not silently replace malformed TOML with defaults.

To recover malformed configuration, move the invalid file aside, start the daemon with the intended `--config` path, and re-enter settings. Keep the old file until required address, adapter, integration, or database values have been recovered.

### Cobble reports that the daemon is unavailable

Start or restart the `cobbled` user service, then use Refresh. If the daemon uses a non-default config path, ensure the service and any manually launched process use the same `--config PATH`. Do not run two daemons against the same watch.

### A save reports a revision conflict

Refresh the page. Confirm the externally changed values, reapply only the intended edits, and save again. Do not blindly retry the stale patch because doing so could overwrite another writer.

### Device Config is partial or unsupported

`partial` means the firmware cannot provide a complete BlobDB2 readback; only reported fields are editable. `unsupported` means the connected firmware does not provide the required preference database. Unknown and unreported fields are never synthesized or written.

### A write disconnects, times out, or is rejected

Reconnect the watch and refresh before retrying. Modern watches are read back after writes; a readback mismatch is an error. Legacy watches can acknowledge a write without supporting complete readback, so verify the changed setting on the watch.

### Reset to Defaults

Reset affects only observed, editable general preferences and uses daemon/library registry defaults. It does not reset health/profile data, unknown records, Quiet Time schedules, state markers, or unsupported firmware variants. Factory Reset is a separate destructive Device Action.

## Physical-watch verification matrix

The automated suite verifies codecs, registry validation, revision/error handling, ordered-write failure behavior, client decoding, and the exported D-Bus contract on a private session bus. It does not emulate firmware or Bluetooth timing.

| Target | Required checks | Status for this phase |
|---|---|---|
| Legacy/non-BlobDB2 watch | Partial snapshot, accepted writes without complete readback, legacy health and backlight variants | Not run; hardware required |
| Basalt or Chalk | Color display, legacy vibration, arbitrary existing backlight color preservation | Not run; hardware required |
| Diorite | Health fields at firmware cutoffs and unsupported HRM fields | Not run; hardware required |
| Emery | HRM availability and text-size offset read/write symmetry | Not run; hardware required |
| Gabbro/current hardware | Current backlight variants, built-in language, text-size offset | Not run; hardware required |
| Any supported watch | Disconnect during refresh, write, and readback | Not run; hardware required |
| Any supported watch | Rejected preference, stale revision, daemon restart, malformed config recovery | Stale revision/config paths automated in focused tests; watch rejection and restart UX require manual verification |

Record the watch platform, firmware version, BlobDB version, tested commit, and result when completing a hardware row. Do not mark the matrix complete based solely on synthetic fixtures.
