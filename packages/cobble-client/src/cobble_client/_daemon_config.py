"""Typed daemon-configuration contracts matching the Rust client."""

from dataclasses import dataclass
from enum import Enum

from ._device_config import ConfigError, ConfigErrorKind

CONFIG_API_VERSION = 1


@dataclass(frozen=True)
class IntervalsIcuConfig:
    enabled: bool
    athlete_id: str
    api_key_configured: bool


@dataclass(frozen=True)
class DaemonConfig:
    address: str
    adapter: str
    verbose: bool
    database_path: str | None
    intervals_icu: IntervalsIcuConfig


@dataclass(frozen=True)
class DaemonConfigSnapshot:
    api_version: int
    revision: int
    config_path: str
    active_database_path: str
    resolved_database_path: str
    active_verbose: bool
    config: DaemonConfig
    error: ConfigError | None


@dataclass(frozen=True)
class IntervalsIcuPatch:
    enabled: bool | None = None
    athlete_id: str | None = None
    api_key: str | None = None
    clear_api_key: bool = False


@dataclass(frozen=True)
class DaemonConfigPatch:
    expected_revision: int
    address: str | None = None
    adapter: str | None = None
    verbose: bool | None = None
    database_path: str | None = None
    use_default_database_path: bool = False
    intervals_icu: IntervalsIcuPatch | None = None


class ApplyDisposition(str, Enum):
    APPLIED_LIVE = "applied_live"
    RECONNECTING = "reconnecting"
    GUI_DATA_SOURCE_REOPEN_REQUIRED = "gui_data_source_reopen_required"
    DAEMON_RESTART_REQUIRED = "daemon_restart_required"
    DAEMON_AND_GUI_RESTART_REQUIRED = "daemon_and_gui_restart_required"


@dataclass(frozen=True)
class DaemonConfigUpdate:
    snapshot: DaemonConfigSnapshot
    fields: dict[str, ApplyDisposition]


def decode_daemon_config(raw: dict) -> DaemonConfigSnapshot:
    api_version = int(raw["api_version"])
    if api_version != CONFIG_API_VERSION:
        raise ValueError(f"unsupported daemon-config API version {api_version}")
    database_path = str(raw["database_path"])
    error = None
    if message := raw.get("error_message"):
        error = ConfigError(kind=ConfigErrorKind.INVALID_DATA, message=str(message))
    return DaemonConfigSnapshot(
        api_version=api_version,
        revision=int(raw["revision"]),
        config_path=str(raw["config_path"]),
        active_database_path=str(raw["active_database_path"]),
        resolved_database_path=str(raw["resolved_database_path"]),
        active_verbose=bool(raw["active_verbose"]),
        config=DaemonConfig(
            address=str(raw["address"]), adapter=str(raw["adapter"]),
            verbose=bool(raw["verbose"]), database_path=database_path or None,
            intervals_icu=IntervalsIcuConfig(
                enabled=bool(raw["intervals_enabled"]),
                athlete_id=str(raw["intervals_athlete_id"]),
                api_key_configured=bool(raw["intervals_api_key_configured"]),
            ),
        ),
        error=error,
    )
