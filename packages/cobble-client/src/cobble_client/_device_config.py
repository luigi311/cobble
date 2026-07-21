"""Typed models and decoder for the versioned Device Config D-Bus snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Generic, TypeVar


class DeviceConfigState(StrEnum):
    DISCONNECTED = "disconnected"
    LOADING = "loading"
    READY = "ready"
    PARTIAL = "partial"
    ERROR = "error"
    UNSUPPORTED = "unsupported"


class FieldAvailability(StrEnum):
    AVAILABLE = "available"
    NOT_RECEIVED = "not_received"
    UNSUPPORTED = "unsupported"
    INVALID = "invalid"


class ConfigErrorKind(StrEnum):
    INTERNAL = "internal"
    NOT_SUPPORTED = "not_supported"
    DISCONNECTED = "disconnected"
    TIMEOUT = "timeout"


@dataclass(frozen=True)
class ConfigError:
    kind: ConfigErrorKind
    message: str
    field: str | None = None


T = TypeVar("T")


@dataclass(frozen=True)
class FieldValue(Generic[T]):
    availability: FieldAvailability
    value: T | None = None
    error: str | None = None


@dataclass(frozen=True)
class WatchIdentity:
    watch_id: str
    platform: str | None = None
    firmware: str | None = None


@dataclass(frozen=True)
class DeviceCapabilities:
    blob_db_version: int
    supported: frozenset[str]


@dataclass(frozen=True)
class HrmConfig:
    enabled: bool
    measurement_interval: int | None
    during_activity: bool | None


@dataclass(frozen=True)
class HeartRateThresholds:
    resting: int
    elevated: int
    maximum: int
    zone_1: int
    zone_2: int
    zone_3: int


@dataclass(frozen=True)
class HealthConfig:
    height_mm: int
    weight_dag: int
    tracking_enabled: bool
    activity_insights_enabled: bool
    sleep_insights_enabled: bool
    age: int
    gender: int
    distance_units: FieldValue[str]
    hrm: FieldValue[HrmConfig]
    heart_rate_thresholds: FieldValue[HeartRateThresholds]


@dataclass(frozen=True)
class PreferenceField:
    availability: FieldAvailability
    value: bool | int | str | None
    raw: bytes
    error: str | None = None


@dataclass(frozen=True)
class DeviceConfigSnapshot:
    api_version: int
    revision: int
    state: DeviceConfigState
    watch: WatchIdentity | None
    capabilities: DeviceCapabilities
    last_read_at_ms: int | None
    health: FieldValue[HealthConfig]
    preferences: dict[str, PreferenceField]
    error: ConfigError | None


def _availability(raw: dict[str, object], key: str) -> FieldAvailability:
    try:
        return FieldAvailability(str(raw.get(key, "not_received")))
    except ValueError:
        return FieldAvailability.INVALID


def decode_device_config(raw: dict[str, object]) -> DeviceConfigSnapshot:
    api_version = int(raw.get("api_version", -1))
    if api_version != 1:
        raise ValueError(f"unsupported device-config API version {api_version}")

    activity_availability = _availability(raw, "health.activity.availability")
    health_value = None
    if activity_availability is FieldAvailability.AVAILABLE:
        units_availability = _availability(raw, "health.units.availability")
        hrm_availability = _availability(raw, "health.hrm.availability")
        thresholds_availability = _availability(raw, "health.thresholds.availability")
        hrm = None
        if hrm_availability is FieldAvailability.AVAILABLE:
            hrm = HrmConfig(
                enabled=bool(raw["health.hrm.enabled"]),
                measurement_interval=(
                    int(raw["health.hrm.measurement_interval"])
                    if "health.hrm.measurement_interval" in raw
                    else None
                ),
                during_activity=(
                    bool(raw["health.hrm.during_activity"])
                    if "health.hrm.during_activity" in raw
                    else None
                ),
            )
        thresholds = None
        if thresholds_availability is FieldAvailability.AVAILABLE:
            thresholds = HeartRateThresholds(
                resting=int(raw["health.thresholds.resting"]),
                elevated=int(raw["health.thresholds.elevated"]),
                maximum=int(raw["health.thresholds.maximum"]),
                zone_1=int(raw["health.thresholds.zone1"]),
                zone_2=int(raw["health.thresholds.zone2"]),
                zone_3=int(raw["health.thresholds.zone3"]),
            )
        health_value = HealthConfig(
            height_mm=int(raw["health.height_mm"]),
            weight_dag=int(raw["health.weight_dag"]),
            tracking_enabled=bool(raw["health.tracking_enabled"]),
            activity_insights_enabled=bool(raw["health.activity_insights_enabled"]),
            sleep_insights_enabled=bool(raw["health.sleep_insights_enabled"]),
            age=int(raw["health.age"]),
            gender=int(raw["health.gender"]),
            distance_units=FieldValue(
                units_availability,
                str(raw["health.distance_units"])
                if "health.distance_units" in raw
                else None,
            ),
            hrm=FieldValue(hrm_availability, hrm),
            heart_rate_thresholds=FieldValue(thresholds_availability, thresholds),
        )

    preference_names = {
        key.removeprefix("preference.").removesuffix(".availability")
        for key in raw
        if key.startswith("preference.") and key.endswith(".availability")
    }
    preferences = {
        name: PreferenceField(
            availability=_availability(raw, f"preference.{name}.availability"),
            value=raw.get(f"preference.{name}.value"),
            raw=bytes(raw.get(f"preference.{name}.raw", b"")),
        )
        for name in preference_names
    }
    watch_id = raw.get("watch_id")
    watch = (
        WatchIdentity(
            watch_id=str(watch_id),
            platform=str(raw["watch_platform"]) if "watch_platform" in raw else None,
            firmware=str(raw["watch_firmware"]) if "watch_firmware" in raw else None,
        )
        if watch_id is not None
        else None
    )
    supported = frozenset(
        key.removeprefix("capability.")
        for key, value in raw.items()
        if key.startswith("capability.") and value is True
    )
    return DeviceConfigSnapshot(
        api_version=api_version,
        revision=int(raw["revision"]),
        state=DeviceConfigState(str(raw["state"])),
        watch=watch,
        capabilities=DeviceCapabilities(
            blob_db_version=int(raw.get("blob_db_version", 0)),
            supported=supported,
        ),
        last_read_at_ms=(
            int(raw["last_read_at_ms"]) if "last_read_at_ms" in raw else None
        ),
        health=FieldValue(activity_availability, health_value),
        preferences=preferences,
        error=(
            ConfigError(
                kind=ConfigErrorKind(str(raw.get("error_kind", "internal"))),
                message=str(raw["error_message"]),
            )
            if "error_message" in raw
            else None
        ),
    )
