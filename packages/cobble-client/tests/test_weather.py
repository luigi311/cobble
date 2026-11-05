"""Check the public forecast API against the JSON fixture consumed by Rust."""

import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from cobble_client import (
    CobbleClient,
    WeatherDailyForecast,
    WeatherForecast,
    WeatherHourlyForecast,
)
from dbus_fast import DBusError


def forecast_fixture():
    """Load the same complete forecast used to validate Rust deserialization."""
    root = Path(__file__).resolve().parents[3]
    raw = json.loads(
        (root / "crates/cobble-contracts/tests/fixtures/weather_forecast.json").read_text()
    )
    fields = dict(raw)
    fields["daily"] = [WeatherDailyForecast(**day) for day in raw["daily"]]
    fields["today_hourly"] = [WeatherHourlyForecast(**hour) for hour in raw["today_hourly"]]
    fields["tomorrow_hourly"] = [WeatherHourlyForecast(**hour) for hour in raw["tomorrow_hourly"]]
    return raw, WeatherForecast(**fields)


async def test_extended_weather_uses_shared_contract_and_original_location_slot():
    raw, forecast = forecast_fixture()
    call = AsyncMock()
    client = CobbleClient()
    client._iface = SimpleNamespace(call_push_weather_forecast=call)
    await client.push_weather_forecast(forecast)
    key, payload = call.call_args.args
    assert key == bytes.fromhex("e4c75d6ae95f4b778c3251a1c2b1d5c4")
    assert json.loads(payload) == raw
    assert asdict(forecast) == raw


async def test_invalid_location_and_nonfinite_coordinates_do_not_reach_dbus():
    _, forecast = forecast_fixture()
    call = AsyncMock()
    client = CobbleClient()
    client._iface = SimpleNamespace(call_push_weather_forecast=call)
    with pytest.raises(ValueError, match="16 bytes"):
        await client.push_weather_forecast(forecast, location_key=b"invalid")
    forecast.latitude = float("nan")
    with pytest.raises(ValueError):
        await client.push_weather_forecast(forecast)
    call.assert_not_awaited()


async def test_weather_errors_are_translated_like_other_client_methods():
    _, forecast = forecast_fixture()
    call = AsyncMock(side_effect=DBusError("org.cobble.Daemon.Failed", "watch rejected weather"))
    client = CobbleClient()
    client._iface = SimpleNamespace(call_push_weather_forecast=call)
    with pytest.raises(DBusError, match="watch rejected weather"):
        await client.push_weather_forecast(forecast)
