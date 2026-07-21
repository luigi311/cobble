"""Codec round-trip tests.

If these pass, a width-pinned value (u16/i8/…) survives the daemon<->client
D-Bus hop and reaches the encoder as the exact width the caller asked for.
"""

from cobble_client import Int, i8, i16, i32, u8, u16, u32
from cobble_client._codec import decode_data_dict, decode_value, encode_data_dict, encode_value
from cobble_client._device_config import (
    DeviceConfigState,
    FieldAvailability,
    decode_device_config,
)


def test_width_pins_survive_round_trip():
    src = {
        0: "hello",
        1: u16(150),
        2: u8(7),
        3: u32(70000),
        4: i8(-3),
        5: i16(-1000),
        6: i32(-5000),
        7: b"\xde\xad\xbe\xef",
        8: 42,
        9: -3,
    }
    back = decode_data_dict(encode_data_dict(src))

    assert back[0] == "hello"
    for key, width, signed in [(1, 2, False), (2, 1, False), (3, 4, False),
                               (4, 1, True), (5, 2, True), (6, 4, True)]:
        v = back[key]
        assert isinstance(v, Int), f"key {key} lost its Int wrapper"
        assert v.width == width and v.signed == signed, f"key {key} width/sign drifted"
    assert back[3].value == 70000
    assert back[6].value == -5000
    assert back[7] == b"\xde\xad\xbe\xef"
    assert back[8] == 42 and not isinstance(back[8], Int)
    assert back[9] == -3 and not isinstance(back[9], Int)


def test_inbound_plain_values():
    assert decode_value(encode_value("x")) == "x"
    assert decode_value(encode_value(5)) == 5
    assert decode_value(encode_value(-5)) == -5
    assert decode_value(encode_value(b"\x00\x01")) == b"\x00\x01"


def test_bool_rejected():
    import pytest

    with pytest.raises(TypeError):
        encode_value(True)


def test_device_config_preserves_availability_and_native_units():
    snapshot = decode_device_config(
        {
            "api_version": 1,
            "revision": 7,
            "state": "partial",
            "blob_db_version": 1,
            "capability.complete_refresh": True,
            "health.activity.availability": "available",
            "health.height_mm": 1805,
            "health.weight_dag": 7555,
            "health.age": 42,
            "health.gender": 2,
            "health.tracking_enabled": True,
            "health.activity_insights_enabled": False,
            "health.sleep_insights_enabled": True,
            "health.units.availability": "not_received",
            "health.hrm.availability": "not_received",
            "health.thresholds.availability": "not_received",
            "preference.clock24h.availability": "available",
            "preference.clock24h.value": True,
            "preference.clock24h.raw": b"\x01",
        }
    )

    assert snapshot.state is DeviceConfigState.PARTIAL
    assert snapshot.health.value.height_mm == 1805
    assert snapshot.health.value.weight_dag == 7555
    assert snapshot.health.value.distance_units.availability is FieldAvailability.NOT_RECEIVED
    assert snapshot.preferences["clock24h"].raw == b"\x01"
