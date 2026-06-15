"""Pebble Protocol — endpoint framing and system message builders.

Every Pebble Protocol message: [u16 length][u16 endpoint][payload].
length is the payload length (not counting the 4-byte header). Big-endian.

This module also builds the replies for the watch's session-keepalive
traffic: PHONE_VERSION (endpoint 17) and PING (endpoint 2001). Shortly after
the session opens, the watch asks for the phone's version/capabilities and
pings periodically; a phone that answers neither is eventually treated as
gone and the watch tears the session down — so we must reply to both.

Layouts follow libpebble2 (PhoneAppVersion / Ping) and Gadgetbridge's
encodePhoneVersion.
"""

from __future__ import annotations

import os
import struct
import uuid as _uuid
from enum import IntEnum


class Endpoint(IntEnum):
    TIME = 11
    PHONE_VERSION = 17
    SYSTEM_MESSAGE = 18
    APP_MESSAGE = 48
    APP_RUN_STATE = 52
    BLOB_DB = 0xB1DB
    PING = 2001
    APP_FETCH = 6001


def pebble_pack(endpoint: Endpoint, payload: bytes) -> bytes:
    return struct.pack(">HH", len(payload), int(endpoint)) + payload


def pebble_unpack(data: bytes) -> tuple[Endpoint | int, bytes]:
    """Parse a Pebble Protocol frame. Returns (endpoint, payload).

    The endpoint is returned as an Endpoint enum member when recognized, or as
    a plain int otherwise — the watch sends many endpoints (system messages,
    factory settings, etc.) we don't model, and an unknown one must not crash
    the reader.
    """
    length, endpoint = struct.unpack(">HH", data[:4])
    try:
        endpoint = Endpoint(endpoint)
    except ValueError:
        pass  # leave as int; not an endpoint we handle
    return endpoint, data[4 : 4 + length]


def uuid_to_bytes(uuid_str: str) -> bytes:
    """16 raw big-endian bytes of a UUID string (watchapp identifiers)."""
    return _uuid.UUID(uuid_str).bytes


# ---------------------------------------------------------------------------
# PHONE_VERSION (endpoint 17)
# ---------------------------------------------------------------------------

# platform_flags: "remote OS + remote capabilities" bitfield, values from
# Gadgetbridge PebbleProtocol.java.
PHONEVERSION_REMOTE_OS_ANDROID = 0x00000002
PHONEVERSION_REMOTE_CAPS_TELEPHONY = 0x00000010
PHONEVERSION_REMOTE_CAPS_SMS = 0x00000020
PHONEVERSION_REMOTE_CAPS_GPS = 0x00000040
PHONEVERSION_REMOTE_CAPS_BTLE = 0x00000080

# protocol_caps: u64 capability flags as modeled by libpebble2's
# ProtocolCapsFlag; bit 0 = AppRunState support (which we implement via
# Pebble.launch_app). We stay conservative and only claim that.
PROTOCOL_CAPS_APP_RUN_STATE = 0x0000000000000001


def build_phone_version_response(major: int = 4, minor: int = 4, bugfix: int = 2) -> bytes:
    """AppVersionResponse payload for the PHONE_VERSION endpoint.

    Layout follows libpebble2's PhoneAppVersion/AppVersionResponse (and
    Gadgetbridge's encodePhoneVersion), big-endian:
      u8  command          0x01 = response
      u32 protocol_version 0xFFFFFFFF
      u32 session_caps     0x80000000 ("gamma ray")
      u32 platform_flags   remote OS + capability bits
      u8  response_version 2
      u8  major / u8 minor / u8 bugfix   (phone app version we impersonate)
      u64 protocol_caps    capability flags
    """
    platform_flags = (
        PHONEVERSION_REMOTE_OS_ANDROID
        | PHONEVERSION_REMOTE_CAPS_TELEPHONY
        | PHONEVERSION_REMOTE_CAPS_SMS
        | PHONEVERSION_REMOTE_CAPS_GPS
        | PHONEVERSION_REMOTE_CAPS_BTLE
    )
    return (
        b"\x01"
        + struct.pack(">III", 0xFFFFFFFF, 0x80000000, platform_flags)
        + struct.pack(">BBBB", 2, major & 0xFF, minor & 0xFF, bugfix & 0xFF)
        + struct.pack(">Q", PROTOCOL_CAPS_APP_RUN_STATE)
    )


# ---------------------------------------------------------------------------
# PING (endpoint 2001)
# ---------------------------------------------------------------------------


def build_pong(cookie: int) -> bytes:
    """PING endpoint reply: u8 command (1 = pong) + u32 echoed cookie."""
    return struct.pack(">BI", 0x01, cookie & 0xFFFFFFFF)


def parse_ping(payload: bytes) -> int | None:
    """Return the cookie if payload is a ping request (command 0), else None."""
    if len(payload) >= 5 and payload[0] == 0x00:
        return struct.unpack_from(">I", payload, 1)[0]
    return None


# ---------------------------------------------------------------------------
# APP_RUN_STATE (endpoint 52)
# ---------------------------------------------------------------------------


class AppRunStateCmd(IntEnum):
    START = 0x01
    STOP = 0x02
    REQUEST = 0x03


def build_app_run_state(cmd: AppRunStateCmd, app_uuid: str) -> bytes:
    return bytes([int(cmd)]) + uuid_to_bytes(app_uuid)


# ---------------------------------------------------------------------------
# TIME (endpoint 11)
# ---------------------------------------------------------------------------


class TimeCmd(IntEnum):
    GET_REQUEST = 0x00  # TIME_GETTIME
    SET_LOCALTIME = 0x02  # TIME_SETTIME (legacy: u32 local-time only)
    SET_UTC = 0x03  # TIME_SETTIME_UTC (u32 UTC + s16 offset + tz name)


def build_set_utc(
    utc_timestamp: int,
    utc_offset_minutes: int,
    tz_name: str = "",
) -> bytes:
    """SET_UTC payload for the TIME endpoint.

    Layout (big-endian), per Gadgetbridge encodeSetTime / libpebble2 SetUTC:
      u8   command            0x03 = TIME_SETTIME_UTC
      u32  unix_timestamp     seconds since epoch, UTC
      s16  utc_offset         local offset from UTC, in MINUTES
      u8   tz_name_length
      ...  tz_name            ASCII, not NUL-terminated

    The watch keeps UTC internally and applies the offset for display, so the
    offset must be the *local* zone's current offset (including DST) — e.g.
    -360 for US Mountain Daylight Time.
    """
    name = tz_name.encode("utf-8")
    if len(name) > 0xFF:
        name = name[:0xFF]
    return (
        struct.pack(">BIh", int(TimeCmd.SET_UTC), utc_timestamp & 0xFFFFFFFF, utc_offset_minutes)
        + struct.pack(">B", len(name))
        + name
    )


# ---------------------------------------------------------------------------
# BlobDB (endpoint 0xb1db) + notifications
# ---------------------------------------------------------------------------


class BlobDBCommand(IntEnum):
    INSERT = 0x01
    DELETE = 0x04
    CLEAR = 0x05


class BlobDBId(IntEnum):
    PIN = 1
    APP = 2
    REMINDER = 3
    NOTIFICATION = 4
    WEATHER = 5


class BlobDBStatus(IntEnum):
    SUCCESS = 1
    GENERAL_FAILURE = 2
    INVALID_OPERATION = 3
    INVALID_DATABASE_ID = 4
    INVALID_DATA = 5
    KEY_DOES_NOT_EXIST = 6
    DATABASE_FULL = 7
    DATA_STALE = 8


def build_blobdb_insert(
    db: BlobDBId, key_uuid: bytes, blob: bytes, token: int | None = None
) -> bytes:
    """BlobDB INSERT payload (endpoint 0xb1db).

    Mirrors Gadgetbridge encodeBlobdb. Endianness is mixed: the section
    after the BE length/endpoint prefix (command, token, db, key_length) is
    LITTLE-endian, the 16-byte UUID key is BIG-endian, and the blob length
    prefix is LITTLE-endian. Get this wrong and the watch drops the insert
    with no visible error.

    Returns the payload only (no Pebble Protocol header); pass it to
    pebble_pack(Endpoint.BLOB_DB, payload).
    """
    if token is None:
        token = int.from_bytes(os.urandom(2), "little")
    if len(key_uuid) != 16:
        msg = f"blobdb key must be 16 bytes, got {len(key_uuid)}"
        raise ValueError(msg)
    return (
        struct.pack("<BHB", int(BlobDBCommand.INSERT), token & 0xFFFF, int(db))
        + struct.pack("<B", len(key_uuid))
        + key_uuid  # 16 raw big-endian UUID bytes (already in BE order)
        + struct.pack("<H", len(blob))
        + blob
    )


def _attr_string(attr_id: int, value: str, max_len: int = 512) -> bytes:
    """One notification attribute holding a string: id(u8) + len(u16 LE) + bytes."""
    raw = value.encode("utf-8")[:max_len]
    return struct.pack("<BH", attr_id, len(raw)) + raw


def _attr_uint32(attr_id: int, value: int) -> bytes:
    """One notification attribute holding a u32: id(u8) + len(u16 LE=4) + u32 LE."""
    return struct.pack("<BHI", attr_id, 4, value & 0xFFFFFFFF)


# Attribute ids from Gadgetbridge / PebbleOS timeline attributes.
ATTR_TITLE = 1
ATTR_SUBTITLE = 2
ATTR_BODY = 3
ATTR_ICON = 4

# A safe generic notification icon. 0x80000000 | id is how Gadgetbridge tags
# system icon ids; this one is a generic notification glyph.
NOTIFICATION_ICON_GENERIC = 0x80000037


def build_notification_blob(
    title: str,
    body: str,
    subtitle: str = "",
    timestamp: int | None = None,
    icon: int = NOTIFICATION_ICON_GENERIC,
) -> bytes:
    """Build the notification pin blob (the value stored under BlobDB NOTIFICATION).

    Layout follows Gadgetbridge encodeNotification's 46-byte pin header plus
    attributes, no actions (first pass keeps it simple: title/subtitle/body
    plus an icon attribute).

    Pin header (46 bytes):
      item_uuid   (16 BE)   random
      parent_uuid (16 BE)   the notifications app uuid
      timestamp   (u32 LE)
      duration    (u16 LE)  0
      type        (u8)      0x01 = notification
      flags       (u16 LE)  0x0001
      layout      (u8)      0x04 = notification layout
      attr+act_len(u16 LE)  total bytes of attributes+actions
      attr_count  (u8)
      act_count   (u8)      0
    """
    if timestamp is None:
        timestamp = int(__import__("time").time())

    # The notifications "app" parent uuid Gadgetbridge uses
    # (UUID_NOTIFICATIONS = b2cae818-10f8-46df-ad2b-98ad2254a3c1).
    parent_uuid = _uuid.UUID("b2cae818-10f8-46df-ad2b-98ad2254a3c1").bytes
    item_uuid = _uuid.uuid4().bytes

    # Build attributes.
    attrs = b""
    attr_count = 0
    for attr_id, value in ((ATTR_TITLE, title), (ATTR_SUBTITLE, subtitle), (ATTR_BODY, body)):
        if value:
            attrs += _attr_string(attr_id, value)
            attr_count += 1
    # Icon attribute (u32).
    attrs += _attr_uint32(ATTR_ICON, icon)
    attr_count += 1

    attributes_length = len(attrs)

    header = (
        item_uuid
        + parent_uuid
        + struct.pack(
            "<IHBHBHBB",
            timestamp & 0xFFFFFFFF,  # u32 timestamp
            0,  # u16 duration
            0x01,  # u8 type = notification
            0x0001,  # u16 flags
            0x04,  # u8 layout = notification
            attributes_length,  # u16 attributes+actions length
            attr_count,  # u8 attribute count
            0,  # u8 action count
        )
    )
    return header + attrs


def build_notification(
    title: str,
    body: str,
    subtitle: str = "",
    timestamp: int | None = None,
    icon: int = NOTIFICATION_ICON_GENERIC,
    token: int | None = None,
) -> bytes:
    """Full BlobDB-INSERT payload that delivers a notification to the watch.

    Returns the payload for Endpoint.BLOB_DB. The blob's key is a fresh random
    UUID (each notification is its own database entry).
    """
    blob = build_notification_blob(title, body, subtitle, timestamp, icon)
    key = _uuid.uuid4().bytes
    return build_blobdb_insert(BlobDBId.NOTIFICATION, key, blob, token=token)


def parse_blobdb_response(payload: bytes) -> tuple[int, int] | None:
    """Parse a BlobDB response: u16 token (LE) + u8 status. Returns (token, status)."""
    if len(payload) < 3:
        return None
    token = int.from_bytes(payload[0:2], "little")
    status = payload[2]
    return token, status
