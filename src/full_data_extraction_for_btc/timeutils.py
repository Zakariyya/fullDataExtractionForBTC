from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def parse_datetime_input(value: str, default_timezone: str = "UTC") -> int:
    tz = ZoneInfo(default_timezone)
    if len(value) == 10:
        parsed = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=tz).astimezone(timezone.utc)
        return int(parsed.timestamp() * 1000)

    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz).astimezone(timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return int(parsed.timestamp() * 1000)


def to_iso_utc(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_iso_local(timestamp_ms: int, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%dT%H:%M:%S%z")


def to_local_date(timestamp_ms: int, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d")
