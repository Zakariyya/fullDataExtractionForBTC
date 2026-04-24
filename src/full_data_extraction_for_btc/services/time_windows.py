from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


def iter_day_windows(start_ms: int, end_ms: int, tz_name: str) -> list[tuple[int, int]]:
    if end_ms <= start_ms:
        return []
    tz = ZoneInfo(tz_name)
    start_local = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).astimezone(tz)
    end_local = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).astimezone(tz)
    day_cursor = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end_local.replace(hour=0, minute=0, second=0, microsecond=0)

    windows: list[tuple[int, int]] = []
    while day_cursor <= end_day:
        day_start_ms = int(day_cursor.astimezone(timezone.utc).timestamp() * 1000)
        next_day = day_cursor + timedelta(days=1)
        day_end_ms = int(next_day.astimezone(timezone.utc).timestamp() * 1000)
        windows.append((max(start_ms, day_start_ms), min(end_ms, day_end_ms)))
        day_cursor = next_day
    return [(left, right) for left, right in windows if right > left]


def bar_to_ms(bar: str) -> int | None:
    value = bar.strip()
    if value.endswith("m"):
        return int(value[:-1]) * 60 * 1000
    if value.endswith("h"):
        return int(value[:-1]) * 60 * 60 * 1000
    if value.endswith("d"):
        return int(value[:-1]) * 24 * 60 * 60 * 1000
    if value.endswith("w"):
        return int(value[:-1]) * 7 * 24 * 60 * 60 * 1000
    return None


def calc_total_minutes(day_start_ms: int, day_end_ms: int) -> int:
    if day_end_ms <= day_start_ms:
        return 0
    return max(1, (day_end_ms - day_start_ms + 60_000 - 1) // 60_000)


def calc_processed_minutes(oldest_in_page: Any, day_start_ms: int, day_end_ms: int, total_minutes: int) -> int:
    if not isinstance(oldest_in_page, int):
        return 0
    clamped_oldest = min(max(oldest_in_page, day_start_ms), day_end_ms)
    covered_ms = max(0, day_end_ms - clamped_oldest)
    processed = (covered_ms + 60_000 - 1) // 60_000
    return max(0, min(total_minutes, processed))


def format_day_key(day_start_ms: int, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    day_dt = datetime.fromtimestamp(day_start_ms / 1000, tz=timezone.utc).astimezone(tz)
    return day_dt.strftime("%Y-%m-%d")
