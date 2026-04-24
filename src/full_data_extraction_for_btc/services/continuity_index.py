from __future__ import annotations

import calendar
import csv
import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from full_data_extraction_for_btc.services.time_windows import bar_to_ms


def day_index_file_path(output_root: Path, instrument_id: str, dataset_path: str, bar: str, tz_name: str) -> Path:
    safe_bar = "".join(ch if ch.isalnum() else "_" for ch in bar)
    safe_tz = "".join(ch if ch.isalnum() else "_" for ch in tz_name)
    return (
        output_root
        / "okx"
        / instrument_id
        / "metadata"
        / "day_indexes"
        / f"{dataset_path}__bar={safe_bar}__tz={safe_tz}.json"
    )


def month_index_file_path(output_root: Path, instrument_id: str, dataset_path: str, bar: str, tz_name: str) -> Path:
    safe_bar = "".join(ch if ch.isalnum() else "_" for ch in bar)
    safe_tz = "".join(ch if ch.isalnum() else "_" for ch in tz_name)
    return (
        output_root
        / "okx"
        / instrument_id
        / "metadata"
        / "month_indexes"
        / f"{dataset_path}__bar={safe_bar}__tz={safe_tz}.json"
    )


def load_day_index(output_root: Path, instrument_id: str, dataset_path: str, bar: str, tz_name: str) -> set[str]:
    path = day_index_file_path(output_root, instrument_id, dataset_path, bar, tz_name)
    return _load_index_values(path, field="days")


def load_month_index(output_root: Path, instrument_id: str, dataset_path: str, bar: str, tz_name: str) -> set[str]:
    path = month_index_file_path(output_root, instrument_id, dataset_path, bar, tz_name)
    return _load_index_values(path, field="months")


def save_day_index(
    output_root: Path,
    instrument_id: str,
    dataset_path: str,
    bar: str,
    tz_name: str,
    days: set[str],
) -> None:
    path = day_index_file_path(output_root, instrument_id, dataset_path, bar, tz_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "dataset": dataset_path,
        "bar": bar,
        "timezone": tz_name,
        "days": sorted(days),
        "updated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def save_month_index(
    output_root: Path,
    instrument_id: str,
    dataset_path: str,
    bar: str,
    tz_name: str,
    months: set[str],
) -> None:
    path = month_index_file_path(output_root, instrument_id, dataset_path, bar, tz_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "dataset": dataset_path,
        "bar": bar,
        "timezone": tz_name,
        "months": sorted(months),
        "updated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")


def compute_complete_months(days: set[str]) -> set[str]:
    by_month: dict[tuple[int, int], set[int]] = {}
    for day in days:
        try:
            dt = datetime.strptime(day, "%Y-%m-%d")
        except ValueError:
            continue
        by_month.setdefault((dt.year, dt.month), set()).add(dt.day)

    complete: set[str] = set()
    for (year, month), day_set in by_month.items():
        total_days = calendar.monthrange(year, month)[1]
        if len(day_set) != total_days:
            continue
        if day_set == set(range(1, total_days + 1)):
            complete.add(f"{year:04d}-{month:02d}")
    return complete


def persist_day_and_month_indexes(
    output_root: Path,
    instrument_id: str,
    dataset_path: str,
    bar: str,
    tz_name: str,
    days: set[str],
) -> set[str]:
    months = compute_complete_months(days)
    save_day_index(output_root, instrument_id, dataset_path, bar, tz_name, days)
    save_month_index(output_root, instrument_id, dataset_path, bar, tz_name, months)
    return months


def is_local_day_continuous(
    output_root: Path,
    instrument_id: str,
    dataset_path: str,
    day_start_ms: int,
    day_end_ms: int,
    bar: str,
) -> bool:
    step_ms = bar_to_ms(bar)
    if step_ms is None or day_end_ms <= day_start_ms:
        return False

    dataset_root = output_root / "okx" / instrument_id / dataset_path
    if not dataset_root.exists():
        return False

    existing_ts: set[int] = set()
    for path in sorted(dataset_root.glob("year=*/month=*/data.csv.gz")):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                raw_ts = row.get("ts")
                if not raw_ts:
                    continue
                try:
                    ts = int(raw_ts)
                except ValueError:
                    continue
                if day_start_ms <= ts < day_end_ms:
                    existing_ts.add(ts)

    if not existing_ts:
        return False
    return all(ts in existing_ts for ts in range(day_start_ms, day_end_ms, step_ms))


def rebuild_day_and_month_index(
    output_root: Path,
    instrument_id: str,
    dataset_path: str,
    bar: str,
    tz_name: str,
    on_day_checked: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[int, int, int]:
    step_ms = bar_to_ms(bar)
    if step_ms is None:
        raise ValueError(f"unsupported bar for day index: {bar}")

    dataset_root = output_root / "okx" / instrument_id / dataset_path
    if not dataset_root.exists():
        save_day_index(output_root, instrument_id, dataset_path, bar, tz_name, days=set())
        save_month_index(output_root, instrument_id, dataset_path, bar, tz_name, months=set())
        return 0, 0, 0

    existing_ts: set[int] = set()
    for path in sorted(dataset_root.glob("year=*/month=*/data.csv.gz")):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                raw_ts = row.get("ts")
                if not raw_ts:
                    continue
                try:
                    existing_ts.add(int(raw_ts))
                except ValueError:
                    continue

    if not existing_ts:
        save_day_index(output_root, instrument_id, dataset_path, bar, tz_name, days=set())
        save_month_index(output_root, instrument_id, dataset_path, bar, tz_name, months=set())
        return 0, 0, 0

    tz = ZoneInfo(tz_name)
    first_ts = min(existing_ts)
    last_ts = max(existing_ts)
    first_local = datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc).astimezone(tz).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    last_local = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).astimezone(tz).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    indexed_days: set[str] = set()
    scanned_days = 0
    day_cursor = first_local
    while day_cursor <= last_local:
        day_start_ms = int(day_cursor.astimezone(timezone.utc).timestamp() * 1000)
        next_day = day_cursor + timedelta(days=1)
        day_end_ms = int(next_day.astimezone(timezone.utc).timestamp() * 1000)
        day_key = day_cursor.strftime("%Y-%m-%d")

        scanned_days += 1
        is_continuous = all(ts in existing_ts for ts in range(day_start_ms, day_end_ms, step_ms))
        if is_continuous:
            indexed_days.add(day_key)

        if on_day_checked is not None:
            on_day_checked(
                {
                    "day": day_key,
                    "day_start_ms": day_start_ms,
                    "day_end_ms": day_end_ms,
                    "is_continuous": is_continuous,
                    "indexed_days": len(indexed_days),
                    "scanned_days": scanned_days,
                }
            )

        day_cursor = next_day

    indexed_months = persist_day_and_month_indexes(
        output_root=output_root,
        instrument_id=instrument_id,
        dataset_path=dataset_path,
        bar=bar,
        tz_name=tz_name,
        days=indexed_days,
    )
    return len(indexed_days), scanned_days, len(indexed_months)


def _load_index_values(path: Path, field: str) -> set[str]:
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()

    values = payload.get(field)
    if not isinstance(values, list):
        return set()
    return {str(item) for item in values if isinstance(item, str)}
