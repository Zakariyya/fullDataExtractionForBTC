from __future__ import annotations

import csv
import gzip
import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from full_data_extraction_for_btc.timeutils import parse_datetime_input, to_iso_local, to_iso_utc

SUPPORTED_KLINE_INTERVALS = {"1m", "5m", "30m", "1h", "1d", "3d", "1w", "1M", "1Y"}


def build_data_summary(output_root: Path, instrument_id: str) -> dict[str, Any]:
    base = output_root / "okx" / instrument_id
    manifests_dir = base / "metadata" / "manifests"
    manifests: dict[str, Any] = {}
    if manifests_dir.exists():
        for path in sorted(manifests_dir.glob("*.json")):
            manifests[path.stem] = json.loads(path.read_text(encoding="utf-8"))

    partitions: dict[str, int] = {}
    for dataset_dir in sorted(base.glob("*")):
        if not dataset_dir.is_dir() or dataset_dir.name == "metadata":
            continue
        count = sum(1 for _ in dataset_dir.glob("year=*/month=*/data.csv.gz"))
        partitions[dataset_dir.name] = count

    return {
        "base_path": str(base),
        "exists": base.exists(),
        "partitions": partitions,
        "manifests": manifests,
    }


def preview_dataset_rows(
    output_root: Path,
    instrument_id: str,
    dataset_path: str,
    limit: int = 50,
    start: str | None = None,
    end: str | None = None,
    input_timezone: str = "Asia/Shanghai",
    kline_interval: str = "raw",
) -> list[dict[str, str]]:
    base = output_root / "okx" / instrument_id / dataset_path
    files = sorted(base.glob("year=*/month=*/data.csv.gz"))

    start_ms = parse_datetime_input(start, default_timezone=input_timezone) if start else None
    end_ms = parse_datetime_input(end, default_timezone=input_timezone) if end else None
    if start_ms is not None and end_ms is not None and end_ms <= start_ms:
        return []

    rows: list[dict[str, str]] = []
    for path in files:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                ts = _row_timestamp(row)
                if ts is None:
                    continue
                if start_ms is not None and ts < start_ms:
                    continue
                if end_ms is not None and ts >= end_ms:
                    continue
                enriched = dict(row)
                enriched.setdefault("beijing_time", enriched.get("local_time_cn", to_iso_local(ts, "Asia/Shanghai")))
                rows.append(enriched)

    interval = kline_interval.strip() if kline_interval else "raw"
    if interval != "raw":
        rows = _aggregate_kline_rows(rows, interval=interval, tz_name=input_timezone)

    rows.sort(key=lambda item: int(_row_timestamp(item) or 0), reverse=True)
    return rows[:limit]


def _row_timestamp(row: dict[str, str]) -> int | None:
    for key in ("ts", "funding_time"):
        value = row.get(key)
        if not value:
            continue
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _aggregate_kline_rows(rows: list[dict[str, str]], interval: str, tz_name: str) -> list[dict[str, str]]:
    if interval not in SUPPORTED_KLINE_INTERVALS:
        return rows
    if not rows:
        return []

    # 聚合 K 线必须有 OHLC 列
    if any(key not in rows[0] for key in ("open", "high", "low", "close")):
        return rows

    buckets: dict[int, dict[str, Any]] = {}
    source_rows = sorted(rows, key=lambda item: int(_row_timestamp(item) or 0))
    for row in source_rows:
        ts = _row_timestamp(row)
        if ts is None:
            continue
        bucket_ts = _bucket_start_ms(ts, interval=interval, tz_name=tz_name)
        open_value = _to_float(row.get("open"))
        high_value = _to_float(row.get("high"))
        low_value = _to_float(row.get("low"))
        close_value = _to_float(row.get("close"))
        if None in {open_value, high_value, low_value, close_value}:
            continue

        bucket = buckets.get(bucket_ts)
        if bucket is None:
            bucket = {
                "ts": bucket_ts,
                "iso_time": to_iso_utc(bucket_ts),
                "beijing_time": to_iso_local(bucket_ts, "Asia/Shanghai"),
                "bar": interval,
                "open": open_value,
                "high": high_value,
                "low": low_value,
                "close": close_value,
                "volume_contracts": 0.0,
                "volume_base": 0.0,
                "volume_quote": 0.0,
            }
            buckets[bucket_ts] = bucket
        else:
            bucket["high"] = max(bucket["high"], high_value)
            bucket["low"] = min(bucket["low"], low_value)
            bucket["close"] = close_value

        bucket["volume_contracts"] += _to_float(row.get("volume_contracts")) or 0.0
        bucket["volume_base"] += _to_float(row.get("volume_base")) or 0.0
        bucket["volume_quote"] += _to_float(row.get("volume_quote")) or 0.0

    result: list[dict[str, str]] = []
    for bucket_ts in sorted(buckets.keys()):
        bucket = buckets[bucket_ts]
        result.append(
            {
                "ts": str(bucket["ts"]),
                "iso_time": bucket["iso_time"],
                "beijing_time": bucket["beijing_time"],
                "bar": bucket["bar"],
                "open": _format_float(bucket["open"]),
                "high": _format_float(bucket["high"]),
                "low": _format_float(bucket["low"]),
                "close": _format_float(bucket["close"]),
                "volume_contracts": _format_float(bucket["volume_contracts"]),
                "volume_base": _format_float(bucket["volume_base"]),
                "volume_quote": _format_float(bucket["volume_quote"]),
            }
        )
    return result


def _bucket_start_ms(ts: int, interval: str, tz_name: str) -> int:
    tz = ZoneInfo(tz_name)
    local_dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).astimezone(tz)

    if interval.endswith("m"):
        step = int(interval[:-1])
        minute = (local_dt.minute // step) * step
        bucket_dt = local_dt.replace(minute=minute, second=0, microsecond=0)
    elif interval.endswith("h"):
        step = int(interval[:-1])
        hour = (local_dt.hour // step) * step
        bucket_dt = local_dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    elif interval == "1d":
        bucket_dt = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif interval == "3d":
        day_start = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        base = date(1970, 1, 1)
        offset = (day_start.date() - base).days
        grouped_date = base + timedelta(days=(offset // 3) * 3)
        bucket_dt = datetime.combine(grouped_date, time.min, tzinfo=tz)
    elif interval == "1w":
        week_start = (local_dt - timedelta(days=local_dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        bucket_dt = week_start
    elif interval == "1M":
        bucket_dt = local_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif interval == "1Y":
        bucket_dt = local_dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return ts
    return int(bucket_dt.astimezone(timezone.utc).timestamp() * 1000)


def _to_float(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _format_float(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.10f}".rstrip("0").rstrip(".")
