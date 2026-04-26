from __future__ import annotations

import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from full_data_extraction_for_btc.services.continuity_index import load_day_index
from full_data_extraction_for_btc.timeutils import parse_datetime_input, to_iso_local, to_iso_utc, to_local_date

SUPPORTED_KLINE_INTERVALS = {"1m", "5m", "30m", "1h", "1d", "3d", "1w", "1M", "1Y"}
MAX_RAW_PREVIEW_POINTS = 12000


def build_data_summary(
    output_root: Path,
    instrument_id: str,
    start: str | None = None,
    end: str | None = None,
    timezone_name: str = "UTC",
    bar: str = "1m",
) -> dict[str, Any]:
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

    continuity: dict[str, Any] = {}
    expected_days = _build_expected_day_keys(start=start, end=end)
    if expected_days:
        for dataset_path in partitions.keys():
            if dataset_path not in {"candles", "mark_price_candles", "index_candles", "funding_rates"}:
                continuity[dataset_path] = {
                    "source": "not_applicable",
                    "bar": bar,
                    "timezone": timezone_name,
                    "range_start": expected_days[0],
                    "range_end": expected_days[-1],
                }
                continue
            indexed_days = load_day_index(
                output_root=output_root,
                instrument_id=instrument_id,
                dataset_path=dataset_path,
                bar=bar,
                tz_name=timezone_name,
            )
            covered_days = sorted(day for day in expected_days if day in indexed_days)
            missing_days = sorted(day for day in expected_days if day not in indexed_days)
            continuity[dataset_path] = {
                "source": "day_index",
                "bar": bar,
                "timezone": timezone_name,
                "range_start": expected_days[0],
                "range_end": expected_days[-1],
                "expected_days_count": len(expected_days),
                "covered_days_count": len(covered_days),
                "missing_days_count": len(missing_days),
                "coverage_ratio": round((len(covered_days) * 100.0) / len(expected_days), 2) if expected_days else None,
                "missing_days": missing_days,
                "missing_ranges": _compress_missing_ranges(missing_days),
            }

    return {
        "base_path": str(base),
        "exists": base.exists(),
        "partitions": partitions,
        "manifests": manifests,
        "continuity": continuity,
    }


def _build_expected_day_keys(start: str | None, end: str | None) -> list[str]:
    if not start or not end:
        return []
    try:
        start_dt = datetime.strptime(start[:10], "%Y-%m-%d").date()
        end_dt = datetime.strptime(end[:10], "%Y-%m-%d").date()
    except ValueError:
        return []
    # Keep the same semantics as download flow: left-closed right-open [start, end).
    if end_dt <= start_dt:
        return []
    days: list[str] = []
    cursor = start_dt
    while cursor < end_dt:
        days.append(cursor.strftime("%Y-%m-%d"))
        cursor += timedelta(days=1)
    return days


def _compress_missing_ranges(missing_days: list[str]) -> list[str]:
    if not missing_days:
        return []
    ranges: list[str] = []
    start = missing_days[0]
    prev = missing_days[0]
    for day in missing_days[1:]:
        try:
            prev_dt = datetime.strptime(prev, "%Y-%m-%d").date()
            day_dt = datetime.strptime(day, "%Y-%m-%d").date()
        except ValueError:
            if start == prev:
                ranges.append(start)
            else:
                ranges.append(f"{start}~{prev}")
            start = day
            prev = day
            continue
        if day_dt == prev_dt + timedelta(days=1):
            prev = day
            continue
        if start == prev:
            ranges.append(start)
        else:
            ranges.append(f"{start}~{prev}")
        start = day
        prev = day
    if start == prev:
        ranges.append(start)
    else:
        ranges.append(f"{start}~{prev}")
    return ranges


def build_data_coverage(
    output_root: Path,
    instrument_id: str,
    timezone_name: str = "Asia/Shanghai",
    dataset_names: set[str] | None = None,
) -> dict[str, Any]:
    base = output_root / "okx" / instrument_id
    datasets: dict[str, Any] = {}
    if not base.exists():
        return {"base_path": str(base), "exists": False, "timezone": timezone_name, "datasets": datasets}

    dataset_dirs = [
        dataset_dir
        for dataset_dir in sorted(base.glob("*"))
        if dataset_dir.is_dir()
        and dataset_dir.name != "metadata"
        and (dataset_names is None or dataset_dir.name in dataset_names)
    ]

    if not dataset_dirs:
        return {"base_path": str(base), "exists": True, "timezone": timezone_name, "datasets": datasets}

    result_by_name: dict[str, dict[str, Any]] = {}
    max_workers = min(4, len(dataset_dirs))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_build_single_dataset_coverage, dataset_dir=dataset_dir, timezone_name=timezone_name): dataset_dir.name
            for dataset_dir in dataset_dirs
        }
        for future in as_completed(future_map):
            dataset_name = future_map[future]
            result_by_name[dataset_name] = future.result()

    for dataset_dir in dataset_dirs:
        datasets[dataset_dir.name] = result_by_name.get(dataset_dir.name, {})

    return {"base_path": str(base), "exists": True, "timezone": timezone_name, "datasets": datasets}


def _build_single_dataset_coverage(dataset_dir: Path, timezone_name: str) -> dict[str, Any]:
    all_dates: set[str] = set()
    row_count = 0
    min_ts: int | None = None
    max_ts: int | None = None

    for path in sorted(dataset_dir.glob("year=*/month=*/data.csv.gz")):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                ts = _row_timestamp(row)
                if ts is None:
                    continue
                row_count += 1
                min_ts = ts if min_ts is None else min(min_ts, ts)
                max_ts = ts if max_ts is None else max(max_ts, ts)
                all_dates.add(to_local_date(ts, timezone_name))

    dates = sorted(all_dates)
    return {
        "rows_with_timestamp": row_count,
        "min_date": dates[0] if dates else None,
        "max_date": dates[-1] if dates else None,
        "distinct_dates_count": len(dates),
        "dates": dates,
        "min_iso_time_utc": to_iso_utc(min_ts) if min_ts is not None else None,
        "max_iso_time_utc": to_iso_utc(max_ts) if max_ts is not None else None,
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
    files = _filter_candidate_files(files, start_ms=start_ms, end_ms=end_ms)

    interval = kline_interval.strip() if kline_interval else "raw"
    if interval == "raw":
        effective_limit = limit
        if effective_limit <= 0:
            effective_limit = MAX_RAW_PREVIEW_POINTS
        rows = _load_raw_rows_with_limit(files=files, start_ms=start_ms, end_ms=end_ms, limit=effective_limit)
        return rows

    rows = _aggregate_kline_rows_from_files(files=files, start_ms=start_ms, end_ms=end_ms, interval=interval, tz_name=input_timezone)
    rows.sort(key=lambda item: int(_row_timestamp(item) or 0), reverse=True)
    if limit <= 0:
        return rows
    return rows[:limit]


def _load_raw_rows_with_limit(
    files: list[Path],
    start_ms: int | None,
    end_ms: int | None,
    limit: int,
) -> list[dict[str, str]]:
    if limit <= 0:
        return []
    rows: list[dict[str, str]] = []
    for path in reversed(files):
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            file_rows: list[dict[str, str]] = []
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
                file_rows.append(enriched)
            file_rows.sort(key=lambda item: int(_row_timestamp(item) or 0), reverse=True)
            for item in file_rows:
                rows.append(item)
                if len(rows) >= limit:
                    return rows[:limit]
    return rows[:limit]


def _filter_candidate_files(files: list[Path], start_ms: int | None, end_ms: int | None) -> list[Path]:
    if start_ms is None and end_ms is None:
        return files

    start_bound = datetime.fromtimestamp((start_ms or 0) / 1000, tz=timezone.utc) if start_ms is not None else None
    end_bound = datetime.fromtimestamp((end_ms - 1) / 1000, tz=timezone.utc) if end_ms is not None and end_ms > 0 else None
    start_min: tuple[int, int] | None = None
    end_max: tuple[int, int] | None = None
    if start_bound is not None:
        start_min = _shift_year_month(start_bound.year, start_bound.month, -1)
    if end_bound is not None:
        end_max = _shift_year_month(end_bound.year, end_bound.month, 1)
    result: list[Path] = []
    for path in files:
        year, month = _parse_year_month_from_path(path)
        if year is None or month is None:
            result.append(path)
            continue
        if start_min is not None and (year, month) < start_min:
            continue
        if end_max is not None and (year, month) > end_max:
            continue
        result.append(path)
    return result


def _shift_year_month(year: int, month: int, delta: int) -> tuple[int, int]:
    # month is 1~12
    total = year * 12 + (month - 1) + delta
    next_year = total // 12
    next_month = total % 12 + 1
    return next_year, next_month


def _parse_year_month_from_path(path: Path) -> tuple[int | None, int | None]:
    year: int | None = None
    month: int | None = None
    for part in path.parts:
        if part.startswith("year="):
            raw = part.split("=", 1)[1]
            if raw.isdigit():
                year = int(raw)
        if part.startswith("month="):
            raw = part.split("=", 1)[1]
            if raw.isdigit():
                month = int(raw)
    return year, month


def _aggregate_kline_rows_from_files(
    files: list[Path],
    start_ms: int | None,
    end_ms: int | None,
    interval: str,
    tz_name: str,
) -> list[dict[str, str]]:
    if interval not in SUPPORTED_KLINE_INTERVALS:
        return _load_raw_rows_with_limit(files=files, start_ms=start_ms, end_ms=end_ms, limit=MAX_RAW_PREVIEW_POINTS)

    buckets: dict[int, dict[str, Any]] = {}
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
                open_value = _to_float(row.get("open"))
                high_value = _to_float(row.get("high"))
                low_value = _to_float(row.get("low"))
                close_value = _to_float(row.get("close"))
                if None in {open_value, high_value, low_value, close_value}:
                    continue
                bucket_ts = _bucket_start_ms(ts, interval=interval, tz_name=tz_name)
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
