from __future__ import annotations

import csv
import gzip
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable
from zoneinfo import ZoneInfo

from full_data_extraction_for_btc.client import OkxPublicClient
from full_data_extraction_for_btc.downloader import CANDLE_DATASETS, DATASET_TO_PATH, collect_dataset_rows
from full_data_extraction_for_btc.storage import DatasetStorage
from full_data_extraction_for_btc.timeutils import parse_datetime_input


@dataclass
class DownloadTask:
    task_id: str
    status: str
    created_at: float
    updated_at: float
    request: dict[str, Any]
    summaries: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    event_queue: Queue[str] = field(default_factory=Queue)
    events_history: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "request": self.request,
            "summaries": self.summaries,
            "error": self.error,
        }


class DownloadService:
    def __init__(self, output_root: Path, client_factory: Callable[[str], Any] | None = None) -> None:
        self.output_root = output_root
        self.client_factory = client_factory or (lambda base_url: OkxPublicClient(base_url=base_url))
        self._lock = threading.Lock()
        self._tasks: dict[str, DownloadTask] = {}

    def start_download(self, request: dict[str, Any]) -> DownloadTask:
        task_id = uuid.uuid4().hex
        now = time.time()
        task = DownloadTask(
            task_id=task_id,
            status="queued",
            created_at=now,
            updated_at=now,
            request=request,
        )
        with self._lock:
            self._tasks[task_id] = task
        self._emit(task, {"type": "task_created", "task_id": task_id})

        worker = threading.Thread(target=self._run_download_task, args=(task_id,), daemon=True)
        worker.start()
        return task

    def get_task(self, task_id: str) -> DownloadTask | None:
        with self._lock:
            return self._tasks.get(task_id)

    def list_tasks(self) -> list[dict[str, Any]]:
        with self._lock:
            tasks = sorted(self._tasks.values(), key=lambda item: item.created_at, reverse=True)
        return [task.as_dict() for task in tasks]

    def iter_events(self, task_id: str, from_index: int) -> tuple[list[str], int]:
        task = self.get_task(task_id)
        if task is None:
            return [], from_index

        with self._lock:
            history = task.events_history[from_index:]
            new_index = len(task.events_history)
        return history, new_index

    def pop_event(self, task_id: str, timeout_seconds: float) -> str | None:
        task = self.get_task(task_id)
        if task is None:
            return None
        try:
            return task.event_queue.get(timeout=timeout_seconds)
        except Empty:
            return None

    def _run_download_task(self, task_id: str) -> None:
        task = self.get_task(task_id)
        if task is None:
            return

        request = task.request
        datasets = request["datasets"]
        instrument_id = request["instrument_id"]
        bar = request["bar"]
        base_url = request["base_url"]
        output = self.output_root / request["output_subdir"]
        input_tz = request.get("input_timezone", "UTC")
        start_ms = parse_datetime_input(request["start"], default_timezone=input_tz)
        end_ms = parse_datetime_input(request["end"], default_timezone=input_tz)

        self._set_status(task, "running")
        self._emit(task, {"type": "task_started", "task_id": task.task_id})

        try:
            client = self.client_factory(base_url)
            storage = DatasetStorage(output, exchange="okx", instrument_id=instrument_id)
            instrument = client.fetch_instrument(inst_type="SWAP", inst_id=instrument_id)
            storage.write_json("metadata/instrument.json", instrument)

            summaries: list[dict[str, Any]] = []
            for dataset in datasets:
                self._emit(task, {"type": "dataset_started", "dataset": dataset})
                if dataset in CANDLE_DATASETS:
                    rows: list[dict[str, Any]] = []
                    for day_start_ms, day_end_ms in _iter_day_windows(
                        start_ms=start_ms,
                        end_ms=end_ms,
                        tz_name=input_tz,
                    ):
                        day_key = _format_day_key(day_start_ms, tz_name=input_tz)
                        day_total_minutes = _calc_total_minutes(day_start_ms, day_end_ms)
                        self._emit(
                            task,
                            {
                                "type": "dataset_day_started",
                                "dataset": dataset,
                                "day": day_key,
                                "day_start_ms": day_start_ms,
                                "day_end_ms": day_end_ms,
                                "total_minutes": day_total_minutes,
                            },
                        )

                        if _is_local_day_continuous(
                            output_root=output,
                            instrument_id=instrument_id,
                            dataset_path=DATASET_TO_PATH[dataset],
                            day_start_ms=day_start_ms,
                            day_end_ms=day_end_ms,
                            bar=bar,
                        ):
                            self._emit(
                                task,
                                {
                                    "type": "dataset_day_skipped",
                                    "dataset": dataset,
                                    "day": day_key,
                                    "day_start_ms": day_start_ms,
                                    "day_end_ms": day_end_ms,
                                    "total_minutes": day_total_minutes,
                                    "reason": "local_data_continuous",
                                },
                            )
                            continue

                        def _on_day_progress(payload: dict[str, Any], ds: str = dataset, day: str = day_key) -> None:
                            processed_minutes = _calc_processed_minutes(
                                oldest_in_page=payload.get("oldest_in_page"),
                                day_start_ms=day_start_ms,
                                day_end_ms=day_end_ms,
                                total_minutes=day_total_minutes,
                            )
                            progress_pct = round((processed_minutes * 100.0) / day_total_minutes, 2) if day_total_minutes > 0 else 0.0
                            self._emit(
                                task,
                                {
                                    "type": "dataset_day_progress",
                                    "dataset": ds,
                                    "day": day,
                                    "day_start_ms": day_start_ms,
                                    "day_end_ms": day_end_ms,
                                    "total_minutes": day_total_minutes,
                                    "processed_minutes": processed_minutes,
                                    "progress_pct": progress_pct,
                                    **payload,
                                },
                            )

                        day_rows = collect_dataset_rows(
                            client=client,
                            dataset=dataset,
                            instrument_id=instrument_id,
                            bar=bar,
                            start_ms=day_start_ms,
                            end_ms=day_end_ms,
                            on_progress=_on_day_progress,
                        )
                        rows.extend(day_rows)
                        self._emit(
                            task,
                            {
                                "type": "dataset_day_finished",
                                "dataset": dataset,
                                "day": day_key,
                                "day_start_ms": day_start_ms,
                                "day_end_ms": day_end_ms,
                                "rows_downloaded": len(day_rows),
                            },
                        )
                else:
                    rows = collect_dataset_rows(
                        client=client,
                        dataset=dataset,
                        instrument_id=instrument_id,
                        bar=bar,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        on_progress=lambda payload, ds=dataset: self._emit(
                            task, {"type": "dataset_progress", "dataset": ds, **payload}
                        ),
                    )
                summary = storage.write_rows(
                    DATASET_TO_PATH[dataset],
                    rows,
                    primary_key="funding_time" if dataset == "funding" else "ts",
                )
                summary["requested_dataset"] = dataset
                if dataset == "funding":
                    summary["note"] = "OKX public funding-rate history is limited to the most recent 3 months."
                summaries.append(summary)
                self._emit(task, {"type": "dataset_finished", "dataset": dataset, "rows_written": summary["rows_written"]})

            task.summaries = summaries
            self._set_status(task, "completed")
            self._emit(task, {"type": "task_completed", "task_id": task.task_id, "summaries": summaries})
        except Exception as exc:  # noqa: BLE001
            task.error = str(exc)
            self._set_status(task, "failed")
            self._emit(task, {"type": "task_failed", "task_id": task.task_id, "error": task.error})

    def _set_status(self, task: DownloadTask, status: str) -> None:
        with self._lock:
            task.status = status
            task.updated_at = time.time()

    def _emit(self, task: DownloadTask, payload: dict[str, Any]) -> None:
        event = json.dumps(payload, ensure_ascii=True)
        with self._lock:
            task.events_history.append(event)
            task.updated_at = time.time()
        task.event_queue.put(event)


def _iter_day_windows(start_ms: int, end_ms: int, tz_name: str) -> list[tuple[int, int]]:
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


def _bar_to_ms(bar: str) -> int | None:
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


def _calc_total_minutes(day_start_ms: int, day_end_ms: int) -> int:
    if day_end_ms <= day_start_ms:
        return 0
    return max(1, (day_end_ms - day_start_ms + 60_000 - 1) // 60_000)


def _calc_processed_minutes(oldest_in_page: Any, day_start_ms: int, day_end_ms: int, total_minutes: int) -> int:
    if not isinstance(oldest_in_page, int):
        return 0
    clamped_oldest = min(max(oldest_in_page, day_start_ms), day_end_ms)
    covered_ms = max(0, day_end_ms - clamped_oldest)
    processed = (covered_ms + 60_000 - 1) // 60_000
    return max(0, min(total_minutes, processed))


def _format_day_key(day_start_ms: int, tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    day_dt = datetime.fromtimestamp(day_start_ms / 1000, tz=timezone.utc).astimezone(tz)
    return day_dt.strftime("%Y-%m-%d")


def _is_local_day_continuous(
    output_root: Path,
    instrument_id: str,
    dataset_path: str,
    day_start_ms: int,
    day_end_ms: int,
    bar: str,
) -> bool:
    step_ms = _bar_to_ms(bar)
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
