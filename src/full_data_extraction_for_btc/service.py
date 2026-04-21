from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable

from full_data_extraction_for_btc.client import OkxPublicClient
from full_data_extraction_for_btc.downloader import DATASET_TO_PATH, collect_dataset_rows
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
