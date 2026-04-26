from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from queue import Queue
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from full_data_extraction_for_btc.client import OkxPublicClient
from full_data_extraction_for_btc.downloader import CANDLE_DATASETS, DATASET_TO_PATH, collect_dataset_rows
from full_data_extraction_for_btc.services.continuity_index import (
    compute_complete_months,
    day_index_file_path,
    is_local_day_continuous,
    load_day_index,
    load_month_index,
    month_index_file_path,
    persist_day_and_month_indexes,
    rebuild_day_and_month_index,
)
from full_data_extraction_for_btc.services.event_logging import log_terminal_event
from full_data_extraction_for_btc.services.models import DownloadTask
from full_data_extraction_for_btc.services.time_windows import (
    calc_processed_minutes,
    calc_total_minutes,
    format_day_key,
    iter_day_windows,
)
from full_data_extraction_for_btc.storage import DatasetStorage
from full_data_extraction_for_btc.timeutils import parse_datetime_input, to_iso_utc


class DownloadService:
    def __init__(self, output_root: Path, client_factory: Callable[[str], Any] | None = None) -> None:
        self.output_root = output_root
        self.client_factory = client_factory or (lambda base_url: OkxPublicClient(base_url=base_url))
        self._lock = threading.Lock()
        self._tasks: dict[str, DownloadTask] = {}
        self._task_queue: Queue[tuple[str, str]] = Queue()
        self._active_task_id: str | None = None
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()

    def start_download(self, request: dict[str, Any]) -> DownloadTask:
        task = self._create_task(request=request)
        self._enqueue_task(task, task_kind="download")
        return task

    def start_rebuild_index(self, request: dict[str, Any]) -> DownloadTask:
        task = self._create_task(request=request, task_kind="rebuild_index")
        self._enqueue_task(task, task_kind="rebuild_index")
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

    def _create_task(self, request: dict[str, Any], task_kind: str | None = None) -> DownloadTask:
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
        payload: dict[str, Any] = {"type": "task_created", "task_id": task_id}
        if task_kind is not None:
            payload["task_kind"] = task_kind
        self._emit(task, payload)
        return task

    def _enqueue_task(self, task: DownloadTask, task_kind: str) -> None:
        with self._lock:
            pending = self._task_queue.qsize()
            has_active = self._active_task_id is not None
            queue_position = pending + (1 if has_active else 0) + 1
        self._task_queue.put((task_kind, task.task_id))
        self._emit(task, {"type": "task_queued", "task_id": task.task_id, "task_kind": task_kind, "queue_position": queue_position})

    def _worker_loop(self) -> None:
        while True:
            task_kind, task_id = self._task_queue.get()
            try:
                with self._lock:
                    self._active_task_id = task_id
                if task_kind == "download":
                    self._run_download_task(task_id)
                elif task_kind == "rebuild_index":
                    self._run_rebuild_index_task(task_id)
                else:
                    task = self.get_task(task_id)
                    if task is not None:
                        task.error = f"unsupported task kind: {task_kind}"
                        self._set_status(task, "failed")
                        self._emit(task, {"type": "task_failed", "task_id": task.task_id, "error": task.error})
            except Exception as exc:  # noqa: BLE001
                task = self.get_task(task_id)
                if task is not None:
                    task.error = str(exc)
                    self._set_status(task, "failed")
                    self._emit(task, {"type": "task_failed", "task_id": task.task_id, "error": task.error})
            finally:
                with self._lock:
                    if self._active_task_id == task_id:
                        self._active_task_id = None
                self._task_queue.task_done()

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
        download_workers = self._normalize_download_workers(request.get("download_workers"))
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
                    summary = self._download_candle_dataset(
                        task=task,
                        storage=storage,
                        dataset=dataset,
                        dataset_path=DATASET_TO_PATH[dataset],
                        instrument_id=instrument_id,
                        base_url=base_url,
                        bar=bar,
                        input_tz=input_tz,
                        download_workers=download_workers,
                        output=output,
                        start_ms=start_ms,
                        end_ms=end_ms,
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

    def _download_candle_dataset(
        self,
        task: DownloadTask,
        storage: DatasetStorage,
        dataset: str,
        dataset_path: str,
        instrument_id: str,
        base_url: str,
        bar: str,
        input_tz: str,
        download_workers: int,
        output: Path,
        start_ms: int,
        end_ms: int,
    ) -> dict[str, Any]:
        day_index = load_day_index(
            output_root=output,
            instrument_id=instrument_id,
            dataset_path=dataset_path,
            bar=bar,
            tz_name=input_tz,
        )
        month_index = load_month_index(
            output_root=output,
            instrument_id=instrument_id,
            dataset_path=dataset_path,
            bar=bar,
            tz_name=input_tz,
        )

        index_dirty = False
        computed_months = compute_complete_months(day_index)
        reconciled_month_index = set(month_index) | computed_months
        missing_months = sorted(computed_months - month_index)
        for month in missing_months:
            self._emit(
                task,
                {
                    "type": "dataset_month_index_added",
                    "dataset": dataset,
                    "month": month,
                    "source": "startup_reconcile",
                },
            )
        if missing_months:
            index_dirty = True
        month_index = reconciled_month_index

        rows_written_total = 0
        min_ts: int | None = None
        max_ts: int | None = None
        all_windows = sorted(
            iter_day_windows(start_ms=start_ms, end_ms=end_ms, tz_name=input_tz),
            key=lambda item: item[0],
            reverse=True,
        )
        request_month_count = len({format_day_key(day_start_ms, tz_name=input_tz)[:7] for day_start_ms, _ in all_windows})
        windows_by_month: dict[str, list[tuple[int, int]]] = defaultdict(list)
        requested_days_by_month: dict[str, list[str]] = defaultdict(list)
        month_skip_emitted: set[str] = set()
        total_days_to_check = len(all_windows)
        checked_days = 0

        for day_start_ms, day_end_ms in all_windows:
            day_key = format_day_key(day_start_ms, tz_name=input_tz)
            month_key = day_key[:7]
            requested_days_by_month[month_key].append(day_key)
            windows_by_month[month_key].append((day_start_ms, day_end_ms))

        ordered_months = list(windows_by_month.keys())
        self._emit(
            task,
            {
                "type": "dataset_check_started",
                "dataset": dataset,
                "total_days": total_days_to_check,
                "months": len(ordered_months),
            },
        )

        parallel_workers = (
            min(download_workers, len(ordered_months))
            if request_month_count > 1 and len(ordered_months) > 1
            else 1
        )
        if parallel_workers > 1:
            self._emit(
                task,
                {
                    "type": "dataset_parallel_started",
                    "dataset": dataset,
                    "months": len(ordered_months),
                    "workers": parallel_workers,
                    "requested_workers": download_workers,
                },
            )

        def _emit_check_progress(day: str, month: str) -> None:
            progress_pct = round((checked_days * 100.0) / total_days_to_check, 2) if total_days_to_check > 0 else 100.0
            self._emit(
                task,
                {
                    "type": "dataset_check_progress",
                    "dataset": dataset,
                    "day": day,
                    "month": month,
                    "checked_days": checked_days,
                    "total_days": total_days_to_check,
                    "progress_pct": progress_pct,
                },
            )

        if parallel_workers > 1:
            futures: list[Any] = []
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                for month_key in ordered_months:
                    month_pending: list[dict[str, Any]] = []
                    month_windows = windows_by_month[month_key]

                    if month_key in month_index:
                        if month_key not in month_skip_emitted:
                            month_skip_emitted.add(month_key)
                            hit_days = requested_days_by_month.get(month_key, [])
                            self._emit(
                                task,
                                {
                                    "type": "dataset_month_skipped",
                                    "dataset": dataset,
                                    "month": month_key,
                                    "days": hit_days,
                                    "days_count": len(hit_days),
                                    "reason": "month_index_hit",
                                },
                            )
                        checked_days += len(month_windows)
                        if month_windows:
                            _emit_check_progress(day=format_day_key(month_windows[-1][0], tz_name=input_tz), month=month_key)
                        continue

                    for day_start_ms, day_end_ms in month_windows:
                        day_key = format_day_key(day_start_ms, tz_name=input_tz)
                        day_total_minutes = calc_total_minutes(day_start_ms, day_end_ms)

                        if day_key in day_index:
                            self._emit(
                                task,
                                {
                                    "type": "dataset_day_skipped",
                                    "dataset": dataset,
                                    "day": day_key,
                                    "day_start_ms": day_start_ms,
                                    "day_end_ms": day_end_ms,
                                    "total_minutes": day_total_minutes,
                                    "reason": "day_index_hit",
                                },
                            )
                            checked_days += 1
                            _emit_check_progress(day=day_key, month=month_key)
                            continue

                        self._emit(
                            task,
                            {
                                "type": "dataset_day_checking",
                                "dataset": dataset,
                                "day": day_key,
                                "day_start_ms": day_start_ms,
                                "day_end_ms": day_end_ms,
                                "total_minutes": day_total_minutes,
                            },
                        )

                        if is_local_day_continuous(
                            output_root=output,
                            instrument_id=instrument_id,
                            dataset_path=dataset_path,
                            day_start_ms=day_start_ms,
                            day_end_ms=day_end_ms,
                            bar=bar,
                        ):
                            if day_key not in day_index:
                                day_index.add(day_key)
                                index_dirty = True
                                self._emit(
                                    task,
                                    {
                                        "type": "dataset_day_index_added",
                                        "dataset": dataset,
                                        "day": day_key,
                                        "source": "continuity_check",
                                    },
                                )
                                month_index = self._update_month_index(
                                    task=task,
                                    dataset=dataset,
                                    day_index=day_index,
                                    month_index=month_index,
                                    source="continuity_check",
                                )

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
                            checked_days += 1
                            _emit_check_progress(day=day_key, month=month_key)
                            continue

                        month_pending.append(
                            {
                                "day_key": day_key,
                                "day_start_ms": day_start_ms,
                                "day_end_ms": day_end_ms,
                                "day_total_minutes": day_total_minutes,
                            }
                        )
                        checked_days += 1
                        _emit_check_progress(day=day_key, month=month_key)

                    if month_pending:
                        futures.append(
                            executor.submit(
                                self._download_month_days,
                                task,
                                dataset,
                                instrument_id,
                                bar,
                                base_url,
                                month_key,
                                month_pending,
                            )
                        )

                for future in as_completed(futures):
                    for plan, day_rows in future.result():
                        rows_written_total, min_ts, max_ts, index_dirty, month_index = self._process_downloaded_day(
                            task=task,
                            storage=storage,
                            dataset=dataset,
                            dataset_path=dataset_path,
                            instrument_id=instrument_id,
                            bar=bar,
                            output=output,
                            plan=plan,
                            day_rows=day_rows,
                            day_index=day_index,
                            month_index=month_index,
                            rows_written_total=rows_written_total,
                            min_ts=min_ts,
                            max_ts=max_ts,
                            index_dirty=index_dirty,
                        )
        else:
            worker_client = self.client_factory(base_url)
            for month_key in ordered_months:
                month_pending: list[dict[str, Any]] = []
                month_windows = windows_by_month[month_key]
                if month_key in month_index:
                    if month_key not in month_skip_emitted:
                        month_skip_emitted.add(month_key)
                        hit_days = requested_days_by_month.get(month_key, [])
                        self._emit(
                            task,
                            {
                                "type": "dataset_month_skipped",
                                "dataset": dataset,
                                "month": month_key,
                                "days": hit_days,
                                "days_count": len(hit_days),
                                "reason": "month_index_hit",
                            },
                        )
                    checked_days += len(month_windows)
                    if month_windows:
                        _emit_check_progress(day=format_day_key(month_windows[-1][0], tz_name=input_tz), month=month_key)
                    continue

                for day_start_ms, day_end_ms in month_windows:
                    day_key = format_day_key(day_start_ms, tz_name=input_tz)
                    day_total_minutes = calc_total_minutes(day_start_ms, day_end_ms)

                    if day_key in day_index:
                        self._emit(
                            task,
                            {
                                "type": "dataset_day_skipped",
                                "dataset": dataset,
                                "day": day_key,
                                "day_start_ms": day_start_ms,
                                "day_end_ms": day_end_ms,
                                "total_minutes": day_total_minutes,
                                "reason": "day_index_hit",
                            },
                        )
                        checked_days += 1
                        _emit_check_progress(day=day_key, month=month_key)
                        continue

                    self._emit(
                        task,
                        {
                            "type": "dataset_day_checking",
                            "dataset": dataset,
                            "day": day_key,
                            "day_start_ms": day_start_ms,
                            "day_end_ms": day_end_ms,
                            "total_minutes": day_total_minutes,
                        },
                    )

                    if is_local_day_continuous(
                        output_root=output,
                        instrument_id=instrument_id,
                        dataset_path=dataset_path,
                        day_start_ms=day_start_ms,
                        day_end_ms=day_end_ms,
                        bar=bar,
                    ):
                        if day_key not in day_index:
                            day_index.add(day_key)
                            index_dirty = True
                            self._emit(
                                task,
                                {
                                    "type": "dataset_day_index_added",
                                    "dataset": dataset,
                                    "day": day_key,
                                    "source": "continuity_check",
                                },
                            )
                            month_index = self._update_month_index(
                                task=task,
                                dataset=dataset,
                                day_index=day_index,
                                month_index=month_index,
                                source="continuity_check",
                            )

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
                        checked_days += 1
                        _emit_check_progress(day=day_key, month=month_key)
                        continue

                    month_pending.append(
                        {
                            "day_key": day_key,
                            "day_start_ms": day_start_ms,
                            "day_end_ms": day_end_ms,
                            "day_total_minutes": day_total_minutes,
                        }
                    )
                    checked_days += 1
                    _emit_check_progress(day=day_key, month=month_key)

                if month_pending:
                    for plan in month_pending:
                        day_rows = self._collect_day_rows(
                            task=task,
                            client=worker_client,
                            dataset=dataset,
                            instrument_id=instrument_id,
                            bar=bar,
                            plan=plan,
                        )
                        rows_written_total, min_ts, max_ts, index_dirty, month_index = self._process_downloaded_day(
                            task=task,
                            storage=storage,
                            dataset=dataset,
                            dataset_path=dataset_path,
                            instrument_id=instrument_id,
                            bar=bar,
                            output=output,
                            plan=plan,
                            day_rows=day_rows,
                            day_index=day_index,
                            month_index=month_index,
                            rows_written_total=rows_written_total,
                            min_ts=min_ts,
                            max_ts=max_ts,
                            index_dirty=index_dirty,
                        )

        self._emit(
            task,
            {
                "type": "dataset_check_finished",
                "dataset": dataset,
                "checked_days": checked_days,
                "total_days": total_days_to_check,
            },
        )

        if index_dirty:
            updated_months = persist_day_and_month_indexes(
                output_root=output,
                instrument_id=instrument_id,
                dataset_path=dataset_path,
                bar=bar,
                tz_name=input_tz,
                days=day_index,
            )
            month_index = updated_months
            self._emit(
                task,
                {
                    "type": "dataset_index_saved",
                    "dataset": dataset,
                    "indexed_days": len(day_index),
                    "indexed_months": len(month_index),
                    "bar": bar,
                    "timezone": input_tz,
                },
            )

        summary = {"dataset": dataset_path, "rows_written": rows_written_total}
        if min_ts is not None and max_ts is not None:
            storage.write_json(
                f"metadata/manifests/{dataset_path}.json",
                {
                    "dataset": dataset_path,
                    "rows_in_batch": rows_written_total,
                    "min_ts": min_ts,
                    "max_ts": max_ts,
                    "min_iso_time": to_iso_utc(min_ts),
                    "max_iso_time": to_iso_utc(max_ts),
                },
            )
        return summary

    def _download_month_days(
        self,
        task: DownloadTask,
        dataset: str,
        instrument_id: str,
        bar: str,
        base_url: str,
        month_key: str,
        plans: list[dict[str, Any]],
    ) -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
        self._emit(task, {"type": "dataset_parallel_month_started", "dataset": dataset, "month": month_key, "days": len(plans)})
        client = self.client_factory(base_url)
        results: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
        for plan in plans:
            day_rows = self._collect_day_rows(
                task=task,
                client=client,
                dataset=dataset,
                instrument_id=instrument_id,
                bar=bar,
                plan=plan,
            )
            results.append((plan, day_rows))
        self._emit(task, {"type": "dataset_parallel_month_finished", "dataset": dataset, "month": month_key, "days": len(plans)})
        return results

    def _collect_day_rows(
        self,
        task: DownloadTask,
        client: Any,
        dataset: str,
        instrument_id: str,
        bar: str,
        plan: dict[str, Any],
    ) -> list[dict[str, Any]]:
        day_key = plan["day_key"]
        day_start_ms = plan["day_start_ms"]
        day_end_ms = plan["day_end_ms"]
        day_total_minutes = plan["day_total_minutes"]

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

        def _on_day_progress(payload: dict[str, Any], ds: str = dataset, day: str = day_key) -> None:
            processed_minutes = calc_processed_minutes(
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

        return collect_dataset_rows(
            client=client,
            dataset=dataset,
            instrument_id=instrument_id,
            bar=bar,
            start_ms=day_start_ms,
            end_ms=day_end_ms,
            on_progress=_on_day_progress,
        )

    def _process_downloaded_day(
        self,
        task: DownloadTask,
        storage: DatasetStorage,
        dataset: str,
        dataset_path: str,
        instrument_id: str,
        bar: str,
        output: Path,
        plan: dict[str, Any],
        day_rows: list[dict[str, Any]],
        day_index: set[str],
        month_index: set[str],
        rows_written_total: int,
        min_ts: int | None,
        max_ts: int | None,
        index_dirty: bool,
    ) -> tuple[int, int | None, int | None, bool, set[str]]:
        day_key = plan["day_key"]
        day_start_ms = plan["day_start_ms"]
        day_end_ms = plan["day_end_ms"]
        day_summary = storage.write_rows(dataset_path, day_rows, primary_key="ts")
        rows_written_total += day_summary["rows_written"]

        if day_rows:
            day_timestamps = [int(row["ts"]) for row in day_rows]
            day_min_ts = min(day_timestamps)
            day_max_ts = max(day_timestamps)
            min_ts = day_min_ts if min_ts is None else min(min_ts, day_min_ts)
            max_ts = day_max_ts if max_ts is None else max(max_ts, day_max_ts)

        self._emit(
            task,
            {
                "type": "dataset_day_finished",
                "dataset": dataset,
                "day": day_key,
                "day_start_ms": day_start_ms,
                "day_end_ms": day_end_ms,
                "rows_downloaded": len(day_rows),
                "rows_written": day_summary["rows_written"],
            },
        )

        if is_local_day_continuous(
            output_root=output,
            instrument_id=instrument_id,
            dataset_path=dataset_path,
            day_start_ms=day_start_ms,
            day_end_ms=day_end_ms,
            bar=bar,
        ) and day_key not in day_index:
            day_index.add(day_key)
            index_dirty = True
            self._emit(
                task,
                {
                    "type": "dataset_day_index_added",
                    "dataset": dataset,
                    "day": day_key,
                    "source": "post_write_validation",
                },
            )
            month_index = self._update_month_index(
                task=task,
                dataset=dataset,
                day_index=day_index,
                month_index=month_index,
                source="post_write_validation",
            )

        return rows_written_total, min_ts, max_ts, index_dirty, month_index

    def _update_month_index(
        self,
        task: DownloadTask,
        dataset: str,
        day_index: set[str],
        month_index: set[str],
        source: str,
    ) -> set[str]:
        computed_months = compute_complete_months(day_index)
        new_months = sorted(computed_months - month_index)
        for month in new_months:
            self._emit(
                task,
                {
                    "type": "dataset_month_index_added",
                    "dataset": dataset,
                    "month": month,
                    "source": source,
                },
            )
        return computed_months

    def _run_rebuild_index_task(self, task_id: str) -> None:
        task = self.get_task(task_id)
        if task is None:
            return

        request = task.request
        datasets = request["datasets"]
        instrument_id = request["instrument_id"]
        bar = request["bar"]
        output = self.output_root / request["output_subdir"]
        tz_name = request.get("input_timezone", "UTC")

        self._set_status(task, "running")
        self._emit(task, {"type": "task_started", "task_id": task.task_id, "task_kind": "rebuild_index"})
        self._emit(task, {"type": "index_rebuild_started", "task_id": task.task_id})

        try:
            summaries: list[dict[str, Any]] = []
            for dataset in datasets:
                if dataset not in CANDLE_DATASETS:
                    self._emit(
                        task,
                        {
                            "type": "index_rebuild_dataset_skipped",
                            "dataset": dataset,
                            "reason": "unsupported_for_day_continuity",
                        },
                    )
                    continue

                dataset_path = DATASET_TO_PATH[dataset]
                self._emit(task, {"type": "index_rebuild_dataset_started", "dataset": dataset})
                indexed_days, scanned_days, indexed_months = rebuild_day_and_month_index(
                    output_root=output,
                    instrument_id=instrument_id,
                    dataset_path=dataset_path,
                    bar=bar,
                    tz_name=tz_name,
                    on_day_checked=lambda payload, ds=dataset: self._emit(
                        task, {"type": "index_rebuild_day_checked", "dataset": ds, **payload}
                    ),
                )
                summaries.append(
                    {
                        "requested_dataset": dataset,
                        "dataset": dataset_path,
                        "indexed_days": indexed_days,
                        "indexed_months": indexed_months,
                        "scanned_days": scanned_days,
                    }
                )
                self._emit(
                    task,
                    {
                        "type": "index_rebuild_dataset_finished",
                        "dataset": dataset,
                        "indexed_days": indexed_days,
                        "indexed_months": indexed_months,
                        "scanned_days": scanned_days,
                    },
                )

            task.summaries = summaries
            self._emit(task, {"type": "index_rebuild_finished", "task_id": task.task_id, "summaries": summaries})
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

    @staticmethod
    def _normalize_download_workers(raw_value: Any) -> int:
        default_workers = 5
        try:
            value = int(raw_value)
        except (TypeError, ValueError):
            return default_workers
        return max(1, min(value, 64))

    def _emit(self, task: DownloadTask, payload: dict[str, Any]) -> None:
        event = json.dumps(payload, ensure_ascii=True)
        with self._lock:
            task.events_history.append(event)
            task.updated_at = time.time()
        log_terminal_event(payload)


# Backward-compatible exports for tests and scripts.
def _day_index_file_path(output_root: Path, instrument_id: str, dataset_path: str, bar: str, tz_name: str) -> Path:
    return day_index_file_path(output_root, instrument_id, dataset_path, bar, tz_name)


def _month_index_file_path(output_root: Path, instrument_id: str, dataset_path: str, bar: str, tz_name: str) -> Path:
    return month_index_file_path(output_root, instrument_id, dataset_path, bar, tz_name)
