from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger("full_data_extraction_for_btc.service")


def log_terminal_event(payload: dict[str, Any]) -> None:
    event_type = str(payload.get("type") or "")
    if event_type == "task_created":
        LOGGER.info("Task created: task_id=%s", payload.get("task_id"))
        return
    if event_type == "task_started":
        LOGGER.info("Task started: task_id=%s", payload.get("task_id"))
        return
    if event_type == "dataset_started":
        LOGGER.info("Dataset started: dataset=%s", payload.get("dataset"))
        return
    if event_type == "dataset_finished":
        LOGGER.info(
            "Dataset finished: dataset=%s rows_written=%s",
            payload.get("dataset"),
            payload.get("rows_written"),
        )
        return
    if event_type == "dataset_day_started":
        LOGGER.info(
            "[%s] %s start | total_minutes=%s",
            payload.get("dataset"),
            payload.get("day"),
            payload.get("total_minutes"),
        )
        return
    if event_type == "dataset_day_checking":
        # Suppress per-day checking spam in terminal logs.
        return
    if event_type == "dataset_check_started":
        LOGGER.info(
            "[%s] checking started | total_days=%s months=%s",
            payload.get("dataset"),
            payload.get("total_days"),
            payload.get("months"),
        )
        return
    if event_type == "dataset_check_progress":
        LOGGER.info(
            "[%s] checking progress | %s/%s (%s%%) month=%s day=%s",
            payload.get("dataset"),
            payload.get("checked_days"),
            payload.get("total_days"),
            payload.get("progress_pct"),
            payload.get("month"),
            payload.get("day"),
        )
        return
    if event_type == "dataset_check_finished":
        LOGGER.info(
            "[%s] checking finished | %s/%s",
            payload.get("dataset"),
            payload.get("checked_days"),
            payload.get("total_days"),
        )
        return
    if event_type == "dataset_month_skipped":
        LOGGER.info(
            "[%s] %s month-index-hit skipped | days=%s",
            payload.get("dataset"),
            payload.get("month"),
            payload.get("days_count"),
        )
        return
    if event_type == "dataset_day_finished":
        LOGGER.info(
            "[%s] %s finished | rows_downloaded=%s rows_written=%s",
            payload.get("dataset"),
            payload.get("day"),
            payload.get("rows_downloaded"),
            payload.get("rows_written"),
        )
        return
    if event_type == "dataset_day_skipped":
        LOGGER.info(
            "[%s] %s skipped | reason=%s",
            payload.get("dataset"),
            payload.get("day"),
            payload.get("reason"),
        )
        return
    if event_type == "dataset_day_index_added":
        LOGGER.info(
            "[%s] %s day-index-added | source=%s",
            payload.get("dataset"),
            payload.get("day"),
            payload.get("source"),
        )
        return
    if event_type == "dataset_parallel_started":
        LOGGER.info(
            "[%s] parallel download started | months=%s workers=%s",
            payload.get("dataset"),
            payload.get("months"),
            payload.get("workers"),
        )
        return
    if event_type == "dataset_parallel_month_started":
        LOGGER.info(
            "[%s] month started | month=%s days=%s",
            payload.get("dataset"),
            payload.get("month"),
            payload.get("days"),
        )
        return
    if event_type == "dataset_parallel_month_finished":
        LOGGER.info(
            "[%s] month finished | month=%s days=%s",
            payload.get("dataset"),
            payload.get("month"),
            payload.get("days"),
        )
        return
    if event_type == "dataset_month_index_added":
        LOGGER.info(
            "[%s] %s month-index-added | source=%s",
            payload.get("dataset"),
            payload.get("month"),
            payload.get("source"),
        )
        return
    if event_type == "dataset_index_saved":
        LOGGER.info(
            "Dataset index saved: dataset=%s indexed_days=%s indexed_months=%s bar=%s timezone=%s",
            payload.get("dataset"),
            payload.get("indexed_days"),
            payload.get("indexed_months"),
            payload.get("bar"),
            payload.get("timezone"),
        )
        return
    if event_type == "index_rebuild_started":
        LOGGER.info("Index rebuild started: task_id=%s", payload.get("task_id"))
        return
    if event_type == "index_rebuild_dataset_started":
        LOGGER.info("Index rebuild dataset started: dataset=%s", payload.get("dataset"))
        return
    if event_type == "index_rebuild_dataset_finished":
        LOGGER.info(
            "Index rebuild dataset finished: dataset=%s indexed_days=%s indexed_months=%s scanned_days=%s",
            payload.get("dataset"),
            payload.get("indexed_days"),
            payload.get("indexed_months"),
            payload.get("scanned_days"),
        )
        return
    if event_type == "index_rebuild_dataset_skipped":
        LOGGER.info(
            "Index rebuild dataset skipped: dataset=%s reason=%s",
            payload.get("dataset"),
            payload.get("reason"),
        )
        return
    if event_type == "index_rebuild_finished":
        LOGGER.info("Index rebuild finished: task_id=%s", payload.get("task_id"))
        return
    if event_type == "task_completed":
        LOGGER.info("Task completed: task_id=%s", payload.get("task_id"))
        return
    if event_type == "task_failed":
        LOGGER.error("Task failed: task_id=%s error=%s", payload.get("task_id"), payload.get("error"))
