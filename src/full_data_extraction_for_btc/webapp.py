from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from full_data_extraction_for_btc.query import build_data_coverage, build_data_summary, preview_dataset_rows
from full_data_extraction_for_btc.service import DownloadService

LOGGER = logging.getLogger("full_data_extraction_for_btc.webapp")


class DownloadRequest(BaseModel):
    start: str
    end: str
    datasets: list[str] = Field(default_factory=lambda: ["candles", "mark", "index", "funding"])
    instrument_id: str = "BTC-USDT-SWAP"
    bar: str = "1m"
    input_timezone: str = "UTC"
    output_subdir: str = "data"
    base_url: str = "https://www.okx.com"
    download_workers: int = Field(default=5, ge=1, le=64)


class RebuildIndexRequest(BaseModel):
    datasets: list[str] = Field(default_factory=lambda: ["candles", "mark", "index", "funding"])
    instrument_id: str = "BTC-USDT-SWAP"
    bar: str = "1m"
    input_timezone: str = "UTC"
    output_subdir: str = "data"


def create_app(output_root: Path) -> FastAPI:
    app = FastAPI(title="BTC Research Workbench")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    service = DownloadService(output_root=output_root)

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/")
    def index() -> FileResponse:
        return FileResponse(static_dir / "workbench.html")

    @app.post("/api/tasks/download")
    def start_download(request: DownloadRequest) -> dict[str, Any]:
        task = service.start_download(request.model_dump())
        return {"task": task.as_dict()}

    @app.post("/api/tasks/rebuild-index")
    def start_rebuild_index(request: RebuildIndexRequest) -> dict[str, Any]:
        task = service.start_rebuild_index(request.model_dump())
        return {"task": task.as_dict()}

    @app.get("/api/tasks")
    def list_tasks() -> dict[str, Any]:
        return {"tasks": service.list_tasks()}

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        task = service.get_task(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return {"task": task.as_dict()}

    @app.get("/api/tasks/{task_id}/events")
    def stream_task_events(task_id: str, from_index: int = 0) -> StreamingResponse:
        if service.get_task(task_id) is None:
            raise HTTPException(status_code=404, detail="task not found")

        def event_stream() -> Any:
            index = max(0, from_index)
            last_heartbeat_at = time.time()

            while True:
                task = service.get_task(task_id)
                if task is None:
                    yield "data: {\"type\":\"task_missing\"}\n\n"
                    break

                history, new_index = service.iter_events(task_id, index)
                if history:
                    for event in history:
                        yield f"data: {event}\n\n"
                    index = new_index
                    continue

                if task.status in {"completed", "failed"}:
                    yield "data: {\"type\":\"stream_end\"}\n\n"
                    break

                now = time.time()
                if now - last_heartbeat_at >= 1.0:
                    yield f"data: {json.dumps({'type': 'heartbeat', 'ts': now}, ensure_ascii=True)}\n\n"
                    last_heartbeat_at = now
                time.sleep(0.2)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.get("/api/data/summary")
    def data_summary(
        instrument_id: str = "BTC-USDT-SWAP",
        output_subdir: str = "data",
        start: str | None = None,
        end: str | None = None,
        timezone_name: str = "UTC",
        bar: str = "1m",
    ) -> dict[str, Any]:
        summary = build_data_summary(
            output_root / output_subdir,
            instrument_id=instrument_id,
            start=start,
            end=end,
            timezone_name=timezone_name,
            bar=bar,
        )
        return {"summary": summary}

    @app.get("/api/data/coverage")
    def data_coverage(
        instrument_id: str = "BTC-USDT-SWAP",
        output_subdir: str = "data",
        timezone_name: str = "Asia/Shanghai",
        datasets: str | None = None,
    ) -> dict[str, Any]:
        dataset_filter: set[str] | None = None
        if datasets:
            dataset_filter = {item.strip() for item in datasets.split(",") if item.strip()}
        coverage = build_data_coverage(
            output_root / output_subdir,
            instrument_id=instrument_id,
            timezone_name=timezone_name,
            dataset_names=dataset_filter,
        )
        return {"coverage": coverage, "requested_datasets": sorted(dataset_filter) if dataset_filter else None}

    @app.get("/api/data/coverage/stream")
    def stream_data_coverage(
        instrument_id: str = "BTC-USDT-SWAP",
        output_subdir: str = "data",
        timezone_name: str = "Asia/Shanghai",
        datasets: str | None = None,
    ) -> StreamingResponse:
        def event_stream() -> Any:
            base = output_root / output_subdir / "okx" / instrument_id
            if datasets:
                requested = [item.strip() for item in datasets.split(",") if item.strip()]
            elif base.exists():
                requested = sorted(
                    dataset_dir.name for dataset_dir in base.glob("*") if dataset_dir.is_dir() and dataset_dir.name != "metadata"
                )
            else:
                requested = []

            yield f"data: {json.dumps({'type': 'coverage_started', 'requested_datasets': requested, 'timezone': timezone_name}, ensure_ascii=True)}\n\n"

            merged_datasets: dict[str, Any] = {}
            total = len(requested)
            done = 0
            for dataset_name in requested:
                coverage = build_data_coverage(
                    output_root / output_subdir,
                    instrument_id=instrument_id,
                    timezone_name=timezone_name,
                    dataset_names={dataset_name},
                )
                datasets_payload = coverage.get("datasets", {})
                if dataset_name in datasets_payload:
                    merged_datasets[dataset_name] = datasets_payload[dataset_name]
                done += 1
                yield (
                    f"data: {json.dumps({'type': 'coverage_chunk', 'dataset': dataset_name, 'done': done, 'total': total, 'coverage': coverage}, ensure_ascii=True)}\n\n"
                )

            final_payload = {
                "type": "coverage_completed",
                "coverage": {
                    "base_path": str(output_root / output_subdir / "okx" / instrument_id),
                    "exists": (output_root / output_subdir / "okx" / instrument_id).exists(),
                    "timezone": timezone_name,
                    "datasets": merged_datasets,
                },
                "done": done,
                "total": total,
            }
            yield f"data: {json.dumps(final_payload, ensure_ascii=True)}\n\n"

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.get("/api/data/preview")
    def data_preview(
        dataset: str,
        instrument_id: str = "BTC-USDT-SWAP",
        output_subdir: str = "data",
        limit: int = 200,
        start: str | None = None,
        end: str | None = None,
        input_timezone: str = "Asia/Shanghai",
        kline_interval: str = "raw",
    ) -> dict[str, Any]:
        LOGGER.info(
            "Preview request: dataset=%s instrument_id=%s start=%s end=%s input_timezone=%s kline_interval=%s limit=%s",
            dataset,
            instrument_id,
            start,
            end,
            input_timezone,
            kline_interval,
            limit,
        )
        rows = preview_dataset_rows(
            output_root=output_root / output_subdir,
            instrument_id=instrument_id,
            dataset_path=dataset,
            limit=max(0, min(limit, 200000)),
            start=start,
            end=end,
            input_timezone=input_timezone,
            kline_interval=kline_interval,
        )
        return {"rows": rows}

    return app
