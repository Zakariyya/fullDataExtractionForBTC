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

from full_data_extraction_for_btc.query import build_data_summary, preview_dataset_rows
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
            history, index = service.iter_events(task_id, from_index)
            for event in history:
                yield f"data: {event}\n\n"

            while True:
                task = service.get_task(task_id)
                if task is None:
                    yield "data: {\"type\":\"task_missing\"}\n\n"
                    break

                item = service.pop_event(task_id, timeout_seconds=1.0)
                if item is not None:
                    yield f"data: {item}\n\n"
                    index += 1
                    continue

                if task.status in {"completed", "failed"}:
                    yield "data: {\"type\":\"stream_end\"}\n\n"
                    break

                yield f"data: {json.dumps({'type': 'heartbeat', 'ts': time.time()}, ensure_ascii=True)}\n\n"

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @app.get("/api/data/summary")
    def data_summary(instrument_id: str = "BTC-USDT-SWAP", output_subdir: str = "data") -> dict[str, Any]:
        summary = build_data_summary(output_root / output_subdir, instrument_id=instrument_id)
        return {"summary": summary}

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
            limit=max(1, min(limit, 5000)),
            start=start,
            end=end,
            input_timezone=input_timezone,
            kline_interval=kline_interval,
        )
        return {"rows": rows}

    return app
