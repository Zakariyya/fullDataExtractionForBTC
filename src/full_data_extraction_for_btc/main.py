from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
import sys

import uvicorn

from full_data_extraction_for_btc.cli import build_parser
from full_data_extraction_for_btc.client import OkxPublicClient
from full_data_extraction_for_btc.downloader import DATASET_TO_PATH, collect_dataset_rows
from full_data_extraction_for_btc.storage import DatasetStorage
from full_data_extraction_for_btc.terminal_logging import InlineConsole, configure_terminal_logging
from full_data_extraction_for_btc.timeutils import parse_datetime_input
from full_data_extraction_for_btc.webapp import create_app

LOGGER = logging.getLogger("full_data_extraction_for_btc")


def _start_serve_alive_indicator(console: InlineConsole, host: str, port: int) -> threading.Event:
    stop_event = threading.Event()
    started = time.monotonic()

    def _run() -> None:
        while not stop_event.is_set():
            elapsed = int(time.monotonic() - started)
            console.inline(f"[serve alive] http://{host}:{port} | uptime={elapsed}s")
            stop_event.wait(1.0)

    threading.Thread(target=_run, daemon=True).start()
    return stop_event


def _print_serve_startup_guide(console: InlineConsole, host: str, port: int) -> None:
    base = f"http://{host}:{port}"
    console.line("BTC 数据工作台服务已启动（仅错误日志 + 存活同行刷新）")
    console.line(f"Web UI: {base}/")
    console.line("常用接口:")
    console.line(f"  - {base}/api/tasks")
    console.line(f"  - {base}/api/data/summary")
    console.line(f"  - {base}/api/data/coverage")
    console.line(f"  - {base}/api/data/preview")
    console.line("退出: Ctrl+C")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "serve":
        app = create_app(Path(args.output_root))
        console = InlineConsole(stream=sys.stdout, force_line_interval_seconds=3600.0)
        _print_serve_startup_guide(console, host=args.host, port=args.port)
        stop_event = _start_serve_alive_indicator(console, host=args.host, port=args.port)
        try:
            uvicorn.run(
                app,
                host=args.host,
                port=args.port,
                access_log=False,
                log_level="error",
            )
        finally:
            stop_event.set()
            console.clear_inline()
        return 0
    if args.command != "download":
        parser.error(f"unsupported command: {args.command}")

    console = configure_terminal_logging(level=logging.INFO)
    start_ms = parse_datetime_input(args.start, default_timezone=args.input_timezone)
    end_ms = parse_datetime_input(args.end, default_timezone=args.input_timezone)
    client = OkxPublicClient(base_url=args.base_url)
    storage = DatasetStorage(Path(args.output), exchange="okx", instrument_id=args.instrument_id)

    instrument = client.fetch_instrument(inst_type="SWAP", inst_id=args.instrument_id)
    storage.write_json("metadata/instrument.json", instrument)

    datasets = [item.strip() for item in args.datasets.split(",") if item.strip()]
    summaries = []
    for dataset in datasets:
        LOGGER.info(
            "Dataset download started: dataset=%s instrument_id=%s start=%s end=%s input_timezone=%s",
            dataset,
            args.instrument_id,
            args.start,
            args.end,
            args.input_timezone,
        )
        rows = collect_dataset_rows(
            client=client,
            dataset=dataset,
            instrument_id=args.instrument_id,
            bar=args.bar,
            start_ms=start_ms,
            end_ms=end_ms,
            on_progress=lambda payload, ds=dataset: console.inline(
                "progress"
                f" | dataset={ds}"
                f" | page={payload.get('page_count')}"
                f" | rows={payload.get('rows_collected')}"
                f" | oldest={payload.get('oldest_in_page')}"
            ),
        )
        console.clear_inline()
        summary = storage.write_rows(DATASET_TO_PATH[dataset], rows, primary_key="funding_time" if dataset == "funding" else "ts")
        summary["requested_dataset"] = dataset
        if dataset == "funding":
            summary["note"] = "OKX public funding-rate history is limited to the most recent 3 months."
        summaries.append(summary)
        LOGGER.info(
            "Dataset download finished: dataset=%s rows_written=%s",
            dataset,
            summary["rows_written"],
        )

    console.clear_inline()
    print(json.dumps({"instrument_id": args.instrument_id, "summaries": summaries}, indent=2, ensure_ascii=True))
    return 0
