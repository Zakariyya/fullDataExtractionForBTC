from __future__ import annotations

import json
import logging
from pathlib import Path

import uvicorn

from full_data_extraction_for_btc.cli import build_parser
from full_data_extraction_for_btc.client import OkxPublicClient
from full_data_extraction_for_btc.downloader import DATASET_TO_PATH, collect_dataset_rows
from full_data_extraction_for_btc.storage import DatasetStorage
from full_data_extraction_for_btc.timeutils import parse_datetime_input
from full_data_extraction_for_btc.webapp import create_app

LOGGER = logging.getLogger("full_data_extraction_for_btc")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "serve":
        app = create_app(Path(args.output_root))
        uvicorn.run(app, host=args.host, port=args.port)
        return 0
    if args.command != "download":
        parser.error(f"unsupported command: {args.command}")

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
            on_progress=lambda payload, ds=dataset: LOGGER.info(
                "Dataset progress: dataset=%s page=%s rows_collected=%s oldest_in_page=%s",
                ds,
                payload.get("page_count"),
                payload.get("rows_collected"),
                payload.get("oldest_in_page"),
            ),
        )
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

    print(json.dumps({"instrument_id": args.instrument_id, "summaries": summaries}, indent=2, ensure_ascii=True))
    return 0
