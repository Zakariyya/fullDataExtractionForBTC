from __future__ import annotations

from collections.abc import Callable
from typing import Any

from full_data_extraction_for_btc.normalize import normalize_candle_row, normalize_funding_row

CANDLE_DATASETS = {"candles", "mark", "index"}
DATASET_TO_PATH = {
    "candles": "candles",
    "mark": "mark_price_candles",
    "index": "index_candles",
    "funding": "funding_rates",
}


def collect_dataset_rows(
    client: Any,
    dataset: str,
    instrument_id: str,
    bar: str,
    start_ms: int,
    end_ms: int,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    request_instrument_id = resolve_request_instrument_id(dataset, instrument_id)
    if dataset in CANDLE_DATASETS:
        return _collect_candle_rows(
            client,
            dataset,
            instrument_id,
            request_instrument_id,
            bar,
            start_ms,
            end_ms,
            on_progress=on_progress,
        )
    if dataset == "funding":
        return _collect_funding_rows(
            client,
            instrument_id,
            request_instrument_id,
            start_ms,
            end_ms,
            on_progress=on_progress,
        )
    raise ValueError(f"unsupported dataset: {dataset}")


def _collect_candle_rows(
    client: Any,
    dataset: str,
    instrument_id: str,
    request_instrument_id: str,
    bar: str,
    start_ms: int,
    end_ms: int,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    # Use end_ms as the first cursor to avoid paging from latest market data.
    after: str | None = str(end_ms)
    previous_after_int: int | None = int(after)
    pagination_stall_count = 0
    collected: dict[int, dict[str, Any]] = {}
    page_count = 0

    while True:
        page = client.fetch_candles(dataset=dataset, inst_id=request_instrument_id, bar=bar, after=after, limit=None)
        if not page:
            break
        page_count += 1

        oldest_in_page: int | None = None
        for raw in page:
            try:
                row = normalize_candle_row(raw, dataset=dataset, instrument_id=instrument_id, bar=bar)
            except ValueError:
                continue
            ts = row["ts"]
            oldest_in_page = ts if oldest_in_page is None else min(oldest_in_page, ts)
            if start_ms <= ts < end_ms:
                collected[ts] = row

        if on_progress is not None:
            on_progress(
                {
                    "dataset": dataset,
                    "page_count": page_count,
                    "rows_collected": len(collected),
                    "oldest_in_page": oldest_in_page,
                }
            )

        if oldest_in_page is None:
            break
        if oldest_in_page <= start_ms:
            break
        next_after = oldest_in_page
        if previous_after_int is not None and next_after >= previous_after_int:
            # Defensive guard: if cursor does not move backward, force it to move
            # to avoid paging the same slice forever.
            pagination_stall_count += 1
            next_after = previous_after_int - 1
            if on_progress is not None:
                on_progress(
                    {
                        "dataset": dataset,
                        "page_count": page_count,
                        "rows_collected": len(collected),
                        "oldest_in_page": oldest_in_page,
                        "pagination_stall_count": pagination_stall_count,
                        "pagination_stall_breaker": True,
                    }
                )
            if next_after <= start_ms or pagination_stall_count >= 5:
                break
        else:
            pagination_stall_count = 0
        after = str(next_after)
        previous_after_int = next_after

    return [collected[key] for key in sorted(collected)]


def _collect_funding_rows(
    client: Any,
    instrument_id: str,
    request_instrument_id: str,
    start_ms: int,
    end_ms: int,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, Any]]:
    # Use end_ms as the first cursor to avoid paging from latest market data.
    after: str | None = str(end_ms)
    previous_after_int: int | None = int(after)
    pagination_stall_count = 0
    collected: dict[int, dict[str, Any]] = {}
    page_count = 0

    while True:
        page = client.fetch_funding_rate_history(inst_id=request_instrument_id, after=after, limit="400")
        if not page:
            break
        page_count += 1

        oldest_in_page: int | None = None
        for raw in page:
            row = normalize_funding_row(raw, instrument_id=instrument_id)
            ts = row["funding_time"]
            oldest_in_page = ts if oldest_in_page is None else min(oldest_in_page, ts)
            if start_ms <= ts < end_ms:
                collected[ts] = row

        if on_progress is not None:
            on_progress(
                {
                    "dataset": "funding",
                    "page_count": page_count,
                    "rows_collected": len(collected),
                    "oldest_in_page": oldest_in_page,
                }
            )

        if oldest_in_page is None:
            break
        if oldest_in_page <= start_ms:
            break
        next_after = oldest_in_page
        if previous_after_int is not None and next_after >= previous_after_int:
            # Defensive guard: if cursor does not move backward, force it to move
            # to avoid paging the same slice forever.
            pagination_stall_count += 1
            next_after = previous_after_int - 1
            if on_progress is not None:
                on_progress(
                    {
                        "dataset": "funding",
                        "page_count": page_count,
                        "rows_collected": len(collected),
                        "oldest_in_page": oldest_in_page,
                        "pagination_stall_count": pagination_stall_count,
                        "pagination_stall_breaker": True,
                    }
                )
            if next_after <= start_ms or pagination_stall_count >= 5:
                break
        else:
            pagination_stall_count = 0
        after = str(next_after)
        previous_after_int = next_after

    return [collected[key] for key in sorted(collected)]


def resolve_request_instrument_id(dataset: str, instrument_id: str) -> str:
    if dataset == "index" and instrument_id.endswith("-SWAP"):
        return instrument_id[: -len("-SWAP")]
    return instrument_id
