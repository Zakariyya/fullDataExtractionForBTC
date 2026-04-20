from __future__ import annotations

from typing import Any

from full_data_extraction_for_btc.timeutils import to_iso_utc


def normalize_candle_row(raw: list[str], dataset: str, instrument_id: str, bar: str) -> dict[str, Any]:
    confirm = raw[-1]
    if confirm != "1":
        raise ValueError("incomplete candle")

    ts = int(raw[0])
    row: dict[str, Any] = {
        "ts": ts,
        "iso_time": to_iso_utc(ts),
        "dataset": dataset,
        "instrument_id": instrument_id,
        "bar": bar,
        "open": raw[1],
        "high": raw[2],
        "low": raw[3],
        "close": raw[4],
    }
    if dataset == "candles":
        row["volume_contracts"] = raw[5]
        row["volume_base"] = raw[6]
        row["volume_quote"] = raw[7]
    return row


def normalize_funding_row(raw: dict[str, str], instrument_id: str) -> dict[str, Any]:
    ts = int(raw["fundingTime"])
    return {
        "funding_time": ts,
        "iso_time": to_iso_utc(ts),
        "instrument_id": instrument_id,
        "funding_rate": raw.get("fundingRate", ""),
        "realized_rate": raw.get("realizedRate", ""),
        "method": raw.get("method", ""),
        "formula_type": raw.get("formulaType", ""),
    }
