from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path
from typing import Any

from full_data_extraction_for_btc.timeutils import to_iso_local, to_iso_utc, to_local_date


class DatasetStorage:
    def __init__(self, root: Path, exchange: str, instrument_id: str) -> None:
        self.base = root / exchange / instrument_id

    def write_rows(self, dataset: str, rows: list[dict[str, Any]], primary_key: str) -> dict[str, Any]:
        if not rows:
            return {"dataset": dataset, "rows_written": 0}

        enriched_rows = [self._enrich_row_with_cn_time(row, primary_key) for row in rows]
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in enriched_rows:
            iso_time = row.get("iso_time") or to_iso_utc(int(row[primary_key]))
            year = iso_time[0:4]
            month = iso_time[5:7]
            grouped.setdefault((year, month), []).append(row)

        total = 0
        for (year, month), month_rows in grouped.items():
            total += self._write_month(dataset, year, month, month_rows, primary_key)

        self._write_manifest(dataset, enriched_rows, primary_key)
        return {"dataset": dataset, "rows_written": total}

    def write_json(self, relative_path: str, payload: Any) -> Path:
        path = self.base / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        return path

    def _write_month(
        self,
        dataset: str,
        year: str,
        month: str,
        rows: list[dict[str, Any]],
        primary_key: str,
    ) -> int:
        path = self.base / dataset / f"year={year}" / f"month={month}" / "data.csv.gz"
        path.parent.mkdir(parents=True, exist_ok=True)

        merged: dict[str, dict[str, Any]] = {}
        if path.exists():
            with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    merged[row[primary_key]] = row

        for row in rows:
            normalized = {key: str(value) for key, value in row.items()}
            merged[normalized[primary_key]] = normalized

        ordered = [merged[key] for key in sorted(merged, key=int)]
        fieldnames = list(ordered[0].keys())
        with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(ordered)
        return len(rows)

    def _write_manifest(self, dataset: str, rows: list[dict[str, Any]], primary_key: str) -> None:
        ordered_keys = sorted((int(row[primary_key]) for row in rows))
        manifest = {
            "dataset": dataset,
            "rows_in_batch": len(rows),
            "min_ts": ordered_keys[0],
            "max_ts": ordered_keys[-1],
            "min_iso_time": to_iso_utc(ordered_keys[0]),
            "max_iso_time": to_iso_utc(ordered_keys[-1]),
        }
        self.write_json(f"metadata/manifests/{dataset}.json", manifest)

    def _enrich_row_with_cn_time(self, row: dict[str, Any], primary_key: str) -> dict[str, Any]:
        enriched = dict(row)
        ts = int(enriched[primary_key])
        enriched["local_time_cn"] = to_iso_local(ts, "Asia/Shanghai")
        enriched["trade_date_cn"] = to_local_date(ts, "Asia/Shanghai")
        return enriched
