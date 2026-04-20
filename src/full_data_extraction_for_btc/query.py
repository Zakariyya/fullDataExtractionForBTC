from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path
from typing import Any


def build_data_summary(output_root: Path, instrument_id: str) -> dict[str, Any]:
    base = output_root / "okx" / instrument_id
    manifests_dir = base / "metadata" / "manifests"
    manifests: dict[str, Any] = {}
    if manifests_dir.exists():
        for path in sorted(manifests_dir.glob("*.json")):
            manifests[path.stem] = json.loads(path.read_text(encoding="utf-8"))

    partitions: dict[str, int] = {}
    for dataset_dir in sorted(base.glob("*")):
        if not dataset_dir.is_dir() or dataset_dir.name == "metadata":
            continue
        count = sum(1 for _ in dataset_dir.glob("year=*/month=*/data.csv.gz"))
        partitions[dataset_dir.name] = count

    return {
        "base_path": str(base),
        "exists": base.exists(),
        "partitions": partitions,
        "manifests": manifests,
    }


def preview_dataset_rows(
    output_root: Path,
    instrument_id: str,
    dataset_path: str,
    limit: int = 50,
) -> list[dict[str, str]]:
    base = output_root / "okx" / instrument_id / dataset_path
    files = sorted(base.glob("year=*/month=*/data.csv.gz"), reverse=True)
    result: list[dict[str, str]] = []
    for path in files:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
            rows.reverse()
            for row in rows:
                result.append(row)
                if len(result) >= limit:
                    return result
    return result
