import csv
import gzip
import json
import tempfile
import unittest
from pathlib import Path

from full_data_extraction_for_btc.query import build_data_summary, preview_dataset_rows


class QueryTests(unittest.TestCase):
    def test_build_summary_and_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "data" / "okx" / "BTC-USDT-SWAP"
            manifest_dir = root / "metadata" / "manifests"
            manifest_dir.mkdir(parents=True, exist_ok=True)
            (manifest_dir / "candles.json").write_text(json.dumps({"rows_in_batch": 3}), encoding="utf-8")

            data_file = root / "candles" / "year=2024" / "month=03" / "data.csv.gz"
            data_file.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(data_file, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["ts", "close"])
                writer.writeheader()
                writer.writerow({"ts": "1", "close": "10"})
                writer.writerow({"ts": "2", "close": "11"})

            summary = build_data_summary(Path(tmp) / "data", "BTC-USDT-SWAP")
            self.assertTrue(summary["exists"])
            self.assertEqual(summary["partitions"]["candles"], 1)
            self.assertEqual(summary["manifests"]["candles"]["rows_in_batch"], 3)

            preview = preview_dataset_rows(Path(tmp) / "data", "BTC-USDT-SWAP", "candles", limit=1)
            self.assertEqual(len(preview), 1)
            self.assertEqual(preview[0]["ts"], "2")
