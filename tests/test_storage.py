import csv
import gzip
import tempfile
import unittest
from pathlib import Path

from full_data_extraction_for_btc.storage import DatasetStorage


class StorageTests(unittest.TestCase):
    def test_merge_rewrites_month_partition_sorted_and_deduped(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = DatasetStorage(Path(tmp), exchange="okx", instrument_id="BTC-USDT-SWAP")
            rows = [
                {"ts": 1704067260000, "iso_time": "2024-01-01T00:01:00Z", "close": "2"},
                {"ts": 1704067200000, "iso_time": "2024-01-01T00:00:00Z", "close": "1"},
            ]
            storage.write_rows("candles", rows, primary_key="ts")
            storage.write_rows(
                "candles",
                [{"ts": 1704067200000, "iso_time": "2024-01-01T00:00:00Z", "close": "9"}],
                primary_key="ts",
            )

            path = Path(tmp) / "okx" / "BTC-USDT-SWAP" / "candles" / "year=2024" / "month=01" / "data.csv.gz"
            with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = list(reader)

            self.assertEqual([row["ts"] for row in rows], ["1704067200000", "1704067260000"])
            self.assertEqual(rows[0]["close"], "9")
