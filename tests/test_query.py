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

            preview_all = preview_dataset_rows(Path(tmp) / "data", "BTC-USDT-SWAP", "candles", limit=0)
            self.assertEqual(len(preview_all), 2)
            self.assertEqual(preview_all[0]["ts"], "2")
            self.assertEqual(preview_all[1]["ts"], "1")

    def test_preview_with_date_range_and_kline_interval(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "data" / "okx" / "BTC-USDT-SWAP"
            data_file = root / "candles" / "year=2026" / "month=04" / "data.csv.gz"
            data_file.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(data_file, "wt", encoding="utf-8", newline="") as handle:
                fieldnames = ["ts", "open", "high", "low", "close", "volume_contracts", "volume_base", "volume_quote"]
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(
                    {
                        "ts": "1774972800000",
                        "open": "100",
                        "high": "110",
                        "low": "95",
                        "close": "105",
                        "volume_contracts": "10",
                        "volume_base": "1.5",
                        "volume_quote": "150",
                    }
                )
                writer.writerow(
                    {
                        "ts": "1774974900000",
                        "open": "105",
                        "high": "112",
                        "low": "102",
                        "close": "111",
                        "volume_contracts": "20",
                        "volume_base": "2.5",
                        "volume_quote": "250",
                    }
                )
                writer.writerow(
                    {
                        "ts": "1774975500000",
                        "open": "111",
                        "high": "120",
                        "low": "108",
                        "close": "118",
                        "volume_contracts": "30",
                        "volume_base": "3.5",
                        "volume_quote": "350",
                    }
                )

            # 2026-04-01 00:00 ~ 01:00 (Asia/Shanghai), 聚合为 30m
            preview = preview_dataset_rows(
                Path(tmp) / "data",
                "BTC-USDT-SWAP",
                "candles",
                start="2026-04-01 00:00:00",
                end="2026-04-01 01:00:00",
                input_timezone="Asia/Shanghai",
                kline_interval="30m",
                limit=10,
            )
            self.assertEqual(len(preview), 2)
            # 返回按 ts 倒序，第一根是 00:30 的桶
            self.assertEqual(preview[0]["open"], "105")
            self.assertEqual(preview[0]["close"], "118")
            self.assertEqual(preview[1]["open"], "100")
            self.assertEqual(preview[1]["close"], "105")
