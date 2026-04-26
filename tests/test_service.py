import tempfile
import time
import unittest
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import csv
import gzip

from full_data_extraction_for_btc.service import DownloadService, _day_index_file_path, _month_index_file_path


class FakeClient:
    def fetch_instrument(self, inst_type, inst_id):  # noqa: ANN001
        return {"instType": inst_type, "instId": inst_id}


class ServiceTests(unittest.TestCase):
    def test_download_parallel_workers_follow_request(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = DownloadService(Path(tmp), client_factory=lambda _base_url: FakeClient())

            def fake_collect(**_kwargs):  # noqa: ANN001
                return [{"ts": 1710000000000, "iso_time": "2024-03-09T16:00:00Z", "close": "100"}]

            with patch("full_data_extraction_for_btc.service.collect_dataset_rows", side_effect=fake_collect):
                task = service.start_download(
                    {
                        "start": "2024-01-31",
                        "end": "2024-02-02",
                        "datasets": ["candles"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1m",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                        "input_timezone": "UTC",
                        "download_workers": 2,
                    }
                )

                deadline = time.time() + 3
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

                snapshot = service.get_task(task.task_id)
                self.assertIsNotNone(snapshot)
                assert snapshot is not None
                self.assertEqual(snapshot.status, "completed")
                events = [json.loads(item) for item in snapshot.events_history]
                parallel_events = [evt for evt in events if evt.get("type") == "dataset_parallel_started"]
                self.assertTrue(parallel_events)
                self.assertEqual(parallel_events[0].get("workers"), 2)
                self.assertEqual(parallel_events[0].get("requested_workers"), 2)

    def test_normalize_download_workers(self) -> None:
        self.assertEqual(DownloadService._normalize_download_workers(None), 5)
        self.assertEqual(DownloadService._normalize_download_workers(""), 5)
        self.assertEqual(DownloadService._normalize_download_workers("7"), 7)
        self.assertEqual(DownloadService._normalize_download_workers(0), 1)
        self.assertEqual(DownloadService._normalize_download_workers(999), 64)

    def test_download_task_completes_and_writes_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = DownloadService(Path(tmp), client_factory=lambda _base_url: FakeClient())

            def fake_collect(**kwargs):  # noqa: ANN001
                on_progress = kwargs.get("on_progress")
                if on_progress is not None:
                    on_progress({"dataset": kwargs["dataset"], "page_count": 1, "rows_collected": 1, "oldest_in_page": 1})
                if kwargs["dataset"] == "funding":
                    return [{"funding_time": 1710000000000, "iso_time": "2024-03-09T16:00:00Z", "funding_rate": "0.1"}]
                return [{"ts": 1710000000000, "iso_time": "2024-03-09T16:00:00Z", "close": "100"}]

            with patch("full_data_extraction_for_btc.service.collect_dataset_rows", side_effect=fake_collect):
                task = service.start_download(
                    {
                        "start": "2024-03-01",
                        "end": "2024-03-10",
                        "datasets": ["candles", "funding"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1m",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                    }
                )

                deadline = time.time() + 3
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

                snapshot = service.get_task(task.task_id)
                self.assertIsNotNone(snapshot)
                self.assertEqual(snapshot.status, "completed")
                self.assertEqual(len(snapshot.summaries), 2)

    def test_download_skips_day_when_local_data_is_continuous(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data" / "okx" / "BTC-USDT-SWAP" / "candles" / "year=2024" / "month=03" / "data.csv.gz"
            data_path.parent.mkdir(parents=True, exist_ok=True)
            day_start = int(datetime(2024, 3, 1, tzinfo=timezone.utc).timestamp() * 1000)
            with gzip.open(data_path, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["ts", "iso_time", "open", "high", "low", "close"])
                writer.writeheader()
                writer.writerow(
                    {
                        "ts": str(day_start),
                        "iso_time": "2024-03-01T00:00:00Z",
                        "open": "100",
                        "high": "101",
                        "low": "99",
                        "close": "100.5",
                    }
                )

            service = DownloadService(root, client_factory=lambda _base_url: FakeClient())

            with patch("full_data_extraction_for_btc.service.collect_dataset_rows") as mocked_collect:
                task = service.start_download(
                    {
                        "start": "2024-03-01",
                        "end": "2024-03-02",
                        "datasets": ["candles"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1d",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                        "input_timezone": "UTC",
                    }
                )

                deadline = time.time() + 3
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

                snapshot = service.get_task(task.task_id)
                self.assertIsNotNone(snapshot)
                self.assertEqual(snapshot.status, "completed")
                self.assertEqual(mocked_collect.call_count, 0)
                assert snapshot is not None
                events = [json.loads(item) for item in snapshot.events_history]
                skip_events = [evt for evt in events if evt.get("type") == "dataset_day_skipped"]
                self.assertTrue(skip_events)
                self.assertEqual(skip_events[0].get("reason"), "local_data_continuous")

    def test_download_emits_day_minute_progress_for_candles(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            service = DownloadService(Path(tmp), client_factory=lambda _base_url: FakeClient())

            def fake_collect(**kwargs):  # noqa: ANN001
                on_progress = kwargs.get("on_progress")
                if on_progress is not None:
                    start_ms = kwargs["start_ms"]
                    end_ms = kwargs["end_ms"]
                    on_progress(
                        {
                            "dataset": kwargs["dataset"],
                            "page_count": 1,
                            "rows_collected": 20,
                            "oldest_in_page": end_ms - 30 * 60 * 1000,
                        }
                    )
                    on_progress(
                        {
                            "dataset": kwargs["dataset"],
                            "page_count": 2,
                            "rows_collected": 40,
                            "oldest_in_page": start_ms,
                        }
                    )
                return [{"ts": 1710000000000, "iso_time": "2024-03-09T16:00:00Z", "close": "100"}]

            with patch("full_data_extraction_for_btc.service.collect_dataset_rows", side_effect=fake_collect):
                task = service.start_download(
                    {
                        "start": "2024-03-01T00:00:00",
                        "end": "2024-03-01T01:00:00",
                        "datasets": ["candles"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1m",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                        "input_timezone": "UTC",
                    }
                )

                deadline = time.time() + 3
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

                snapshot = service.get_task(task.task_id)
                self.assertIsNotNone(snapshot)
                assert snapshot is not None
                self.assertEqual(snapshot.status, "completed")
                events = [json.loads(item) for item in snapshot.events_history]
                progress_events = [evt for evt in events if evt.get("type") == "dataset_day_progress"]
                self.assertTrue(progress_events)
                self.assertEqual(progress_events[0].get("processed_minutes"), 30)
                self.assertEqual(progress_events[0].get("total_minutes"), 60)
                self.assertEqual(progress_events[0].get("progress_pct"), 50.0)
                self.assertEqual(progress_events[-1].get("progress_pct"), 100.0)

    def test_download_skips_day_when_index_hit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            index_path = _day_index_file_path(
                output_root=root / "data",
                instrument_id="BTC-USDT-SWAP",
                dataset_path="candles",
                bar="1d",
                tz_name="UTC",
            )
            index_path.parent.mkdir(parents=True, exist_ok=True)
            index_path.write_text(
                json.dumps(
                    {
                        "dataset": "candles",
                        "bar": "1d",
                        "timezone": "UTC",
                        "days": ["2024-03-01"],
                    }
                ),
                encoding="utf-8",
            )

            service = DownloadService(root, client_factory=lambda _base_url: FakeClient())
            with patch("full_data_extraction_for_btc.service.collect_dataset_rows") as mocked_collect:
                task = service.start_download(
                    {
                        "start": "2024-03-01",
                        "end": "2024-03-02",
                        "datasets": ["candles"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1d",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                        "input_timezone": "UTC",
                    }
                )

                deadline = time.time() + 3
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

                snapshot = service.get_task(task.task_id)
                self.assertIsNotNone(snapshot)
                self.assertEqual(snapshot.status, "completed")
                self.assertEqual(mocked_collect.call_count, 0)
                assert snapshot is not None
                events = [json.loads(item) for item in snapshot.events_history]
                skip_events = [evt for evt in events if evt.get("type") == "dataset_day_skipped"]
                self.assertTrue(skip_events)
                self.assertEqual(skip_events[0].get("reason"), "day_index_hit")

    def test_download_skips_day_when_month_index_hit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            month_index_path = _month_index_file_path(
                output_root=root / "data",
                instrument_id="BTC-USDT-SWAP",
                dataset_path="candles",
                bar="1d",
                tz_name="UTC",
            )
            month_index_path.parent.mkdir(parents=True, exist_ok=True)
            month_index_path.write_text(
                json.dumps(
                    {
                        "dataset": "candles",
                        "bar": "1d",
                        "timezone": "UTC",
                        "months": ["2024-03"],
                    }
                ),
                encoding="utf-8",
            )

            service = DownloadService(root, client_factory=lambda _base_url: FakeClient())
            with patch("full_data_extraction_for_btc.service.collect_dataset_rows") as mocked_collect:
                task = service.start_download(
                    {
                        "start": "2024-03-01",
                        "end": "2024-03-02",
                        "datasets": ["candles"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1d",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                        "input_timezone": "UTC",
                    }
                )

                deadline = time.time() + 3
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

                snapshot = service.get_task(task.task_id)
                self.assertIsNotNone(snapshot)
                self.assertEqual(snapshot.status, "completed")
                self.assertEqual(mocked_collect.call_count, 0)
                assert snapshot is not None
                events = [json.loads(item) for item in snapshot.events_history]
                skip_events = [evt for evt in events if evt.get("type") == "dataset_month_skipped"]
                self.assertTrue(skip_events)
                self.assertEqual(skip_events[0].get("reason"), "month_index_hit")

    def test_rebuild_index_generates_day_index_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_path = root / "data" / "okx" / "BTC-USDT-SWAP" / "candles" / "year=2024" / "month=03" / "data.csv.gz"
            data_path.parent.mkdir(parents=True, exist_ok=True)
            day_one = int(datetime(2024, 3, 1, tzinfo=timezone.utc).timestamp() * 1000)
            day_two = int(datetime(2024, 3, 2, tzinfo=timezone.utc).timestamp() * 1000)
            with gzip.open(data_path, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["ts", "iso_time", "open", "high", "low", "close"])
                writer.writeheader()
                writer.writerow({"ts": str(day_one), "iso_time": "2024-03-01T00:00:00Z", "open": "1", "high": "1", "low": "1", "close": "1"})
                writer.writerow({"ts": str(day_two), "iso_time": "2024-03-02T00:00:00Z", "open": "1", "high": "1", "low": "1", "close": "1"})

            service = DownloadService(root, client_factory=lambda _base_url: FakeClient())
            task = service.start_rebuild_index(
                {
                    "datasets": ["candles"],
                    "instrument_id": "BTC-USDT-SWAP",
                    "bar": "1d",
                    "output_subdir": "data",
                    "input_timezone": "UTC",
                }
            )

            deadline = time.time() + 3
            while time.time() < deadline:
                snapshot = service.get_task(task.task_id)
                if snapshot is not None and snapshot.status in {"completed", "failed"}:
                    break
                time.sleep(0.05)

            snapshot = service.get_task(task.task_id)
            self.assertIsNotNone(snapshot)
            self.assertEqual(snapshot.status, "completed")

            index_path = _day_index_file_path(
                output_root=root / "data",
                instrument_id="BTC-USDT-SWAP",
                dataset_path="candles",
                bar="1d",
                tz_name="UTC",
            )
            payload = json.loads(index_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["days"], ["2024-03-01", "2024-03-02"])

            month_index_path = _month_index_file_path(
                output_root=root / "data",
                instrument_id="BTC-USDT-SWAP",
                dataset_path="candles",
                bar="1d",
                tz_name="UTC",
            )
            month_payload = json.loads(month_index_path.read_text(encoding="utf-8"))
            self.assertEqual(month_payload["months"], [])

    def test_month_index_written_after_full_month_completed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = DownloadService(root, client_factory=lambda _base_url: FakeClient())

            def fake_collect(**kwargs):  # noqa: ANN001
                start_ms = kwargs["start_ms"]
                return [{"ts": start_ms, "iso_time": "2024-03-01T00:00:00Z", "close": "100"}]

            with patch("full_data_extraction_for_btc.service.collect_dataset_rows", side_effect=fake_collect):
                task = service.start_download(
                    {
                        "start": "2024-03-01",
                        "end": "2024-04-01",
                        "datasets": ["candles"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1d",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                        "input_timezone": "UTC",
                    }
                )

                deadline = time.time() + 5
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

                snapshot = service.get_task(task.task_id)
                self.assertIsNotNone(snapshot)
                self.assertEqual(snapshot.status, "completed")

            month_index_path = _month_index_file_path(
                output_root=root / "data",
                instrument_id="BTC-USDT-SWAP",
                dataset_path="candles",
                bar="1d",
                tz_name="UTC",
            )
            month_payload = json.loads(month_index_path.read_text(encoding="utf-8"))
            self.assertEqual(month_payload["months"], ["2024-03"])

    def test_month_index_handles_leap_year_february(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            service = DownloadService(root, client_factory=lambda _base_url: FakeClient())

            def fake_collect(**kwargs):  # noqa: ANN001
                start_ms = kwargs["start_ms"]
                return [{"ts": start_ms, "iso_time": "2024-02-01T00:00:00Z", "close": "100"}]

            with patch("full_data_extraction_for_btc.service.collect_dataset_rows", side_effect=fake_collect):
                task = service.start_download(
                    {
                        "start": "2024-02-01",
                        "end": "2024-03-01",
                        "datasets": ["candles"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1d",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                        "input_timezone": "UTC",
                    }
                )

                deadline = time.time() + 5
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

                snapshot = service.get_task(task.task_id)
                self.assertIsNotNone(snapshot)
                self.assertEqual(snapshot.status, "completed")

            month_index_path = _month_index_file_path(
                output_root=root / "data",
                instrument_id="BTC-USDT-SWAP",
                dataset_path="candles",
                bar="1d",
                tz_name="UTC",
            )
            month_payload = json.loads(month_index_path.read_text(encoding="utf-8"))
            self.assertEqual(month_payload["months"], ["2024-02"])

    def test_download_uses_month_parallel_pool_when_span_exceeds_one_month(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            factory_calls: list[str] = []

            def client_factory(_base_url):  # noqa: ANN001
                factory_calls.append("called")
                return FakeClient()

            service = DownloadService(root, client_factory=client_factory)

            def fake_collect(**kwargs):  # noqa: ANN001
                start_ms = kwargs["start_ms"]
                return [{"ts": start_ms, "iso_time": "2024-03-31T00:00:00Z", "close": "100"}]

            with patch("full_data_extraction_for_btc.service.collect_dataset_rows", side_effect=fake_collect):
                task = service.start_download(
                    {
                        "start": "2024-03-31",
                        "end": "2024-04-02",
                        "datasets": ["candles"],
                        "instrument_id": "BTC-USDT-SWAP",
                        "bar": "1d",
                        "output_subdir": "data",
                        "base_url": "https://www.okx.com",
                        "input_timezone": "UTC",
                    }
                )

                deadline = time.time() + 5
                while time.time() < deadline:
                    snapshot = service.get_task(task.task_id)
                    if snapshot is not None and snapshot.status in {"completed", "failed"}:
                        break
                    time.sleep(0.05)

            snapshot = service.get_task(task.task_id)
            self.assertIsNotNone(snapshot)
            self.assertEqual(snapshot.status, "completed")
            # main task client + one client per month-worker (2 months)
            self.assertGreaterEqual(len(factory_calls), 3)
            events = [json.loads(item) for item in snapshot.events_history]
            parallel_events = [evt for evt in events if evt.get("type") == "dataset_parallel_started"]
            self.assertTrue(parallel_events)
            self.assertEqual(parallel_events[0].get("workers"), 2)
