import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from full_data_extraction_for_btc.service import DownloadService


class FakeClient:
    def fetch_instrument(self, inst_type, inst_id):  # noqa: ANN001
        return {"instType": inst_type, "instId": inst_id}


class ServiceTests(unittest.TestCase):
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
