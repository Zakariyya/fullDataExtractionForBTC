import unittest

from full_data_extraction_for_btc.downloader import collect_dataset_rows, resolve_request_instrument_id


class FakeClient:
    def __init__(self) -> None:
        self.calls = []
        self.pages = [
            [
                ["1704067380000", "3", "4", "2", "3.5", "1", "1", "1", "1"],
                ["1704067320000", "2", "3", "1", "2.5", "1", "1", "1", "1"],
            ],
            [
                ["1704067260000", "1", "2", "0.5", "1.5", "1", "1", "1", "1"],
                ["1704067200000", "0.5", "1.5", "0.1", "1.0", "1", "1", "1", "1"],
            ],
        ]

    def fetch_candles(self, dataset, inst_id, bar, after=None, limit=None):  # noqa: ANN001
        self.calls.append(after)
        return self.pages.pop(0) if self.pages else []


class FakeStalledClient:
    def __init__(self) -> None:
        self.calls = []
        self.page = [
            ["1704067380000", "3", "4", "2", "3.5", "1", "1", "1", "1"],
            ["1704067320000", "2", "3", "1", "2.5", "1", "1", "1", "1"],
        ]

    def fetch_candles(self, dataset, inst_id, bar, after=None, limit=None):  # noqa: ANN001
        self.calls.append(after)
        # Simulate an exchange cursor bug: every request returns the same page.
        return list(self.page)


class DownloaderTests(unittest.TestCase):
    def test_collect_dataset_rows_pages_backward_and_clips_range(self) -> None:
        client = FakeClient()
        rows = collect_dataset_rows(
            client=client,
            dataset="candles",
            instrument_id="BTC-USDT-SWAP",
            bar="1m",
            start_ms=1704067260000,
            end_ms=1704067380000,
        )

        self.assertEqual([row["ts"] for row in rows], [1704067260000, 1704067320000])
        self.assertEqual(client.calls, ["1704067380000", "1704067320000"])

    def test_index_dataset_uses_index_instrument_id(self) -> None:
        self.assertEqual(resolve_request_instrument_id("index", "BTC-USDT-SWAP"), "BTC-USDT")

    def test_collect_dataset_rows_breaks_pagination_stall(self) -> None:
        client = FakeStalledClient()
        rows = collect_dataset_rows(
            client=client,
            dataset="candles",
            instrument_id="BTC-USDT-SWAP",
            bar="1m",
            start_ms=1704067000000,
            end_ms=1704067400000,
        )

        # Dedup by ts should keep stable result without infinite loop.
        self.assertEqual([row["ts"] for row in rows], [1704067320000, 1704067380000])
        # First call uses end cursor, subsequent calls should force backward movement.
        self.assertEqual(client.calls[0], "1704067400000")
        self.assertTrue(len(client.calls) > 1)
        self.assertTrue(all(int(client.calls[i]) < int(client.calls[i - 1]) for i in range(2, len(client.calls))))
