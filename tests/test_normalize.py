import unittest

from full_data_extraction_for_btc.normalize import normalize_candle_row
from full_data_extraction_for_btc.timeutils import parse_datetime_input


class TimeParsingTests(unittest.TestCase):
    def test_parse_date_string_as_utc_midnight(self) -> None:
        self.assertEqual(parse_datetime_input("2026-04-20"), 1776643200000)


class NormalizeCandleTests(unittest.TestCase):
    def test_normalize_trade_candle_row(self) -> None:
        row = normalize_candle_row(
            [
                "1713571200000",
                "65000.1",
                "65020.2",
                "64990.3",
                "65010.4",
                "123.4",
                "1.9",
                "123456.7",
                "1",
            ],
            dataset="candles",
            instrument_id="BTC-USDT-SWAP",
            bar="1m",
        )

        self.assertEqual(row["ts"], 1713571200000)
        self.assertEqual(row["open"], "65000.1")
        self.assertEqual(row["volume_contracts"], "123.4")
        self.assertEqual(row["instrument_id"], "BTC-USDT-SWAP")

    def test_skip_incomplete_candle(self) -> None:
        with self.assertRaises(ValueError):
            normalize_candle_row(
                ["1713571200000", "1", "2", "0.5", "1.5", "0", "0", "0", "0"],
                dataset="candles",
                instrument_id="BTC-USDT-SWAP",
                bar="1m",
            )
