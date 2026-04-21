import io
import unittest
from contextlib import redirect_stdout

from full_data_extraction_for_btc.cli import build_parser


class CliTests(unittest.TestCase):
    def test_parser_supports_download_command(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "download",
                "--start",
                "2024-01-01",
                "--end",
                "2024-01-02",
                "--datasets",
                "candles,mark",
                "--input-timezone",
                "Asia/Shanghai",
            ]
        )
        self.assertEqual(args.command, "download")
        self.assertEqual(args.datasets, "candles,mark")
        self.assertEqual(args.input_timezone, "Asia/Shanghai")

    def test_help_prints_download_subcommand(self) -> None:
        parser = build_parser()
        buffer = io.StringIO()
        with self.assertRaises(SystemExit):
            with redirect_stdout(buffer):
                parser.parse_args(["--help"])
        self.assertIn("download", buffer.getvalue())

    def test_parser_supports_serve_command(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["serve", "--host", "0.0.0.0", "--port", "9000"])
        self.assertEqual(args.command, "serve")
        self.assertEqual(args.host, "0.0.0.0")
        self.assertEqual(args.port, 9000)
