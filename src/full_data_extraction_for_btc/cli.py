from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="full-data-extraction-for-btc")
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser("download", help="Download OKX historical datasets.")
    download.add_argument("--start", required=True, help="UTC date or datetime.")
    download.add_argument("--end", required=True, help="UTC date or datetime.")
    download.add_argument("--datasets", default="candles,mark,index,funding")
    download.add_argument("--instrument-id", default="BTC-USDT-SWAP")
    download.add_argument("--bar", default="1m")
    download.add_argument("--output", default="data")
    download.add_argument("--base-url", default="https://www.okx.com")

    serve = subparsers.add_parser("serve", help="Run web workbench server.")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8766)
    serve.add_argument("--output-root", default=".")
    return parser
