"""Project-local run entry so zsh `run` can start the web server directly."""

from __future__ import annotations

import sys
from pathlib import Path


# Ensure local src layout works even without editable install.
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if SRC.exists():
    sys.path.insert(0, str(SRC))

from full_data_extraction_for_btc.main import main


if __name__ == "__main__":
    sys.argv = [
        "run.py",
        "serve",
        "--host",
        "127.0.0.1",
        "--port",
        "8766",
    ]
    raise SystemExit(main())
