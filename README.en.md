# 🚀 BTC Perpetual Data Workbench (OKX)

A local, user-friendly tool to download OKX perpetual historical data and manage tasks in a web UI with live logs and preview.

🌐 中文版: [README.md](README.md)

## ✨ What You Can Do

- 📥 Download multiple datasets: `candles` / `mark` / `index` / `funding`
- 🖥️ Start tasks from a web page (no complex setup flow)
- 📡 Watch live SSE progress logs
- 🧾 Use continuity indexes to skip already complete days/months
- 🔎 Preview data by date range and K-line interval

## 🤖 AI Quick Read

- Project: `full-data-extraction-for-btc`
- Python: `>=3.10`
- Core deps: `fastapi`, `uvicorn`
- Web start command:
  - `python -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766`
- CLI download command:
  - `python -m full_data_extraction_for_btc download --start <date> --end <date> --datasets candles,mark,index,funding --input-timezone Asia/Shanghai --bar 1m --output data`
- Default UI URL: `http://127.0.0.1:8766/`
- Funding note: OKX public API usually provides only ~last 3 months for funding history

## 🐍 Python Runtime Requirements

- Python version: `3.10+`
- Prefer using `python` inside a virtual environment
- Quick check:

```bash
python3 --version
python3 -m pip --version
```

## ✅ Standard Startup Flow (Recommended)

```bash
# 1. Install base environment
sudo apt update
sudo apt install -y python3-pip python3-venv

# 2. Create virtual environment
python3 -m venv .venv

# 3. Activate environment
source .venv/bin/activate

# 4. Install dependencies (important)
python -m pip install -U pip
python -m pip install -e .

# 5. Start service
python -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
```

👉 Notes:

- `-e .` reads `pyproject.toml` and installs dependencies automatically (including `uvicorn`)
- After that, no `PYTHONPATH` is needed
- Open UI at: `http://127.0.0.1:8766/`

## ⚡ Optional: uv Workflow

If you prefer `uv` for dependency/runtime management:

```bash
# 1) Install uv (if not installed yet)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2) Enter project
cd /mnt/d/me/project/fullDataExtractionForBTC

# 3) Sync environment from pyproject.toml + uv.lock
uv sync

# 4) Start service
uv run python -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
```

## 📥 Download Example

```bash
python -m full_data_extraction_for_btc download \
  --start 2026-04-01 \
  --end 2026-04-03 \
  --datasets candles,mark,index,funding \
  --input-timezone Asia/Shanghai \
  --bar 1m \
  --output data
```

## 🖱️ Common UI Actions

- Start download: click `启动下载`
- Rebuild index: click `重建索引`
- View live logs: `进度日志 (SSE)`
- View progress board: `下载进度看板`
- Refresh quality summary: `刷新摘要`
- Refresh coverage board: `展开覆盖看板` + `刷新看板`

## 🧪 Run Tests

After recommended installation:

```bash
pytest -q
```

If you run directly from source (without `pip install -e .`):

```bash
PYTHONPATH=src pytest -q
```

## ⚠️ Common Issues

- UI looks old: restart service and hard refresh browser (`Ctrl+F5`)
- Import error: usually `.venv` is not active or `pip install -e .` was skipped
- Limited funding history: expected due to OKX public API window

## 📝 Documentation Style (This Repo)

- User-first writing: executable steps first, deeper details second
- Keep Chinese and English docs aligned
- Use emoji to improve scan readability
- Keep an `AI Quick Read` block for fast model parsing
