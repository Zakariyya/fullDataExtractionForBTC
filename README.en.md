# 🚀 BTC Perpetual Historical Data Downloader (OKX)

A local tool for everyday users to download `BTC-USDT-SWAP` historical data, with a web workbench, live task logs, continuity indexes, and quick data preview.

## 🌐 Language Versions

- 中文: [`README.md`](README.md)
- English (current): [`README.en.md`](README.en.md)

## ✨ What This Project Does

- Downloads multiple datasets: `candles` / `mark` / `index` / `funding`
- Provides a web workbench (start tasks, watch logs, preview data)
- Streams live events via SSE
- Maintains day index + month index for continuity checks and smart skip

## 📂 Output Paths

The output root is your `--output` argument (for example `data`).

Data is saved under:

- `data/okx/BTC-USDT-SWAP/candles/`
- `data/okx/BTC-USDT-SWAP/funding_rates/`
- `data/okx/BTC-USDT-SWAP/index_candles/`
- `data/okx/BTC-USDT-SWAP/mark_price_candles/`
- `data/okx/BTC-USDT-SWAP/metadata/`

Input dataset names in command are: `candles,mark,index,funding`.  
They are automatically mapped to the five folders above.

## 🧠 How Each Dataset Helps Quant Strategies

- `candles` 📈  
  Trade-price OHLCV. Core input for most models (signals, backtest execution, volatility/volume factors).
- `mark_price_candles` 🛡️  
  Mark-price perspective for derivatives risk control (reduces bias from trade-price-only assumptions).
- `index_candles` 🌍  
  Index-price perspective for benchmark/fair-value checks (spread, deviation, anomaly filters).
- `funding_rates` 💸  
  Funding cost stream for PnL realism, especially important for medium/long holding, basis and carry styles.  
  Note: OKX public API only keeps recent ~3 months.
- `metadata` 🧾  
  Not market bars. Stores instrument snapshot, manifests, and day/month continuity indexes.  
  Helps with data governance: skip complete ranges, verify coverage, rebuild continuity state.

## ✅ Simple Recommended Workflow

- Start with `candles` to build and validate the core signal logic.
- Add `funding_rates` to model real holding costs.
- Add `mark_price_candles` if liquidation/risk behavior matters.
- Add `index_candles` for spread/deviation and fair-value style filters.
- Keep `metadata` indexes healthy (refresh/rebuild when needed) for continuity quality.

## ⚡ Run Without Installation (Clone-and-Run)

If you clone this repo and do not install it (`pip install -e .`), use:

```bash
PYTHONPATH=src python3 -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
```

Open: `http://127.0.0.1:8766/`

Download example:

```bash
PYTHONPATH=src python3 -m full_data_extraction_for_btc download \
  --start 2026-04-01 \
  --end 2026-04-02 \
  --input-timezone Asia/Shanghai \
  --bar 1m \
  --datasets candles,mark,index,funding \
  --output data
```

Run tests (exact command requested):

```bash
PYTHONPATH=src pytest -q
```

## 🧰 Recommended for New Environments: Editable Install

For long-term use or team collaboration:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -e .
pip install pytest
```

Then run commands without `PYTHONPATH=src`:

```bash
python -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
pytest -q
```

## 🕒 Time Fields

- `ts` / `iso_time`: UTC (source of truth)
- `local_time_cn`: China local time (`Asia/Shanghai`)
- `trade_date_cn`: China local calendar date

For OKX CN app style date boundaries, use `--input-timezone Asia/Shanghai`.

## ⚠️ Known Limitation

- `funding` history from the OKX public API is limited to roughly the most recent 3 months.
