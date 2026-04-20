# OKX BTC Perpetual Historical Downloader

Download `OKX` `BTC-USDT-SWAP` historical datasets for backtesting.

## Supported datasets

- `candles`
- `mark`
- `index`
- `funding`

## Quick start

```bash
PYTHONPATH=src python3 -m full_data_extraction_for_btc download \
  --start 2024-01-01 \
  --end 2024-01-02 \
  --datasets candles,mark,index
```

Funding-rate history on OKX public API is limited to the most recent 3 months.

## Web workbench (FastAPI)

```bash
PYTHONPATH=src python3 -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
```

Then open `http://127.0.0.1:8766/`.

Capabilities in v1:

- create and track download tasks
- live task events via SSE
- data summary from manifests and partitions
- latest-row preview for saved datasets
