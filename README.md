# OKX BTC Perpetual Historical Downloader

## Quick Start
在项目根目录 fullDataExtractionForBTC 运行。

  启动网页：

  PYTHONPATH=src python3 -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766

  打开：http://127.0.0.1:8766/

  可用命令目前就两个：
  PYTHONPATH=src python3 -m full_data_extraction_for_btc download \
    --start 2026-04-01 \
    --end 2026-04-02 \
    --input-timezone Asia/Shanghai \
    --bar 1m \
    --output data
  - 网页服务帮助：PYTHONPATH=src python3 -m full_data_extraction_for_btc serve --help

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
  --input-timezone UTC \
  --datasets candles,mark,index
```

Funding-rate history on OKX public API is limited to the most recent 3 months.
If you compare with OKX app in China local time, set `--input-timezone Asia/Shanghai`.

## Time fields

Saved rows keep UTC as the source of truth:

- `ts` and `iso_time` are UTC.

For easier comparison with OKX CN app views, rows also include:

- `local_time_cn` (`Asia/Shanghai`)
- `trade_date_cn` (`Asia/Shanghai` natural date)

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
