# OKX BTC Perpetual Data Lake Design

## Scope

Build a local downloader for `OKX` `BTC-USDT-SWAP` historical data that is suitable as a backtesting input dataset for long and short strategies.

Version 1 includes:

- trade candles (`/api/v5/market/history-candles`)
- mark price candles (`/api/v5/market/history-mark-price-candles`)
- index candles (`/api/v5/market/history-index-candles`)
- funding rate history (`/api/v5/public/funding-rate-history`)
- instrument metadata snapshot (`/api/v5/public/instruments`)

Version 1 does not include:

- authenticated account data
- historical order book or tick-level trades
- historical open interest snapshots

## Data Model

All timestamps are stored as UTC milliseconds and ISO-8601 UTC strings.

Datasets are normalized into partitioned `csv.gz` files:

- `data/okx/BTC-USDT-SWAP/candles/year=YYYY/month=MM/data.csv.gz`
- `data/okx/BTC-USDT-SWAP/mark_price_candles/year=YYYY/month=MM/data.csv.gz`
- `data/okx/BTC-USDT-SWAP/index_candles/year=YYYY/month=MM/data.csv.gz`
- `data/okx/BTC-USDT-SWAP/funding_rates/year=YYYY/month=MM/data.csv.gz`
- `data/okx/BTC-USDT-SWAP/metadata/instrument.json`
- `data/okx/BTC-USDT-SWAP/metadata/manifests/<dataset>.json`

Each partition rewrite is idempotent:

- load existing month partition if present
- merge by primary timestamp key
- sort ascending
- rewrite compressed file

## Downloader Behavior

The downloader pages backward from the requested end timestamp using OKX `after` pagination for time-series endpoints.

Rules:

- request only completed candles (`confirm=1`)
- keep rows in `[start, end)`
- deduplicate by timestamp
- persist each dataset independently
- continue when one dataset fails if the caller requested multiple datasets

## API Constraints

- `history-candles`: up to `300` rows per request
- `history-index-candles`: up to `100` rows per request
- `history-mark-price-candles`: up to `100` rows per request
- `funding-rate-history`: up to `400` rows per request and only up to the most recent 3 months

The downloader must surface the funding-rate retention limit explicitly in logs and manifest output.

## CLI

One command is enough for V1:

`python -m full_data_extraction_for_btc download --start 2020-01-01 --end 2020-02-01 --datasets candles,mark,index,funding`

The CLI will:

- parse UTC date or datetime inputs
- fetch requested datasets
- write normalized files
- print a per-dataset summary

## Testing

Tests cover:

- timestamp parsing
- response normalization
- backward pagination and range clipping
- month partition merge and dedupe
- funding-rate limit warning behavior
