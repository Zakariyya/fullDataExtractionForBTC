# 🚀 BTC 永续历史数据下载工具（OKX）

一个面向普通用户的本地工具：下载 `BTC-USDT-SWAP` 历史数据，支持网页工作台、任务日志、连续性索引与数据预览。

## 🌐 语言版本

- 中文（当前）：[`README.md`](README.md)
- English: [`README.en.md`](README.en.md)

## ✨ 这个项目能做什么

- 下载多类历史数据：`candles` / `mark` / `index` / `funding`
- 提供网页工作台（任务发起、进度日志、数据预览）
- 提供 SSE 实时事件流
- 提供日索引 + 月索引（用于时间连续性校验与跳过已完整数据）

## 📂 输出路径说明

默认输出根目录是你命令里的 `--output`（例如 `data`）。

实际数据会写到：

- `data/okx/BTC-USDT-SWAP/candles/`
- `data/okx/BTC-USDT-SWAP/funding_rates/`
- `data/okx/BTC-USDT-SWAP/index_candles/`
- `data/okx/BTC-USDT-SWAP/mark_price_candles/`
- `data/okx/BTC-USDT-SWAP/metadata/`

你在命令里填的是下载数据集名：`candles,mark,index,funding`。  
落盘目录会自动映射为上面 5 类路径。

## 🧠 五类数据怎么用在量化策略里

- `candles` 📈  
  含成交价 OHLCV，是大多数策略的主输入（信号生成、回测撮合、波动率与成交量特征）。
- `mark_price_candles` 🛡️  
  标记价格视角，适合做合约风控与强平风险评估（避免只看成交价造成偏差）。
- `index_candles` 🌍  
  指数价格视角，常用于“市场基准价”判断（价差、偏离、过滤异常波动）。
- `funding_rates` 💸  
  资金费率，用于估算持仓成本/收益侵蚀，对中长持仓、套利和基差策略很关键。  
  注意：OKX 公共接口仅近 3 个月。
- `metadata` 🧾  
  不是行情本体，包含合约信息、清单、日/月连续性索引。  
  作用是保证数据治理：快速跳过已完整数据、追踪覆盖范围、辅助重建索引。

## ✅ 推荐使用方式（轻松版）

- 先用 `candles` 跑通策略主逻辑。
- 再叠加 `funding_rates` 做真实成本校正。
- 对永续合约风险敏感策略，再引入 `mark_price_candles`。
- 需要做基准偏离或价差判断时，引入 `index_candles`。
- 定期重建或刷新 `metadata` 索引，保证时间连续性可追踪。

## ⚡ 免安装直接运行（仓库拿来即用）

如果你只是直接拉仓库，不做 `pip install -e .`，推荐使用下面这种方式：

```bash
PYTHONPATH=src python3 -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
```

打开：`http://127.0.0.1:8766/`

下载示例：

```bash
PYTHONPATH=src python3 -m full_data_extraction_for_btc download \
  --start 2026-04-01 \
  --end 2026-04-02 \
  --input-timezone Asia/Shanghai \
  --bar 1m \
  --datasets candles,mark,index,funding \
  --output data
```

运行测试（你指定的命令）：

```bash
PYTHONPATH=src pytest -q
```

## 🧰 推荐方式：新环境可编辑安装

如果是长期开发或多人协作，建议先创建虚拟环境并安装项目：

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -e .
pip install pytest
```

之后可直接运行（无需 `PYTHONPATH=src`）：

```bash
python -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
pytest -q
```

## 🕒 时间字段说明

- `ts` / `iso_time`：UTC（统一基准时间）
- `local_time_cn`：北京时间（`Asia/Shanghai`）
- `trade_date_cn`：北京时间自然日

如果你希望和 OKX 国内时间视角对齐，下载时请使用 `--input-timezone Asia/Shanghai`。

## ⚠️ 已知限制

- `funding`（资金费率）受 OKX 公共接口限制，仅能获取最近约 3 个月历史。
