# 🚀 BTC 永续数据工作台（OKX）

面向普通用户的本地工具：下载 OKX 永续合约历史数据，并在网页里查看任务进度、日志与数据预览。

🌐 英文版文档： [README.en.md](README.en.md)

## ✨ 你可以用它做什么

- 📥 一键下载多类数据：`candles` / `mark` / `index` / `funding`
- 🖥️ 打开网页工作台，直接点按钮启动任务
- 📡 实时查看 SSE 进度日志
- 🧾 自动维护连续性索引（跳过已完整日期，减少重复下载）
- 🔎 按时间范围预览数据，支持多周期 K 线查看

## 🤖 给 AI 代理的一句话（可直接复制）

```
你现在维护的项目是 `full-data-extraction-for-btc`（Python `>=3.10`，核心依赖 `fastapi`、`uvicorn`），请优先用 `python -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766` 启动网页服务（默认地址 `http://127.0.0.1:8766/`），下载命令使用 `python -m full_data_extraction_for_btc download --start <date> --end <date> --datasets candles,mark,index,funding --input-timezone Asia/Shanghai --bar 1m --output data`，并注意 OKX 公共接口的资金费率历史通常仅约最近 3 个月。
```

## 🐍 底层 Python 环境要求（重要）

- Python 版本：`3.10+`
- 建议命令优先使用 `python`（在虚拟环境内）
- 建议先检查：

```bash
python3 --version
python3 -m pip --version
```

## ✅ 标准启动流程（推荐）

```bash
# 1. 安装基础环境
sudo apt update
sudo apt install -y python3-pip python3-venv

# 2. 创建虚拟环境
python3 -m venv .venv

# 3. 激活环境
source .venv/bin/activate

# 4. 安装依赖（关键）
python -m pip install -U pip
python -m pip install -e .

# 5. 启动服务
python -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
```

👉 注意：

- `-e .` 会读取 `pyproject.toml` 自动安装依赖（包含 `uvicorn`）
- 完成后不需要再设置 `PYTHONPATH`
- 浏览器打开：`http://127.0.0.1:8766/`

## ⚡ uv 方式安装与启动（可选）

如果你更喜欢用 `uv` 管理依赖和运行命令，可以用这套：

```bash
# 1) 安装 uv（如果尚未安装）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2) 进入项目
cd /mnt/d/me/project/fullDataExtractionForBTC

# 3) 创建/同步环境（会按 pyproject.toml + uv.lock 安装依赖）
uv sync

# 4) 启动服务
uv run python -m full_data_extraction_for_btc serve --host 127.0.0.1 --port 8766
```

## 📥 下载命令示例

```bash
python -m full_data_extraction_for_btc download \
  --start 2026-04-01 \
  --end 2026-04-03 \
  --datasets candles,mark,index,funding \
  --input-timezone Asia/Shanghai \
  --bar 1m \
  --output data
```

## 🖱️ 网页里常见操作

- 启动下载：点击「启动下载」
- 重建索引：点击「重建索引」
- 看实时日志：查看「进度日志 (SSE)」
- 看下载进度：查看「下载进度看板」
- 看数据质量：点击「刷新摘要」
- 看覆盖日期：点击「展开覆盖看板」+「刷新看板」

## 🧪 运行测试

使用推荐安装流程后：

```bash
pytest -q
```

如果你是直接源码运行（未 `pip install -e .`）：

```bash
PYTHONPATH=src pytest -q
```

## ⚠️ 常见问题

- 页面看起来没更新：先重启服务，再浏览器强刷（`Ctrl+F5`）
- 模块导入失败：通常是没激活 `.venv` 或没执行 `pip install -e .`
- funding 数据偏少：属于 OKX 公共接口历史窗口限制

## 📝 文档风格约定（本仓库）

- 面向普通用户，先给可执行步骤，再给原理
- 中英文双文档保持同步
- 适度使用 emoji 提高扫描效率
- 增加 `AI Quick Read`，方便 AI 直接抽取命令与约束
