# OKX BTC Perpetual Downloader Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local OKX `BTC-USDT-SWAP` historical downloader with normalized monthly storage for backtesting inputs.

**Architecture:** A small Python package exposes a CLI that drives an OKX public REST client, normalizes raw API payloads into typed rows, and persists deduplicated monthly `csv.gz` partitions plus metadata manifests. The downloader fetches candle-like datasets backward in time and stores instrument metadata separately.

**Tech Stack:** Python standard library, `unittest`, gzip CSV storage, OKX public REST API

---

### Task 1: Project skeleton and docs

**Files:**
- Create: `pyproject.toml`
- Create: `README.md`
- Create: `src/full_data_extraction_for_btc/__init__.py`

- [ ] **Step 1: Write the failing import smoke test**
- [ ] **Step 2: Run the test to verify import failure**
- [ ] **Step 3: Create the package skeleton**
- [ ] **Step 4: Re-run the test to verify import success**

### Task 2: Normalization and pagination

**Files:**
- Create: `src/full_data_extraction_for_btc/models.py`
- Create: `src/full_data_extraction_for_btc/normalize.py`
- Create: `src/full_data_extraction_for_btc/timeutils.py`
- Test: `tests/test_normalize.py`

- [ ] **Step 1: Write failing tests for candle normalization and date parsing**
- [ ] **Step 2: Run the tests to verify failure**
- [ ] **Step 3: Implement timestamp parsing and normalization**
- [ ] **Step 4: Re-run the tests to verify pass**

### Task 3: Storage and dedupe

**Files:**
- Create: `src/full_data_extraction_for_btc/storage.py`
- Test: `tests/test_storage.py`

- [ ] **Step 1: Write failing tests for monthly partition merge and dedupe**
- [ ] **Step 2: Run the tests to verify failure**
- [ ] **Step 3: Implement compressed partition persistence**
- [ ] **Step 4: Re-run the tests to verify pass**

### Task 4: OKX client and download orchestration

**Files:**
- Create: `src/full_data_extraction_for_btc/client.py`
- Create: `src/full_data_extraction_for_btc/downloader.py`
- Test: `tests/test_downloader.py`

- [ ] **Step 1: Write failing tests for backward pagination and dataset summaries**
- [ ] **Step 2: Run the tests to verify failure**
- [ ] **Step 3: Implement REST client and orchestration**
- [ ] **Step 4: Re-run the tests to verify pass**

### Task 5: CLI and verification

**Files:**
- Create: `src/full_data_extraction_for_btc/cli.py`
- Create: `src/full_data_extraction_for_btc/__main__.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write failing CLI tests**
- [ ] **Step 2: Run the tests to verify failure**
- [ ] **Step 3: Implement CLI output and argument parsing**
- [ ] **Step 4: Re-run the tests to verify pass**
- [ ] **Step 5: Run full test suite**
- [ ] **Step 6: Run a small live download window against OKX**
