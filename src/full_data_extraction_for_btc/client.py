from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any


class OkxPublicClient:
    def __init__(self, base_url: str = "https://www.okx.com", timeout: float = 20.0, sleep_seconds: float = 0.12) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.user_agent = "Mozilla/5.0 (compatible; full-data-extraction-for-btc/0.1.0; +https://www.okx.com/)"

    def fetch_candles(
        self,
        dataset: str,
        inst_id: str,
        bar: str,
        after: str | None = None,
        limit: str | None = None,
    ) -> list[list[str]]:
        endpoint = {
            "candles": "/api/v5/market/history-candles",
            "mark": "/api/v5/market/history-mark-price-candles",
            "index": "/api/v5/market/history-index-candles",
        }[dataset]
        params = {"instId": inst_id, "bar": bar}
        if after is not None:
            params["after"] = after
        if limit is not None:
            params["limit"] = limit
        payload = self._get_json(endpoint, params)
        return payload["data"]

    def fetch_funding_rate_history(
        self,
        inst_id: str,
        after: str | None = None,
        limit: str = "400",
    ) -> list[dict[str, str]]:
        params = {"instId": inst_id, "limit": limit}
        if after is not None:
            params["after"] = after
        payload = self._get_json("/api/v5/public/funding-rate-history", params)
        return payload["data"]

    def fetch_instrument(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        payload = self._get_json("/api/v5/public/instruments", {"instType": inst_type, "instId": inst_id})
        if not payload["data"]:
            raise RuntimeError(f"instrument not found: {inst_id}")
        return payload["data"][0]

    def _get_json(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        query = urllib.parse.urlencode(params)
        request = urllib.request.Request(
            f"{self.base_url}{path}?{query}",
            headers={
                "User-Agent": self.user_agent,
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        time.sleep(self.sleep_seconds)
        if payload.get("code") not in (None, "0", 0):
            raise RuntimeError(f"OKX API error for {path}: {payload}")
        return payload
