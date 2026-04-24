from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

LOGGER = logging.getLogger("full_data_extraction_for_btc.client")


class OkxPublicClient:
    def __init__(
        self,
        base_url: str = "https://www.okx.com",
        timeout: float = 20.0,
        sleep_seconds: float = 0.12,
        max_retries: int = 5,
        retry_backoff_seconds: float = 0.8,
        retry_backoff_max_seconds: float = 8.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.retry_backoff_max_seconds = max(0.0, float(retry_backoff_max_seconds))
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
        last_error: Exception | None = None
        total_attempts = self.max_retries + 1
        for attempt in range(1, total_attempts + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= total_attempts or not _is_retryable_error(exc):
                    raise
                backoff = min(self.retry_backoff_max_seconds, self.retry_backoff_seconds * (2 ** (attempt - 1)))
                LOGGER.warning(
                    "OKX request failed, retrying: path=%s attempt=%s/%s wait=%.2fs error=%s",
                    path,
                    attempt,
                    total_attempts,
                    backoff,
                    exc,
                )
                if backoff > 0:
                    time.sleep(backoff)
        else:
            if last_error is not None:
                raise last_error
            raise RuntimeError(f"request failed without exception: path={path}")

        time.sleep(self.sleep_seconds)
        if payload.get("code") not in (None, "0", 0):
            raise RuntimeError(f"OKX API error for {path}: {payload}")
        return payload


def _is_retryable_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, ConnectionResetError):
        return True
    if isinstance(exc, urllib.error.HTTPError):
        # 4xx usually means caller/input issue, do not retry.
        return exc.code >= 500
    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        if isinstance(reason, TimeoutError):
            return True
        if isinstance(reason, ConnectionResetError):
            return True
        if isinstance(reason, OSError):
            return True
        if isinstance(reason, str):
            lowered = reason.lower()
            if "timed out" in lowered or "temporary failure in name resolution" in lowered:
                return True
        return False
    if isinstance(exc, OSError):
        return True
    return False
