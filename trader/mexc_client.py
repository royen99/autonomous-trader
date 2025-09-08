from __future__ import annotations
import asyncio, logging, random, time
import httpx
from typing import Any, Dict, Optional
from utils import hmac_sha256, now_ms
from pydantic import BaseModel

class MexcClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        api_key_header: str = "X-MEXC-APIKEY",
        recv_window: int = 5000,
        timeout: float = 10.0,
        max_retries: int = 5,
        backoff_base: float = 0.5,   # seconds
        backoff_cap: float = 8.0     # seconds
    ):
        self.base = base_url.rstrip("/")
        self.key = api_key
        self.secret = api_secret
        self.api_key_header = api_key_header
        self.recv = int(recv_window)
        self._client = httpx.AsyncClient(timeout=timeout)
        self._timeout = timeout
        self._max_retries = int(max_retries)
        self._backoff_base = float(backoff_base)
        self._backoff_cap = float(backoff_cap)

    async def close(self):
        await self._client.aclose()

    # -------- core retry wrapper (status-aware) --------
    def _should_retry(self, exc: Optional[Exception], resp: Optional[httpx.Response]) -> bool:
        if exc is not None:
            return isinstance(exc, (
                httpx.ConnectError,
                httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.RemoteProtocolError,
                httpx.WriteError,
            ))
        if resp is not None:
            if resp.status_code in (429, 418):   # rate limited / banned temp
                return True
            if 500 <= resp.status_code <= 599:   # server errors
                return True
        return False

    def _retry_after(self, resp: Optional[httpx.Response], attempt: int) -> float:
        # Honor Retry-After if present, otherwise exp backoff with jitter
        if resp is not None:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    return max(0.0, float(ra))
                except Exception:
                    pass
        base = min(self._backoff_cap, self._backoff_base * (2 ** (attempt - 1)))
        jitter = random.uniform(0, 0.25)
        return base + jitter

    # -------- signed request with retries --------
    async def _signed(self, method: str, path: str, params: Dict[str, Any]) -> Any:
        headers = {self.api_key_header: self.key}
        last_exc = None
        last_resp = None

        for attempt in range(1, self._max_retries + 1):
            # Rebuild the signed URL each attempt so timestamp stays fresh
            p = {**params, "timestamp": now_ms(), "recvWindow": self.recv}
            qs = str(httpx.QueryParams(p))
            sig = hmac_sha256(self.secret, qs)

            url = f"{self.base}{path}?{qs}&signature={sig}" if qs else f"{self.base}{path}?signature={sig}"

            try:
                r = await self._client.request(method, url, headers=headers)
                last_resp = r
                if self._should_retry(None, r):
                    delay = self._retry_after(r, attempt)
                    logging.warning("MEXC %s %s -> %s; retrying in %.2fs (attempt %d/%d)",
                                    method, path, r.status_code, delay, attempt, self._max_retries)
                    await asyncio.sleep(delay)
                    continue
                r.raise_for_status()
                return r.json()

            except Exception as exc:
                last_exc = exc
                if self._should_retry(exc, None) and attempt < self._max_retries:
                    delay = self._retry_after(None, attempt)
                    logging.warning("MEXC %s %s network error: %s; retry in %.2fs (attempt %d/%d)",
                                    method, path, exc.__class__.__name__, delay, attempt, self._max_retries)
                    await asyncio.sleep(delay)
                    continue
                # no retry or out of attempts
                break

        # Exhausted retries or non-retryable error
        if last_resp is not None:
            try:
                text = last_resp.text
            except Exception:
                text = "<no-body>"
            logging.error("MEXC %s %s failed after %d attempts: %s | %s",
                          method, path, self._max_retries, last_resp.status_code, text)
            last_resp.raise_for_status()
        raise last_exc or httpx.HTTPError("MEXC request failed")

    # -------- public GET with retries (for klines, etc.) --------
    async def public_get(self, path: str, params: Dict[str, Any]) -> Any:
        last_exc = None
        last_resp = None
        for attempt in range(1, self._max_retries + 1):
            url = httpx.URL(f"{self.base}{path}", params=params)
            try:
                r = await self._client.get(url)
                last_resp = r
                if self._should_retry(None, r):
                    delay = self._retry_after(r, attempt)
                    logging.warning("MEXC GET %s -> %s; retrying in %.2fs (attempt %d/%d)",
                                    path, r.status_code, delay, attempt, self._max_retries)
                    await asyncio.sleep(delay)
                    continue
                r.raise_for_status()
                return r.json()
            except Exception as exc:
                last_exc = exc
                if self._should_retry(exc, None) and attempt < self._max_retries:
                    delay = self._retry_after(None, attempt)
                    logging.warning("MEXC GET %s network error: %s; retry in %.2fs (attempt %d/%d)",
                                    path, exc.__class__.__name__, delay, attempt, self._max_retries)
                    await asyncio.sleep(delay)
                    continue
                break

        if last_resp is not None:
            try:
                text = last_resp.text
            except Exception:
                text = "<no-body>"
            logging.error("MEXC GET %s failed after %d attempts: %s | %s",
                          path, self._max_retries, last_resp.status_code, text)
            last_resp.raise_for_status()
        raise last_exc or httpx.HTTPError("MEXC public GET failed")

    # -------- convenience endpoints using the retrying core --------
    async def klines(self, path: str, symbol: str, interval: str, limit: int = 300):
        return await self.public_get(path, {"symbol": symbol, "interval": interval, "limit": limit})

    async def account(self, path: str):
        return await self._signed("GET", path, {})

    async def order_status(self, path: str, symbol: str, client_order_id: Optional[str] = None, order_id: Optional[str] = None):
        params: Dict[str, Any] = {"symbol": symbol}
        if client_order_id: params["origClientOrderId"] = client_order_id
        if order_id: params["orderId"] = order_id
        return await self._signed("GET", path, params)

    async def open_orders(self, path: str, symbol: Optional[str] = None):
        params: Dict[str, Any] = {}
        if symbol: params["symbol"] = symbol
        return await self._signed("GET", path, params)

    async def my_trades(self, path: str, symbol: str, limit: int = 50, order_id: Optional[str] = None):
        params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if order_id: params["orderId"] = order_id
        return await self._signed("GET", path, params)

    async def new_order(
        self,
        path: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: str,
        price: Optional[str] = None,
        tif: str = "GTC",
        client_order_id: Optional[str] = None
    ):
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity,
            "timeInForce": tif
        }
        if price is not None:
            params["price"] = price
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._signed("POST", path, params)
