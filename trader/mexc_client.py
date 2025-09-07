from __future__ import annotations
import asyncio, httpx, time
from typing import Dict, Any, Optional
from utils import hmac_sha256, now_ms
from pydantic import BaseModel

class MexcClient:
    def __init__(self, base_url: str, api_key: str, api_secret: str, api_key_header: str, recv_window: int = 5000):
        self.base = base_url.rstrip("/")
        self.key = api_key
        self.secret = api_secret
        self.api_key_header = api_key_header
        self.recv = recv_window
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=5.0)
        )

    async def close(self):
        await self._client.aclose()

    async def _public_get(self, path: str, params: Dict[str, Any]) -> Any:
        r = await self._client.get(self.base + path, params=params)
        r.raise_for_status()
        return r.json()

    async def _signed(self, method: str, path: str, params: Dict[str, Any]) -> Any:
        # Add mandatory signed params
        p = {**params, "timestamp": now_ms(), "recvWindow": self.recv}

        # Build canonical query string
        qs = str(httpx.QueryParams(p))  # httpx.QueryParams -> encoded string

        # Sign the encoded query string
        sig = hmac_sha256(self.secret, qs)
        headers = {self.api_key_header: self.key}

        # Build URL with signature
        url = f"{self.base}{path}?{qs}&signature={sig}" if qs else f"{self.base}{path}?signature={sig}"

        r = await self._client.request(method, url, headers=headers)

        # Surface exchange error payloads for easier debugging
        if r.status_code >= 400:
            try:
                err_body = r.text
            except Exception:
                err_body = ""
            raise httpx.HTTPStatusError(
                f"{r.status_code} {r.reason_phrase} | {err_body}",
                request=r.request,
                response=r,
            )

        return r.json()

    # --- Endpoints (defaults match /api/v3 on MEXC-style APIs) ---
    async def klines(self, path: str, symbol: str, interval: str, limit: int = 300):
        return await self._public_get(path, {"symbol": symbol, "interval": interval, "limit": limit})

    async def account(self, path: str):
        return await self._signed("GET", path, {})

    async def new_order(self, path: str, symbol: str, side: str, order_type: str, quantity: str,
                        price: Optional[str] = None, tif: str = "GTC", client_order_id: Optional[str] = None):
        params = {"symbol": symbol, "side": side, "type": order_type, "quantity": quantity, "timeInForce": tif}
        if price is not None:
            params["price"] = price
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._signed("POST", path, params)

    async def order_status(self, path: str, symbol: str, order_id: Optional[int] = None, client_order_id: Optional[str] = None):
        params = {"symbol": symbol}
        if order_id: params["orderId"] = order_id
        if client_order_id: params["origClientOrderId"] = client_order_id
        return await self._signed("GET", path, params)

    async def open_orders(self, path: str, symbol: str | None = None):
        params = {}
        if symbol:
            params["symbol"] = symbol
        return await self._signed("GET", path, params)

    async def my_trades(self, path: str, symbol: str, limit: int = 50, order_id: str | None = None):
        params = {"symbol": symbol, "limit": limit}
        if order_id:
            params["orderId"] = order_id
        return await self._signed("GET", path, params)
