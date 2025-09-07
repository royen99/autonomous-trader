from __future__ import annotations
import asyncio, os, datetime as dt
from typing import Any, Dict, List, Optional
from collections import defaultdict
import orjson
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text

# ---------- Config / DB ----------
PG_HOST=os.getenv("PG_HOST","db")
PG_PORT=int(os.getenv("PG_PORT","5432"))
PG_DB=os.getenv("PG_DB","trader")
PG_USER=os.getenv("PG_USER","trader")
PG_PASS=os.getenv("PG_PASS","traderpass")
ASYNC_DSN=f"postgresql+asyncpg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

engine = create_async_engine(ASYNC_DSN, echo=False, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

def oj(data: Any) -> JSONResponse:
    return JSONResponse(content=orjson.loads(orjson.dumps(data)))

# ---------- App ----------
app = FastAPI(title="Autonomous Trader Dashboard")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---------- Helpers ----------
async def q_scalar(session: AsyncSession, sql: str, **params):
    r = await session.execute(text(sql), params)
    row = r.first()
    return row[0] if row else None

async def q_all(session: AsyncSession, sql: str, **params):
    r = await session.execute(text(sql), params)
    cols = r.keys()
    return [dict(zip(cols, row)) for row in r.fetchall()]

async def get_symbols(session: AsyncSession) -> List[str]:
    rows = await q_all(session, "SELECT symbol FROM symbols ORDER BY symbol")
    # Fallback: infer symbols from candles if symbols table empty
    if not rows:
        rows = await q_all(session, "SELECT DISTINCT symbol FROM candles ORDER BY symbol")
    return [r["symbol"] for r in rows]

async def get_candles(session: AsyncSession, symbol: str, limit: int = 300):
    rows = await q_all(session, """
      SELECT symbol, ts, open, high, low, close, volume
      FROM candles
      WHERE symbol = :s
      ORDER BY ts DESC
      LIMIT :lim
    """, s=symbol, lim=limit)
    rows.reverse()  # ascending for chart
    # shape for Chart.js financial: {x, o,h,l,c}
    data = [{"x": r["ts"].isoformat(), "o": r["open"], "h": r["high"], "l": r["low"], "c": r["close"]} for r in rows]
    return data

async def get_orders(session: AsyncSession, limit: int = 50):
    rows = await q_all(session, """
      SELECT id, symbol, side, type, price, qty, status, client_order_id, exch_order_id, created_at, updated_at
      FROM orders
      ORDER BY created_at DESC
      LIMIT :lim
    """, lim=limit)
    return rows

async def get_trades(session: AsyncSession, limit: int = 50):
    rows = await q_all(session, """
      SELECT id, symbol, side, price, qty, fee_asset, fee, order_client_id, ts
      FROM trades
      ORDER BY ts DESC
      LIMIT :lim
    """, lim=limit)
    return rows

async def get_summary(session: AsyncSession) -> Dict[str, Any]:
    # latest USDT balance
    last_usdt = await q_all(session, """
      SELECT free, ts FROM balances
      WHERE asset='USDT'
      ORDER BY ts DESC
      LIMIT 1
    """)
    usdt = last_usdt[0]["free"] if last_usdt else 0.0
    last_ts = last_usdt[0]["ts"].isoformat() if last_usdt else None

    # USDT 24h ago (approx)
    usdt_24 = await q_all(session, """
      SELECT free FROM balances
      WHERE asset='USDT' AND ts <= (NOW() AT TIME ZONE 'utc' - INTERVAL '24 hours')
      ORDER BY ts DESC
      LIMIT 1
    """)
    usdt_day_ago = usdt_24[0]["free"] if usdt_24 else None
    delta_24 = (usdt - usdt_day_ago) if (usdt_day_ago is not None) else None

    # open orders count
    open_orders = await q_scalar(session, "SELECT COUNT(*) FROM orders WHERE status IN ('NEW','PARTIALLY_FILLED')") or 0

    # last trade summary
    lt = await q_all(session, "SELECT symbol, side, price, qty, ts FROM trades ORDER BY ts DESC LIMIT 1")
    last_trade = None
    if lt:
        r = lt[0]
        last_trade = f"{r['ts'].isoformat()} · {r['symbol']} {r['side']} {r['qty']:.6f} @ {r['price']:.6f}"

    return {
        "usdt": usdt,
        "usdt_ts": last_ts,
        "delta_24h": delta_24,
        "open_orders": int(open_orders),
        "last_trade": last_trade
    }

async def get_last_prices(session: AsyncSession) -> Dict[str, float]:
    rows = await q_all(session, """
      SELECT DISTINCT ON (symbol) symbol, close
      FROM candles
      ORDER BY symbol, ts DESC
    """)
    return {r["symbol"]: float(r["close"]) for r in rows}

async def get_positions(session: AsyncSession):
    # Pull all trades ordered by time (and id for tie-breaks)
    trades = await q_all(session, """
      SELECT symbol, side, price, qty, ts, id
      FROM trades
      ORDER BY symbol ASC, ts ASC, id ASC
    """)
    last_px = await get_last_prices(session)

    lots = defaultdict(list)  # symbol -> list of open BUY lots [{qty, price, ts}]
    for t in trades:
        sym = t["symbol"]
        side = (t["side"] or "").upper()
        qty = float(t["qty"] or 0.0)
        px  = float(t["price"] or 0.0)
        ts  = t["ts"]
        if side == "BUY":
            lots[sym].append({"qty": qty, "price": px, "ts": ts})
        elif side == "SELL":
            remaining = qty
            while remaining > 1e-18 and lots[sym]:
                take = min(lots[sym][0]["qty"], remaining)
                lots[sym][0]["qty"] -= take
                remaining -= take
                if lots[sym][0]["qty"] <= 1e-18:
                    lots[sym].pop(0)

    positions = []
    for sym, open_lots in lots.items():
        qty_open = sum(l["qty"] for l in open_lots)
        if qty_open <= 1e-18:
            continue
        vwap = sum(l["qty"] * l["price"] for l in open_lots) / qty_open
        last = float(last_px.get(sym, vwap))
        upnl_pct = (last / vwap - 1.0) if vwap > 0 else 0.0
        positions.append({
            "symbol": sym,
            "qty": qty_open,
            "avg_entry": vwap,
            "upnl_pct": upnl_pct
        })

    positions.sort(key=lambda r: r["symbol"])
    return positions

# ---------- Routes ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/api/symbols")
async def api_symbols():
    async with SessionLocal() as s:
        return oj(await get_symbols(s))

@app.get("/api/candles")
async def api_candles(symbol: str = Query(...), limit: int = Query(180, ge=10, le=1000)):
    async with SessionLocal() as s:
        return oj(await get_candles(s, symbol, limit))

@app.get("/api/orders")
async def api_orders(limit: int = Query(50, ge=1, le=200)):
    async with SessionLocal() as s:
        return oj(await get_orders(s, limit))

@app.get("/api/trades")
async def api_trades(limit: int = Query(50, ge=1, le=200)):
    async with SessionLocal() as s:
        return oj(await get_trades(s, limit))

@app.get("/api/summary")
async def api_summary():
    async with SessionLocal() as s:
        return oj(await get_summary(s))

@app.get("/api/positions")
async def api_positions():
    async with SessionLocal() as s:
        return oj(await get_positions(s))

# ---------- WebSocket: pushes periodic updates ----------
@app.websocket("/ws")
async def ws_feed(ws: WebSocket):
    await ws.accept()
    # read desired symbol, default to first available
    symbol = None
    try:
        # first message can optionally set {"symbol":"BTCUSDT"}
        try:
            first = await asyncio.wait_for(ws.receive_text(), timeout=0.2)
            if first:
                import json
                symbol = json.loads(first).get("symbol")
        except Exception:
            pass

        async with SessionLocal() as s:
            if not symbol:
                syms = await get_symbols(s)
                symbol = syms[0] if syms else "BTCUSDT"

        while True:
            async with SessionLocal() as s:
                payload = {
                    "type": "tick",
                    "symbol": symbol,
                    "summary": await get_summary(s),
                    "orders": await get_orders(s, limit=20),
                    "trades": await get_trades(s, limit=20),
                    "candles": await get_candles(s, symbol, limit=180),
                    "positions": await get_positions(s),
                }
            await ws.send_text(orjson.dumps(payload).decode())
            await asyncio.sleep(2.0)
    except WebSocketDisconnect:
        return
    except Exception as e:
        try:
            await ws.send_text(orjson.dumps({"type":"error","message":str(e)}).decode())
        except:
            pass
        return
