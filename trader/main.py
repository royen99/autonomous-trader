from __future__ import annotations
import asyncio, os, time, logging
from dotenv import load_dotenv
import orjson as jsonfast
from typing import Tuple, List
from utils import env
from mexc_client import MexcClient
from features import klines_to_df, compute_features, df_to_candle_rows
from agent_llm import llm_decide
from policy_risk import RiskState, allow_trade, size_for_trade
from executor import round_dec, make_limit_price
from db import init_db, SessionLocal, upsert_candles, insert_balance, insert_order, insert_trade, set_order_status, fetch_open_orders, set_order_exch_id, get_open_position

load_dotenv()
def jload(p): return jsonfast.loads(open(p, "rb").read())
CFG = jload("config.json")
HEARTBEAT = int(env("HEARTBEAT_SEC","2"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.info("Trader booting…")

logging.info(
    "MODE=%s | symbols=%s | OLLAMA_HOST=%s | MODEL=%s | DB=%s:%s/%s",
    env("MODE","paper"),
    ", ".join(CFG.get("symbols", [])),
    env("OLLAMA_HOST"),
    env("OLLAMA_MODEL","qwen2.5:7b"),
    os.getenv("PG_HOST","db"),
    os.getenv("PG_PORT","5432"),
    os.getenv("PG_DB","trader"),
)

def split_symbol(sym: str):
    # crude but effective: strip common quotes
    quotes = ("USDT","USDC","BTC","ETH","BUSD","USD","EUR")
    for q in quotes:
        if sym.endswith(q):
            return sym[: -len(q)], q
    # fallback: last 3/4 as quote
    return sym[:-4], sym[-4:]

async def persist_candles(sym, df, session):
    rows = df_to_candle_rows(df, sym)     # <- pass symbol
    await upsert_candles(session, sym, rows)

async def paper_fill(side: str, price: float, qty: float) -> dict:
    return {"status": "FILLED", "price": price, "executedQty": qty}

async def run_symbol(sym: str):
    client = MexcClient(
        base_url=CFG["exchange"]["base_url"],
        api_key=env("MEXC_API_KEY"),
        api_secret=env("MEXC_API_SECRET"),
        api_key_header=CFG["exchange"]["api_key_header"],
        recv_window=CFG["exchange"]["recvWindow"],
    )
    rs = RiskState(usdt_balance=CFG["paper"]["starting_usdt"])
    mode = env("MODE","paper")
    model = env("OLLAMA_MODEL","qwen2.5:7b")
    ollama_host = env("OLLAMA_HOST","http://127.0.0.1:11434")

    logging.info("[%s] worker start (mode=%s)", sym, mode)
    async with SessionLocal() as session:
        while True:
            try:
                # 1) Market data
                kl = await client.klines(CFG["exchange"]["endpoints"]["klines"], sym, CFG["interval"], CFG["klines_limit"])
                df = klines_to_df(kl)
                await persist_candles(sym, df, session)
                await session.commit()
                last_ts = df["ts"].iloc[-1].isoformat() if "ts" in df.columns else "n/a"

                # 2) Features + LLM decision
                feats = compute_features(df)
                price = feats["price"]

                # position-aware features
                fee_bps        = float(CFG["execution"].get("fee_bps", 20))
                min_profit_bps = float(CFG["execution"].get("min_profit_bps", 15))
                time_stop_min  = int(CFG["execution"].get("time_stop_min", 90))
                stop_loss_pct  = float(CFG["risk"].get("stop_loss_pct", 0.008))

                # fetch open position (FIFO) from DB
                pos_qty, avg_entry, entry_ts = await get_open_position(session, sym)

                # compute derived fields
                px = feats["price"]
                if (pos_qty or 0.0) > 0 and (avg_entry or 0.0) > 0:
                    breakeven_px = avg_entry * (1.0 + (2.0 * fee_bps + min_profit_bps) / 10000.0)
                    upnl_pct = (px / avg_entry - 1.0) if avg_entry > 0 else 0.0
                    age_min = ((time.time() - entry_ts.timestamp()) / 60.0) if entry_ts else 0.0
                    stop_ok = (px <= avg_entry * (1.0 - abs(stop_loss_pct))) or (age_min >= time_stop_min)
                else:
                    breakeven_px = None
                    upnl_pct = 0.0
                    age_min = 0.0
                    stop_ok = False

                pos_ctx = {
                    "qty": float(pos_qty or 0.0),
                    "avg_entry": float(avg_entry or 0.0) if avg_entry else None,
                    "breakeven_px": float(breakeven_px) if breakeven_px else None,
                    "unrealized_pct": float(upnl_pct),
                    "in_position_min": int(age_min),
                    "stop_ok": bool(stop_ok)
                }

                # now call LLM with position context
                dca_step_bps = float(CFG["execution"].get("dca_step_bps", 20))
                decision = await llm_decide(ollama_host, model, sym, feats, pos_ctx, dca_step_bps=dca_step_bps)

                # 3) Heartbeat log ALWAYS
                if decision.action == "HOLD":
                    action_emoji = "🔄"
                elif decision.action == "SELL" and pos_qty == 0:
                    action_emoji = "❌"
                elif decision.action == "BUY":
                    action_emoji = "➕"
                logging.info("[%s] %s %s price=%.6f conf=%.2f last_ts=%s reason=%s",
                             sym, action_emoji, decision.action, price, decision.confidence, last_ts, decision.reason)

                # 4) HOLD => sleep
                if decision.action == "HOLD" or not allow_trade(decision.confidence, price, CFG, rs):
                    await asyncio.sleep(HEARTBEAT)
                    continue

                # 5) Trade sizing & rounding
                qty = size_for_trade(price, CFG, rs)
                limit_px = make_limit_price(price, decision.action, CFG["execution"]["slippage_bps"])
                price_str = round_dec(limit_px, CFG["execution"]["round_price_dp"])
                qty_str = round_dec(qty, CFG["execution"]["round_qty_dp"])
                client_id = f"ai_{sym}_{int(time.time())}"
                dp_price = CFG["execution"]["round_price_dp"]
                dp_qty   = CFG["execution"]["round_qty_dp"]
                min_notional = float(CFG["execution"].get("min_notional_usd", 5))
                fee_bps = float(CFG["execution"].get("fee_bps", 20))

                base, quote = split_symbol(sym)

                free_base = free_quote = None
                if mode == "live":
                    # pull balances once per decision so we can size properly
                    acct = await client.account(CFG["exchange"]["endpoints"]["account"])
                    bals = (
                        acct.get("balances")
                        or acct.get("data", {}).get("balances")
                        or acct.get("data", [])
                        or acct.get("coins")
                        or []
                    )
                    if isinstance(bals, dict):
                        bals = list(bals.values())

                    def _free(asset: str) -> float:
                        for b in bals:
                            a = (b.get("asset") or b.get("currency") or b.get("coin") or "").upper()
                            if a == asset.upper():
                                for k in ("free","available","availableBalance","availableAmt"):
                                    if k in b:
                                        try: return float(b[k])
                                        except: pass
                        return 0.0

                    free_base  = _free(base)
                    free_quote = _free(quote)

                # --- SIZING ---
                limit_px = make_limit_price(price, decision.action, CFG["execution"]["slippage_bps"])

                if decision.action == "BUY":
                    # price with slippage, rounded down to dp_price
                    price_str = round_dec(limit_px, dp_price)
                    px = float(price_str)

                    # Budget = min(max_per_trade, wallet) minus a fee buffer
                    wallet_q = (free_quote if mode == "live" else rs.usdt_balance) or 0.0
                    max_per  = float(CFG["risk"]["max_per_trade_usd"])
                    budget   = min(wallet_q, max_per)
                    # reserve a fee cushion in quote so we never exceed free_quote
                    budget  *= max(0.0, 1.0 - fee_bps / 10000.0)

                    if budget < min_notional or px <= 0:
                        logging.info("[%s] SKIP BUY: budget %.4f < min %.2f", sym, budget, min_notional)
                        await asyncio.sleep(HEARTBEAT)
                        continue

                    # Fit qty to budget and step (dp_qty)
                    unit = 10 ** (-dp_qty)
                    qty_fit = (budget / px) // unit * unit
                    qty_str = round_dec(qty_fit, dp_qty)
                    qty_val = float(qty_str)
                    notional = px * qty_val

                    # Final guards
                    if qty_val <= 0 or notional < min_notional:
                        logging.info("[%s] SKIP BUY: notional %.4f < min %.2f (qty %.8f)", sym, notional, min_notional, qty_val)
                        await asyncio.sleep(HEARTBEAT)
                        continue

                    if (pos_qty or 0.0) > 0 and (avg_entry or 0.0) > 0:
                        dca_thresh = (avg_entry) * (1.0 - dca_step_bps / 10000.0)
                        if price > dca_thresh:
                            logging.info("[%s] SKIP BUY: DCA rule (spot=%.6f > thresh=%.6f, avg=%.6f, step=%sbps)",
                                        sym, price, dca_thresh, avg_entry, dca_step_bps)
                            await asyncio.sleep(HEARTBEAT)
                            continue

                    if mode == "live" and notional > (free_quote - 1e-6):
                        # shave one step if needed
                        qty_fit = max(0.0, qty_val - unit)
                        qty_str = round_dec(qty_fit, dp_qty)
                        qty_val = float(qty_str)
                        notional = px * qty_val
                        if qty_val <= 0 or notional < min_notional:
                            logging.info("[%s] SKIP BUY: shaved notional %.4f < min %.2f", sym, notional, min_notional)
                            await asyncio.sleep(HEARTBEAT)
                            continue

                elif decision.action == "SELL":
                    # --- sizing: start from risk target, then cap by wallet & open position ---
                    price_str = round_dec(limit_px, dp_price)
                    px = float(price_str)

                    # target notional per trade (USD), converted to qty
                    qty_f = size_for_trade(px, CFG, rs=rs, available_quote=CFG["risk"]["max_per_trade_usd"])
                    if mode == "live":
                        qty_f = min(qty_f, (free_base or 0.0))

                    # also cap by open position (FIFO)
                    pos_qty, avg_entry, entry_ts = await get_open_position(session, sym)
                    if (pos_qty or 0.0) <= 1e-12:
                        logging.info("[%s] SKIP SELL: no open position (pos=%.8f)", sym, pos_qty or 0.0)
                        await asyncio.sleep(HEARTBEAT)
                        continue
                    qty_f = min(qty_f, pos_qty)

                    qty_str  = round_dec(qty_f, dp_qty)
                    qty_val  = float(qty_str)
                    notional = px * qty_val

                    # dust guard (respect exchange min notional)
                    if notional < min_notional or qty_val <= 0:
                        logging.info("[%s] SKIP SELL: notional %.4f < min %.2f (qty %.8f)", sym, notional, min_notional, qty_val)
                        await asyncio.sleep(HEARTBEAT)
                        continue

                    # --- position-aware profit floor (unless stop/time-stop) ---
                    fee_bps        = float(CFG["execution"].get("fee_bps", 20))
                    min_profit_bps = float(CFG["execution"].get("min_profit_bps", 15))
                    time_stop_min  = int(CFG["execution"].get("time_stop_min", 90))
                    stop_loss_pct  = float(CFG["risk"].get("stop_loss_pct", 0.008))

                    breakeven_px = avg_entry * (1.0 + (2.0 * fee_bps + min_profit_bps) / 10000.0)
                    spot_px      = price  # latest spot from features

                    allow_stop = spot_px <= avg_entry * (1.0 - abs(stop_loss_pct))
                    allow_time = False
                    if entry_ts is not None:
                        age_min = (time.time() - entry_ts.timestamp()) / 60.0
                        allow_time = age_min >= time_stop_min

                    if spot_px < breakeven_px and not (allow_stop or allow_time):
                        logging.info("[%s] SKIP SELL: below breakeven %.6f (spot=%.6f, entry=%.6f) — stop/time-stop not met",
                                    sym, breakeven_px, spot_px, avg_entry)
                        await asyncio.sleep(HEARTBEAT)
                        continue

                client_id = f"ai_{sym}_{int(time.time())}"

                # 7) Execute
                if mode == "paper":
                    await set_order_status(session, client_id, "FILLED", price=float(price_str))
                    await insert_trade(session,
                        symbol=sym, side=decision.action, price=float(price_str), qty=float(qty_str),
                        fee_asset="USDT", fee=0.0, order_client_id=client_id
                    )
                    await session.commit()
                    rs.usdt_balance += (1 if decision.action == "SELL" else -1) * float(price_str) * float(qty_str)
                    logging.info("[%s] PAPER %s %s @ %s | USDT=%.2f",
                                 sym, decision.action, qty_str, price_str, rs.usdt_balance)
                else:
                    # LIVE: place on exchange
                    order = await client.new_order(
                        CFG["exchange"]["endpoints"]["order"],
                        symbol=sym, side=decision.action, order_type="LIMIT",
                        quantity=qty_str, price=price_str, tif=CFG["execution"]["time_in_force"],
                        client_order_id=client_id
                    )
                    # (B) Update the exch_order_id so we can track it later
                    exch_id = str(order.get("orderId") or order.get("data", {}).get("orderId") or "")
                    if exch_id:
                        await set_order_exch_id(session, client_id, exch_id)
                        await session.commit()

                    # Persist NEW order after placing it
                    await insert_order(
                        session,
                        symbol=sym,
                        side=decision.action,
                        type="LIMIT",
                        price=float(price_str),
                        qty=float(qty_str),
                        status="NEW",
                        client_order_id=client_id,
                        exch_order_id=exch_id
                    )
                    await session.commit()

                    logging.info("[%s] LIVE placed %s %s @ %s id=%s",
                                sym, decision.action, qty_str, price_str, order)

                # 8) Snapshot balance
                await insert_balance(session, asset="USDT", free=rs.usdt_balance, locked=0.0)
                await session.commit()

                await asyncio.sleep(HEARTBEAT)

            except Exception as e:
                logging.exception("[%s] loop error: %s", sym, e)
                await asyncio.sleep(max(HEARTBEAT, 5))

    await client.close()

async def balance_poller():
    """Periodically fetch live exchange balances and snapshot to DB."""
    if env("MODE","paper") != "live":
        logging.info("Balance poller disabled (MODE!=live)")
        return

    client = MexcClient(
        base_url=CFG["exchange"]["base_url"],
        api_key=env("MEXC_API_KEY"),
        api_secret=env("MEXC_API_SECRET"),
        api_key_header=CFG["exchange"]["api_key_header"],
        recv_window=CFG["exchange"]["recvWindow"],
    )
    poll_sec = int(env("BALANCE_POLL_SEC","25"))
    logging.info("Balance poller starting (every %ss)", poll_sec)

    try:
        while True:
            try:
                acct = await client.account(CFG["exchange"]["endpoints"]["account"])
                # Try common shapes (Binance-like / MEXC variants)
                balances = (
                    acct.get("balances")
                    or acct.get("data", {}).get("balances")
                    or acct.get("data", [])
                    or acct.get("coins")
                    or []
                )
                # Some APIs nest differently; if it’s a dict, coerce to list
                if isinstance(balances, dict):
                    balances = list(balances.values())

                wrote = 0
                async with SessionLocal() as session:
                    for b in balances:
                        asset = b.get("asset") or b.get("currency") or b.get("coin")
                        if not asset:
                            continue
                        def f(x):
                            try: return float(x)
                            except: return 0.0
                        free = f(b.get("free") or b.get("available") or b.get("availableBalance") or b.get("availableAmt"))
                        locked = f(b.get("locked") or b.get("frozen") or b.get("hold") or b.get("frozenAmt"))
                        if free > 0 or locked > 0:
                            await insert_balance(session, asset=asset.upper(), free=free, locked=locked)
                            wrote += 1
                    await session.commit()

                # Pretty log of USDT if present
                usdt = next((b for b in balances if (b.get("asset") or b.get("currency") or b.get("coin") or "").upper()=="USDT"), None)
                usdt_free = None
                if usdt:
                    for key in ("free","available","availableBalance","availableAmt"):
                        if key in usdt:
                            try: usdt_free = float(usdt[key]); break
                            except: pass

                logging.info("[BAL] wrote %d assets%s",
                             wrote,
                             f" | USDT={usdt_free:.2f}" if usdt_free is not None else "")

            except Exception as e:
                logging.exception("[BAL] poll error: %s", e)

            await asyncio.sleep(poll_sec)
    finally:
        await client.close()

async def order_status_poller():
    """Poll exchange for open orders and update DB (NEW->PARTIALLY_FILLED/FILLED/CANCELED).
       On FILLED, insert an aggregated trade row."""
    if env("MODE","paper") != "live":
        logging.info("Order status poller disabled (MODE!=live)")
        return

    poll_sec = max(3, HEARTBEAT)  # don’t hammer the API
    client = MexcClient(
        base_url=CFG["exchange"]["base_url"],
        api_key=env("MEXC_API_KEY"),
        api_secret=env("MEXC_API_SECRET"),
        api_key_header=CFG["exchange"]["api_key_header"],
        recv_window=CFG["exchange"]["recvWindow"],
    )
    try:
        while True:
            try:
                # 1) load all DB-open orders
                async with SessionLocal() as session:
                    rows = await fetch_open_orders(session, limit=200)

                if not rows:
                    await asyncio.sleep(poll_sec)
                    continue

                for o in rows:
                    symbol = o["symbol"]
                    cid    = o["client_order_id"]
                    try:
                        # 2) query order status by client id
                        osr = await client.order_status(
                            CFG["exchange"]["endpoints"]["orderQuery"],
                            symbol=symbol,
                            client_order_id=cid
                        )
                        # Support several payload shapes (Binance-style or nested data)
                        data  = osr.get("data", osr)
                        status = (data.get("status") or data.get("state") or "").upper()

                        # executed quantity & quote spent
                        def fnum(x, default=0.0):
                            try: return float(x)
                            except Exception: return default
                        executed = fnum(data.get("executedQty") or data.get("executed_qty") or 0)
                        cquote   = fnum(data.get("cummulativeQuoteQty") or data.get("cum_quote") or 0)
                        price    = fnum(data.get("price") or 0.0)

                        # Approx avg price if we have quote+qty, else fall back to limit price
                        avg_px = (cquote / executed) if (executed and cquote) else (price or o["price"] or 0.0)

                        # 3) persist status transitions
                        async with SessionLocal() as session:
                            if status in ("NEW","PARTIALLY_FILLED"):
                                await set_order_status(session, cid, status, price=avg_px if executed else None)
                                await session.commit()
                            elif status in ("FILLED","CANCELED","EXPIRED","REJECTED"):
                                # On FILLED: write one aggregated trade record
                                if status == "FILLED" and executed > 0:
                                    await insert_trade(
                                        session,
                                        symbol=symbol,
                                        side=o["side"],
                                        price=avg_px,
                                        qty=executed,
                                        fee_asset=None,
                                        fee=0.0,
                                        order_client_id=cid
                                    )
                                await set_order_status(session, cid, status, price=avg_px if executed else None)
                                await session.commit()

                        logging.info("[ORD] %s %s status=%s exec=%.10f avg=%.8f", symbol, cid, status, executed, avg_px)

                    except Exception as e:
                        logging.exception("[ORD] poll error for %s %s: %s", symbol, cid, e)

                await asyncio.sleep(poll_sec)

            except Exception as e:
                logging.exception("[ORD] loop error: %s", e)
                await asyncio.sleep(poll_sec)
    finally:
        await client.close()

async def main():
    await init_db()
    tasks = [run_symbol(s) for s in CFG["symbols"]]
    # start balance poller in live mode
    if env("MODE","paper") == "live":
        tasks.append(balance_poller())
        tasks.append(order_status_poller())
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
