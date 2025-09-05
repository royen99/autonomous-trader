from __future__ import annotations
import asyncio, os, time, logging
from dotenv import load_dotenv
import orjson as jsonfast
from utils import env
from mexc_client import MexcClient
from features import klines_to_df, compute_features, df_to_candle_rows
from agent_llm import llm_decide
from policy_risk import RiskState, allow_trade, size_for_trade
from executor import round_dec, make_limit_price
from db import init_db, SessionLocal, upsert_candles, insert_balance, insert_order, insert_trade, set_order_status

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

async def persist_candles(sym: str, df, session):
    rows = df_to_candle_rows(df)
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
                decision = await llm_decide(ollama_host, model, sym, feats)

                # 3) Heartbeat log ALWAYS
                logging.info("[%s] %s price=%.6f conf=%.2f last_ts=%s",
                             sym, decision.action, price, decision.confidence, last_ts)

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

                base, quote = split_symbol(sym)

                if mode == "live":
                    # Pull account once to verify funds
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

                    def get_free(asset: str) -> float:
                        for b in bals:
                            a = (b.get("asset") or b.get("currency") or b.get("coin") or "").upper()
                            if a == asset.upper():
                                for k in ("free","available","availableBalance","availableAmt"):
                                    if k in b:
                                        try: return float(b[k])
                                        except: pass
                        return 0.0

                    need_qty = float(qty_str)
                    need_quote = float(price_str) * need_qty

                    if decision.action == "SELL":
                        free_base = get_free(base)
                        if free_base + 1e-12 < need_qty:
                            logging.info("[%s] SKIP SELL: free %s=%.6f < qty %.6f", sym, base, free_base, need_qty)
                            await asyncio.sleep(HEARTBEAT)
                            continue

                    elif decision.action == "BUY":
                        free_quote = get_free(quote)
                        if free_quote + 1e-8 < need_quote:
                            logging.info("[%s] SKIP BUY: free %s=%.2f < cost %.2f", sym, quote, free_quote, need_quote)
                            await asyncio.sleep(HEARTBEAT)
                            continue

                # 6) Persist NEW order
                await insert_order(session,
                    symbol=sym, side=decision.action, type="LIMIT",
                    price=float(price_str), qty=float(qty_str),
                    status="NEW", client_order_id=client_id, exch_order_id=None
                )
                await session.commit()

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
                    order = await client.new_order(
                        CFG["exchange"]["endpoints"]["order"],
                        symbol=sym, side=decision.action, order_type="LIMIT",
                        quantity=qty_str, price=price_str, tif=CFG["execution"]["time_in_force"],
                        client_order_id=client_id
                    )
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

async def main():
    await init_db()
    tasks = [run_symbol(s) for s in CFG["symbols"]]
    # start balance poller in live mode
    if env("MODE","paper") == "live":
        tasks.append(balance_poller())
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
