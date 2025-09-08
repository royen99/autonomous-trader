#!/usr/bin/env python3
import argparse, json, os, sys, re, time
from datetime import datetime, timezone
import httpx
import orjson as jsonfast
import pandas as pd
from dotenv import load_dotenv
# from main import CFG

load_dotenv()
def jload(p): return jsonfast.loads(open(p, "rb").read())
CFG = jload("config.json")

# --- import your feature helpers ---
try:
    from features import klines_to_df, compute_features
except Exception as e:
    print("ERROR: cannot import features. Make sure this script is in the same folder as features.py")
    raise

SYSTEM = (
  "You are a trading decision engine. OUTPUT STRICT JSON ONLY:\n"
  '{"action":"BUY|SELL|HOLD","confidence":0.0-1.0,"price_hint":number|null,"breakeven_px":number|null,"reason":"short text"}\n'
  "No prose. No Markdown. No backticks."
)

PROMPT_TEMPLATE = """Decide BUY/SELL/HOLD for {symbol}.

Features (floats):
{features_json}

Position context:
{position_json}

Hard rules:
- If position.qty > 0:
  - Do NOT output BUY unless features.price <= position.avg_entry * (1 - {dca_step_bps}/10000).
  - Do NOT output SELL unless features.price >= position.breakeven_px, unless position.stop_ok is true.
- If position.qty == 0 or position.avg_entry is null: BUY rule above does not apply.

Pre-Decision overrides (apply BEFORE scoring):
- Let profitable = (position.qty > 0 AND features.price >= position.breakeven_px).
- If profitable AND position.profit_protect_armed == true AND position.drawdown_from_peak_pct >= {trail_drawdown_pct}:
    -> action=SELL, confidence=0.65..0.85 (scale with drawdown), reason="trail drawdown"
    -> SKIP remaining rules.
- If profitable AND NOT (ema_fast_above_slow==1 AND trend_15m>0 AND trend_1h>0 AND price_vs_vwap60>0):
    -> action=SELL, confidence=0.60..0.80 (scale with profit and weakness), reason="take profit"
    -> SKIP remaining rules.

Scoring rubric (use signs and magnitudes, keep it simple):
- Momentum (short): smom = 2*ret_1 + 1*ret_5 + 0.5*ret_20.
- Momentum (long):  +0.10 if ret_60>0, +0.10 if ret_120>0, +0.10 if ret_360>0 (subtract if negative).
- Trend filter:
  - +0.25 if ema_fast_above_slow==1; -0.25 if 0.
  - +0.20 if above_ema200==1; -0.20 if 0.
  - +0.15 if ema50_slope>0;  -0.15 if <0.
- HTF alignment:
  - +0.20 if trend_15m>0 and trend_1h>0; -0.20 if both <0; else 0.
- MACD:   +0.20 if macd_hist > +h; -0.20 if macd_hist < -h (use h = small = 0.0002 or near-zero for the symbol scale).
- RSI:    +0.15 if 45..60; -0.20 if >=70; +0.20 if <=35.
- VWAP:   +0.10 if price_vs_vwap60>0; +0.10 if price_vs_vwap240>0; subtract if negative.
- Volume: +0.10 if vol_z20>=+0.5; -0.10 if <=-0.5.
- Volatility sanity: if atr_pct_14>0.02 (very high), subtract 0.10 from BUY and from SELL confidence (more HOLD bias).

Edge = sum of above (including smom).

Decision (only if no Pre-Decision override fired):
- if Edge >= +0.25 and BUY rule is satisfied -> BUY
- if Edge <= -0.10 -> SELL
- else -> HOLD

Confidence (discrete; no 0.50):
- BUY/SELL: nearest of [0.55,0.60,0.65,0.70,0.80,0.85] with base 0.55 + min(0.30, |Edge|)*0.8
- HOLD: 0.52

Price hint = features.price (echo).
Output STRICT JSON only.
"""

def strict_json(text: str):
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise ValueError("LLM did not return JSON")
    return json.loads(m.group(0))

def mexc_klines(symbol: str, interval: str, limit: int, timeout=10.0):
    base = "https://api.mexc.com"
    path = "/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    url = httpx.URL(base + path, params=params)
    print(f"[HTTP] GET {url}")
    with httpx.Client(timeout=timeout) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json()

def build_pos_ctx(price: float, qty: float, avg_entry: float | None,
                  fee_bps: float, min_profit_bps: float,
                  stop_loss_pct: float, time_stop_min: int,
                  age_min: float = 0.0,
                  min_pos_usdt: float = 1.0):
    """Mimic the bot's position context rules."""
    notional = (qty or 0.0) * price
    is_dust = not avg_entry or qty <= 0 or notional < min_pos_usdt
    breakeven = None
    if avg_entry and qty > 0:
        breakeven = avg_entry * (1.0 + (2.0*fee_bps + min_profit_bps) / 10000.0)
    stop_ok = False
    if avg_entry and qty > 0:
        stop_ok = (price <= avg_entry * (1.0 - abs(stop_loss_pct))) or (age_min >= time_stop_min)
    upnl_pct = (price / avg_entry - 1.0) if (avg_entry or 0) > 0 else 0.0

    return {
        "qty": float(qty or 0.0),
        "avg_entry": None if is_dust else float(avg_entry),
        "breakeven_px": None if (is_dust or not breakeven) else float(breakeven),
        "unrealized_pct": float(upnl_pct),
        "in_position_min": int(age_min),
        "stop_ok": bool(stop_ok),
        # trailing-related fields (optional, zero by default here)
        "profit_protect_armed": True,
        "peak_upnl_pct": 0.0,
        "drawdown_from_peak_pct": 0.0,
    }

def call_ollama(ollama_host: str, model: str, prompt: str, temperature: float = 0.35, timeout=20.0):
    url = f"{ollama_host.rstrip('/')}/api/generate"
    payload = {
        "model": model,
        "prompt": f"{SYSTEM}\n\n{prompt}",
        "stream": False,
        "format": "json",
        "options": {"temperature": temperature}
    }
    print(f"[HTTP] POST {url}")
    with httpx.Client(timeout=timeout) as client:
        r = client.post(url, json=payload)
        r.raise_for_status()
        return r.json(), payload

def main():
    ap = argparse.ArgumentParser(description="Test LLM decision with live MEXC klines.")
    ap.add_argument("--symbol", default="SOLUSDT")
    ap.add_argument("--interval", default="1m")
    ap.add_argument("--limit", type=int, default=600)
    ap.add_argument("--ollama", default=os.environ.get("OLLAMA_HOST", "http://localhost:11434"))
    ap.add_argument("--model", default=os.environ.get("OLLAMA_MODEL", "your-model-name"))
    ap.add_argument("--temp", type=float, default=0.35)
    # position (optional; defaults = flat)
    ap.add_argument("--qty", type=float, default=0.0, help="Open qty (base asset).")
    ap.add_argument("--avg-entry", type=float, default=None, help="Avg entry price (quote).")
    ap.add_argument("--age-min", type=float, default=0.0, help="Minutes since entry (for stop_ok/time stop).")
    # policy knobs (match your config)
    ap.add_argument("--fee-bps", type=float, default=20.0)
    ap.add_argument("--min-profit-bps", type=float, default=15.0)
    ap.add_argument("--stop-loss-pct", type=float, default=0.008)
    ap.add_argument("--time-stop-min", type=int, default=90)
    ap.add_argument("--dca-step-bps", type=float, default=20.0)
    ap.add_argument("--min-pos-usdt", type=float, default=1.0)
    args = ap.parse_args()

    # 1) Fetch klines
    kl = mexc_klines(args.symbol, args.interval, args.limit)
    if not kl:
        print("No klines returned.")
        sys.exit(2)

    # 2) Build DF + features
    df = klines_to_df(kl)
    if df.empty:
        print("Empty DataFrame after parsing klines.")
        sys.exit(3)

    feats = compute_features(df)
    px = float(feats.get("price", 0.0))
    print("\n=== Features (sent to LLM) ===")
    print(json.dumps(feats, indent=2, sort_keys=True))

    # 3) Build position context
    pos_ctx = build_pos_ctx(
        price=px,
        qty=args.qty,
        avg_entry=args.avg_entry,
        fee_bps=args.fee_bps,
        min_profit_bps=args.min_profit_bps,
        stop_loss_pct=args.stop_loss_pct,
        time_stop_min=args.time_stop_min,
        age_min=args.age_min,
        min_pos_usdt=args.min_pos_usdt,
    )
    print("\n=== Position Context (sent to LLM) ===")
    print(json.dumps(pos_ctx, indent=2, sort_keys=True))

    # 4) Build prompt exactly like the bot
    prompt = PROMPT_TEMPLATE.format(
        symbol=args.symbol,
        features_json=json.dumps(feats),
        position_json=json.dumps(pos_ctx),
        dca_step_bps=args.dca_step_bps,
        trail_drawdown_pct=float(CFG["profit_protect"].get("trail_drawdown_pct", 0.01))
    )

    print("\n=== Full Prompt (exact text sent to LLM) ===\n")
    print(f"{SYSTEM}\n\n{prompt}")

    # 5) Call Ollama
    try:
        resp_json, payload = call_ollama(args.ollama, args.model, prompt, temperature=args.temp)
    except Exception as e:
        print("\n[ERROR] LLM call failed:", e)
        sys.exit(4)

    # 6) Show raw response and parsed decision
    raw_text = resp_json.get("response", "")
    print("\n=== Raw LLM Response ===\n")
    print(raw_text)

    try:
        decision = strict_json(raw_text)
    except Exception as e:
        print("\n[WARN] Could not parse strict JSON from response:", e)
        decision = {}

    print("\n=== Parsed Decision ===")
    print(json.dumps(decision, indent=2, sort_keys=True))

    # Friendly footer
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"\nDone @ {ts}")

if __name__ == "__main__":
    main()
