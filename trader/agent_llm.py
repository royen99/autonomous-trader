from __future__ import annotations
import httpx, json, re
from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class Decision(BaseModel):
    action: Literal["BUY","SELL","HOLD"]
    confidence: float = Field(ge=0, le=1)
    price_hint: Optional[float] = None
    reason: str = ""

SYSTEM = (
  "You are a trading decision engine. OUTPUT STRICT JSON ONLY:\n"
  '{"action":"BUY|SELL|HOLD","confidence":0.0-1.0,"price_hint":number|null,"reason":"short text"}\n'
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

Decision:
- if Edge >= +0.25 and BUY rule is satisfied -> BUY
- if Edge <= -0.25 -> SELL (respect SELL rule above)
- else -> HOLD

Confidence (discrete; no 0.50):
- BUY/SELL: nearest of [0.55,0.60,0.65,0.70,0.80,0.85] with base 0.55 + min(0.30, |Edge|)*0.8
- HOLD: 0.52

Price hint = features.price (echo).
Output STRICT JSON only.
"""

def _strict_json(text: str) -> Dict[str, Any]:
    import json, re
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise ValueError("LLM did not return JSON.")
    return json.loads(m.group(0))

def _enforce_dca(features: Dict[str, Any], pos_ctx: Dict[str, Any], decision: "Decision", dca_step_bps: float) -> "Decision":
    """If already in position, forbid BUY above avg_entry*(1 - step). Downgrade to HOLD."""
    qty = float(pos_ctx.get("qty") or 0.0)
    avg = float(pos_ctx.get("avg_entry") or 0.0)
    px  = float(features.get("price") or 0.0)
    if qty > 0 and avg > 0 and decision.action == "BUY":
        thresh = avg * (1.0 - dca_step_bps / 10000.0)
        if px > thresh:
            decision.action = "HOLD"
            decision.confidence = 0.52
            decision.price_hint = px
            decision.reason = "above avg; DCA rule"
    return decision

@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
    retry=retry_if_exception_type((httpx.ConnectError, httpx.ReadTimeout)),
)
async def _call_ollama(ollama_host: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(f"{ollama_host.rstrip('/')}/api/generate", json=payload)
        r.raise_for_status()
        return r.json()

async def llm_decide(
    ollama_host: str,
    model: str,
    symbol: str,
    features: Dict[str, Any],
    pos_ctx: Optional[Dict[str, Any]] = None,
    dca_step_bps: float = 20.0
) -> Decision:
    position_json = json.dumps(pos_ctx or {
        "qty": 0.0, "avg_entry": None, "breakeven_px": None,
        "unrealized_pct": 0.0, "in_position_min": 0, "stop_ok": False
    })
    prompt = f"{SYSTEM}\n\n" + PROMPT_TEMPLATE.format(
        symbol=symbol,
        features_json=json.dumps(features),
        position_json=position_json,
        dca_step_bps=dca_step_bps
    )
    payload = {"model": model, "prompt": prompt, "stream": False, "format": "json", "options": {"temperature": 0.35}}

    try:
        out = await _call_ollama(ollama_host, payload)
        data = _strict_json(out.get("response", ""))

        action = str(data.get("action", "HOLD")).upper()
        if action not in ("BUY","SELL","HOLD"): action = "HOLD"
        try: conf = float(data.get("confidence", 0.0))
        except: conf = 0.0
        conf = max(0.0, min(1.0, conf))
        ph = data.get("price_hint", None)
        try: ph = float(ph) if ph is not None else None
        except: ph = None
        reason = data.get("reason") or "auto"

        dec = Decision(action=action, confidence=conf, price_hint=ph, reason=reason)
        # HARD ENFORCEMENT: never BUY above avg_entry*(1 - step)
        dec = _enforce_dca(features, pos_ctx or {}, dec, dca_step_bps)
        return dec

    except Exception as e:
        return Decision(action="HOLD", confidence=0.0, price_hint=None, reason=f"LLM unavailable: {e.__class__.__name__}")
