from __future__ import annotations
import httpx, json, re
from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, Any

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

PROMPT_TEMPLATE = """Decide BUY/SELL/HOLD for {symbol} from these features (floats):
{features_json}

Scoring rubric (compute simple scores, then combine):
1) Momentum score (smom):
   - smom = 2*ret_1 + 1*ret_5 + 0.5*ret_20
2) MACD score (smacd):
   - if macd_hist >= 0.0 -> +0.4
   - if macd_hist < 0.0  -> -0.4
   - if |macd_hist| < 0.0002 -> add 0 (treat as neutral)
3) RSI score (srsi):
   - if rsi_14 <= 35 -> +0.4 (oversold)
   - if rsi_14 >= 65 -> -0.4 (overbought)
   - if 45 <= rsi_14 <= 55 -> 0 (neutral)
   - else -> +/-0.2 toward the side of 50 (below 50 = +0.2, above 50 = -0.2)

Edge:
- edge = smom + smacd + srsi

Decision:
- if edge >= +0.25 -> action=BUY
- if edge <= -0.25 -> action=SELL
- else -> action=HOLD

Confidence (DISCRETE; do NOT output 0.50):
- For BUY/SELL: base = min(0.85, 0.55 + min(0.30, abs(edge))*0.8)
  Round confidence to the nearest of: [0.55, 0.60, 0.65, 0.70, 0.80, 0.85]
- For HOLD: confidence = 0.52

Price hint:
- Use the current spot {features_json} price as price_hint (echo it). The executor will apply side-specific slippage.

Output STRICT JSON ONLY with fields: action, confidence, price_hint, reason (<= 8 words).
"""

def _strict_json(text: str) -> Dict[str, Any]:
    import json, re
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise ValueError("LLM did not return JSON.")
    return json.loads(m.group(0))

async def _call_ollama(ollama_host: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(f"{ollama_host.rstrip('/')}/api/generate", json=payload)
        r.raise_for_status()
        return r.json()
    
async def llm_decide(ollama_host: str, model: str, symbol: str, features: Dict[str, Any]) -> Decision:
    payload = {
        "model": model,
        "prompt": f"{SYSTEM}\n\n" + PROMPT_TEMPLATE.format(symbol=symbol, features_json=json.dumps(features)),
        "stream": False,
        # If your Ollama build supports it, this helps enforce valid JSON:
        "format": "json",
        "options": {"temperature": 0.2}
    }
    try:
        out = await _call_ollama(ollama_host, payload)   # your retried call
        txt = out.get("response", "")
        data = _strict_json(txt)

        # ---- repair/normalize missing or goofy fields ----
        action = str(data.get("action", "HOLD")).upper()
        if action not in ("BUY", "SELL", "HOLD"):
            action = "HOLD"

        conf = data.get("confidence", 0.0)
        try:
            conf = float(conf)
        except Exception:
            conf = 0.0
        conf = max(0.0, min(1.0, conf))

        price_hint = data.get("price_hint", None)
        if price_hint is not None:
            try:
                price_hint = float(price_hint)
            except Exception:
                price_hint = None

        reason = data.get("reason") or f"auto-filled; model={model}"

        return Decision(action=action, confidence=conf, price_hint=price_hint, reason=reason)

    except Exception as e:
        # Fail-safe: HOLD with low confidence so the bot keeps running
        return Decision(action="HOLD", confidence=0.0, price_hint=None, reason=f"LLM unavailable: {e.__class__.__name__}")
