from __future__ import annotations
from typing import Dict, Any, Optional
from decimal import Decimal, ROUND_DOWN

def round_dec(v: float, dp: int) -> str:
    q = Decimal(10) ** -dp
    return str(Decimal(v).quantize(q, rounding=ROUND_DOWN))

def make_limit_price(price: float, side: str, bps: int) -> float:
    # simple slippage guard
    if side == "BUY":
        return price * (1 + bps/10000)
    elif side == "SELL":
        return price * (1 - bps/10000)
    return price

def make_stop_take(price: float, cfg: Dict[str, Any]):
    sl = price * (1 - cfg["risk"]["stop_loss_pct"])
    tp = price * (1 + cfg["risk"]["take_profit_pct"])
    return sl, tp
