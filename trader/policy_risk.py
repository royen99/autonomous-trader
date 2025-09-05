from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
import time

@dataclass
class RiskState:
    usdt_balance: float = 0.0
    unrealized_pnl: float = 0.0
    today_loss: float = 0.0
    last_loss_ts: float = 0.0

def allow_trade(decision_conf: float, price: float, cfg: Dict[str, Any], rs: RiskState) -> bool:
    if decision_conf < cfg["risk"]["min_confidence"]:
        return False
    if rs.today_loss <= -abs(cfg["risk"]["daily_max_loss_usd"]):
        return False
    if time.time() - rs.last_loss_ts < cfg["risk"]["cooldown_after_loss_s"]:
        return False
    return True

def size_for_trade(price: float, cfg: Dict[str, Any], rs: RiskState) -> float:
    max_per = cfg["risk"]["max_per_trade_usd"]
    qty = max_per / price
    return qty

def update_loss(rs: RiskState, delta: float):
    rs.today_loss += delta
    if delta < 0:
        rs.last_loss_ts = time.time()
