from __future__ import annotations
import pandas as pd
from features import compute_features
from agent_llm import llm_decide
from policy_risk import RiskState, allow_trade, size_for_trade
from executor import make_limit_price

async def run_backtest(df: pd.DataFrame, symbol: str, cfg, ollama_host: str, model: str):
    rs = RiskState(usdt_balance=cfg["paper"]["starting_usdt"])
    c = df["c"].reset_index(drop=True)
    fills = []
    for i in range(cfg["agent"]["history_bars"], len(c)):
        window = df.iloc[: i+1].copy()
        feats = compute_features(window)
        decision = await llm_decide(ollama_host, model, symbol, feats)
        price = feats["price"]
        if decision.action == "BUY" and allow_trade(decision.confidence, price, cfg, rs):
            qty = size_for_trade(price, cfg, rs)
            limit = make_limit_price(price, "BUY", cfg["execution"]["slippage_bps"])
            cost = limit * qty
            if rs.usdt_balance >= cost:
                rs.usdt_balance -= cost
                fills.append(("BUY", limit, qty))
        elif decision.action == "SELL" and fills:
            # Close one lot for simplicity
            side, buy_px, qty = fills.pop(0)
            pnl = (price - buy_px) * qty
            rs.usdt_balance += price * qty
            rs.today_loss += pnl
    return rs.usdt_balance
