from __future__ import annotations
import pandas as pd, numpy as np
from typing import Dict, Any, List

def klines_to_df(klines: List[List]) -> pd.DataFrame:
    # Expect [open_time, open, high, low, close, volume, ...]
    cols = ["t","o","h","l","c","v","ct","qv","n","tb","tq","i"][:len(klines[0])]
    df = pd.DataFrame(klines, columns=cols)
    for col in ("o","h","l","c","v"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "t" in df.columns:
        df["ts"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    return df

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def _rsi(close: pd.Series, length: int = 14) -> float:
    if len(close) < length + 1:
        return 50.0
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = _ema(gain, length)
    avg_loss = _ema(loss, length)
    rs = avg_gain / (avg_loss.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    val = float(rsi.iloc[-1])
    if np.isnan(val):  # if avg_loss==0 -> rs=inf -> rsi=100, handle NaN edge
        return 100.0
    return val

def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    if len(close) < slow + signal:
        macd_line = 0.0
        signal_line = 0.0
        hist = 0.0
        return macd_line, signal_line, hist
    ema_fast = _ema(close, fast)
    ema_slow = _ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = _ema(macd_line, signal)
    hist = macd_line - signal_line
    return float(macd_line.iloc[-1]), float(signal_line.iloc[-1]), float(hist.iloc[-1])

def compute_features(df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    c = df["c"]
    n = len(c)
    out["ret_1"] = float((c.iloc[-1] / c.iloc[-2] - 1.0)) if n >= 2 else 0.0
    out["ret_5"] = float((c.iloc[-1] / c.iloc[-6] - 1.0)) if n >= 6 else 0.0
    out["ret_20"] = float((c.iloc[-1] / c.iloc[-21] - 1.0)) if n >= 21 else 0.0

    rsi14 = _rsi(c, 14)
    macd_line, macd_signal, macd_hist = _macd(c, 12, 26, 9)
    out["rsi_14"] = rsi14
    out["macd"] = macd_line
    out["macd_signal"] = macd_signal
    out["macd_hist"] = macd_hist

    returns = c.pct_change().dropna()
    out["vol_20"] = float(returns.tail(20).std()) if len(returns) >= 20 else 0.0

    out["price"] = float(c.iloc[-1])
    out["price_prev"] = float(c.iloc[-2]) if n >= 2 else out["price"]
    return out

def df_to_candle_rows(df):
    r = df.iloc[-1]
    return [{
        "ts": r["ts"].to_pydatetime(),
        "o": float(r["o"]),
        "h": float(r["h"]),
        "l": float(r["l"]),
        "c": float(r["c"]),
        "v": float(r["v"]),
    }]
