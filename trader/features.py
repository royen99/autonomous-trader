# features.py
from __future__ import annotations
from typing import Any, Dict, List
import pandas as pd
import numpy as np
import re

# ------------- KLINE PARSERS -------------

def klines_to_df(klines: List[Any]) -> pd.DataFrame:
    """
    Normalize MEXC/Binance-style klines (list of lists OR list of dicts)
    into a tidy DataFrame with columns: ts,o,h,l,c,v
    - ts is timezone-aware UTC
    - o/h/l/c/v are floats
    Supported shapes:
      List: [openTime, open, high, low, close, volume, closeTime, ...]
      Dict: keys like {t/openTime, o/open, h/high, l/low, c/close, v/volume}
    """
    rows = []
    for k in klines or []:
        if isinstance(k, (list, tuple)) and len(k) >= 6:
            ts_ms = k[0]
            o, h, l, c, v = k[1], k[2], k[3], k[4], k[5]
        elif isinstance(k, dict):
            ts_ms = k.get("t") or k.get("openTime") or k.get("T") or k.get("startTime")
            o = k.get("o") or k.get("open")
            h = k.get("h") or k.get("high")
            l = k.get("l") or k.get("low")
            c = k.get("c") or k.get("close")
            v = k.get("v") or k.get("volume")
        else:
            continue

        # coerce
        try:
            ts = pd.to_datetime(int(ts_ms), unit="ms", utc=True)
        except Exception:
            # if seconds provided
            ts = pd.to_datetime(ts_ms, utc=True)

        def f(x):
            try:
                return float(x)
            except Exception:
                return np.nan

        rows.append({
            "ts": ts,
            "o": f(o), "h": f(h), "l": f(l), "c": f(c), "v": f(v)
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["ts", "o", "h", "l", "c", "v"])

    df = df.dropna(subset=["ts"]).sort_values("ts")
    df = df.drop_duplicates(subset=["ts"], keep="last")
    # ensure dtypes
    for col in ["o", "h", "l", "c", "v"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.dropna(subset=["o", "h", "l", "c", "v"])
    return df.reset_index(drop=True)

def df_to_candle_rows(df: pd.DataFrame, symbol: str) -> List[Dict[str, Any]]:
    """
    Convert candles df to rows expected by upsert_candles:
    keys: symbol, ts, o, h, l, c, v
    """
    if df.empty:
        return []
    out: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        out.append({
            "symbol": symbol,
            "ts": pd.to_datetime(r["ts"], utc=True).to_pydatetime(),
            "o": float(r["o"]),
            "h": float(r["h"]),
            "l": float(r["l"]),
            "c": float(r["c"]),
            "v": float(r["v"]),
        })
    return out

# ------------- INDICATORS / FEATURES -------------

def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def _rsi(close: pd.Series, length: int = 14) -> float:
    if len(close) < length + 1: return 50.0
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    ag = gain.ewm(alpha=1/length, adjust=False).mean()
    al = loss.ewm(alpha=1/length, adjust=False).mean()
    rs = ag / al.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    v = float(rsi.iloc[-1])
    return 100.0 if np.isnan(v) else v

def _macd(close: pd.Series, fast=12, slow=26, signal=9):
    if len(close) < slow + signal: return 0.0, 0.0, 0.0
    ema_f = _ema(close, fast); ema_s = _ema(close, slow)
    macd = ema_f - ema_s
    sig = _ema(macd, signal)
    hist = macd - sig
    return float(macd.iloc[-1]), float(sig.iloc[-1]), float(hist.iloc[-1])

def _atr_pct(df: pd.DataFrame, length: int = 14) -> float:
    if len(df) < length + 1: return 0.0
    h, l, c = df["h"], df["l"], df["c"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/length, adjust=False).mean()
    return float((atr.iloc[-1] / c.iloc[-1]) if c.iloc[-1] else 0.0)

def _vwap_rolling(df: pd.DataFrame, window: int) -> float:
    tail = df.tail(window)
    if tail.empty: return 0.0
    tp = (tail["h"] + tail["l"] + tail["c"]) / 3.0
    denom = float(tail["v"].sum())
    if denom <= 0: return float(tail["c"].iloc[-1])
    return float((tp * tail["v"]).sum() / denom)

def _vol_z(df: pd.DataFrame, window: int = 20) -> float:
    v = df["v"].astype(float)
    if len(v) < window + 1: return 0.0
    win = v.tail(window)
    m, s = win.mean(), win.std(ddof=0)
    return float((v.iloc[-1] - m) / s) if s > 0 else 0.0

def _normalize_freq(freq: str) -> str:
    f = str(freq).strip()
    # 15T -> 15min, 1H -> 1h (case-insensitive)
    f = re.sub(r'(?i)\b(\d+)\s*T\b', r'\1min', f)
    f = re.sub(r'(?i)\b(\d+)\s*H\b', r'\1h',   f)
    return f

def _trend_resample(c: pd.Series, freq: str, points: int) -> float:
    """Resample close to `freq`, take last N points, return pct change over that span."""
    freq = _normalize_freq(freq)
    rc = c.resample(freq).last().dropna()
    if len(rc) < max(2, points):
        return 0.0
    rc = rc.tail(points)
    first, last = float(rc.iloc[0]), float(rc.iloc[-1])
    if first == 0:
        return 0.0
    return (last - first) / first

def compute_features(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Input df: columns ['ts','o','h','l','c','v'] (1m candles).
    Returns a dict of numeric features (no NaNs).
    """
    # ensure datetime index for resampling
    dfi = df.copy()
    if "ts" in dfi.columns:
        dfi["ts"] = pd.to_datetime(dfi["ts"], utc=True)
        dfi = dfi.set_index("ts")
    dfi = dfi.sort_index()

    out: Dict[str, Any] = {}
    c = dfi["c"].astype(float); n = len(c)

    # short returns
    out["ret_1"]  = float((c.iloc[-1]/c.iloc[-2]-1.0)) if n >= 2 else 0.0
    out["ret_5"]  = float((c.iloc[-1]/c.iloc[-6]-1.0)) if n >= 6 else 0.0
    out["ret_20"] = float((c.iloc[-1]/c.iloc[-21]-1.0)) if n >= 21 else 0.0

    # longer returns
    out["ret_60"]  = float((c.iloc[-1]/c.iloc[-61]-1.0)) if n >= 61 else 0.0
    out["ret_120"] = float((c.iloc[-1]/c.iloc[-121]-1.0)) if n >= 121 else 0.0
    out["ret_360"] = float((c.iloc[-1]/c.iloc[-361]-1.0)) if n >= 361 else 0.0

    # classic indicators
    rsi14 = _rsi(c, 14)
    macd_line, macd_signal, macd_hist = _macd(c, 12, 26, 9)
    out["rsi_14"] = rsi14
    out["macd"] = macd_line
    out["macd_signal"] = macd_signal
    out["macd_hist"] = macd_hist

    # trend / regime
    ema20 = _ema(c, 20)
    ema50 = _ema(c, 50)
    ema200 = _ema(c, 200) if n >= 200 else pd.Series([c.iloc[-1]]*n, index=c.index)
    out["ema20"] = float(ema20.iloc[-1])
    out["ema50"] = float(ema50.iloc[-1])
    out["ema200"] = float(ema200.iloc[-1])
    out["ema_fast_above_slow"] = 1 if out["ema20"] > out["ema50"] else 0
    out["above_ema200"] = 1 if float(c.iloc[-1]) >= out["ema200"] else 0
    out["ema50_slope"] = float((ema50.iloc[-1] - ema50.iloc[-2]) / c.iloc[-1]) if n >= 2 else 0.0

    # volatility / participation
    out["atr_pct_14"] = _atr_pct(dfi, 14)
    out["atr_pct_100"] = _atr_pct(dfi, 100) if n >= 101 else out["atr_pct_14"]
    out["vol_z20"] = _vol_z(dfi, 20)

    # VWAPs
    vwap60 = _vwap_rolling(dfi, 60 if n >= 60 else max(3, n))
    out["vwap60"] = vwap60
    out["price_vs_vwap60"] = float(((c.iloc[-1] - vwap60) / vwap60) if vwap60 else 0.0)
    vwap240 = _vwap_rolling(dfi, 240) if n >= 240 else vwap60
    out["vwap240"] = vwap240
    out["price_vs_vwap240"] = float(((c.iloc[-1] - vwap240) / vwap240) if vwap240 else 0.0)

    # higher timeframe trend confirmations
    out["trend_15m"] = _trend_resample(c, "15min", 8)
    out["trend_1h"]  = _trend_resample(c, "1h", 8)

    # convenience echoes
    px = float(c.iloc[-1]); out["price"] = px
    out["price_prev"] = float(c.iloc[-2]) if n >= 2 else px

    # micro 5m trend (last 5 bars of 1m)
    if n >= 6:
        c5 = c.tail(6).reset_index(drop=True)
        out["trend_5m"] = float((c5.iloc[-1] - c5.iloc[0]) / c5.iloc[0])
    else:
        out["trend_5m"] = 0.0

    # realized vol last 20
    returns = c.pct_change().dropna()
    out["vol_20"] = float(returns.tail(20).std()) if len(returns) >= 20 else 0.0

    # ensure no NaNs
    for k, v in list(out.items()):
        if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
            out[k] = 0.0
    return out
