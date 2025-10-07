from typing import Dict, Any, Optional
import pandas as pd
from indicators import ema, rsi, macd, atr

def _body_ratio(o, c, h, l):
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    return body / rng

def detect_three_white_soldiers(df: pd.DataFrame,
                                use_ema=True, use_rsi=True, use_macd=True, use_vol=True,
                                min_body_ratio=0.6, max_upper_wick=0.35,
                                rsi_min=50, rsi_max=72) -> Optional[Dict[str, Any]]:
    if len(df) < 60:
        return None
    last = df.iloc[-3:]
    # 3 бычьих крупных тела, минимум длинных верхних теней
    for i in range(3):
        o, c, h, l = (last['open'].iloc[i], last['close'].iloc[i],
                      last['high'].iloc[i], last['low'].iloc[i])
        if c <= o:
            return None
        if _body_ratio(o, c, h, l) < min_body_ratio:
            return None
        uw = (h - max(o, c)) / max(h - l, 1e-12)
        if uw > max_upper_wick:
            return None
    if not (last['close'].iloc[1] > last['close'].iloc[0] and last['close'].iloc[2] > last['close'].iloc[1]):
        return None

    ema50 = ema(df['close'], 50)
    ema200 = ema(df['close'], 200)
    if use_ema and not (ema50.iloc[-1] > ema200.iloc[-1]):
        return None

    r = rsi(df['close'], 14)
    if use_rsi and not (rsi_min <= float(r.iloc[-1]) <= rsi_max):
        return None

    macd_line, signal_line, hist = macd(df['close'])
    if use_macd and not (hist.iloc[-1] > 0):
        return None

    if use_vol and 'volume' in df.columns:
        v = df['volume'].iloc[-20:]
        if v.mean() <= 0 or v.iloc[-1] < 0.75 * v.mean():
            return None

    atr14 = atr(df, 14).iloc[-1]
    low2 = last['low'].iloc[1]
    close3 = last['close'].iloc[2]
    entry_after_close = close3
    entry_retest = (last['open'].iloc[1] + last['close'].iloc[1]) / 2.0
    sl = min(low2, entry_after_close - atr14 * 0.5)
    tp = entry_after_close + atr14 * 3.6
    return {
        "side": "LONG",
        "entry_close": float(entry_after_close),
        "entry_retest": float(entry_retest),
        "sl": float(sl),
        "tp": float(tp),
        "atr": float(atr14),
        "ema50": float(ema50.iloc[-1]),
        "ema200": float(ema200.iloc[-1]),
        "rsi": float(r.iloc[-1]),
        "macd_hist": float(hist.iloc[-1]),
    }

def detect_three_black_crows(df: pd.DataFrame,
                             use_ema=True, use_rsi=True, use_macd=True, use_vol=True,
                             min_body_ratio=0.6, max_lower_wick=0.35,
                             rsi_min=28, rsi_max=50) -> Optional[Dict[str, Any]]:
    if len(df) < 60:
        return None
    last = df.iloc[-3:]
    for i in range(3):
        o, c, h, l = (last['open'].iloc[i], last['close'].iloc[i],
                      last['high'].iloc[i], last['low'].iloc[i])
        if c >= o:
            return None
        if _body_ratio(o, c, h, l) < min_body_ratio:
            return None
        lw = (min(o, c) - l) / max(h - l, 1e-12)
        if lw > max_lower_wick:
            return None
    if not (last['close'].iloc[1] < last['close'].iloc[0] and last['close'].iloc[2] < last['close'].iloc[1]):
        return None

    ema50 = ema(df['close'], 50)
    ema200 = ema(df['close'], 200)
    if use_ema and not (ema50.iloc[-1] < ema200.iloc[-1]):
        return None

    r = rsi(df['close'], 14)
    if use_rsi and not (rsi_min <= float(r.iloc[-1]) <= rsi_max):
        return None

    macd_line, signal_line, hist = macd(df['close'])
    if use_macd and not (hist.iloc[-1] < 0):
        return None

    if use_vol and 'volume' in df.columns:
        v = df['volume'].iloc[-20:]
        if v.mean() <= 0 or v.iloc[-1] < 0.75 * v.mean():
            return None

    atr14 = atr(df, 14).iloc[-1]
    high2 = last['high'].iloc[1]
    close3 = last['close'].iloc[2]
    entry_after_close = close3
    entry_retest = (last['open'].iloc[1] + last['close'].iloc[1]) / 2.0
    sl = max(high2, entry_after_close + atr14 * 0.5)
    tp = entry_after_close - atr14 * 3.6
    return {
        "side": "SHORT",
        "entry_close": float(entry_after_close),
        "entry_retest": float(entry_retest),
        "sl": float(sl),
        "tp": float(tp),
        "atr": float(atr14),
        "ema50": float(ema50.iloc[-1]),
        "ema200": float(ema200.iloc[-1]),
        "rsi": float(r.iloc[-1]),
        "macd_hist": float(hist.iloc[-1]),
    }
