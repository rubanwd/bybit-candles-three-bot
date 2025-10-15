from typing import List
import pandas as pd
import numpy as np
from bybit_api import BybitAPI

def get_universe(api: BybitAPI, category: str, quote: str, top_n: int, mode: str,
                 interval: str, vol_lookback: int) -> List[str]:
    tick = api.get_tickers(category=category)
    result = tick.get("result", {})
    rows = result.get("list", []) or []
    df = pd.DataFrame(rows)
    if df.empty:
        return []
    df = df[df["symbol"].str.endswith(quote)]
    df["turnover24h"] = pd.to_numeric(df["turnover24h"], errors="coerce")

    if mode.upper() == "TURNOVER":
        df = df.sort_values("turnover24h", ascending=False).head(top_n)
        return df["symbol"].tolist()

    candidates = df.sort_values("turnover24h", ascending=False).head(int(top_n*1.5))["symbol"].tolist()
    vol_rows = []
    for sym in candidates:
        kl = api.get_kline(category=category, symbol=sym, interval=interval, limit=max(vol_lookback+5, 50))
        lst = (kl.get("result", {}) or {}).get("list", []) or []
        if len(lst) < vol_lookback:
            continue
        kdf = pd.DataFrame(lst, columns=["start","open","high","low","close","volume","turnover"])
        kdf["close"] = pd.to_numeric(kdf["close"], errors="coerce")
        kdf = kdf.sort_values("start").tail(vol_lookback+1)
        ret = np.log(kdf["close"]).diff().dropna()
        std = float(ret.std())
        vol_rows.append((sym, std))
    vol_rows.sort(key=lambda x: x[1], reverse=True)
    return [s for s,_ in vol_rows[:top_n]]

def get_ohlcv(api: BybitAPI, symbol: str, category: str, interval: str, limit: int):
    resp = api.get_kline(category=category, symbol=symbol, interval=interval, limit=limit)
    lst = (resp.get("result", {}) or {}).get("list", []) or []
    if not lst:
        import pandas as pd
        return pd.DataFrame()
    cols = ["start","open","high","low","close","volume","turnover"]
    import pandas as pd
    df = pd.DataFrame(lst, columns=cols)
    for c in ["open","high","low","close","volume","turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("start").reset_index(drop=True)
    return df
