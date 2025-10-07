import time
from typing import Dict, Any, Optional, Tuple, List
from bybit_api import BybitAPI

def _link_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time()*1000)}"

def can_open_for_symbol(api: BybitAPI, category: str, symbol: str, max_open_per_symbol: int = 1) -> bool:
    # check open positions qty and open orders
    pos = api.get_positions(category=category, symbol=symbol)
    positions = (pos.get("result", {}) or {}).get("list", []) or []
    nonzero = [p for p in positions if abs(float(p.get("size", 0) or 0)) > 0]
    if len(nonzero) >= max_open_per_symbol:
        return False
    od = api.get_open_orders(category=category, symbol=symbol)
    orders = (od.get("result", {}) or {}).get("list", []) or []
    if len(orders) >= max_open_per_symbol:
        return False
    return True

def place_signal_order(api: BybitAPI, category: str, symbol: str, side: str, entry_price: float,
                       position_usd: float, leverage: int, order_type: str = "Limit",
                       time_in_force: str = "GTC") -> Tuple[Optional[str], float]:
    # qty = notional / price
    qty = round(max(position_usd / max(entry_price, 1e-9), 0.0001), 8)
    api.set_leverage(category=category, symbol=symbol, buy_leverage=leverage, sell_leverage=leverage)
    link_id = _link_id("ENT")
    payload = {
        "category": category,
        "symbol": symbol,
        "side": "Buy" if side == "LONG" else "Sell",
        "orderType": order_type,
        "qty": str(qty),
        "timeInForce": time_in_force,
        "orderLinkId": link_id,
    }
    if order_type == "Limit":
        payload["price"] = str(entry_price)
    resp = api.place_order(**payload)
    order_id = (resp.get("result", {}) or {}).get("orderId")
    return order_id, qty

def attach_sltp(api: BybitAPI, category: str, symbol: str, sl: float, tp: float, reduce_only: bool = True):
    # positionIdx: 0(one-way), 1(buy), 2(sell). Для упрощения — 0.
    return api.set_sl_tp(category=category, symbol=symbol, positionIdx=0,
                         stopLoss=str(sl), takeProfit=str(tp),
                         reduceOnly=reduce_only)

def cancel_stale_orders(api: BybitAPI, category: str, symbol: str, ttl_minutes: int = 60):
    # отменяем лимитки старше ttl
    now_ms = int(time.time()*1000)
    od = api.get_open_orders(category=category, symbol=symbol)
    orders = (od.get("result", {}) or {}).get("list", []) or []
    for o in orders:
        # Bybit v5 realtime returns createTime or updatedTime in ms strings
        ts = int(o.get("createdTime", o.get("updatedTime", now_ms)))
        if (now_ms - ts) >= ttl_minutes*60*1000:
            api.cancel_order(category=category, symbol=symbol, orderId=o.get("orderId"))
