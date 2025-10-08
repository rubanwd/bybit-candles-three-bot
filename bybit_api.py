import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from typing import Dict, Any, Optional

class BybitAPI:
    def __init__(self, base_url: str, api_key: str = "", api_secret: str = ""):
        self.base = base_url.rstrip("/")
        self.key = api_key or ""
        self.secret = api_secret or ""

    # ---------------- PUBLIC ----------------
    def get_tickers(self, category="linear", symbol: Optional[str] = None) -> Dict[str, Any]:
        params = {"category": category}
        if symbol:
            params["symbol"] = symbol
        return self._get("/v5/market/tickers", params)

    def get_kline(self, category="linear", symbol="BTCUSDT", interval="60", limit=200) -> Dict[str, Any]:
        params = {"category": category, "symbol": symbol, "interval": interval, "limit": limit}
        return self._get("/v5/market/kline", params)

    # ---------------- PRIVATE ----------------
    def set_position_mode(self, category="linear", mode="one_way") -> Dict[str, Any]:
        m = 0 if mode == "one_way" else 1
        payload = {"category": category, "mode": m}
        return self._post("/v5/position/switch-mode", payload, auth=True)

    def set_leverage(self, category="linear", symbol="BTCUSDT", buy_leverage=10, sell_leverage=10) -> Dict[str, Any]:
        payload = {"category": category, "symbol": symbol, "buyLeverage": str(buy_leverage), "sellLeverage": str(sell_leverage)}
        return self._post("/v5/position/set-leverage", payload, auth=True)

    def place_order(self, **kwargs) -> Dict[str, Any]:
        return self._post("/v5/order/create", kwargs, auth=True)

    def cancel_order(self, category="linear", symbol="", orderId: Optional[str]=None, orderLinkId: Optional[str]=None):
        payload = {"category": category, "symbol": symbol}
        if orderId: payload["orderId"] = orderId
        if orderLinkId: payload["orderLinkId"] = orderLinkId
        return self._post("/v5/order/cancel", payload, auth=True)

    def get_open_orders(self, category="linear", symbol: Optional[str]=None):
        params = {"category": category}
        if symbol: params["symbol"] = symbol
        return self._get("/v5/order/realtime", params, auth=True)

    def get_positions(self, category="linear", symbol: Optional[str]=None):
        params = {"category": category}
        if symbol: params["symbol"] = symbol
        return self._get("/v5/position/list", params, auth=True)

    def set_sl_tp(self, category="linear", symbol="", positionIdx: int=0,
                  takeProfit: Optional[str]=None, stopLoss: Optional[str]=None,
                  tpTriggerBy="LastPrice", slTriggerBy="LastPrice", reduceOnly=True):
        payload = {"category": category, "symbol": symbol, "positionIdx": str(positionIdx)}
        if takeProfit is not None: payload["takeProfit"] = str(takeProfit)
        if stopLoss is not None: payload["stopLoss"] = str(stopLoss)
        payload["tpTriggerBy"] = tpTriggerBy
        payload["slTriggerBy"] = slTriggerBy
        payload["reduceOnly"] = "true" if reduceOnly else "false"
        return self._post("/v5/position/trading-stop", payload, auth=True)

    # ---------------- low-level ----------------
    def _get(self, path: str, params: Dict[str, Any], auth: bool = False) -> Dict[str, Any]:
        url = self.base + path
        headers = self._auth_headers(params) if auth else None
        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: Dict[str, Any], auth: bool = False) -> Dict[str, Any]:
        url = self.base + path
        headers = self._auth_headers(body) if auth else {"Content-Type": "application/json"}
        r = requests.post(url, json=body, headers=headers, timeout=30)
        r.raise_for_status()
        return r.json()

    def _auth_headers(self, payload: Dict[str, Any]) -> Dict[str, str]:
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        body_str = urlencode(sorted(payload.items())) if payload else ""
        to_sign = ts + self.key + recv_window + body_str
        sign = hmac.new(self.secret.encode(), to_sign.encode(), hashlib.sha256).hexdigest()
        return {
            "X-BAPI-API-KEY": self.key,
            "X-BAPI-SIGN": sign,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }
