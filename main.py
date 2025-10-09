import os
import time
import logging
from dotenv import load_dotenv
from tenacity import retry, wait_fixed, stop_after_attempt

from bybit_api import BybitAPI
from data_fetch import get_universe, get_ohlcv
from patterns import detect_three_white_soldiers, detect_three_black_crows
from telegram_utils import TelegramClient
from utils import fmt_price, risk_summary
from trader import can_open_for_symbol, place_signal_order, attach_sltp, cancel_stale_orders

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def load_cfg():
    load_dotenv()
    cfg = {
        "BYBIT_BASE": os.getenv("BYBIT_BASE", "https://api-demo.bybit.com"),
        "BYBIT_API_KEY": os.getenv("BYBIT_API_KEY", ""),
        "BYBIT_API_SECRET": os.getenv("BYBIT_API_SECRET", ""),
        "MARKET_CATEGORY": os.getenv("MARKET_CATEGORY", "linear"),
        "QUOTE": os.getenv("QUOTE", "USDT"),
        "UNIVERSE_MODE": os.getenv("UNIVERSE_MODE", "TURNOVER"),
        "TOP_N": int(os.getenv("TOP_N", "120")),
        "VOL_LOOKBACK": int(os.getenv("VOL_LOOKBACK", "96")),
        "TIMEFRAME": os.getenv("TIMEFRAME", "60"),
        "SCAN_TF_LIST": os.getenv("SCAN_TF_LIST", "30,60,240"),
        "CANDLES_LIMIT": int(os.getenv("CANDLES_LIMIT", "300")),
        "USE_EMA": os.getenv("USE_EMA", "1") == "1",
        "USE_RSI": os.getenv("USE_RSI", "1") == "1",
        "USE_MACD": os.getenv("USE_MACD", "1") == "1",
        "USE_VOLUME": os.getenv("USE_VOLUME", "1") == "1",
        "MIN_BODY_RATIO": float(os.getenv("MIN_BODY_RATIO", "0.6")),
        "MAX_UPPER_WICK": float(os.getenv("MAX_UPPER_WICK", "0.35")),
        "MAX_LOWER_WICK": float(os.getenv("MAX_LOWER_WICK", "0.35")),
        "RSI_MIN_LONG": int(os.getenv("RSI_MIN_LONG", "50")),
        "RSI_MAX_LONG": int(os.getenv("RSI_MAX_LONG", "72")),
        "RSI_MIN_SHORT": int(os.getenv("RSI_MIN_SHORT", "28")),
        "RSI_MAX_SHORT": int(os.getenv("RSI_MAX_SHORT", "50")),
        "ATR_LEN": int(os.getenv("ATR_LEN", "14")),
        "SL_ATR_MULT": float(os.getenv("SL_ATR_MULT", "1.8")),
        "TP_ATR_MULT": float(os.getenv("TP_ATR_MULT", "3.6")),
        "MACD_TOL": float(os.getenv("MACD_TOL", "0.0008")),
        "VOL_MIN_RATIO": float(os.getenv("VOL_MIN_RATIO", "0.6")),
        "RELAX_MODE": os.getenv("RELAX_MODE", "1") == "1",
        "USE_LAST_CANDLE": os.getenv("USE_LAST_CANDLE", "1") == "1",
        "TG_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", ""),
        "TG_CHAT": os.getenv("TELEGRAM_CHAT_ID", ""),
        "TG_SIGNAL_CHAT": os.getenv("TELEGRAM_SIGNAL_CHAT_ID", ""),
        "RUN_MODE": os.getenv("RUN_MODE", "LOOP").upper(),
        "INTERVAL_SECONDS": int(os.getenv("INTERVAL_SECONDS", "600")),
        "SEND_STARTUP_TEST": os.getenv("SEND_STARTUP_TEST", "1") == "1",
        "AUTO_TRADE": os.getenv("AUTO_TRADE", "1") == "1",
        "POSITION_USD": float(os.getenv("POSITION_USD", "100")),
        "LEVERAGE": int(os.getenv("LEVERAGE", "10")),
        "POSITION_MODE": os.getenv("POSITION_MODE", "one_way"),
        "ORDER_TTL_MINUTES": int(os.getenv("ORDER_TTL_MINUTES", "60")),
        "MAX_OPEN_PER_SYMBOL": int(os.getenv("MAX_OPEN_PER_SYMBOL", "1")),
        "ENTRY_MODE": os.getenv("ENTRY_MODE", "close"),
        "ORDER_TYPE": os.getenv("ORDER_TYPE", "Limit"),
        "REDUCE_ONLY_SLTP": os.getenv("REDUCE_ONLY_SLTP", "1") == "1",
    }
    return cfg

def _pretty_tf(tf_minutes: str) -> str:
    try:
        n = int(tf_minutes)
        return f"{n//60}H" if n % 60 == 0 else f"{n}m"
    except Exception:
        return f"{tf_minutes}m"

def format_signal_text(symbol: str, side: str, info: dict) -> str:
    tf = _pretty_tf(info.get("timeframe", "?"))
    arrow = "📈" if side == "LONG" else "📉"
    badge = "🟩 LONG" if side == "LONG" else "🟥 SHORT"
    entry = info["entry_close"]; sl = info["sl"]; tp = info["tp"]
    rr = ((tp - entry) / max(entry - sl, 1e-12)) if side == "LONG" else ((entry - tp) / max(sl - entry, 1e-12))
    lines = [
        f"{arrow} <b>{symbol}</b> {badge} [{tf}]",
        f"💰 Entry: {fmt_price(info['entry_close'])}  →  Retest: {fmt_price(info['entry_retest'])}",
        f"🧱 SL: {fmt_price(sl)}   |   🎯 TP: {fmt_price(tp)}   |   R:R ≈ {rr:.2f}",
        f"📊 EMA50: {fmt_price(info['ema50'])}  |  EMA200: {fmt_price(info['ema200'])}",
        f"⚙️ RSI: {info['rsi']:.1f}  |  MACD: {info['macd_hist']:.4f}  |  ATR: {fmt_price(info['atr'])}",
    ]
    return "\n".join(lines)

def pick_entry_price(info: dict, mode: str) -> float:
    return float(info["entry_retest"] if mode == "retest" else info["entry_close"])

def process_symbol(api: BybitAPI, cfg, sym: str, tf: str):
    df = get_ohlcv(api, sym, cfg["MARKET_CATEGORY"], tf, cfg["CANDLES_LIMIT"])
    if df.empty or len(df) < 60:
        return None

    long_info = detect_three_white_soldiers(
        df,
        use_ema=cfg["USE_EMA"], use_rsi=cfg["USE_RSI"], use_macd=cfg["USE_MACD"], use_vol=cfg["USE_VOLUME"],
        min_body_ratio=cfg["MIN_BODY_RATIO"], max_upper_wick=cfg["MAX_UPPER_WICK"],
        rsi_min=cfg["RSI_MIN_LONG"], rsi_max=cfg["RSI_MAX_LONG"],
        macd_tol=cfg["MACD_TOL"], vol_min_ratio=cfg["VOL_MIN_RATIO"],
        relax_mode=cfg["RELAX_MODE"], use_last_candle=cfg["USE_LAST_CANDLE"]
    )
    if long_info:
        long_info["timeframe"] = tf
        return ("LONG", sym, long_info)

    short_info = detect_three_black_crows(
        df,
        use_ema=cfg["USE_EMA"], use_rsi=cfg["USE_RSI"], use_macd=cfg["USE_MACD"], use_vol=cfg["USE_VOLUME"],
        min_body_ratio=cfg["MIN_BODY_RATIO"], max_lower_wick=cfg["MAX_LOWER_WICK"],
        rsi_min=cfg["RSI_MIN_SHORT"], rsi_max=cfg["RSI_MAX_SHORT"],
        macd_tol=cfg["MACD_TOL"], vol_min_ratio=cfg["VOL_MIN_RATIO"],
        relax_mode=cfg["RELAX_MODE"], use_last_candle=cfg["USE_LAST_CANDLE"]
    )
    if short_info:
        short_info["timeframe"] = tf
        return ("SHORT", sym, short_info)

    return None

@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def run_once(cfg) -> int:
    api = BybitAPI(cfg["BYBIT_BASE"], cfg["BYBIT_API_KEY"], cfg["BYBIT_API_SECRET"])
    tg = TelegramClient(cfg["TG_TOKEN"], cfg["TG_CHAT"], cfg["TG_SIGNAL_CHAT"])
    if cfg["SEND_STARTUP_TEST"]:
        tg.send("🤖 Bot started: scanning for <b>Three White Soldiers / Three Black Crows</b>...")

    symbols = get_universe(
        api, cfg["MARKET_CATEGORY"], cfg["QUOTE"], cfg["TOP_N"], cfg["UNIVERSE_MODE"],
        cfg["TIMEFRAME"], cfg["VOL_LOOKBACK"]
    )
    logging.info(f"Universe: {len(symbols)} symbols (mode={cfg['UNIVERSE_MODE']})")
    found = []

    tf_list = [t.strip() for t in str(cfg["SCAN_TF_LIST"]).split(",") if t.strip()]

    for sym in symbols:
        cancel_stale_orders(api, cfg["MARKET_CATEGORY"], sym, cfg["ORDER_TTL_MINUTES"])

        sig = None
        for tf in tf_list:
            sig = process_symbol(api, cfg, sym, tf)
            if sig: break
        if not sig:
            continue

        side, symbol, info = sig
        tf = info.get("timeframe", cfg["TIMEFRAME"])
        # fast signal header
        tg.send(f"⚡️ {symbol} {side} [{_pretty_tf(tf)}]"
                f"\nEntry: {fmt_price(pick_entry_price(info, cfg['ENTRY_MODE']))} | SL {fmt_price(info['sl'])} | TP {fmt_price(info['tp'])}",
                to_signal=True)

        if cfg["AUTO_TRADE"] and can_open_for_symbol(api, cfg["MARKET_CATEGORY"], symbol, cfg["MAX_OPEN_PER_SYMBOL"]):
            entry_price = pick_entry_price(info, cfg["ENTRY_MODE"])
            order_id, qty = place_signal_order(
                api, cfg["MARKET_CATEGORY"], symbol, side, entry_price,
                cfg["POSITION_USD"], cfg["LEVERAGE"], order_type=cfg["ORDER_TYPE"], time_in_force="GTC"
            )
            if order_id:
                tg.send(f"🧾 Placed {side} {symbol} qty≈{qty} @ {fmt_price(entry_price)} (orderId={order_id})")
                attach_sltp(api, cfg["MARKET_CATEGORY"], symbol, sl=info["sl"], tp=info["tp"], reduce_only=cfg["REDUCE_ONLY_SLTP"])

        found.append(sig)

    if not found:
        tg.send("🫥 Нет сигналов по паттернам на текущем скане.")
        return 0

    # detailed report
    cards = []
    for side, sym, info in found:
        cards.append(format_signal_text(sym, side, info))
    tg.send("📊 <b>Signals found</b>:\n\n" + "\n\n".join(cards))
    return len(found)

def main():
    cfg = load_cfg()
    try:
        while True:
            count = run_once(cfg)
            logging.info(f"Scan complete. Signals: {count}")
            if cfg["RUN_MODE"] == "ONCE":
                break
            time.sleep(cfg["INTERVAL_SECONDS"])
    except KeyboardInterrupt:
        logging.info("Stopped by user.")

if __name__ == "__main__":
    main()
