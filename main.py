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
        "TOP_N": int(os.getenv("TOP_N", "100")),
        "VOL_LOOKBACK": int(os.getenv("VOL_LOOKBACK", "96")),
        "TIMEFRAME": os.getenv("TIMEFRAME", "60"),
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
        "TG_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", ""),
        "TG_CHAT": os.getenv("TELEGRAM_CHAT_ID", ""),
        "TG_SIGNAL_CHAT": os.getenv("TELEGRAM_SIGNAL_CHAT_ID", ""),
        "RUN_MODE": os.getenv("RUN_MODE", "ONCE").upper(),
        "INTERVAL_SECONDS": int(os.getenv("INTERVAL_SECONDS", "600")),
        "SEND_STARTUP_TEST": os.getenv("SEND_STARTUP_TEST", "1") == "1",
        "AUTO_TRADE": os.getenv("AUTO_TRADE", "0") == "1",
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

def format_signal_text(symbol: str, side: str, info: dict) -> str:
    line1 = f"<b>{symbol}</b> — <b>{'LONG' if side=='LONG' else 'SHORT'}</b>"
    line2 = f"Entry({('close' if True else 'retest')}): <b>{fmt_price(info['entry_close'])}</b>; Retest: <b>{fmt_price(info['entry_retest'])}</b>"
    line3 = f"SL: <b>{fmt_price(info['sl'])}</b> | TP: <b>{fmt_price(info['tp'])}</b> | {risk_summary(side, info['entry_close'], info['sl'], info['tp'])}"
    line4 = f"EMA50: {fmt_price(info['ema50'])} | EMA200: {fmt_price(info['ema200'])} | RSI: {info['rsi']:.1f} | MACD hist: {info['macd_hist']:.4f} | ATR: {fmt_price(info['atr'])}"
    return "\n".join([line1, line2, line3, line4])

def pick_entry_price(info: dict, mode: str) -> float:
    return float(info["entry_retest"] if mode == "retest" else info["entry_close"])

def process_symbol(api: BybitAPI, cfg, sym: str):
    df = get_ohlcv(api, sym, cfg["MARKET_CATEGORY"], cfg["TIMEFRAME"], cfg["CANDLES_LIMIT"])
    if df.empty or len(df) < 60:
        return None

    long_info = detect_three_white_soldiers(
        df,
        use_ema=cfg["USE_EMA"], use_rsi=cfg["USE_RSI"], use_macd=cfg["USE_MACD"], use_vol=cfg["USE_VOLUME"],
        min_body_ratio=cfg["MIN_BODY_RATIO"], max_upper_wick=cfg["MAX_UPPER_WICK"],
        rsi_min=cfg["RSI_MIN_LONG"], rsi_max=cfg["RSI_MAX_LONG"]
    )
    if long_info:
        return ("LONG", sym, long_info)

    short_info = detect_three_black_crows(
        df,
        use_ema=cfg["USE_EMA"], use_rsi=cfg["USE_RSI"], use_macd=cfg["USE_MACD"], use_vol=cfg["USE_VOLUME"],
        min_body_ratio=cfg["MIN_BODY_RATIO"], max_lower_wick=cfg["MAX_LOWER_WICK"],
        rsi_min=cfg["RSI_MIN_SHORT"], rsi_max=cfg["RSI_MAX_SHORT"]
    )
    if short_info:
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

    for sym in symbols:
        sig = process_symbol(api, cfg, sym)
        if not sig:
            # также чистим старые лимитки по символу, чтобы не застаивались
            cancel_stale_orders(api, cfg["MARKET_CATEGORY"], sym, cfg["ORDER_TTL_MINUTES"])
            continue
        side, symbol, info = sig
        found.append(sig)

        # быстрый сигнал
        tg.send(f"⚡️ {symbol} {side}\nEntry: {fmt_price(pick_entry_price(info, cfg['ENTRY_MODE']))} | SL {fmt_price(info['sl'])} | TP {fmt_price(info['tp'])}", to_signal=True)

        # ордера, если включено
        if cfg["AUTO_TRADE"] and can_open_for_symbol(api, cfg["MARKET_CATEGORY"], symbol, cfg["MAX_OPEN_PER_SYMBOL"]):
            entry_price = pick_entry_price(info, cfg["ENTRY_MODE"])
            order_id, qty = place_signal_order(
                api, cfg["MARKET_CATEGORY"], symbol, side, entry_price,
                cfg["POSITION_USD"], cfg["LEVERAGE"], order_type=cfg["ORDER_TYPE"], time_in_force="GTC"
            )
            if order_id:
                tg.send(f"🧾 Placed {side} {symbol} qty≈{qty} @ {fmt_price(entry_price)} (orderId={order_id})")
                # сразу SL/TP — для one_way позиция появится после частичного/полного исполнения,
                # но Bybit позволяет задать trading-stop.
                attach_sltp(api, cfg["MARKET_CATEGORY"], symbol, sl=info["sl"], tp=info["tp"], reduce_only=cfg["REDUCE_ONLY_SLTP"])

    if not found:
        tg.send("🫥 Нет сигналов по паттернам на текущем скане.")
        return 0

    # подробный отчёт
    blocks = []
    for side, sym, info in found:
        blocks.append(
            f"{sym} <b>{side}</b>\n"
            f"Entry(close): <b>{fmt_price(info['entry_close'])}</b>; Retest: <b>{fmt_price(info['entry_retest'])}</b>\n"
            f"SL <b>{fmt_price(info['sl'])}</b> | TP <b>{fmt_price(info['tp'])}</b>\n"
            f"EMA50 {fmt_price(info['ema50'])} | EMA200 {fmt_price(info['ema200'])} | RSI {info['rsi']:.1f} | MACD {info['macd_hist']:.4f} | ATR {fmt_price(info['atr'])}"
        )
    report = "📊 <b>Signals found:</b>\n\n" + "\n\n".join(blocks)
    tg.send(report)
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
