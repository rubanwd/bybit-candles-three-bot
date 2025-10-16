
import os
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
import pytz

# --- SQLAlchemy (optional) ---
_USE_SA = bool(os.getenv("DATABASE_URL"))
_ENGINE = None
_TABLE = None

def _sa_setup():
    global _ENGINE, _TABLE
    from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, Index
    url = os.getenv("DATABASE_URL")
    _ENGINE = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
    md = MetaData()
    _TABLE = Table(
        "signals", md,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("run_id", String(64), nullable=False),
        Column("symbol", String(32), nullable=False),
        Column("side", String(16), nullable=False),
        Column("timeframe", String(16), nullable=False),
        Column("entry_close", Float, nullable=False),
        Column("entry_retest", Float, nullable=False),
        Column("sl", Float, nullable=False),
        Column("tp", Float, nullable=False),
        Column("ema50", Float, nullable=False),
        Column("ema200", Float, nullable=False),
        Column("rsi", Float, nullable=False),
        Column("macd_hist", Float, nullable=False),
        Column("atr", Float, nullable=False),
        Column("rr", Float, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        sqlite_autoincrement=True,
    )
    Index("idx_signals_runid", _TABLE.c.run_id)
    Index("idx_signals_symbol_timeframe", _TABLE.c.symbol, _TABLE.c.timeframe)
    md.create_all(_ENGINE)

# --- SQLite fallback (original) ---
import sqlite3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    entry_close REAL NOT NULL,
    entry_retest REAL NOT NULL,
    sl REAL NOT NULL,
    tp REAL NOT NULL,
    ema50 REAL NOT NULL,
    ema200 REAL NOT NULL,
    rsi REAL NOT NULL,
    macd_hist REAL NOT NULL,
    atr REAL NOT NULL,
    rr REAL NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_signals_runid ON signals(run_id);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_timeframe ON signals(symbol, timeframe);
"""

def _ensure_dir(path: str):
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

def open_db(db_path: str) -> sqlite3.Connection:
    _ensure_dir(db_path)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.executescript(SCHEMA_SQL)
    return conn

def _kyiv_now_dt() -> datetime:
    tz = pytz.timezone("Europe/Kyiv")
    return datetime.now(tz)

def _parse_iso_any_tz(s: str) -> datetime:
    # Supports legacy 'Z' UTC entries and offset-aware ISO strings
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        return datetime.fromisoformat(s)
    except Exception:
        # Fallback: treat as naive UTC
        return datetime.fromtimestamp(0, tz=pytz.UTC)

def _insert_sqlite(db_path: str, run_id: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn = open_db(db_path)
    try:
        cur = conn.cursor()
        now_iso = _kyiv_now_dt().isoformat(timespec="seconds")
        payload = []
        for r in rows:
            payload.append((
                run_id,
                r["symbol"],
                r["side"],
                str(r.get("timeframe", "")),
                float(r["entry_close"]),
                float(r["entry_retest"]),
                float(r["sl"]),
                float(r["tp"]),
                float(r["ema50"]),
                float(r["ema200"]),
                float(r["rsi"]),
                float(r["macd_hist"]),
                float(r["atr"]),
                float(r["rr"]),
                now_iso,
            ))
        cur.executemany(
            "INSERT INTO signals (run_id,symbol,side,timeframe,entry_close,entry_retest,sl,tp,ema50,ema200,rsi,macd_hist,atr,rr,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            payload
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()

def _insert_sa(run_id: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    # lazy init
    if _ENGINE is None:
        _sa_setup()
    from sqlalchemy import insert
    now_dt = _kyiv_now_dt()
    payload = []
    for r in rows:
        payload.append({
            "run_id": run_id,
            "symbol": r["symbol"],
            "side": r["side"],
            "timeframe": str(r.get("timeframe","")),
            "entry_close": float(r["entry_close"]),
            "entry_retest": float(r["entry_retest"]),
            "sl": float(r["sl"]),
            "tp": float(r["tp"]),
            "ema50": float(r["ema50"]),
            "ema200": float(r["ema200"]),
            "rsi": float(r["rsi"]),
            "macd_hist": float(r["macd_hist"]),
            "atr": float(r["atr"]),
            "rr": float(r["rr"]),
            "created_at": now_dt,
        })
    with _ENGINE.begin() as conn:
        conn.execute(insert(_TABLE), payload)
    return len(payload)

def insert_signals(db_path: str, run_id: str, rows: List[Dict[str, Any]]) -> int:
    """Insert rows either via SQLAlchemy (if DATABASE_URL provided) or SQLite file path.
    API stays the same for the caller."""
    if os.getenv("DATABASE_URL"):
        return _insert_sa(run_id, rows)
    else:
        return _insert_sqlite(db_path, run_id, rows)

def has_recent_signal(db_path: str, symbol: str, within_hours: int = 24) -> bool:
    """Return True if there is any signal for symbol within the last `within_hours` hours (Kyiv time).
    Works with SQLite or SQLAlchemy depending on DATABASE_URL.
    """
    lookback_dt = _kyiv_now_dt() - timedelta(hours=within_hours)
    if os.getenv("DATABASE_URL"):
        if _ENGINE is None:
            _sa_setup()
        from sqlalchemy import select
        with _ENGINE.begin() as conn:
            stmt = select(_TABLE.c.id).where(
                (_TABLE.c.symbol == symbol) & (_TABLE.c.created_at >= lookback_dt)
            ).limit(1)
            row = conn.execute(stmt).first()
            return row is not None
    else:
        conn = open_db(db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT created_at FROM signals WHERE symbol=? ORDER BY id DESC LIMIT 1", (symbol,))
            row = cur.fetchone()
            if not row:
                return False
            created_at_str = row[0]
            created_dt = _parse_iso_any_tz(created_at_str)
            # Normalize to Kyiv for comparison
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone("Europe/Kyiv"))
            else:
                created_dt = created_dt.astimezone(pytz.timezone("Europe/Kyiv"))
            return created_dt >= lookback_dt
        finally:
            conn.close()
