"""SQLite state — fair values, last signals, filings dedup, alert cooldown."""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = os.environ.get("DCF50_DB", str(Path(__file__).parent.parent / "data" / "state.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS fair_values (
    ticker TEXT PRIMARY KEY,
    fair_low REAL,
    fair_base REAL,
    fair_high REAL,
    wacc REAL,
    fcf_avg REAL,
    shares REAL,
    net_debt REAL,
    computed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS last_signals (
    ticker TEXT PRIMARY KEY,
    signal TEXT,
    upside_pct REAL,
    price REAL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts_sent (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    signal TEXT,
    upside_pct REAL,
    sent_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_ticker_time ON alerts_sent(ticker, sent_at);
CREATE TABLE IF NOT EXISTS last_filings (
    ticker TEXT NOT NULL,
    accession_number TEXT NOT NULL,
    filing_type TEXT,
    filing_date TEXT,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (ticker, accession_number)
);
"""


@dataclass
class FairValue:
    ticker: str
    fair_low: float | None
    fair_base: float | None
    fair_high: float | None
    wacc: float | None
    fcf_avg: float | None
    shares: float | None
    net_debt: float | None
    computed_at: str


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def _conn():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    try:
        yield c
        c.commit()
    finally:
        c.close()


def init_schema():
    with _conn() as c:
        c.executescript(SCHEMA)


def upsert_fair_value(fv: FairValue):
    with _conn() as c:
        c.execute(
            """INSERT INTO fair_values (ticker, fair_low, fair_base, fair_high, wacc, fcf_avg, shares, net_debt, computed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(ticker) DO UPDATE SET
                   fair_low=excluded.fair_low,
                   fair_base=excluded.fair_base,
                   fair_high=excluded.fair_high,
                   wacc=excluded.wacc,
                   fcf_avg=excluded.fcf_avg,
                   shares=excluded.shares,
                   net_debt=excluded.net_debt,
                   computed_at=excluded.computed_at""",
            (fv.ticker, fv.fair_low, fv.fair_base, fv.fair_high, fv.wacc,
             fv.fcf_avg, fv.shares, fv.net_debt, fv.computed_at),
        )


def load_fair_values() -> dict[str, FairValue]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM fair_values").fetchall()
    return {r["ticker"]: FairValue(**dict(r)) for r in rows}


def get_fair_value(ticker: str) -> FairValue | None:
    with _conn() as c:
        r = c.execute("SELECT * FROM fair_values WHERE ticker = ?", (ticker,)).fetchone()
    return FairValue(**dict(r)) if r else None


def last_signal(ticker: str) -> tuple[str | None, float | None]:
    with _conn() as c:
        r = c.execute("SELECT signal, upside_pct FROM last_signals WHERE ticker = ?", (ticker,)).fetchone()
    return (r["signal"], r["upside_pct"]) if r else (None, None)


def upsert_signal(ticker: str, signal: str, upside_pct: float | None, price: float | None):
    with _conn() as c:
        c.execute(
            """INSERT INTO last_signals (ticker, signal, upside_pct, price, updated_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(ticker) DO UPDATE SET
                   signal=excluded.signal,
                   upside_pct=excluded.upside_pct,
                   price=excluded.price,
                   updated_at=excluded.updated_at""",
            (ticker, signal, upside_pct, price, _utcnow()),
        )


def recently_alerted(ticker: str, alert_type: str, hours: int = 12) -> bool:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with _conn() as c:
        r = c.execute(
            "SELECT 1 FROM alerts_sent WHERE ticker=? AND alert_type=? AND sent_at > ? LIMIT 1",
            (ticker, alert_type, cutoff),
        ).fetchone()
    return r is not None


def record_alert(ticker: str, alert_type: str, signal: str | None = None, upside: float | None = None):
    with _conn() as c:
        c.execute(
            "INSERT INTO alerts_sent (ticker, alert_type, signal, upside_pct, sent_at) VALUES (?, ?, ?, ?, ?)",
            (ticker, alert_type, signal, upside, _utcnow()),
        )


def recent_alerts(hours: int = 24) -> list[sqlite3.Row]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with _conn() as c:
        return c.execute(
            "SELECT * FROM alerts_sent WHERE sent_at > ? ORDER BY sent_at ASC", (cutoff,)
        ).fetchall()


def filing_processed(ticker: str, accession_number: str) -> bool:
    with _conn() as c:
        r = c.execute(
            "SELECT 1 FROM last_filings WHERE ticker=? AND accession_number=?",
            (ticker, accession_number),
        ).fetchone()
    return r is not None


def record_filing(ticker: str, accession_number: str, filing_type: str | None, filing_date: str | None):
    with _conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO last_filings (ticker, accession_number, filing_type, filing_date, processed_at) VALUES (?, ?, ?, ?, ?)",
            (ticker, accession_number, filing_type, filing_date, _utcnow()),
        )


if __name__ == "__main__":
    init_schema()
    print(f"schema initialized at {DB_PATH}")
