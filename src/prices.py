"""Batch price fetcher — chunked to keep memory bounded."""
from __future__ import annotations

import logging

import yfinance as yf

log = logging.getLogger(__name__)

CHUNK = 25


def _fetch_chunk(tickers: list[str]) -> dict[str, float]:
    try:
        df = yf.download(
            " ".join(tickers),
            period="2d",
            interval="1d",
            progress=False,
            group_by="ticker",
            threads=False,
            auto_adjust=True,
        )
    except Exception as e:
        log.error("chunk price fetch failed: %s", e)
        return {}

    out: dict[str, float] = {}
    if df is None or df.empty:
        return out

    for tic in tickers:
        try:
            if len(tickers) == 1:
                sub = df
            else:
                top = df.columns.get_level_values(0) if hasattr(df.columns, "get_level_values") else []
                if tic not in top:
                    continue
                sub = df[tic]
            if sub is None or sub.empty:
                continue
            close = sub["Close"].dropna()
            if len(close) == 0:
                continue
            price = float(close.iloc[-1])
            if price > 0:
                out[tic] = price
        except (KeyError, IndexError):
            continue
        except Exception as e:
            log.debug("%s price extract failed: %s", tic, e)
            continue
    return out


def fetch_prices_batch(tickers: list[str]) -> dict[str, float]:
    """Return {ticker: last_close}. Uses chunked downloads to keep RAM low."""
    if not tickers:
        return {}
    out: dict[str, float] = {}
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        out.update(_fetch_chunk(chunk))
    log.info("prices: %d/%d tickers resolved", len(out), len(tickers))
    return out
