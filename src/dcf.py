"""Two-stage DCF with CAPM-based WACC and sensitivity band."""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import numpy as np
import yfinance as yf

log = logging.getLogger(__name__)

RISK_FREE_FALLBACK = 0.043  # 10Y Treasury fallback
ERP = 0.055                  # equity risk premium (Damodaran-ish)
TERMINAL_GROWTH = 0.025
PROJECTION_YEARS = 5
GROWTH_CLAMP = (0.02, 0.15)
DEFAULT_TAX = 0.21


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _get_risk_free() -> float:
    try:
        t = yf.Ticker("^TNX")
        hist = t.history(period="5d")
        if len(hist) > 0:
            # ^TNX is 10Y yield *10 (e.g. 43 = 4.3%)
            return float(hist["Close"].iloc[-1]) / 100.0
    except Exception as e:
        log.warning("risk-free fetch failed: %s", e)
    return RISK_FREE_FALLBACK


@dataclass
class DCFResult:
    ticker: str
    price: float | None
    fair_low: float | None
    fair_base: float | None
    fair_high: float | None
    upside_pct: float | None
    wacc: float | None
    growth_used: float | None
    fcf_avg: float | None
    shares: float | None
    net_debt: float | None
    pe: float | None
    ev_ebitda: float | None
    ps: float | None
    peg: float | None
    market_cap: float | None
    status: str
    note: str = ""


def _fcf_series(cashflow) -> list[float]:
    if cashflow is None or cashflow.empty:
        return []
    idx_candidates = [
        "Free Cash Flow",
        "FreeCashFlow",
        "Operating Cash Flow",
    ]
    for k in idx_candidates:
        if k in cashflow.index:
            row = cashflow.loc[k].dropna()
            if len(row) > 0:
                return [float(v) for v in row.values if not math.isnan(float(v))]
    # fallback: OCF - CapEx
    try:
        ocf = cashflow.loc["Total Cash From Operating Activities"].dropna()
        capex = cashflow.loc["Capital Expenditures"].dropna()
        vals = []
        for col in ocf.index:
            if col in capex.index:
                v = float(ocf[col]) + float(capex[col])  # capex already negative
                if not math.isnan(v):
                    vals.append(v)
        return vals
    except Exception:
        return []


def _growth_rate(fcfs: list[float]) -> float | None:
    if len(fcfs) < 2:
        return None
    # newest first in yfinance — reverse to chronological
    series = list(reversed(fcfs))
    # use geometric mean of positive YoY; if first or last negative, fallback to arithmetic
    rates = []
    for i in range(1, len(series)):
        prev, curr = series[i - 1], series[i]
        if prev and prev != 0:
            rates.append((curr - prev) / abs(prev))
    if not rates:
        return None
    g = float(np.mean(rates))
    return _clamp(g, GROWTH_CLAMP[0], GROWTH_CLAMP[1])


def _wacc(beta: float, cost_of_debt: float, debt: float, equity: float, tax: float, rf: float) -> float:
    total = debt + equity
    if total <= 0:
        return rf + beta * ERP
    we = equity / total
    wd = debt / total
    ke = rf + beta * ERP
    kd = cost_of_debt * (1 - tax)
    return we * ke + wd * kd


def _two_stage_dcf(fcf0: float, g: float, wacc: float) -> float:
    if wacc <= TERMINAL_GROWTH:
        wacc = TERMINAL_GROWTH + 0.01
    pv = 0.0
    fcf = fcf0
    for t in range(1, PROJECTION_YEARS + 1):
        fcf = fcf * (1 + g)
        pv += fcf / (1 + wacc) ** t
    tv = fcf * (1 + TERMINAL_GROWTH) / (wacc - TERMINAL_GROWTH)
    pv += tv / (1 + wacc) ** PROJECTION_YEARS
    return pv


def compute(ticker: str) -> DCFResult:
    t = yf.Ticker(ticker)
    try:
        info = t.info or {}
    except Exception as e:
        log.warning("%s: info fetch failed: %s", ticker, e)
        info = {}

    price = info.get("currentPrice") or info.get("regularMarketPrice")
    shares = info.get("sharesOutstanding")
    beta = info.get("beta") or 1.0
    market_cap = info.get("marketCap")
    pe = info.get("trailingPE")
    ev_ebitda = info.get("enterpriseToEbitda")
    ps = info.get("priceToSalesTrailing12Months")
    peg = info.get("pegRatio") or info.get("trailingPegRatio")

    debt = info.get("totalDebt") or 0.0
    cash = info.get("totalCash") or 0.0
    net_debt = (debt or 0.0) - (cash or 0.0)

    try:
        cashflow = t.cashflow
    except Exception:
        cashflow = None

    fcfs = _fcf_series(cashflow)
    fcf_avg = float(np.mean(fcfs)) if fcfs else None

    if not fcfs or fcf_avg is None or fcf_avg <= 0:
        return DCFResult(
            ticker, price, None, None, None, None, None, None,
            fcf_avg, shares, net_debt, pe, ev_ebitda, ps, peg, market_cap,
            status="SKIP", note="negative/missing FCF"
        )

    g = _growth_rate(fcfs)
    if g is None:
        g = 0.05  # conservative default

    rf = _get_risk_free()
    equity = market_cap or ((price or 0) * (shares or 0))
    cost_of_debt = rf + 0.01
    wacc = _wacc(beta, cost_of_debt, debt, equity, DEFAULT_TAX, rf)

    # Base, low, high
    def fair_per_share(growth: float, wacc_v: float) -> float | None:
        try:
            ev = _two_stage_dcf(fcfs[0], growth, wacc_v)
            equity_val = ev - net_debt
            if not shares or shares <= 0:
                return None
            return equity_val / shares
        except Exception as e:
            log.warning("%s DCF calc failed: %s", ticker, e)
            return None

    fair_base = fair_per_share(g, wacc)
    fair_low = fair_per_share(g * 0.8, wacc + 0.01)
    fair_high = fair_per_share(g * 1.2, max(wacc - 0.01, TERMINAL_GROWTH + 0.01))

    upside = None
    if fair_base and price:
        upside = (fair_base - price) / price * 100

    return DCFResult(
        ticker, price, fair_low, fair_base, fair_high, upside,
        wacc, g, fcf_avg, shares, net_debt, pe, ev_ebitda, ps, peg, market_cap,
        status="OK"
    )
