"""HTML render — extracted from run.py so schedulers can call it directly."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

TEMPLATES = Path(__file__).parent / "templates"

SUSPECT_UPSIDE_HI = 500
SUSPECT_UPSIDE_LO = -100


def signal_of(upside: float | None) -> str:
    if upside is None:
        return "na"
    if upside > SUSPECT_UPSIDE_HI or upside < SUSPECT_UPSIDE_LO:
        return "suspect"
    if upside >= 30:
        return "buy"
    if upside <= -20:
        return "sell"
    return "hold"


def _fmt(v, prefix=""):
    if v is None:
        return '<span class="na">—</span>'
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(fv) >= 1000:
        return f"{prefix}{fv:,.0f}"
    return f"{prefix}{fv:,.2f}"


def _fmt_pct(v):
    if v is None:
        return '<span class="na">—</span>'
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.1f}%"


def render_html(rows: list[dict]) -> str:
    def _sort_key(r):
        order = {"buy": 0, "hold": 1, "sell": 2, "suspect": 3, "na": 4}
        return (order.get(r["signal"], 5), -(r["upside_pct"] or -9999))
    rows = sorted(rows, key=_sort_key)

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES)),
        autoescape=select_autoescape(["html"]),
    )
    env.globals["fmt"] = _fmt
    env.globals["fmt_pct"] = _fmt_pct

    buy = sum(1 for r in rows if r["signal"] == "buy")
    sell = sum(1 for r in rows if r["signal"] == "sell")
    computed = sum(1 for r in rows if r["status"] in ("OK", "SUSPECT"))
    skipped = len(rows) - computed

    return env.get_template("index.html.j2").render(
        rows=rows,
        updated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        total=len(rows),
        computed=computed,
        skipped=skipped,
        buy_count=buy,
        sell_count=sell,
    )
