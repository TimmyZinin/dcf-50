"""DCF-50 main runner: compute all tickers, render HTML, save to docs/."""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

sys.path.insert(0, str(Path(__file__).parent))
from dcf import compute
from universe import all_tickers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("dcf50")

ROOT = Path(__file__).parent.parent
OUT = ROOT / "docs"
TEMPLATES = Path(__file__).parent / "templates"


def _signal(upside):
    if upside is None:
        return "na"
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


def run():
    tickers = all_tickers()
    log.info("universe: %d tickers", len(tickers))

    rows = []
    computed = 0
    skipped = 0
    buy = 0
    sell = 0

    for i, (tic, name, sector, region) in enumerate(tickers, 1):
        log.info("[%d/%d] %s …", i, len(tickers), tic)
        try:
            r = compute(tic)
        except Exception as e:
            log.error("%s failed: %s", tic, e)
            r = None

        if r is None or r.status != "OK":
            skipped += 1
            note = r.note if r else "fetch failed"
            status = r.status if r else "ERR"
            rows.append({
                "ticker": tic, "name": name, "sector": sector, "region": region,
                "price": r.price if r else None,
                "fair_low": None, "fair_base": None, "fair_high": None,
                "upside_pct": None, "wacc_pct": None,
                "pe": r.pe if r else None, "ev_ebitda": r.ev_ebitda if r else None,
                "ps": r.ps if r else None, "peg": r.peg if r else None,
                "status": status, "note": note, "signal": "na",
            })
            continue

        computed += 1
        sig = _signal(r.upside_pct)
        if sig == "buy":
            buy += 1
        elif sig == "sell":
            sell += 1

        rows.append({
            "ticker": tic, "name": name, "sector": sector, "region": region,
            "price": r.price, "fair_low": r.fair_low, "fair_base": r.fair_base,
            "fair_high": r.fair_high, "upside_pct": r.upside_pct,
            "wacc_pct": (r.wacc or 0) * 100 if r.wacc else None,
            "pe": r.pe, "ev_ebitda": r.ev_ebitda, "ps": r.ps, "peg": r.peg,
            "status": "OK", "note": "", "signal": sig,
        })

    # sort: buy signals first by upside desc, then ok, then sell, then na
    def _sort_key(r):
        order = {"buy": 0, "hold": 1, "sell": 2, "na": 3}
        return (order.get(r["signal"], 4), -(r["upside_pct"] or -9999))
    rows.sort(key=_sort_key)

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES)),
        autoescape=select_autoescape(["html"]),
    )
    env.globals["fmt"] = _fmt
    env.globals["fmt_pct"] = _fmt_pct

    tmpl = env.get_template("index.html.j2")
    html = tmpl.render(
        rows=rows,
        updated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        total=len(tickers),
        computed=computed,
        skipped=skipped,
        buy_count=buy,
        sell_count=sell,
    )

    OUT.mkdir(exist_ok=True)
    out_file = OUT / "index.html"
    out_file.write_text(html, encoding="utf-8")
    log.info("wrote %s (%d rows, computed=%d, skipped=%d, buy=%d, sell=%d)",
             out_file, len(rows), computed, skipped, buy, sell)

    # top picks summary for TG
    top_buys = [r for r in rows if r["signal"] == "buy"][:5]
    top_sells = [r for r in rows if r["signal"] == "sell"][:5]
    summary = {
        "total": len(tickers),
        "computed": computed,
        "skipped": skipped,
        "buy": buy,
        "sell": sell,
        "top_buys": top_buys,
        "top_sells": top_sells,
    }
    return summary


if __name__ == "__main__":
    run()
