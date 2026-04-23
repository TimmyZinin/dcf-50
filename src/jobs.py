"""Scheduler jobs: price_tick, daily_close_digest, weekly_sweep, earnings_sweep."""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import state, tg
from .dcf import compute
from .prices import fetch_prices_batch
from .render import render_html, signal_of
from .universe import all_tickers

log = logging.getLogger(__name__)


# ---------- helpers ----------

def _universe_map():
    return {t: (name, sector, region) for (t, name, sector, region) in all_tickers()}


def _us_tickers() -> list[tuple[str, str, str]]:
    return [(t, name, sector) for (t, name, sector, region) in all_tickers() if region == "US"]


async def _send_tg(text: str):
    if not text:
        return
    client = tg.TG()
    try:
        await client.send(text)
    finally:
        await client.close()


# ---------- price_tick: every 10 min during market hours ----------

def price_tick():
    log.info("== price_tick start ==")
    tickers = [t for (t, *_rest) in all_tickers()]
    fairs = state.load_fair_values()
    if not fairs:
        log.warning("price_tick: fair_values empty — run weekly_sweep first")
        return

    prices = fetch_prices_batch(tickers)
    log.info("fetched prices for %d/%d tickers", len(prices), len(tickers))
    umap = _universe_map()

    crossings = []
    for tic, price in prices.items():
        fv = fairs.get(tic)
        if not fv or fv.fair_base is None:
            continue
        upside = (fv.fair_base - price) / price * 100 if price > 0 else None
        new_sig = signal_of(upside)
        old_sig, _old_up = state.last_signal(tic)
        state.upsert_signal(tic, new_sig, upside, price)

        # alert only on entering BUY/SELL from a different zone
        if new_sig in ("buy", "sell") and old_sig != new_sig and old_sig is not None:
            if not state.recently_alerted(tic, f"edge_{new_sig}", hours=12):
                crossings.append((tic, old_sig, new_sig, upside, price, fv))

    log.info("price_tick: %d crossings", len(crossings))

    async def _post_all():
        for tic, old_sig, new_sig, upside, price, fv in crossings:
            text = tg.fmt_edge_alert(
                ticker=tic, old_sig=old_sig, new_sig=new_sig,
                upside=upside, price=price, fair_base=fv.fair_base,
                wacc=fv.wacc, computed_at=fv.computed_at,
            )
            try:
                await _send_tg(text)
                state.record_alert(tic, f"edge_{new_sig}", signal=new_sig, upside=upside)
                log.info("alert sent: %s %s (%.1f%%)", tic, new_sig.upper(), upside or 0)
            except Exception as e:
                log.error("alert post failed for %s: %s", tic, e)
    if crossings:
        asyncio.run(_post_all())
    log.info("== price_tick done ==")


# ---------- daily_close_digest: mon-fri 16:15 ET ----------

def daily_close_digest():
    log.info("== daily_close_digest start ==")
    alerts = state.recent_alerts(hours=24)
    new_buys: list[tuple[str, float | None]] = []
    new_sells: list[tuple[str, float | None]] = []
    back_to_hold: list[tuple[str, float | None]] = []

    for a in alerts:
        at = a["alert_type"]
        if at == "edge_buy":
            new_buys.append((a["ticker"], a["upside_pct"]))
        elif at == "edge_sell":
            new_sells.append((a["ticker"], a["upside_pct"]))
        elif at == "edge_hold":
            back_to_hold.append((a["ticker"], a["upside_pct"]))

    # dedupe by ticker, keep most extreme
    def _dedupe(items, pick_max=True):
        seen: dict[str, float | None] = {}
        for t, u in items:
            if t not in seen:
                seen[t] = u
            else:
                cur = seen[t] or 0
                uu = u or 0
                if pick_max and uu > cur:
                    seen[t] = u
                elif not pick_max and uu < cur:
                    seen[t] = u
        return sorted(seen.items(), key=lambda kv: -(kv[1] or 0) if pick_max else (kv[1] or 0))

    new_buys = _dedupe(new_buys, pick_max=True)
    new_sells = _dedupe(new_sells, pick_max=False)
    back_to_hold = list({t: u for t, u in back_to_hold}.items())

    date_str = datetime.now(timezone.utc).strftime("%d %b")
    text = tg.fmt_daily_close(new_buys, new_sells, back_to_hold, date_str)
    if not text:
        log.info("daily_close_digest: nothing to post")
        return
    asyncio.run(_send_tg(text))
    log.info("daily_close_digest: posted (%d BUY, %d SELL, %d HOLD)",
             len(new_buys), len(new_sells), len(back_to_hold))


# ---------- weekly_sweep: sun 04:00 UTC ----------

def weekly_sweep():
    log.info("== weekly_sweep start ==")
    from . import github_push

    tickers = all_tickers()
    rows = []
    computed = 0
    for i, (tic, name, sector, region) in enumerate(tickers, 1):
        log.info("[%d/%d] %s", i, len(tickers), tic)
        try:
            r = compute(tic)
        except Exception as e:
            log.error("%s failed: %s", tic, e)
            r = None

        if r is None or r.status != "OK":
            rows.append({
                "ticker": tic, "name": name, "sector": sector, "region": region,
                "price": r.price if r else None,
                "fair_low": None, "fair_base": None, "fair_high": None,
                "upside_pct": None, "wacc_pct": None,
                "pe": r.pe if r else None, "ev_ebitda": r.ev_ebitda if r else None,
                "ps": r.ps if r else None, "peg": r.peg if r else None,
                "status": (r.status if r else "ERR"),
                "note": (r.note if r else "fetch failed"),
                "signal": "na",
            })
            continue

        sig = signal_of(r.upside_pct)
        note = r.note or ""
        if sig == "suspect":
            note = (note + " " if note else "") + "extreme upside — data quality"

        rows.append({
            "ticker": tic, "name": name, "sector": sector, "region": region,
            "price": r.price, "fair_low": r.fair_low, "fair_base": r.fair_base,
            "fair_high": r.fair_high, "upside_pct": r.upside_pct,
            "wacc_pct": (r.wacc or 0) * 100 if r.wacc else None,
            "pe": r.pe, "ev_ebitda": r.ev_ebitda, "ps": r.ps, "peg": r.peg,
            "status": "OK" if sig != "suspect" else "SUSPECT",
            "note": note, "signal": sig,
        })

        if sig != "na":
            state.upsert_fair_value(state.FairValue(
                ticker=tic, fair_low=r.fair_low, fair_base=r.fair_base, fair_high=r.fair_high,
                wacc=r.wacc, fcf_avg=r.fcf_avg, shares=r.shares, net_debt=r.net_debt,
                computed_at=datetime.now(timezone.utc).isoformat(),
            ))
            computed += 1

    html = render_html(rows)
    local_path = Path(__file__).parent.parent / "docs" / "index.html"
    local_path.parent.mkdir(exist_ok=True)
    local_path.write_text(html, encoding="utf-8")
    log.info("wrote local %s", local_path)

    ok = github_push.put_file(
        "docs/index.html", html,
        message=f"weekly DCF sweep {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
    )
    log.info("github push: %s", "ok" if ok else "FAILED")

    # weekly digest
    buy_rows = [r for r in rows if r["signal"] == "buy"]
    sell_rows = [r for r in rows if r["signal"] == "sell"]
    buy_rows.sort(key=lambda r: -(r["upside_pct"] or 0))
    sell_rows.sort(key=lambda r: (r["upside_pct"] or 0))
    top_under = [(r["ticker"], r["name"], r["upside_pct"]) for r in buy_rows[:10]]
    top_over = [(r["ticker"], r["name"], r["upside_pct"]) for r in sell_rows[:5]]

    text = tg.fmt_weekly_digest(
        top_under, top_over,
        updated_at=datetime.now(timezone.utc).isoformat(),
        universe=len(tickers), computed=computed,
    )
    asyncio.run(_send_tg(text))
    log.info("== weekly_sweep done: %d computed ==", computed)


# ---------- earnings_sweep: tue/thu 22:00 UTC ----------

def earnings_sweep():
    log.info("== earnings_sweep start ==")
    from . import edgar

    us_set = {t for (t, *_rest) in _us_tickers()}
    umap = _universe_map()
    new_filings = edgar.find_new_filings(us_set)
    if not new_filings:
        log.info("no new filings")
        return

    for f in new_filings:
        tic = f["ticker"]
        old_fv = state.get_fair_value(tic)
        try:
            r = compute(tic)
        except Exception as e:
            log.error("%s recompute failed: %s", tic, e)
            state.record_filing(tic, f["accession"], f["type"], f.get("updated"))
            continue

        if r.status == "OK" and r.fair_base:
            new_sig = signal_of(r.upside_pct)
            if new_sig != "suspect":
                state.upsert_fair_value(state.FairValue(
                    ticker=tic, fair_low=r.fair_low, fair_base=r.fair_base, fair_high=r.fair_high,
                    wacc=r.wacc, fcf_avg=r.fcf_avg, shares=r.shares, net_debt=r.net_debt,
                    computed_at=datetime.now(timezone.utc).isoformat(),
                ))
            old_up = None
            if old_fv and old_fv.fair_base and r.price:
                old_up = (old_fv.fair_base - r.price) / r.price * 100

            text = tg.fmt_earnings_alert(
                ticker=tic,
                old_fair=old_fv.fair_base if old_fv else None,
                new_fair=r.fair_base,
                old_up=old_up,
                new_up=r.upside_pct,
                filing_type=f["type"],
                filing_url=f["link"],
            )
            asyncio.run(_send_tg(text))
            state.record_alert(tic, "earnings", signal=new_sig, upside=r.upside_pct)

        state.record_filing(tic, f["accession"], f["type"], f.get("updated"))
    log.info("== earnings_sweep done ==")


# ---------- CLI entry ----------

JOBS = {
    "price_tick": price_tick,
    "daily_close_digest": daily_close_digest,
    "weekly_sweep": weekly_sweep,
    "earnings_sweep": earnings_sweep,
    "bootstrap_weekly": weekly_sweep,  # alias
}


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    state.init_schema()
    if len(sys.argv) < 2:
        print(f"usage: python -m src.jobs <{'|'.join(JOBS)}>", file=sys.stderr)
        sys.exit(2)
    name = sys.argv[1]
    fn = JOBS.get(name)
    if not fn:
        print(f"unknown job: {name}", file=sys.stderr)
        sys.exit(2)
    fn()


if __name__ == "__main__":
    main()
