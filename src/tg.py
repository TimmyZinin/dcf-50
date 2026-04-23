"""Telethon poster for the 'Альфа от Ходлера' channel."""
from __future__ import annotations

import logging
import os

from telethon import TelegramClient
from telethon.sessions import StringSession

log = logging.getLogger(__name__)

API_ID = int(os.environ.get("TELEGRAM_API_ID", "0"))
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING", "")
CHANNEL_ID = int(os.environ.get("TG_CHANNEL_ID", "0"))


class TG:
    def __init__(self):
        if not (API_ID and API_HASH and SESSION_STRING and CHANNEL_ID):
            raise RuntimeError("TELEGRAM_* env vars not configured")
        self._client: TelegramClient | None = None

    async def _connect(self) -> TelegramClient:
        if self._client is None:
            self._client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
            await self._client.connect()
            if not await self._client.is_user_authorized():
                raise RuntimeError("Telethon session not authorized — regenerate TELEGRAM_SESSION_STRING")
        return self._client

    async def send(self, text: str):
        cli = await self._connect()
        await cli.send_message(CHANNEL_ID, text, link_preview=False)

    async def close(self):
        if self._client:
            await self._client.disconnect()
            self._client = None


def _fmt_pct(v):
    if v is None:
        return "—"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.1f}%"


def _fmt_money(v):
    if v is None:
        return "—"
    if abs(v) >= 1000:
        return f"${v:,.0f}"
    return f"${v:,.2f}"


def fmt_edge_alert(ticker: str, old_sig: str | None, new_sig: str,
                   upside: float, price: float, fair_base: float,
                   wacc: float | None, computed_at: str) -> str:
    emoji = "🟢" if new_sig == "buy" else "🔴"
    label = "BUY" if new_sig == "buy" else "SELL"
    was = (old_sig or "new").upper()
    wacc_str = f"{wacc*100:.1f}%" if wacc else "—"
    return (
        f"{emoji} {label} signal: {ticker}  (was {was})\n"
        f"Price {_fmt_money(price)} · Fair {_fmt_money(fair_base)} · upside {_fmt_pct(upside)}\n"
        f"WACC {wacc_str} · last DCF {computed_at[:10]}"
    )


def fmt_daily_close(new_buys: list, new_sells: list, back_to_hold: list, date_str: str) -> str:
    lines = [f"📊 Daily close · {date_str}", ""]
    if new_buys:
        lines.append(f"🟢 New BUY ({len(new_buys)}):")
        for t, up in new_buys[:10]:
            lines.append(f"  {t}  {_fmt_pct(up)}")
        lines.append("")
    if new_sells:
        lines.append(f"🔴 New SELL ({len(new_sells)}):")
        for t, up in new_sells[:10]:
            lines.append(f"  {t}  {_fmt_pct(up)}")
        lines.append("")
    if back_to_hold:
        lines.append(f"⚪ Back to HOLD: {', '.join(t for t, _ in back_to_hold[:15])}")
        lines.append("")
    if not (new_buys or new_sells or back_to_hold):
        return ""  # no movement — don't post
    lines.append("Dashboard: https://timzinin.com/dcf-50/")
    return "\n".join(lines)


def fmt_weekly_digest(top_under: list, top_over: list, updated_at: str, universe: int, computed: int) -> str:
    lines = [f"📊 Weekly DCF digest · {updated_at[:10]}", "",
             f"Universe {universe} · computed {computed}", ""]
    if top_under:
        lines.append("Top undervalued (base case):")
        for i, (t, name, up) in enumerate(top_under[:10], 1):
            lines.append(f"{i}. {t}  {_fmt_pct(up)}  ({name})")
        lines.append("")
    if top_over:
        lines.append("Top overvalued:")
        for i, (t, name, up) in enumerate(top_over[:5], 1):
            lines.append(f"{i}. {t}  {_fmt_pct(up)}  ({name})")
        lines.append("")
    lines.append("Full dashboard: https://timzinin.com/dcf-50/")
    return "\n".join(lines)


def fmt_earnings_alert(ticker: str, old_fair: float | None, new_fair: float | None,
                       old_up: float | None, new_up: float | None, filing_type: str, filing_url: str) -> str:
    return (
        f"📄 {ticker} · {filing_type} filed\n"
        f"Fair (base): {_fmt_money(old_fair)} → {_fmt_money(new_fair)}\n"
        f"Upside: {_fmt_pct(old_up)} → {_fmt_pct(new_up)}\n"
        f"{filing_url}"
    )
