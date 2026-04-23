"""SEC EDGAR — fetch recent 10-Q / 10-K filings, match by CIK to our universe."""
from __future__ import annotations

import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)

USER_AGENT = os.environ.get("EDGAR_USER_AGENT", "DCF-50 Radar tim.zinin@gmail.com")
CACHE_PATH = Path(os.environ.get("EDGAR_TICKER_CACHE", "/tmp/edgar_tickers.json"))


def _load_ticker_cik_map(us_tickers: set[str]) -> dict[str, str]:
    """Return {TICKER: zero-padded CIK10} for US tickers in our universe."""
    if CACHE_PATH.exists() and CACHE_PATH.stat().st_size > 1000:
        import json
        try:
            data = json.loads(CACHE_PATH.read_text())
            return {t: data[t] for t in us_tickers if t in data}
        except Exception:
            pass

    try:
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": USER_AGENT},
            timeout=30,
        )
        r.raise_for_status()
        raw = r.json()
    except Exception as e:
        log.error("EDGAR ticker map fetch failed: %s", e)
        return {}

    out: dict[str, str] = {}
    for _, row in raw.items():
        tic = str(row.get("ticker", "")).upper()
        cik = str(row.get("cik_str", "")).zfill(10)
        if tic:
            out[tic] = cik

    try:
        import json
        CACHE_PATH.write_text(json.dumps(out))
    except Exception:
        pass

    return {t: out[t] for t in us_tickers if t in out}


def _fetch_recent_filings(filing_types=("10-K", "10-Q"), count: int = 100) -> list[dict]:
    """Fetch EDGAR recent filings atom feed. Returns list of {cik, accession, type, date, link}."""
    params = {
        "action": "getcurrent",
        "type": ",".join(filing_types),
        "output": "atom",
        "count": count,
    }
    try:
        r = requests.get(
            "https://www.sec.gov/cgi-bin/browse-edgar",
            params=params,
            headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"},
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        log.error("EDGAR recent fetch failed: %s", e)
        return []

    root = ET.fromstring(r.text)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    entries = []
    for entry in root.findall("a:entry", ns):
        title = (entry.findtext("a:title", default="", namespaces=ns) or "").strip()
        link_el = entry.find("a:link", ns)
        link = link_el.get("href") if link_el is not None else ""
        updated = entry.findtext("a:updated", default="", namespaces=ns)
        m_cik = re.search(r"\(CIK (\d{10})\)|\(\d+\)\s*\(CIK (\d{10})\)", title)
        # fallback — CIK in link path
        if not m_cik:
            m_cik = re.search(r"/data/(\d+)/", link)
        cik = ""
        if m_cik:
            cik = next((g for g in m_cik.groups() if g), "").zfill(10)
        acc = ""
        m_acc = re.search(r"(\d{10}-\d{2}-\d{6})", link + " " + title)
        if m_acc:
            acc = m_acc.group(1)
        ftype = ""
        m_ft = re.search(r"\b(10-[KQ])(?:/A)?\b", title)
        if m_ft:
            ftype = m_ft.group(1)
        if cik and acc and ftype:
            entries.append({
                "cik": cik,
                "accession": acc,
                "type": ftype,
                "updated": updated,
                "link": link,
            })
    return entries


def find_new_filings(us_tickers: set[str]) -> list[dict]:
    """Return filings from US universe not yet processed. Each item carries ticker."""
    from . import state

    tic_to_cik = _load_ticker_cik_map(us_tickers)
    cik_to_tic = {cik: tic for tic, cik in tic_to_cik.items()}
    log.info("EDGAR: %d US tickers mapped to CIKs", len(tic_to_cik))

    filings = _fetch_recent_filings()
    log.info("EDGAR: %d recent filings fetched", len(filings))

    new = []
    for f in filings:
        tic = cik_to_tic.get(f["cik"])
        if not tic:
            continue
        if state.filing_processed(tic, f["accession"]):
            continue
        f["ticker"] = tic
        new.append(f)
    log.info("EDGAR: %d new filings in universe", len(new))
    return new
