# DCF-50

Automated two-stage DCF fair-value radar for Top-50 US + Top-50 China ADR equities.

**Live:** https://timzinin.com/dcf-50/

## What it does

- Pulls fundamentals (FCF, shares, debt, beta) from yfinance
- Computes two-stage DCF (5Y projection + terminal g=2.5%) with CAPM-based WACC
- Produces low/base/high sensitivity band (growth ±20%, WACC ±1%)
- Renders a sortable, filterable HTML dashboard (`docs/index.html`)
- Deployed via GitHub Pages, auto-refreshed weekly

## Run locally

```bash
pip install -r requirements.txt
python src/run.py
open docs/index.html
```

## Stack

- Python 3.11+
- yfinance, pandas, numpy, Jinja2
- GitHub Pages for hosting

## Methodology

**Fair value per share:**
```
Σ FCF[t] · (1+g)^t / (1+WACC)^t + TV/(1+WACC)^5 − NetDebt
─────────────────────────────────────────────────────────
                    Shares Outstanding
```

- `g` — clamped average YoY FCF growth of last 5Y (2%–15%)
- `g_term` — 2.5% (long-term US inflation proxy)
- `WACC` — CAPM: rf (10Y T-Note) + β · 5.5% ERP, weighted with after-tax cost of debt

**Signals:**
- BUY: upside ≥ +30%
- SELL: upside ≤ −20%

## Caveats

- Negative/missing FCF → skipped (not a DCF candidate)
- Chinese ADRs have VIE-structure risk not discounted here
- Not investment advice.

## License

MIT
