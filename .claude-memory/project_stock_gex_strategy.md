---
name: Stock GEX Strategy - DEPLOYED & CALIBRATED
description: Stock GEX support bounce - 67 stocks, Mon-Wed 94% WR, +$2,354/month on $600 capital
type: project
---

## Stock GEX Support Bounce Strategy — FINAL (Real-Pricing Calibrated)

### STATUS: Deployed. Telegram connected. Ready for Monday.

### REALISTIC RETURNS (calibrated with optionstrat.com)
- **94% WR** (Mon-Wed only, A+/A grades)
- **+$2,354/month** on $600 capital
- **+$28,254/year**
- **~8 trades/month**, 1.8/week

### GRADING (trade A+/A, alert B/C)
| Grade | When | WR | Real Avg Winner | EV/trade | Action |
|-------|------|-----|----------------|----------|--------|
| A+ | Wed + ratio>3 | 96% | +243% | +$459 | TRADE |
| A | Mon-Wed + ratio>2 | 93% | +57-243% | +$61-459 | TRADE |
| B | Thu | 20% | +370% | -$12 | Alert only |
| C | Fri | 36% | +275% | +$70 | Alert only |

### CAPITAL
- Per trade: $200
- Capital needed: $600 (max 3 concurrent)
- Monthly deployed: ~$1,600
- Monthly ROI: +392%

### SETUP
- Same-day GEX (fresh OI each morning)
- Filters: ratio>2, support below, magnets above, spot above -GEX, skip 09:30
- Entry: OTM call at -GEX strike when stock hits -GEX minus 1%
- T1: -GEX recovery, T2: +GEX magnet
- Max loss = premium

### INFRASTRUCTURE
- Scanner: `app/stock_gex_live.py` (67 stocks)
- Dashboard: `/stock-gex-live`
- Telegram: -1003725132413
- Error alerts: API failures, crashes -> Telegram
- Data backup: `G:\My Drive\Python\MyProject\stock_gex_data_backup\`
- Railway env needed: `TELEGRAM_CHAT_ID_STOCK_GEX=-1003725132413`

### IMPORTANT: Option pricing correction
Our BS model estimates returns 5x higher than real. Always verify with actual bid/ask at entry.
Model says +1000% -> real is ~+200%. Model says +400% -> real is ~+80%.
WR is accurate (based on price action). Dollar returns are approximate.
