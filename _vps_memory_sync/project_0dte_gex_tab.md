---
name: 0DTE GEX Tab Implementation
description: Add 0DTE tab to Stock GEX Live dashboard for SPX/SPY/QQQ/IWM GEX dip bounce strategy
type: project
---

## 0DTE GEX Tab — Implementation Plan

### What to build
Add "0DTE" tab to Stock GEX Live dashboard (https://0dtealpha.com/stock-gex-live) for SPX/SPY/QQQ/IWM.

### Architecture
- **Backend:** Add SPX/SPY/QQQ/IWM to `app/stock_gex_live.py` scan logic
  - Pull 0DTE chains at market open (same as stocks but with same-day expiration)
  - Compute GEX levels: highest strike -GEX (first support wall) + first +GEX above
  - Monitor spot vs -GEX levels every 2 min
  - Alert on dip below -GEX
- **Frontend:** New "0DTE" tab in `app/stock_gex_live_page.py`
  - One page with all 4 symbols
  - Per symbol: chart, -GEX level, +GEX level, current spot, dip status
  - Trading log with same format as stocks
- **Telegram:** Same channel as stock GEX alerts

### Key files to modify
- `app/stock_gex_live_page.py` — add 0DTE tab HTML/JS, change logo
- `app/stock_gex_live.py` — add 0DTE scan logic + API getters
- `app/main.py` — add API endpoints for 0DTE data
- `Stock GEX Logo.jpg` — use as logo (replace the "G" gradient)

### Logo change
- Replace `<div class="logo">G</div>` with actual logo image
- File: `Stock GEX Logo.jpg` (purple V with pink x, dark background)
- Serve as static file or base64 embed
- No white background (already dark)

### 0DTE specific differences from stocks
- **Chains:** 0DTE expiration (same day), not weekly/opex
- **Symbols:** SPX uses SPXW root, others use own root
- **Strike interval:** SPX=$5, others=$1
- **Min dip:** SPX=10pts, SPY=1pt, QQQ=1pt, IWM=0.5pt
- **Trading:** Options (buy calls on dip), not stock
- **Timeframe:** Intraday only (9:30-16:00), all positions close EOD

### Strategy summary (from research)
- 75-80% T1 recovery rate (price recovers above -GEX)
- ~11 trades/week across 4 symbols
- Median recovery time: 10-20 min
- T1 = sell at -GEX recovery, T2 = hold for +GEX magnet
