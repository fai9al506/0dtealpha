---
name: Stock GEX Live Implementation Plan
description: Implementation plan for the stock GEX support bounce scanner, alerting, and dashboard
type: project
---

## Implementation Plan

### Module: `app/stock_gex_live.py` (core scanner)
Self-contained module. Receives `engine`, `api_get`, `send_telegram` via `init()`.

**Two scheduler jobs:**
1. `run_gex_scan()` — every 30 min during market hours (9:30-16:00 ET)
   - Fetch chain for all 52 stocks via TS API (batch quotes + per-stock chain)
   - Compute GEX per stock (OI x gamma, BS gamma from IV)
   - Identify levels: top 3 +GEX, top 3 -GEX
   - Apply filters: ratio>3, support below, magnet above, spot above -GEX
   - Output: watchlist with levels + limit prices
   - Save to DB table `stock_gex_levels`

2. `run_spot_monitor()` — every 1-2 min during market hours
   - Only monitors watchlist stocks (from last scan)
   - Fetch batch stock quotes via TS API
   - Check: did any stock hit -GEX -1%?
   - On trigger: fetch option quote for specific strike, log entry, send Telegram
   - After entry: monitor for T1/T2 exits
   - On exit: fetch option quote, log exit, send Telegram

**State tracking:**
- `_watchlist`: dict of {stock: {levels, filter_results}} from last scan
- `_active_trades`: list of open positions with entry details
- `_trade_log`: completed trades with full PNL

**DB tables:**
- `stock_gex_levels` — current GEX levels per stock (updated every 30 min)
- `stock_gex_trades` — trade log (entry, exit, option prices, greeks, PNL)

### Dashboard: `app/stock_gex_live_page.py`
New page at `/stock-gex-live` with tabs:
1. **Watchlist** — stocks passing filters today, with levels + distance to trigger
2. **Active Trades** — currently open positions with live PNL
3. **Trade Log** — historical trades with full details
4. **GEX Chart** — per-stock GEX bar chart (like 0DTE exposure chart)

### Telegram (new channel)
- `TELEGRAM_CHAT_ID_STOCK_GEX` env var
- Entry alert: stock, strike, limit price, delta, premium, T1/T2 targets
- Exit alert: stock, exit reason, option PNL, hold time
- EOD summary: trades today, wins/losses, total PNL

### Integration in main.py
- `init()` call at startup
- Two scheduler jobs
- API endpoints for dashboard
- Minimal touchpoints (same pattern as stock_gex_scanner.py)
