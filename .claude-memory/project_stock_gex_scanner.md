---
name: Stock GEX Scanner
description: Independent weekly GEX scanner for ~23 stocks — scans Monday, monitors price vs levels during week, alerts on support/magnet setups
type: project
---

## Stock GEX Scanner (added 2026-03-21)

Completely independent system from 0DTE SPX pipeline. Lives in `app/stock_gex_scanner.py`.

**Origin:** User previously scanned stocks manually using an Unusual Whales scraper (Selenium, pixel heights from SVG charts). Now automated with real TS API chain data.

**Why:** User's manual GEX stock trades were highly profitable (LULU 300%, NVDA 500% weekly options) but too time-consuming to scan 23 stocks weekly. Automation solves the scaling problem.

**How to apply:** This is a separate system — never mix with 0DTE SPX logic. Changes to the scanner should never affect SPX pipeline. Module receives `api_get` (not `ts_access_token`) for chain fetching.

**Status:** Data collection mode. No alerts, no signals, no Telegram. Collecting data for future backtesting.

**Key files:**
- `app/stock_gex_scanner.py` — self-contained module
- DB table: `stock_gex_scans` (exp_label column: 'weekly' or 'opex')
- API: `/api/stock-gex/levels`, `/api/stock-gex/detail`, `/api/stock-gex/history`, `/api/stock-gex/status`, `/api/stock-gex/scan`

**Two expirations per stock:**
- `weekly` — this week's nearest Friday
- `opex` — nearest 3rd Friday of month (monthly OpEx)
- During OpEx week, they merge into single 'opex' entry (no duplicate)
- Expirations cached per symbol per day (avoid redundant API calls)

**Next steps:**
- Deploy and collect Monday scans
- Validate GEX levels against manual LULU/NVDA examples
- Build dashboard tab to visualize stock GEX levels
- Backtest signal accuracy across weeks of data
- Consider multi-expiration aggregation for fuller GEX picture

**User's old stock list:** SNOW, AAPL, NVDA, BA, AMZN, AMD, BABA, ENPH, META, MSFT, NFLX, PYPL, QCOM, TSLA, SHOP, COST, AVGO, GOOGL, SMCI, ROKU, LULU, RBLX, JNJ
**Old script:** `G:\My Drive\Investment\Python\GEX for stocks.py`
**Old manual logs:** `G:\My Drive\Investment\GEX - Monday for all stocks\`
