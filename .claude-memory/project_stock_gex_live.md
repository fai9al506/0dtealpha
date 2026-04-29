---
name: Stock GEX Live Scanner
description: Stock GEX live scanner page - architecture, bugs fixed, design decisions, current state
type: project
---

## Stock GEX Live Scanner — Built & Launched 2026-03-23/24

### Architecture
- `app/stock_gex_live.py` — backend: 56 stocks, 30-min GEX scan + 2-min spot monitor
- `app/stock_gex_live_page.py` — frontend: Unusual Whales-inspired dark theme, Inter font
- Route: `/stock-gex-live` (auth required)
- API: `/api/stock-gex-live/{watchlist,active,trades,levels,status,scan}`
- DB tables: `stock_gex_live_levels`, `stock_gex_live_trades`
- Startup scan runs regardless of market hours (uses last-close prices)
- DB levels loaded on init so page works after hours

### Strategy
- Buy OTM weekly call at -GEX strike when stock dips 1% below -GEX support
- Targets: T1 = -GEX recovery, T2 = +GEX magnet
- Filters: ratio >= 2, support below, magnets above (spot position no longer excluded)

### Key Bugs Fixed (2026-03-23)
1. **JS syntax error** — Python `\'` vs `\\'` in triple-quoted string killed entire script
2. **API path missing `/`** — `marketdata/quotes/...` instead of `/marketdata/quotes/...` → 404
3. **api_get returns Response, not JSON** — wrongly assumed r.json() return, broke .json() calls
4. **TS snapshot endpoint 404 for stocks** — switched to streaming `/marketdata/stream/options/chains/{symbol}`
5. **strikeInterval must be integer** — 2.5 rejected by TS API
6. **Stream hangs after hours** — added 3s read timeout
7. **Float prices from TS API** — `Last` comes as string, needed float() cast
8. **highest_neg >= lowest_pos gate** — killed stocks with $1 strike intervals (BAC, ROKU, UBER etc)
9. **DB migration crashed init** — ALTER TABLE in same transaction poisoned everything
10. **Option quote 404** — `_fetch_option_quote` still used snapshot endpoint

### Design Decisions
- **GEX is NET** (call + put per strike), not separate
- **Strike display uses decimals** when needed ($49 vs $49.5) via `fmtK()` helper
- **Chart shows ALL non-zero GEX strikes** (not just top 3)
- **Structure score**: CLEAN (>=70%) / MIXED (30-69%) / MESSY (<30%) — measures zone separation
- **Weak GEX ignored** in structure calc — only strikes >= 10% of max count
- **Auto-refresh every 2 min** (matches spot monitor, was 30s)
- **All timestamps in ET**
- **Telegram only for trades and errors** (removed 30-min scan summary)

### Stocks: 56 (as of Mar 24)
Removed: SQ (dead ticker→XYZ), AMC ($1 penny), SNAP ($4.54 too cheap), MARA ($9 crypto), GME (meme), ABNB/LRCX/AI/WBD/LCID/JNJ/AFRM (low options volume <10K)

### User Preferences for This Page
- Prefers CLEAN GEX structure (all -GEX below, all +GEX above). MESSY = skip.
- Wants to see "Spot vs -GEX" distance, not "Zone Width"
- Wants sort/filter on All Levels tab
- Font should be light weight (400), not bold. Compact layout.
- Multi-day hold possible (weekly expiry), not just intraday
