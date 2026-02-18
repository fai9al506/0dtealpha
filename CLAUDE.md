# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Session Continuity Protocol (MANDATORY)

### On Session Start
1. **Read `PROJECT_BRAIN.md`** from memory directory — contains vision, ideas backlog, design decisions, pending items
2. **Read `SESSION_LOG.md`** from memory directory — contains what was done in recent sessions
3. **Scan `references/`** — glob all files, compare against `references/INDEX.md`. If new files found, read them and update the index with a short summary. This keeps knowledge current without the user having to notify.
4. You now have full context. Do NOT ask the user to explain what the system does or what was done before.
5. **Print a 3-5 line brief** to the user summarizing: what was done recently, what the current state is, and what the current priority is. Do NOT ask "what do you want to work on?" — just show you know where we are. The user will then tell you what to do.

### On Session End (when user says "bye", "done", "session end", "that's all", or similar)
1. **Update `SESSION_LOG.md`** — add entry for this session: what was done, decisions made, ideas discussed
2. **Update `MEMORY.md`** — if system state, priorities, or architecture changed
3. **Update `PROJECT_BRAIN.md`** — if new ideas, design decisions, or pending items emerged
4. **Update this CLAUDE.md** — if new features, components, or technical details were added to the codebase
5. **Confirm to the user**: print a short message like "Session logged. Files updated. See you next time." so the user knows it's safe to close the window. Do NOT close without confirming.

### After Any Code Changes
- Update the relevant sections of CLAUDE.md (architecture, features, technical details)
- Update MEMORY.md if intervals, tables, or key parameters changed

Memory directory: see the path in MEMORY.md header.

---

## Trading References Library (`references/`)

When answering trading questions, improving setup detection, or discussing strategy — check the `references/` folder for relevant documents before responding. Read on-demand, not every session.

| Folder | Contents |
|--------|----------|
| `references/volland/` | User guide, white paper, Discord community insights, charm/vanna/gamma interpretation |
| `references/gex/` | Gamma exposure studies, dealer hedging mechanics, GEX frameworks |
| `references/orderflow/` | Order flow, delta, CVD, absorption patterns, footprint charts |
| `references/general/` | Options Greeks, market microstructure, 0DTE strategies |

Supported formats: `.md`, `.txt`, `.pdf`, `.png`, `.jpg`

---

## Project Overview

0DTE Alpha is a real-time options trading dashboard for SPX/SPXW 0DTE (zero days to expiration) options. It combines:
- **FastAPI web service** (`app/main.py`) - serves live options chain data, charts, and a dashboard
- **Setup detector** (`app/setup_detector.py`) - scoring module for GEX Long, AG Short, BofA Scalp, ES Absorption, Paradigm Reversal, and DD Exhaustion (log-only) setups
- **Volland scraper worker** (`volland_worker_v2.py`) - Playwright route-based scraper for charm/vanna/gamma exposure data from vol.land
- **ES cumulative delta** (integrated in `app/main.py`) - scheduler job pulls ES 1-min bars from TradeStation API
- **ES quote stream** (integrated in `app/main.py`) - WebSocket stream builds bid/ask delta range bars from TradeStation ES quotes

## IMPORTANT: Volland Worker Versions (v1 vs v2)

**`volland_worker_v2.py` is the ACTIVE production worker. `volland_worker.py` (v1) is SUSPENDED/legacy.**

- **v2** (`volland_worker_v2.py`): Single all-in-one workspace, Playwright route-based capture. This is what runs in production.
- **v1** (`volland_worker.py`): Old two-workspace approach with JS injection hooks. **DO NOT run v1.** It remains in the repo for reference only.

When the user mentions "volland worker" or "volland not working", they mean **v2**.

### v2 Key Design

- Uses `page.route()` to intercept `**/api/v1/data/exposure` POST requests (route handlers persist across `page.goto()` navigations in Playwright)
- Uses `page.on("response")` to capture paradigm and spot-vol-beta GET endpoints
- Captures all 10 exposure types (charm, vanna x4, gamma x4, deltaDecay)
- Statistics from paradigm API response (not DOM scraping like v1)
- Synced to Volland's 120s refresh cycle via `lastModified` tracking
- Single vol.land session (avoids 3-device limit that v1 hit)

### v2 Known Issues & Fixes (2026-02-13)

**Overnight sync hang:** The sync phase waits for `lastModified` to change by polling the page. After overnight idle (~17 hours), the page goes stale and widgets stop auto-refreshing. Fix: reload workspace page (`page.goto()`) at the start of each sync phase + 2-minute timeout fallback.

**Pipeline health alert silent skip:** The `api_data_freshness()` query defaults to `status: "closed"`. If the query fails or returns no rows during market hours, `check_pipeline_health()` was silently skipping (treating it as market closed). Fix: treat `"closed"` during market hours as `"error"` and send Telegram alert.

### v2 Features Added (2026-02-14)

- **0-points Telegram alert:** Tracks `_total_pts == 0` (not per-exposure). If total points = 0 for 3 consecutive cycles during market hours (9:30-16:00 ET), sends alert. Uses `is_market_hours()` (9:30-16:00) separate from `market_open_now()` (9:20-16:10).
- **Auto browser restart:** After 5 consecutive 0-point cycles (~10 min) during market hours, kills browser, launches fresh one, re-logs in, forces re-sync, sends Telegram. Recovers from stale sessions (e.g., after long weekends).
- **Auto re-login on session expiry:** Error handler checks `page.url` for `/sign-in`, calls `login_if_needed()`, sends Telegram alert on failure during market hours.
- **Telegram integration:** Volland service has its own `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` env vars on Railway for direct alerts.

## CRITICAL WARNING: volland_worker_v2.py

**DO NOT MODIFY `volland_worker_v2.py` WITHOUT EXTREME CAUTION.**

This file scrapes a third-party website using Playwright with carefully tuned:
- Login flow timing and selectors
- Route-based network interception for exposure data
- Response handler for paradigm and spot-vol-beta endpoints
- Session handling and modal dismissal

**Any changes can silently break the entire Charm/Vanna data pipeline.** The scraper may appear to run but produce no data, or worse, corrupt the database with malformed entries.

**When adding new features:**
- Create separate components/files rather than modifying volland_worker_v2.py
- If changes to the worker are unavoidable, test extensively in isolation first
- Monitor the volland_snapshots table after deployment to verify data is still flowing

## Repo Structure

- `app/` — production code (main.py, setup_detector.py) — **this is the main codebase**
- `volland_worker_v2.py` — **ACTIVE** Playwright scraper (see warning above)
- `volland_worker.py` — **LEGACY/SUSPENDED** v1 scraper (do NOT run)
- `0dtealpha/` — git submodule (separate repo, NOT the main codebase)
- `trade-analyses.md` — running log of trade performance analysis and tuning decisions

## Trade Analysis

When analysing trading setups or reviewing outcomes, **always refer to `trade-analyses.md`**. This file contains:
- Historical performance data and win/loss breakdowns
- Volland metric observations (DD hedging, charm, paradigm shifts)
- Proposed and implemented tuning changes with before/after comparisons
- Pending improvements waiting for more data validation

Append new analysis sections to this file after each review session.

## Architecture

### Data Flow
1. TradeStation API → FastAPI app → PostgreSQL (chain_snapshots table)
2. Volland website → `volland_worker_v2.py` (Playwright) → PostgreSQL (volland_snapshots, volland_exposure_points tables)
3. TradeStation API → `pull_es_delta()` scheduler job → PostgreSQL (es_delta_snapshots, es_delta_bars tables)
4. PostgreSQL → FastAPI endpoints → Plotly.js dashboard

### Key Components

**app/main.py** (FastAPI web service):
- Background scheduler pulls SPX options chain every 30 seconds during market hours (9:30-16:00 ET)
- Saves snapshots to PostgreSQL every 2 minutes
- Calculates GEX (Gamma Exposure) from options chain data
- Serves dashboard with embedded Plotly.js charts at `/`
- Pipeline health monitoring: sends Telegram alerts when data sources go stale
- 401 Telegram alert: `_alert_401()` with 5-min cooldown for persistent TradeStation auth failures
- `/api/health` endpoint: component-level freshness (chain, volland, ES delta stream) with overall status
- ES cumulative delta: `pull_es_delta()` runs every 30s, fetches @ES 1-min bars, calculates delta from UpVolume/DownVolume
- ES quote stream: WebSocket connection to TradeStation, builds 5-pt range bars with bid/ask delta, CVD OHLC
- ES absorption detector: `_run_absorption_detection()` evaluates swing-based CVD divergence on range bars (see setup_detector.py)
- Thread safety: `_es_delta_lock` and `_es_quote_lock` protect shared ES state from concurrent access
- Dashboard auto-refresh: per-tab polling with `Plotly.react()` (no page reload), tab persistence via `sessionStorage`
- Setup cooldown persistence: saves to `setup_cooldowns` DB table, restored on startup
- Admin password from `ADMIN_PASSWORD` env var (not hardcoded)
- API endpoints: `/api/series`, `/api/snapshot`, `/api/history`, `/api/volland/*`, `/api/es/delta/*`, `/api/es/delta/rangebars`, `/api/health`

**app/setup_detector.py** (Setup scoring module):
- Self-contained module — receives all data as parameters, no imports from main.py
- **GEX Long**: Scores support proximity, upside range, floor cluster, target cluster, risk/reward
- **AG Short**: Bearish counterpart to GEX Long
- **BofA Scalp**: LIS-based scalp with charm/stability/width scoring
- **ES Absorption** (swing-based, rewritten 2026-02-15): See "ES Absorption Detector" section below
- **DD Exhaustion** (log-only, added 2026-02-18): See "DD Exhaustion Detector" section below
- Cooldown persistence: `export_cooldowns()` / `import_cooldowns()` serialize state to/from DB

**volland_worker_v2.py** (Playwright scraper — ACTIVE):
- Logs into vol.land, intercepts network requests via Playwright route handlers
- Captures charm/vanna/gamma/deltaDecay exposure data from intercepted API responses
- Captures paradigm, LIS, aggregatedCharm, spot-vol-beta from response handlers
- Runs on 120-second cycle synced to Volland's refresh interval
- Sync phase at market open: reloads page to avoid stale overnight state, 2-min timeout
- Per-exposure 0-points alert (3 consecutive cycles during market hours)
- Auto browser restart after 5 consecutive 0-point cycles
- Auto re-login on session expiry

### ES Absorption Detector (Swing-Based CVD Divergence)

Detects passive buyer/seller absorption by comparing CVD at current bar vs historical swing points. Rewritten 2026-02-15 to replace the old slope-based lookback window approach.

**Architecture (3 components):**

1. **Swing Tracker** (`_update_swings`, `_add_swing`):
   - Pivot detection: left=2, right=2, using `<=` for lows and `>=` for highs (not strict)
   - Alternating enforcement: L-H-L-H — after a low, next must be a high and vice versa
   - Adaptive invalidation: lower low replaces previous swing low, higher high replaces previous swing high
   - State persists across calls within a session (`_swing_tracker` dict)

2. **Volume Trigger**: Fire only when trigger bar volume >= 1.4x of 10-bar rolling average. Only the trigger bar needs elevated volume; swing reference bars don't.

3. **Divergence Scan** (`evaluate_absorption`):
   - Bullish: `swing.low <= trigger.low AND trigger.cvd < swing.cvd` (price holding, CVD dropping = passive buyers absorbing)
   - Bearish: `swing.high >= trigger.high AND trigger.cvd > swing.cvd` (price failing, CVD rising = passive sellers absorbing)
   - CVD gap scored as z-score: `cvd_gap / rolling_std_dev(bar-to-bar CVD changes, 20 bars)`
   - Price distance scored as ATR multiple: `price_dist / avg(|close-to-close|, 20 bars)`
   - **Detection-first**: fires on ALL divergences with z >= 0.5 (no grade-based suppression)
   - Grade defaults to "C" if composite score below thresholds (never returns None for qualifying divergences)
   - Logs ALL confirming swings with full breakdown (cvd_z, price_atr, score)
   - Best swing by score for primary display and Telegram

**Key settings** (tunable via dashboard admin panel):
- `abs_pivot_left/right`: 2 (pivot neighbor count)
- `abs_min_vol_ratio`: 1.4 (volume trigger threshold)
- `abs_cvd_z_min`: 0.5 (minimum z-score to fire)
- `abs_cvd_std_window`: 20 (rolling window for CVD std dev)
- `abs_vol_window`: 10 (rolling average for volume gate)

### DD Exhaustion Detector (Log-Only Mode)

Detects DD-Charm divergence as a contrarian exhaustion signal. Based on Analysis #3 backtest (24 trades, 58% WR, +54.2 pts, PF 1.55x over Feb 11-17).

**Signal logic:**
- LONG: DD shifts bearish (< -$200M) while charm stays positive → dealers over-hedged, price bounces
- SHORT: DD shifts bullish (> +$200M) while charm stays negative → dealers over-positioned, price fades

**Data flow:**
- Volland API string (e.g. "$7,298,110,681") → `_parse_dd_numeric()` in main.py → numeric value
- `update_dd_tracker()` computes shift (current - previous cycle) with daily reset
- `evaluate_dd_exhaustion()` checks signal + time window (10:00-15:30 ET)

**Key settings** (DEFAULT_DD_EXHAUST_SETTINGS):
- `dd_shift_threshold`: $200M minimum shift to trigger
- `dd_cooldown_minutes`: 30 (per direction)
- `dd_target_pts`: 10, `dd_stop_pts`: 20
- `dd_market_start`: "10:00", `dd_market_end`: "15:30"

**Log-only mode:** Grade always "LOG", score always 0. Telegram messages tagged `[LOG-ONLY]`. Target: 50+ live signals before enabling as real setup.

### Database Tables
- `chain_snapshots` - options chain data with Greeks
- `volland_snapshots` - raw scraped data with statistics (paradigm, LIS, charm, etc.)
- `volland_exposure_points` - parsed exposure points by strike (charm, vanna, gamma, deltaDecay)
- `es_delta_snapshots` - ES cumulative delta state (every 30s, from TradeStation @ES bars)
- `es_delta_bars` - ES 1-minute delta bars (UpVolume - DownVolume per bar)
- `setup_cooldowns` - persisted cooldown state (trade_date, JSONB state including swing tracker)

## Railway Deployment

Deployed on Railway using Docker. The Dockerfile uses the official Playwright image.

### Railway Services

There are **2 separate Railway services** in the `0dte` project:

| Service Name | Start Command | Notes |
|-------------|---------------|-------|
| `0dtealpha` | `uvicorn app.main:app --host 0.0.0.0 --port $PORT` | Web service + ES delta scheduler (via Procfile `web`) |
| `Volland` | `python volland_worker_v2.py` | **Separate Railway service** (NOT in Procfile) |

**Important:** The Volland worker runs as its own Railway service named `Volland` (capital V). It is NOT in the Procfile. To manage it:

```bash
# Check Volland logs
railway logs -s Volland --lines 30

# Restart Volland (e.g., if stuck in sync)
railway restart -s Volland --yes

# Check web service logs
railway logs -s 0dtealpha --lines 30
```

### Procfile

```bash
web: uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

The Volland worker is **not** in the Procfile — it is a separate Railway service.
ES cumulative delta runs as a scheduler job inside the web process (no separate worker).

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (required for volland_worker_v2)
playwright install chromium

# Run the FastAPI web server locally
uvicorn app.main:app --host 0.0.0.0 --port 8080

# Run the Volland v2 scraper worker
python volland_worker_v2.py

```

## Required Environment Variables

```
# TradeStation API (for options chain data)
TS_CLIENT_ID
TS_CLIENT_SECRET
TS_REFRESH_TOKEN

# PostgreSQL connection
DATABASE_URL

# Volland scraper credentials
VOLLAND_EMAIL
VOLLAND_PASSWORD
VOLLAND_URL              # Charm workspace URL (v2 uses VOLLAND_WORKSPACE_URL with fallback to this)
VOLLAND_WORKSPACE_URL    # v2 all-in-one workspace URL (preferred)

# Telegram alerts
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID           # General alerts (pipeline health, LIS, paradigm)
TELEGRAM_CHAT_ID_SETUPS    # Setup detector alerts

# Admin
ADMIN_PASSWORD             # Dashboard admin panel password (default: "changeme")
```

**Note:** The Volland Railway service also has `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` set separately for its own 0-points and session expiry alerts.

## Full Backups

When the user says **"make full backup"**:
1. `git add -A` all files
2. Commit with message `BACKUP: <description>`
3. Create a tag: `stable-YYYYMMDD-HHMMSS`
4. Add the tag to the list in `Backup_tags.md` (with sequence number, date/time, tag name, and notes)
5. Show the tag name when done

See `Backup_tags.md` for the full list of backup tags.

## Key Technical Details

- Market hours check: `dtime(9, 30) <= t.time() <= dtime(16, 0)` in US/Eastern timezone
- Volland worker market hours: `dtime(9, 20) <= t.time() <= dtime(16, 10)` (slightly wider for pre/post scraping)
- Volland worker `is_market_hours()`: `dtime(9, 30) <= t.time() <= dtime(16, 0)` (strict, for 0-points alerts)
- Options chain fetches use streaming endpoint with 5-second timeout, falls back to snapshot endpoint
- GEX calculation: `call_gex = gamma * OI * 100`, `put_gex = -gamma * OI * 100`
- Volland v2 uses Playwright `page.route()` to intercept exposure API calls (route handlers survive `page.goto()` navigations)
- ES quote stream: WebSocket to TradeStation, builds 5-pt range bars with bid/ask delta. Bars have `{idx, open, high, low, close, volume, delta, buy_volume, sell_volume, cvd, cvd_open, cvd_high, cvd_low, cvd_close, ts_start, ts_end, status}`
- ES absorption: swing-based CVD divergence detector runs on each new completed range bar (see "ES Absorption Detector" section)
- Thread safety: `_es_delta_lock` for ES 1-min delta state, `_es_quote_lock` for ES quote stream range bars
- Dashboard: no page reload — uses per-tab polling timers with `Plotly.react()`, tab persisted via `sessionStorage`
- Setup cooldowns: saved to DB after each evaluation via `setup_cooldowns` table (JSONB), loaded on startup
- Charm thresholds (setup_detector.py): calibrated to actual data — brackets are [50M, 100M, 250M, 500M] (not the original [500, 2K, 5K, 10K])
- Pipeline health: checks data freshness every 30s during market hours, sends Telegram on error/recovery
  - TS API: ok < 2min, stale < 5min, error >= 5min
  - Volland: ok < 3min, stale < 10min, error >= 10min
- 401 alert: `_alert_401()` with 5-min cooldown, wired into `api_get()`, ES delta stream, ES quote stream

## Troubleshooting

### Volland not updating (dashboard shows stale Vol timestamp)

1. **Check logs:** `railway logs -s Volland --lines 30`
2. **If stuck in sync** (only see `[sync] Waiting for Volland refresh...`): Restart with `railway restart -s Volland --yes`
3. **If 0 pts captured** (see `exposure: charm/TODAY (0 pts)` repeatedly): vol.land data may not be available yet (early morning) or session expired — restart the service
4. **If login errors**: Check VOLLAND_EMAIL/VOLLAND_PASSWORD env vars on Railway
5. **After restart**: Verify with `railway logs -s Volland --lines 10 --filter "saved"` — should see `[volland-v2] saved ... exposures=10 points=XXXX`

### Pipeline Telegram alerts not firing

- `check_pipeline_health()` runs in the `finally` block of `run_market_job()` every 30s
- Logs are prefixed with `[pipeline]` — check with `railway logs -s 0dtealpha --filter "pipeline"`
- If freshness query fails, status defaults to `"closed"` which is now treated as error during market hours
- Verify Telegram works: check for `[telegram] sent:` in logs
