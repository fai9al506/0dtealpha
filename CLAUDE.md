# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

0DTE Alpha is a real-time options trading dashboard for SPX/SPXW 0DTE (zero days to expiration) options. It combines:
- **FastAPI web service** (`app/main.py`) - serves live options chain data, charts, and a dashboard
- **Volland scraper worker** (`volland_worker_v2.py`) - Playwright route-based scraper for charm/vanna/gamma exposure data from vol.land
- **ES cumulative delta** (integrated in `app/main.py`) - scheduler job pulls ES 1-min bars from TradeStation API

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
- ES cumulative delta: `pull_es_delta()` runs every 30s, fetches @ES 1-min bars, calculates delta from UpVolume/DownVolume
- API endpoints: `/api/series`, `/api/snapshot`, `/api/history`, `/api/volland/*`, `/api/es/delta/*`

**volland_worker_v2.py** (Playwright scraper — ACTIVE):
- Logs into vol.land, intercepts network requests via Playwright route handlers
- Captures charm/vanna/gamma/deltaDecay exposure data from intercepted API responses
- Captures paradigm, LIS, aggregatedCharm, spot-vol-beta from response handlers
- Runs on 120-second cycle synced to Volland's refresh interval
- Sync phase at market open: reloads page to avoid stale overnight state, 2-min timeout

### Database Tables
- `chain_snapshots` - options chain data with Greeks
- `volland_snapshots` - raw scraped data with statistics (paradigm, LIS, charm, etc.)
- `volland_exposure_points` - parsed exposure points by strike (charm, vanna, gamma, deltaDecay)
- `es_delta_snapshots` - ES cumulative delta state (every 30s, from TradeStation @ES bars)
- `es_delta_bars` - ES 1-minute delta bars (UpVolume - DownVolume per bar)

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
```

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
- Options chain fetches use streaming endpoint with 5-second timeout, falls back to snapshot endpoint
- GEX calculation: `call_gex = gamma * OI * 100`, `put_gex = -gamma * OI * 100`
- Volland v2 uses Playwright `page.route()` to intercept exposure API calls (route handlers survive `page.goto()` navigations)
- Pipeline health: checks data freshness every 30s during market hours, sends Telegram on error/recovery
  - TS API: ok < 2min, stale < 5min, error >= 5min
  - Volland: ok < 3min, stale < 10min, error >= 10min

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
