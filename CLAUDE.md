# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

0DTE Alpha is a real-time options trading dashboard for SPX/SPXW 0DTE (zero days to expiration) options. It combines:
- **FastAPI web service** (`app/main.py`) - serves live options chain data, charts, and a dashboard
- **Volland scraper worker** (`volland_worker.py`) - headless browser scraper for charm/vanna exposure data from vol.land

## CRITICAL WARNING: volland_worker.py

**DO NOT MODIFY `volland_worker.py` WITHOUT EXTREME CAUTION.**

This file is extremely fragile. It scrapes a third-party website using Playwright with carefully tuned:
- Login flow timing and selectors
- JavaScript injection hooks for network interception
- Response parsing logic that depends on exact API response structures
- Session handling and modal dismissal

**Any changes can silently break the entire Charm/Vanna data pipeline.** The scraper may appear to run but produce no data, or worse, corrupt the database with malformed entries.

**When adding new features:**
- Create separate components/files rather than modifying volland_worker.py
- If changes to the worker are unavoidable, test extensively in isolation first
- Monitor the volland_snapshots table after deployment to verify data is still flowing

## Architecture

### Data Flow
1. TradeStation API → FastAPI app → PostgreSQL (chain_snapshots table)
2. Volland website → Playwright scraper → PostgreSQL (volland_snapshots, volland_exposure_points tables)
3. PostgreSQL → FastAPI endpoints → Plotly.js dashboard

### Key Components

**app/main.py** (FastAPI web service):
- Background scheduler pulls SPX options chain every 30 seconds during market hours (9:30-16:00 ET)
- Saves snapshots to PostgreSQL every 5 minutes
- Calculates GEX (Gamma Exposure) from options chain data
- Serves dashboard with embedded Plotly.js charts at `/`
- API endpoints: `/api/series`, `/api/snapshot`, `/api/history`, `/api/volland/*`

**volland_worker.py** (Playwright scraper):
- Logs into vol.land, captures network requests via injected JavaScript hooks
- Parses charm/vanna exposure data from intercepted API responses
- Runs on configurable interval (default 60 seconds)

### Database Tables
- `chain_snapshots` - options chain data with Greeks
- `volland_snapshots` - raw scraped data with statistics
- `volland_exposure_points` - parsed exposure points by strike

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (required for volland_worker)
playwright install chromium

# Run the FastAPI web server locally
uvicorn app.main:app --host 0.0.0.0 --port 8080

# Run the Volland scraper worker
python volland_worker.py
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
VOLLAND_URL          # Charm workspace URL
VOLLAND_STATS_URL    # Statistics page URL (optional)
```

## Deployment

Deployed on Railway using Docker. The Dockerfile uses the official Playwright image.

```bash
# Procfile runs:
web: uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

## Key Technical Details

- Market hours check: `dtime(9, 30) <= t.time() <= dtime(16, 0)` in US/Eastern timezone
- Options chain fetches use streaming endpoint with 2-second timeout, falls back to snapshot endpoint
- GEX calculation: `call_gex = gamma * OI * 100`, `put_gex = -gamma * OI * 100`
- Volland scraper injects JavaScript to hook `fetch`, `XHR`, and `WebSocket` to capture API responses
