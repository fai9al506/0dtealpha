# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Session Continuity Protocol (MANDATORY)

### On Session Start
1. **Check ET time FIRST.** Run `powershell -Command "[System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, 'Eastern Standard Time').ToString('yyyy-MM-dd HH:mm:ss dddd')"`. If the time falls **Mon–Fri between 09:30 and 16:00 ET**, print this warning prominently at the TOP of your session brief (before anything else):

   > ⚠️ **MARKET HOURS ACTIVE — NO `git push` UNTIL 16:10 ET.** Commits OK; defer all pushes. If a deploy is needed, add it to `Tasks.md` as a scheduled post-market task.

   This applies to EVERY session, no exceptions. Do not skip this check even for quick questions.
2. **Read `PROJECT_BRAIN.md`** from memory directory — contains vision, ideas backlog, design decisions, pending items
3. **Read `SESSION_LOG.md`** from memory directory — contains what was done in recent sessions
4. **Read `Tasks.md`** from repo root — check SCHEDULED TASKS table. If any task is due (based on trigger condition and current date/time), alert the user: "Task S# is due: [description]". Include all due tasks in your session brief.
5. **Scan `references/`** — glob all files, compare against `references/INDEX.md`. If new files found, read them and update the index with a short summary. This keeps knowledge current without the user having to notify.
6. You now have full context. Do NOT ask the user to explain what the system does or what was done before.
7. **Print a 3-5 line brief** to the user summarizing: what was done recently, what the current state is, and what the current priority is. Include any due scheduled tasks. Do NOT ask "what do you want to work on?" — just show you know where we are. The user will then tell you what to do.

### On Session End (when user says "bye", "done", "session end", "that's all", or similar)
1. **Update `SESSION_LOG.md`** — add entry for this session: what was done, decisions made, ideas discussed
2. **Update `MEMORY.md`** — if system state, priorities, or architecture changed
3. **Update `PROJECT_BRAIN.md`** — if new ideas, design decisions, or pending items emerged
4. **Update `Tasks.md`** — mark completed tasks, add new tasks discovered during session, update statuses
5. **Update this CLAUDE.md** — if new features, components, or technical details were added to the codebase
6. **Confirm to the user**: print a short message like "Session logged. Files updated. See you next time." so the user knows it's safe to close the window. Do NOT close without confirming.

### After Any Code Changes
- Update the relevant sections of CLAUDE.md (architecture, features, technical details)
- Update MEMORY.md if intervals, tables, or key parameters changed

## Analysis Validation Protocol (MANDATORY)

**This protocol is NON-NEGOTIABLE. Violations cost real money (session 46: 4 errors caught by user, not by Claude).**

Before presenting ANY trading study, backtest, performance report, or parameter recommendation:

### Gate 1: Data Quality (MUST PASS before running analysis)
1. **Source check**: ALL numbers from DB queries or code output. Never manual math, never from memory files.
2. **Date range**: State explicitly. Check for known outages (Mar 26 TS outage, any logged in SESSION_LOG).
3. **Parameter history**: Did SL, filter version, grading, or trail params change during the period? If yes, **split the data at the boundary** and only use the era matching current live config.
4. **Staleness scan**: Check for frozen spot prices (same spot across consecutive snapshots = outage). Query: `SELECT ts, spot FROM chain_snapshots WHERE ts::date = X ORDER BY ts` — look for repeating values.
5. **Timezone**: Verify ET conversion handles DST (2nd Sunday Mar = spring forward, 1st Sunday Nov = fall back). Use `zoneinfo.ZoneInfo("America/New_York")`, NEVER hardcode UTC-4 or UTC-5.
6. **Contamination**: Any trade with MFE > 50 or MAE < -30 gets individually verified against known market conditions.

### Gate 2: Cross-Check (MUST PASS before presenting results)
1. **Sim vs DB**: If using OHLC simulation, compare baseline outcome match rate against DB actuals. **If < 90% match, the simulation is broken — DO NOT present results.**
2. **Known totals**: `SUM(outcome_pnl)` from DB for the same filter MUST match within 5% of your computed total. If not, find the discrepancy before proceeding.
3. **Sanity check**: If a 1-parameter change claims >50% PnL improvement, that's a red flag. Verify the mechanism (why would this work?) before presenting.

### Gate 3: Presentation Requirements
1. **State clean sample size** prominently (e.g., "42 clean post-Mar 18 V12 trades" — not "151 trades")
2. **State what was excluded** and why (contaminated dates, wrong SL era, etc.)
3. **State confidence level**: <50 trades = "directional signal only", 50-100 = "moderate confidence", 100+ = "high confidence"
4. **If recommending a real-money change**: explicitly state worst-case scenario and what could go wrong
5. **Never present a number without its source** (DB query, simulation, or calculation shown)

### What Requires This Protocol
- Backtest results (PnL, WR, MaxDD)
- Setup performance comparisons
- Filter/parameter optimization studies
- Trade outcome reports with numerical conclusions
- Any recommendation that leads to code changes affecting real money

### What Does NOT Require This Protocol
- Configuration documentation, code explanations, debugging
- Strategic discussions without numerical claims
- Simple queries ("how many trades today?")

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
- **Setup detector** (`app/setup_detector.py`) - scoring module for GEX Long, AG Short, BofA Scalp, ES Absorption, Paradigm Reversal, DD Exhaustion, and Skew Charm setups (Vanna Pivot Bounce disabled)
- **Volland scraper worker** (`volland_worker_v2.py`) - Playwright route-based scraper for charm/vanna/gamma exposure data from vol.land
- **ES cumulative delta** (integrated in `app/main.py`) - scheduler job pulls ES 1-min bars from TradeStation API
- **ES quote stream** (integrated in `app/main.py`) - WebSocket stream builds bid/ask delta range bars from TradeStation ES quotes
- **VPS data bridge** (`vps_data_bridge.py`) - tails Sierra Chart `.scid` files on VPS, builds 5pt + 10pt range bars + VX ticks, POSTs to Railway

## ES Data Source: Sierra (default since 2026-04-30)

Single env var routes the entire ES bar pipeline:
- `ES_DATA_SOURCE=sierra` (code default, unset on Railway) → reads from `vps_es_range_bars`
- `ES_DATA_SOURCE=rithmic` → reads from `es_range_bars WHERE source='rithmic'`

**Key code paths in `app/main.py`:**
- `_es_data_source()` — central routing helper (one env var read)
- `_es_bars_table_filter()` — returns `(table, where_clause)` tuple for SQL queries
- `get_es_bars()` / `get_es_bars_10pt()` / `get_es_state()` — neutral accessors that route by env var
- In-memory mirror: `_sierra_bars_5pt` / `_sierra_bars_10pt` populated by `/api/vps/es/bar` POSTs, hydrated from DB at startup (`_hydrate_sierra_bars_from_db()`)
- `/api/vps/es/bar` callback: routes LIVE detection (sierra) or shadow (rithmic) via `_trigger_shadow_detection()`

**Reverting Rithmic anytime (no code change):**
```bash
railway variables --service 0dtealpha --set "ES_DATA_SOURCE=rithmic" --unset "RITHMIC_DISABLED"
```

**Real money trading is FEED-AGNOSTIC** — `real_trader.py`, `auto_trader.py`, `eval_trader.py` use SPX `chain_snapshots` + TS `@ES` quote stream. None of them read ES range bars. Switching feeds has zero impact on SC/AG/VPB/DD live trades.

**Phase 1 shadow path** (`setup_log_shadow` table + `_shadow_run_5pt`/`_shadow_run_10pt`) stays in place but only fires when `ES_DATA_SOURCE=rithmic` — gives free Sierra-vs-Rithmic comparison the day Rithmic is restored.

## IMPORTANT: Volland hosted on VPS, not Railway (post-2026-05-21)

**As of 2026-05-21, the Volland scraper runs on the USER'S VPS via VISIBLE Chrome, NOT on Railway.**

Vol.land deployed JS-level workspace-page bot detection overnight 2026-05-20→2026-05-21. Headless Chromium on Railway can log in but the React widget tree refuses to mount → 0 exposure API calls fire. Direct API login from datacenter IPs returns 409 device challenge. Vol.land binds sessions IP-side, so even a JWT from user's browser can't be used from Railway.

**Solution**: visible Chrome (`headless=False`) on user's VPS (Windows Server 2022 with RDP GUI, APNIC IP) bypasses all checks. Same VBS-launcher pattern as `eval_trader.py` + `vps_data_bridge.py`.

**Active stack**:
- VPS runs `_tmp_run_volland_local.py` which monkey-patches `BrowserType.launch` to force `headless=False` + auto-restart loop + anti-detection arg
- That wrapper imports and runs `volland_worker_v2.py` (the existing worker code, unchanged)
- Output writes to same Railway Postgres `volland_snapshots` + `volland_exposure_points` tables → portal + setup_detector code unchanged

**Discipline rules**:
- 🚨 NEVER sign into vol.land on PC while VPS worker is running (1-session-per-account). Kicks VPS off.
- 🚨 NEVER redeploy Railway Volland service. It's `railway down`'d for a reason.
- 🚨 NEVER set `VOLLAND_HTTP_MODE=true` on Railway. The HTTP worker exists for future use but vol.land 401s Railway's IP.

**Daily action required**: NONE. VBS auto-start handles boot. Visible Chrome auto-relogs on session expiry. Same robustness as eval_trader.

**Recovery procedures**: see `reference_volland_vps_architecture.md` in memory directory.

---

## IMPORTANT: Volland Worker Versions (v1 vs v2)

**`volland_worker_v2.py` is the ACTIVE production worker. `volland_worker.py` (v1) is SUSPENDED/legacy.**

- **v2** (`volland_worker_v2.py`): Single all-in-one workspace, Playwright route-based capture. NOW HOSTED ON VPS (see section above).
- **v1** (`volland_worker.py`): Old two-workspace approach with JS injection hooks. **DO NOT run v1.** It remains in the repo for reference only.

When the user mentions "volland worker" or "volland not working", they mean **v2 on VPS**.

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

- `app/` — production code (main.py, setup_detector.py, auto_trader.py, stock_gex_scanner.py) — **this is the main codebase**
- `eval_trader.py` — local E2T auto-trader (polls Railway API → OIF → NT8 → MES). Runs on user's VPS, not Railway.
- `volland_worker_v2.py` — **ACTIVE** Playwright scraper, **runs on VPS via `_tmp_run_volland_local.py` wrapper** (not Railway anymore — see section above)
- `_tmp_run_volland_local.py` — VPS launcher; monkey-patches Playwright to force visible Chrome + auto-restart loop
- `volland_http_worker.py` — pure-HTTP scraper (paused — vol.land IP-binds JWTs, can't run on Railway)
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
- Live outcome tracking: `_setup_open_trades` list tracks open setups, `_check_setup_outcomes(spot)` checks each ~30s cycle for WIN/LOSS/EXPIRED, sends per-trade Telegram. `_compute_setup_levels(r)` extracts target/stop from any setup result dict. ES-based setups (ES Absorption, SB Absorption) use `_es_based` flag for consistent ES price space in all checks.
- EOD summary: `_send_setup_eod_summary()` cron at 16:05 ET — expires remaining open trades, sends daily summary Telegram (trades, wins/losses, net P&L, win rate)
- Admin password from `ADMIN_PASSWORD` env var (not hardcoded)
- API endpoints: `/api/series`, `/api/snapshot`, `/api/history`, `/api/volland/*`, `/api/es/delta/*`, `/api/es/delta/rangebars`, `/api/health`, `/api/eval/signals` (Bearer token auth, returns signals+outcomes+es_price)

**app/setup_detector.py** (Setup scoring module):
- Self-contained module — receives all data as parameters, no imports from main.py
- **GEX Long**: Force alignment framework — LIS as support/magnet (±5 pts), -GEX as support/magnet, +GEX/target as magnets up. SL=8, trail BE@8/activation=10/gap=5. Blocks GEX-TARGET and GEX-MESSY paradigm subtypes.
- **AG Short**: Bearish counterpart to GEX Long
- **BofA Scalp**: LIS-based scalp with charm/stability/width scoring
- **ES Absorption**: Volume-gated CVD divergence with Volland confluence scoring. See "ES Absorption Detector" section below
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

### ES Absorption Detector (Restored 2026-03-11)

Volume-gated price vs CVD divergence on ES 5-pt range bars, with Volland confluence scoring. Originally replaced by "CVD Divergence" (simple swing-to-swing, no quality gates) on Mar 7, but CVD Divergence was net negative (39% WR, -140 pts across 11 dates). Original restored because with alignment filter: 76% WR, +88.1 pts at alignment +3; 67% WR, +117.6 pts at alignment >= 0.

**Architecture:**

1. **Volume Gate**: Trigger bar volume must be >= 1.5x the 20-bar average. This filters out low-quality signals.

2. **Lookback Divergence** (`evaluate_absorption`):
   Over an 8-bar lookback window, compares normalized CVD slope vs price slope:
   - **Bullish**: CVD falling (norm < -0.15) while price holds/rises (gap > 0.2)
   - **Bearish**: CVD rising (norm > 0.15) while price stalls/falls (gap > 0.2)
   - Divergence raw score: 1-4 based on gap magnitude (0.2, 0.4, 0.8, 1.2 thresholds)

3. **Volland Confluence** (weighted scoring):
   - DD Hedging: +1 if DD aligns with direction (long+long or short+short)
   - Paradigm: +1 if paradigm aligns (GEX for bullish, AG for bearish)
   - LIS proximity: +2 if within 5 pts, +1 if within 15 pts

4. **Weighted Composite** (0-100): Divergence 25% + Volume 25% + DD 15% + Paradigm 15% + LIS 20%

**Grading v3 (Apr 13):** Direction-aware scoring via `grade_absorption_v3()`. Key insight: div_raw and vol_raw have OPPOSITE effects for bulls vs bears. v2 was anti-predictive (r=-0.024). v3: r=+0.184, cross-validated r=+0.141. Takes alignment parameter from main.py (re-graded after alignment computed). Thresholds: A+ >= 70, A >= 62, B >= 54, C >= 46, LOG < 46. Performance: A+=79% WR, A=58%, B=51%, C=39%, LOG=22%.

**Cooldown:** Bar-index based (10 bars between same-direction signals) + checked_idx dedup.

**Risk Management:** Fixed SL=8pt / T=10pt. Entry at ES price from Rithmic range bars.

**V12-fix filter (current):** V11 + gap longs-only block before 10:00. **Gap rule: block LONGS before 10:00 ET when |gap| > 30 pts.** Rule A (all-day gap-up block) REMOVED — V12 base filter already cleans gap-up longs (72% WR). Shorts before 10:00 NOT blocked (71% WR). V11 rules unchanged: SC/DD blocked 14:30-15:00 ET (charm dead zone, 35% WR). SC/DD blocked 15:30+ ET (too little time). BofA blocked after 14:30 ET (0% WR). **SC grade gate: only A+/A/B pass (C and LOG blocked).** V10 base: Longs alignment >= +2 AND (VIX <= 22 OR overvix >= +2), SC exempt from VIX gate. Shorts whitelist: Skew Charm (all), AG Short (all), DD Exhaustion (align!=0). **SC/DD shorts blocked when paradigm = GEX-LIS** (24t, 43% WR — LIS acts as support floor). Overvix = VIX - VIX3M; when >= +2 means market overvixed. `_passes_live_filter()` accepts `paradigm` and `grade` params, all 5 callers pass both. Filter history: R1 → V7 → V7+AG → V8 → V9-SC → V10 → V11 → V12 → **V12-fix**.

**Charm S/R Limit Entry (shorts only, added 2026-03-12):** For short setups, uses charm per-strike S/R levels to improve entry price. Queries `volland_exposure_points` for strongest positive charm strike above spot (resistance) and strongest negative below (support). If entry is NOT in the top 30% of the S/R range, places a LIMIT order at `resistance - range × 0.3` instead of MARKET. Backtest: +822 pts improvement, WR 69%→81%, DD halved.

- **Two-phase order flow:** Phase 1 places LIMIT entry only. Phase 2 (on fill) places stop+target using actual fill price.
- **30-min timeout:** Unfilled limit entries auto-cancelled after 30 min.
- **DB column:** `setup_log.charm_limit_entry` stores SPX limit price for shorts.
- **SPX→MES conversion:** `mes_limit = es_price + (charm_limit_spx - spot)` in main.py before passing to auto_trader/eval_trader.
- **Longs unchanged:** Only shorts use charm S/R. Long entries remain MARKET.
- **Status:** `pending_limit` in auto_trader `_active_orders`, `pending_limit: True` in eval_trader position dict.
- **Telegram:** Shows `[CHARM S/R]` tags on limit placement, fill, timeout.
- **EOD:** `flatten_all_eod()` cancels pending limit entries. Eval trader `flatten()` handles pending_limit gracefully.

**Data contamination warning:** `es_range_bars` table has overlapping `bar_idx` from `live` and `rithmic` sources on same dates. Always filter by `source = 'rithmic'` in queries.

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

**Outcome tracking — continuous trail** (updated 2026-02-19):
- DD uses a continuous trailing stop: activation=20 pts, gap=5 pts
- Once max profit reaches 20 pts, trail engages at max_profit - 5
- Replaces rung-based trail (activation=7, step=5, lock=rung-2) which triggered prematurely on contrarian setups
- Simulation: +41.9 pts (continuous) vs +4.0 pts (old rung-based) across 8 DD trades

**Log-only mode:** Grade always "LOG", score always 0. Telegram messages tagged `[LOG-ONLY]`. Target: 50+ live signals before enabling as real setup.

### MES Auto-Trader (SIM Execution — Split-Target)

Self-contained module (`app/auto_trader.py`) that auto-trades **10 MES** futures on TradeStation SIM account when setups fire. Receives `engine`, `ts_access_token`, and `send_telegram_setups` via `init()` — no imports from main.py.

**Safety:** Hardcoded to `sim-api.tradestation.com` (cannot hit live). Master kill switch `AUTO_TRADE_ENABLED` env var (default OFF). Per-setup toggles all default OFF. 10 MES contracts.

**Config:**
- `AUTO_TRADE_ENABLED`: master switch (default `false`)
- `ES_TRADE_SYMBOL`: MES symbol (default `@MES`)
- `TOTAL_QTY=10`, `T1_QTY=5`, `T2_QTY=5`, `FIRST_TARGET_PTS=10.0`
- SIM account: `SIM2609239F`, hardcoded

**Two order flows:**
- **Flow A — Single target** (BofA Scalp, ES Absorption, Paradigm Reversal): Bracket (BRK group) — 10 MES market entry + Limit 10 @ +10pts + StopMarket 10
- **Flow B — Split target** (GEX Long, AG Short, DD Exhaustion): Market entry 10 MES + separate orders:
  - T1: Limit 5 @ +10pts (first target)
  - T2: Limit 5 @ full Volland target (DD: trail-only, no T2 limit)
  - Stop: StopMarket 10 (qty reduced on T1/T2 fills)

**Split-target qty management:** When T1 fills, stop qty reduced 10→5 via PUT. When T2 fills, stop qty reduced further. When stop fills, remaining limits cancelled. `_adjust_stop_qty()` handles all transitions.

**MES price conversion:** SPX point distances applied to current MES price from quote stream (same tick size as ES).

**place_trade() signature:** `place_trade(setup_log_id, setup_name, direction, es_price, target_pts, stop_pts, full_target_pts=None, limit_entry_price=None)` — `full_target_pts` is the Volland full target distance for T2. `limit_entry_price` is MES-space charm S/R limit entry (shorts only, None = market).

**Integration points in main.py (7):**
1. Startup init after Rithmic
2. `auto_trade_orders` table in `db_init()`
3. `place_trade()` after setup fires (both main loop and ES Absorption path) — passes `full_target_pts` + `limit_entry_price` (charm S/R)
4. `update_stop()` after trail advances
5. `close_trade()` on outcome resolution
6. `poll_order_status()` at top of `_check_setup_outcomes()`
7. Health endpoint + admin API (`/api/auto-trade/status`, `/api/auto-trade/toggle`)

**Crash recovery:** Active orders persisted to `auto_trade_orders` table (JSONB), restored on startup.

**Margin pre-check (2026-03-07):** `_get_buying_power()` queries account balance before `place_trade()`. Skips if buying power < TOTAL_QTY × $2,737/MES. Prevents cascade of rejected orders when margin consumed.

**EOD flatten retry (2026-03-07):** Phase 1b waits 3s (was 1s) for margin release. Phase 1c retries close order up to 4 times with increasing waits (0/3/5/10s). Each retry re-checks position, detects explicit rejection in TS response (Error=FAILED), sends `_alert_critical` if all attempts fail.

### SPX 0DTE Options Trader (`app/options_trader.py`)

Self-contained module that buys SPXW 0DTE options at ~0.30 delta when Skew Charm fires. Same init pattern as auto_trader.py — receives `engine`, `get_token_fn`, `send_telegram_fn` via `init()`.

**Safety:** Hardcoded to `sim-api.tradestation.com`. Equities SIM account `SIM2609238M` (separate from futures SIM).

**Config:** `OPTIONS_TRADE_ENABLED` (master switch, default OFF), `OPTIONS_SIM_ACCOUNT`, `OPTIONS_QTY` (default 1), `OPTIONS_TARGET_DELTA` (0.50), `OPTIONS_STRATEGY` ("credit_spread" or "single_leg"), `OPTIONS_SPREAD_WIDTH` (1 or 2, default 2).

**Two strategies:**
- **credit_spread** (default, added 2026-03-19): Sells ATM credit spreads. Bullish = bull put spread (sell ATM put + buy lower put). Bearish = bear call spread (sell ATM call + buy higher call). Theta works FOR us. No time exit needed. Two separate orders (SELLTOOPEN + BUYTOOPEN), tracked as single spread in state.
- **single_leg**: Original behavior — buys call/put. 90-min time exit. Theta works against.

**Credit spread backtest (Mar 18):** Single-leg lost -$79 on 26 trades (+132 setup pts). Credit spread $2-wide ATM would have made +$587. Key: theta ate -$742 on single-leg; credit spreads collect theta instead.

**Order flow:** Limit orders only. Entry: short leg at bid (SELLTOOPEN), long leg at ask (BUYTOOPEN). Close: short leg at ask (BUYTOCLOSE), long leg at bid (SELLTOCLOSE). `_get_option_quote()` uses live TS API.

**Key functions:** `place_trade()` dispatches to `_place_credit_spread()` or `_place_single_leg()`. `close_trade()` dispatches to `_close_credit_spread()` or `_close_single_leg()`. `_find_strike_in_rows()`, `_find_exact_strike()` for chain scanning.

**Credit spread state fields:** `strategy`, `short_symbol`, `long_symbol`, `short_strike`, `long_strike`, `spread_width`, `theo_credit`, `theo_debit`, `theo_pnl` (pre-computed at close). Backward-compatible: `symbol` = short_sym, `theo_entry_price` = credit, `entry_price` = net SIM credit.

**DB table:** `options_trade_orders` (setup_log_id PK, JSONB state, crash recovery).

**EOD flatten (added 2026-03-17):** `_options_trade_eod_flatten()` at 15:55 ET closes all open option positions. EOD summary at 16:05 ET also calls `close_trade()` for expired trades. Poll errors are logged (not silently swallowed).

**CRITICAL: TS SIM fills are fake.** SIM returns stale per-strike prices for option exits (e.g., C670 always $1.61, P668 always $7.87). Always use `theo_exit` (from live API `_get_option_bid()`) for real P&L, never `sim_exit`.

### Eval Trader (`eval_trader.py` — Local E2T Auto-Trader)

Standalone local script that polls Railway for setup signals and places MES orders on NinjaTrader 8 for E2T (Earn2Trade) evaluation account. Runs on the user's local PC, NOT on Railway.

**Architecture:** Railway `/api/eval/signals` → `APIPoller` → `ComplianceGate` → `NT8Bridge` (OIF file) → NinjaTrader 8 → Rithmic → E2T

**Key classes:**
- `APIPoller` — polls `/api/eval/signals` every 2s with Bearer token auth. Returns `(signals, outcomes, es_price)`. Tracks `_seen_signals` set to prevent re-emitting. Daily reset. State in `eval_trader_api_state.json`.
- `NT8Bridge` — writes OIF files (`oif{timestamp}.txt`) to NT8 incoming folder. Reads fill/reject from outgoing folder (`{account}_{orderID}.txt`).
- `ComplianceGate` — E2T 50K TCP rules: daily loss limit, max contracts, max losses/day, market hours, daily P&L cap.
- **Daily P&L cap incl. unrealized + cap-flatten (S204, 2026-06-04):** the cap (`e2t_daily_pnl_cap`) gates new entries on realized + open unrealized, AND when realized+unrealized ≥ cap on 2 consecutive 5s checks the main loop flattens all slots, sets persisted `cap_hit_today` (blocks rest of day). NO Telegram — eval is silent (trade channel = TSRT only; alerts channel = issues only). Kill switch `cap_flatten_enabled` (default true). Cap policy: cap = account's best day − ~$25 buffer (E2T 30% consistency rule — profit over cap raises required total $3.33:1). LONG $740 / SHORT $525.
- `PositionTracker` — open position state, trailing stop, NT8 fill detection, reversal, stale overnight auto-flatten. State in `eval_trader_position.json`.

**Critical design points:**
- **ES price for stops**: SPX and MES differ by ~15-20 pts (variable spread). Railway sends `es_price` from quote stream. Stop/target calculated from ES price, NOT SPX spot.
- **OIF naming**: NT8 ATI requires prefix `oif`, extension `.txt`. Example: `oif1740422400000.txt`.
- **Signal staleness**: `MAX_SIGNAL_AGE_S = 120` — signals older than 2 min are skipped (prevents stale entries after restart).
- **Trailing stop**: DD Exhaustion=continuous trail (activation=20, gap=5). GEX Long=rung-based (start=12, step=5, lock=rung-2). Others=breakeven at `+be_trigger_pts`.
- **Reversal**: Opposite-direction signal closes current position, opens new one. Checks compliance for new position first.
- **Stale overnight**: On startup, if position date < today → auto-flatten.
- **Orphan working-order guard (2026-06-08, commit `4d90ee7`)**: E2T FAILS the eval if ANY position OR working order rests on the book past **3:50pm CT** (a SHORT eval was failed this way — a synth-stop's CANCEL was dropped by NT8 ATI and the slot was untracked, leaving an orphan stop that E2T killed at 15:53 CT). Two layers: (1) `_register_orphan_cancel()`/`_verify_pending_cancels()` verify every synth-stop orphan CANCEL vs broker and re-send up to 4× (8s grace), escalating to `cancel_all()` once broker-flat — runs every loop even when the tracker is flat; (2) main loop fires an unconditional `nt8.cancel_all()` (CANCELALLORDERS) every 60s from `flatten_time`→16:55 ET regardless of `is_open`. Both StackingTracker + PositionTracker. Constants `_CANCEL_VERIFY_GRACE_S=8` / `_CANCEL_MAX_RETRIES=4`.
- **Test mode**: `python eval_trader.py --test buy` or `--test sell` for manual testing.

**Config files (local, gitignored state files):**
- `eval_trader_config.json` — setup rules, E2T params, API URL/key, NT8 paths, qty
- `eval_trader_state.json` — daily P&L, trade count, compliance state
- `eval_trader_api_state.json` — `last_id`, `seen_signals`, `seen_outcomes` (daily reset)
- `eval_trader_position.json` — open position for crash recovery

### SPY Option Chain (added 2026-03-10)

Completely isolated from SPX — separate table, globals, scheduler job, lock.

- **DB table:** `spy_chain_snapshots` (same schema as `chain_snapshots`, NO shared columns/migration)
- **Globals:** `latest_spy_df`, `_spy_df_lock`, `_last_spy_run_status`, `_last_spy_saved_at`
- **Scheduler:** `run_spy_market_job()` at same interval as SPX, independent thread
- **SPY params:** symbol=`SPY`, strike_interval=1, strike_proximity=25 (50 strikes ±$25)
- **Functions parameterized:** `get_0dte_exp(symbol="$SPXW.X")`, `get_chain_rows(exp, spot, symbol="$SPXW.X", strike_interval=5, strike_proximity=125)` — backward-compatible defaults
- **API:** `/api/snapshot?symbol=SPY`, `/api/history?symbol=SPY`, `/download/history.csv?symbol=SPY`
- **Portal:** `/table` has SPXW/SPY toggle buttons
- **NOT used by:** setup detection, auto-trader, eval trader, pipeline health — analysis/portal only
- **Rollback:** `stable-20260310-spy-before-push` tag

### Dip-Buy Detector (`app/dipbuy_detector.py`) — added 2026-05-31 (S196, PORTAL/LOG-ONLY)

Self-contained Discord-pro-inspired momentum dip-buy. **NOT in TSRT / eval / auto_trader — portal logging only.** Detects from live SPX spot in `_run_setup_check()` (called via `dipbuy_detector.on_cycle(now_et(), spot, _vix_last)` in a try/except so it never breaks the loop). Trigger: 8pt dip off session high + 4pt bounce confirm, 9:30–11:30 ET entry window, **one trade/day**, target +10 / stop −8, exit by 16:00. Logs to `setup_log` as `setup_name="Dip-Buy"`, tracks its own outcomes and UPDATEs the DB (zero coupling to live trading). Hydrates open trades on restart. Grades A+/A/B on `prior_close_ok` (entry ≥ prior-day close −2) + `vx_diverge_ok` (tick VX made no new high during the dip, from `vps_vix_ticks`) — **both logged as hypotheses in `abs_details` JSON; NEITHER proven robust out-of-sample** (prior-close failed full-history Nov–Jan & range days; 2026-06-03 high-res backtest also found NO edge for prior_close_ok). Goal: collect 50+ forward signals, then validate which grade tier predicts before any real-money use. Init at startup; portal trade-log dropdown filter "Dip-Buy". No Telegram (silent collection).

**Dip-Buy v2 (S201, 2026-06-03):** parallel held-confirm variant logged as `setup_name="Dip-Buy v2"` in the same module. Same 8pt dip trigger/window/one-per-day, but bounce must hold ≥3pt off dip low for **8 consecutive ~30s cycles (~4 min)** before entry; exit **T+8 / S−12** / EOD. From ES-mirrored 30s backtest (Feb 24–Jun 3, 68t): 77.9% WR / +244p / maxDD −28, era-stable, walk-forward 86% WR blind May–Jun (the live v1 rules graded only 38–42% WR on the same path — the original 60% backtest was a 2-min-sampling artifact). v1 stays as control; after ~30 forward days promote whichever holds live WR. Portal dropdown "Dip-Buy v2 ✦". See trade-analyses.md Analysis #17.

### Dark Mate Framework (`app/darkmate.py` + `app/live_filter.py`) — added 2026-06-11 (MONITORING-ONLY)

Self-contained, fail-soft, **zero touch to the trade loop**. Productionizes the validated semi-confirmation sizing study + the Dark Mate gamma/vanna framework map. Three parts:

- **(A) Tech-basket capture:** `darkmate.capture()` runs every **1 min** during market hours (scheduler job `darkmate_capture`). Fetches TradeStation quotes for NVDA/AMD/AVGO/META/MSFT/GOOGL, computes basket %-from-session-open, upserts into **`semi_basket`** table (`et` PK, `basket_pct`, `n_names`, `details` jsonb per-symbol). On Railway (not VPS/Yahoo). Session opens tracked in-memory, daily reset.
- **(B) Sizing results — page `/darkmate`** (`app/darkmate_page.py`, API `/api/darkmate/results?date=` + `/results-history?days=`): per-trade & daily **Baseline · Semi · Gamma · Semi+Gamma · Real-TSRT** on the V16 set. Semi mult 2×/1×/0.5× by basket-vs-direction; gamma favorability = multi-expiry `gamma_below − gamma_above`. **Validated finding: semi-only = 1.38× + halves drawdown ($775→$415); gamma adds nothing (+$105, DD worse) — kept visible for monitoring only.** Real-TSRT col from `real_trade_orders` fills.
- **(C) Framework map — page `/darkmate-fw`** (`app/darkmate_fw_page.py`, API `/api/darkmate/levels?at=&greek=`): live multi-expiry **gamma + vanna** per-strike near spot. **+G = barrier (support below / resist above), −G = accelerator.** ✦ cluster markers where expiries agree. Key levels labeled. Live auto-refresh (60s) + history time-picker. Manual-trade aid.

**`app/live_filter.py` = CANONICAL live-filter (V16):** `passes_v16(row, gaps)` exactly mirrors `main.py:_tlPassesStrategy(l,'v16')` (~line 18833), validated **920 trades / +3408.1 pts** (all-time). `backfill_live_pass(engine)` stamps `setup_log.live_pass` + `live_filter_ver` so recall is `WHERE live_pass=true`. Daily re-stamp job `live_pass_restamp` (16:25 ET). `live_filter_recall.py` (root) = manual backfill runner. **On a filter change (V17): edit `app/live_filter.py` + re-run.** See memory `reference_live_filter_recall.md`.

### Stock GEX Scanner (`app/stock_gex_scanner.py`) — added 2026-03-21

Completely independent from 0DTE SPX pipeline. **Data collection only** — no alerts, no signals, no Telegram. Scans ~23 stocks every 30 min during market hours, saves GEX + price to DB for future backtesting.

**Data collected per scan:**
- Current spot price for each stock
- Full options chain → GEX per strike (gamma × OI × 100)
- Key levels: -GEX support, +GEX magnets, strongest positive/negative
- Two expirations per stock: **weekly** (this week's Friday) and **opex** (nearest 3rd Friday / monthly)

**Isolation:** Zero imports from main.py/setup_detector.py. Receives `engine`, `api_get` via `init()`. Own DB table, own scheduler job, own state.

**Stock list (23):** AAPL, MSFT, GOOGL, META, NVDA, AMD, QCOM, AMZN, SHOP, NFLX, PYPL, BA, AVGO, SMCI, TSLA, COST, LULU, RBLX, ROKU, SNOW, BABA, ENPH, JNJ

**Scheduler:** `run_scan()` interval every 30 min. Market hours guard (9:30-16:00 ET). Batch quotes (1 API call for all stocks), then chain fetch per stock per expiration. Expirations cached per day.

**DB table:** `stock_gex_scans` — symbol, scan_ts, scan_date, spot, expiration, exp_label (weekly/opex), key_levels JSONB, gex_data JSONB, totals.

**API endpoints:**
- `GET /api/stock-gex/levels` — latest levels grouped by symbol → weekly/opex
- `GET /api/stock-gex/detail?symbol=NVDA` — full per-strike GEX data (both expirations)
- `GET /api/stock-gex/history?symbol=NVDA&days=5&exp_label=opex` — scan history for backtesting
- `GET /api/stock-gex/status` — scanner status
- `POST /api/stock-gex/scan` — manual trigger (async)

**DB tables:** `stock_gex_scans` (weekly scans), `stock_gex_alerts` (triggered alerts)

### 0DTE GEX Scanner (`app/dte0_gex_scanner.py`) — added 2026-05-12

Independent module collecting 0DTE GEX data for SPX/SPY/QQQ/IWM every 30 min during market hours. Data collection only — no alerts, no setup detection.

- **Symbols:** SPX (`$SPXW.X`), SPY, QQQ, IWM with their respective strike intervals/proximity
- **Schedule:** `cron minute="0,30"` (anchored to wall clock) during 9:30-16:00 ET
- **DB table:** `dte0_gex_scans` (id, symbol, scan_ts, scan_date, spot, expiration, exp_label='0dte', key_levels JSONB, gex_data JSONB, totals)
- **Reuses:** `get_chain_rows()` + `get_0dte_exp()` from main.py via lazy import (no circular dep)
- **API:** `/api/dte0-gex/levels`, `/detail?symbol=`, `/history?symbol=&days=`, `/status`, `POST /scan`
- **Dashboard:** `/dte0-gex` — 4-card visualization with key levels, magnets, support, GEX mass above/below
- **Per-symbol error isolation:** SPX failure won't block IWM

### Real-trade margin dashboard — added 2026-05-12

- `/real-trade` HTML page surfaces TS `BalanceDetail.InitialMargin` + `DayTradeMargin` per account
- Auto-refresh 30s; verdict logic: if InitialMargin>0 AND DayTradeMargin=0 → OVERNIGHT RATE warning
- Reads `/api/real-trade/status` which calls `real_trader.get_full_status()`

### S55: MES-driven trail simulation (portal realism) — added 2026-05-13

Productionized from `_tmp_s55_mes_trail_prototype.py`. Portal-side **realism fix**,
NOT new alpha — explains the gap between portal "simulated outcome" (walks SPX
30s chain) and real broker fills (walk MES 5pt range bars). Validated on 80 V14
trades Apr 15-May 12: mean |real - mes_sim| = 2.35pt vs |real - chain_sim| = 6.70pt
(MES-sim 2.85× more honest as a predictor of real outcomes).

- **Module:** `app/mes_sim_backfill.py` — `mes_walk()` simulator + `compute_mes_sim_outcome()`
  high-level wrapper + `backfill_for_date()` / `backfill_range()` for historical fill.
- **Live integration:** `_check_setup_outcomes()` in `app/main.py` calls
  `compute_mes_sim_outcome` immediately after the chain-sim UPDATE, scoped to the
  V14 real-trader whitelist (SC / AG Short / VPB / VIX Div / ES Abs). Failure is
  silent — pre-migration the column UPDATE errors out and the code skips. Live
  cycle never crashes.
- **Data source:** `vps_es_range_bars` (range_pts=5) for ES H/L per bar.
- **Within-bar ordering:** conservative adverse-first (stop fills before favorable
  extreme on whipsaw bars) — matches real stop-market behavior.
- **DB columns** (post-migration `_tmp_s55_db_migration.sql`): three on `setup_log`:
  - `mes_sim_outcome_pnl` NUMERIC — MES-walk simulated P&L in points
  - `mes_sim_outcome_result` TEXT — WIN / LOSS / EXPIRED
  - `mes_sim_max_fav` NUMERIC — MES-walk MFE in points
- **Portal display:** trade-log dropdown rows show a small `✦±N.N` badge next to
  P&L when `mes_sim_outcome_pnl` is populated. Tooltip: "MES-sim (S55 — matches
  real broker ±2.35pt)". `/api/setup/log_with_outcomes` and `/api/setup/eod-review`
  both try `SELECT mes_sim_*` first and fall back to legacy on column-missing.
- **Backfill runner:** `_tmp_s55_backfill_runner.py` — defaults to 2026-04-15 → today,
  V14 whitelist only. Has a market-hours guard. `--dry-run` to preview.

**Run sequence post-migration:**

```bash
# 1. apply migration (post-16:10 ET)
psql $DATABASE_URL -f _tmp_s55_db_migration.sql

# 2. backfill historical rows
python _tmp_s55_backfill_runner.py

# 3. push code (which starts live writes for new outcomes)
git push
```

**Reading the badge:** ✦+24.5 means the MES-sim says +24.5pt P&L. If chain-sim
shows +42.0pt and MES-sim shows +24.5pt, the portal was over-stating the trade
by ~17.5pt — that's the trail-tag-early divergence on big runners (S55's main
finding).

### Database Tables
- `chain_snapshots` - SPX/SPXW options chain data with Greeks
- `spy_chain_snapshots` - SPY options chain data (same schema, isolated table)
- `dte0_gex_scans` - 0DTE GEX snapshots for SPX/SPY/QQQ/IWM every 30 min (S84)
- `volland_snapshots` - raw scraped data with statistics (paradigm, LIS, charm, etc.)
- `volland_exposure_points` - parsed exposure points by strike (charm, vanna, gamma, deltaDecay)
- `es_delta_snapshots` - ES cumulative delta state (every 30s, from TradeStation @ES bars)
- `es_delta_bars` - ES 1-minute delta bars (UpVolume - DownVolume per bar)
- `setup_cooldowns` - persisted cooldown state (trade_date, JSONB state including swing tracker)
- `auto_trade_orders` - MES SIM auto-trade order state (setup_log_id PK, JSONB state with split-target tracking, crash recovery)
- `stock_gex_scans` - Stock GEX data every 30 min (symbol, scan_date, spot, expiration, exp_label weekly/opex, key_levels JSONB, gex_data JSONB)
- `setup_log.mes_sim_outcome_pnl / mes_sim_outcome_result / mes_sim_max_fav` (S55, 2026-05-13) - MES-driven trail simulation outcomes (portal realism, not new alpha; populated for V14 whitelist setups only)
- `tsrt_daily_stmt` (S204, 2026-06-04) - TSRT per-day broker-truth statement rows (day PK, gross/comm/net, n_trades, n_wins, trades JSONB) — persisted so weekly report history survives TS's 90-day lookback. **THE source of truth for day-$** — never sum `real_trade_orders.state` per-lid on multi-concurrent days (S210)
- `vol_event_alerts` (S209, 2026-06-07) - dedup keys for vol-event Telegram alerts (key PK: `intraday-<date>` / `confirmed-<trigger date>`)

### TSRT Weekly Statement (`app/tsrt_weekly_report.py`) — added 2026-06-04 (S204)

Fully-automatic weekly capital statement → Telegram **"0DTE Alpha Trades" channel** (`TELEGRAM_CHAT_ID_SETUPS` env; changed from Researchs 2026-06-06 per user). Failure ping → general alerts channel (`TELEGRAM_CHAT_ID`). Cron **Friday 16:20 ET** in main.py scheduler. Self-contained module, `init(engine, ts_access_token)`.

- **Broker truth:** pulls `/historicalorders` **+ `/orders` merged with OrderID dedup** — CRITICAL: `/historicalorders` excludes same-day fills, and the cron runs same-day, so both endpoints are required
- FIFO round-trip matching per account per ET day (accounts flat overnight → per-day matching is safe); upserts to `tsrt_daily_stmt`
- Era anchored: start 2026-05-19 (post-V16.1), starting capital $4,896.99 (verified vs live equity 2026-06-04)
- Report: dark-themed HTML (Inter font), $ + SAR (×3.75 peg), equity-curve + daily-PnL charts (matplotlib base64), per-day comments (curated early-era dict + auto-generated), statistics, projection, drift check vs live equity **net of unrealized P&L**
- Fail-soft: never raises; on error sends a failure ping to the same channel
- Local test harness: `_tmp_s204_local_test.py` (intercepts the Telegram send, writes HTML to disk)
- Manual fallback scripts: `_tmp_tsrt_daily_statement.py` + `_tmp_tsrt_weekly_report.py`; if local ISP blocks api.telegram.org, relay via DB + `railway ssh` (see `reference_telegram_isp_block_relay.md` in memory dir)

### Vol-Event Detector (`app/vol_event_alert.py`) — added 2026-06-07 (S209)

Wizard of Ops spot-vol framework: a "vol event" = spot-vol deviation closes ≥2σ on a down day (panic vol overpricing) → ~93% revisit of prior close within 3 weeks (his stat; our DB n=1: Mar 6 2026 hit target in 4 days then fell 480pts more — bounce ≠ bottom). **Pure alerting, zero trading logic.** Reads `volland_snapshots.payload->statistics->spot_vol_beta` (scraper already captures it every ~2 min). Cron every 5 min, 9-16 ET weekdays. Two alerts → MAIN alerts channel (`TELEGRAM_CHAT_ID`): (1) intraday "LIKELY" when deviation ≥2.0 on a down day (fresh snapshot ≤10 min required); (2) "CONFIRMED" when Volland's `vixEvents` array populates (after 16:15 VIX settle — usually visible next session's snapshots) with target price + 3-week deadline. Dedup persisted in `vol_event_alerts` table. Fail-soft. NOTE: SVB as a *trade filter* was REFUTED 2026-05-30 — this is context only.

### EOD Daily Chart (in `app/eod_report.py`) — rebuilt 2026-06-07

`generate_trades_chart()` shows **TSRT-placed trades only** (`setup_log JOIN real_trade_orders`) — portal-only detectors excluded. Entry ▲/▼ at broker fill, exit ✕ at exit fill, dashed entry→exit path, per-trade label `SKW -14.0p -$70`, broker-$ stats/cum-curve. Uses the **bot's-own-fills view** (pre-FIFO-reconcile precedence) — totals sum exactly to broker gross. PDF report path unchanged (still all resolved setups).

### FIFO reconcile S210 invariants (2026-06-07)

`app/fifo_reconcile.py` rewrites BOTH `close_fill_price` AND `stop_fill_price` (audit in `*_pre_fifo_reconcile`) — consumers read stop first, so both must carry FIFO truth. Conservation guard: bot-exit multiset must equal broker-exit multiset, else refuse + Telegram (FIFO pairing is a permutation of the same fills; a partial/visible-mix rewrite once made Jun 5 look −$378 vs true −$292.5).

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

# Auto-trader (optional — disabled by default)
AUTO_TRADE_ENABLED         # Master switch (default: "false")
ES_TRADE_SYMBOL            # Front-month ES contract (default: "ESM25")
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
- ES Absorption: simple swing-to-swing CVD divergence detector runs on each new completed range bar (see "ES Absorption Detector" section)
- Thread safety: `_es_delta_lock` for ES 1-min delta state, `_es_quote_lock` for ES quote stream range bars
- Dashboard: no page reload — uses per-tab polling timers with `Plotly.react()`, tab persisted via `sessionStorage`
- Setup cooldowns: saved to DB after each evaluation via `setup_cooldowns` table (JSONB), loaded on startup
- Charm thresholds (setup_detector.py): calibrated to actual data — brackets are [50M, 100M, 250M, 500M] (not the original [500, 2K, 5K, 10K])
- Pipeline health: checks data freshness every 30s during market hours, sends Telegram on error/recovery
  - TS API: ok < 2min, stale < 5min, error >= 5min
  - Volland: ok < 3min, stale < 10min, error >= 10min
- 401 alert: `_alert_401()` with 5-min cooldown, wired into `api_get()`, ES delta stream, ES quote stream
- **DB transaction discipline (2026-06-03 outage):** long read loops against prod Postgres MUST use autocommit or commit per chunk. A single long/idle transaction holds AccessShareLock that blocks `db_init()`'s startup ALTERs → crash-loops every deploy. `gex_long_v3._build_cache()` commits per iteration; `on_startup` retries db_init 3× with `lock_timeout=5s` then continues with a Telegram alert. **S205 follow-up (2026-06-04):** when db_init fails 3×, `on_startup` now still runs the 5 post-init loaders (`load_alert_settings`, `load_setup_settings`, `_load_cooldowns`, `_backfill_outcomes`, `_restore_open_trades`) — before this fix a lock-contention start silently ran on in-code defaults (caused the Jun-4 general-channel alert spam) with no cooldowns/open-trade restore. In-code `_alert_settings` defaults also now mirror the quiet DB config. Live remediation without a deploy: POST correct values to `/api/alerts/settings` (session login required); `_tmp_pg_lock_check.py` shows current lock holders.

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


# Communication Style

- Summarize code changes in plain English, no diffs
- For errors: one sentence explanation + whether you fixed it or need my input
- Keep responses short and conversational
