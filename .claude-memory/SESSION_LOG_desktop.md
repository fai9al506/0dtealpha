# Session Log

## 2026-03-31 (session 61) — VPS Setup: NT8 + Eval Trader Migration Planning

**What was done:**
1. Discussed moving NT8 + eval_trader from desktop to Kamatera VPS (103.54.56.210).
2. NT8 installed on VPS, connected to Rithmic (3 E2T accounts green).
3. Created `C:\0dtealpha\restart_nt8.bat` — daily NT8 restart script.
4. Task Scheduler job created: daily 9:15 AM ET → kill NT8 → wait 10s → relaunch. Covers the 5-6 PM ET Rithmic disconnect issue.
5. Windows auto-logon configured via registry (AutoAdminLogon, DefaultUserName, DefaultPassword in Winlogon).
6. Updated S17 in Tasks.md with NT8 daily restart item.
7. User switching to VPS Claude Code session to complete eval_trader config + test.

**Still to do (on VPS session):**
- Create eval_trader_config_vps.json (account, API key, NT8 paths for Administrator)
- Test eval_trader with `--test buy`
- Verify OIF files write to NT8 incoming folder
- Disable eval_trader on desktop, enable on VPS

**Next session:** Continue VPS eval_trader setup on VPS machine.

## 2026-03-29 (session 60) — R2 Charm Near Spot Study: Redundant with V12

**What was done:**
1. R2 research: per-strike charm near spot as filter for SC and ES Absorption.
2. SC (228 trades): On unfiltered data, -5M to 0M bucket = 90% WR. But on V12-passing SC (155t, 76% WR), best charm filter blocks trades at 69% WR — mostly winners. V12 already captures what charm measures. **No filter change for SC.**
3. ES Absorption (276 trades): Bearish charm < -20M = 27% WR, -61.8 pts (extremely toxic). But V12 already blocks ALL ES Abs bearish. Proposed short whitelist (align<=-2, charm>=0M) = 48t, 58.3% WR, +67 pts — too thin to implement.
4. Committed eval_trader fixes (E2T drawdown floor cap + AG trail 15→12). Marked S15 as DONE (already pushed).
5. Updated S17 status: Python, Git, VS Code, GitHub CLI, Railway CLI, Sierra, Google Drive installed on VPS.

**Key insight:** Charm near spot is a real signal on unfiltered data, but V12's alignment + VIX + grade gate already captures the same information. Same pattern as F7 Charm Support Gate (rejected for V8). V12 is robust.

**Next session:** Sunday — VPS setup with ES & VIX data bridge (S17/S18). Market opens 6 PM ET.

## 2026-03-29 (session 59) — Outcome Tracker Bug Hunt: 42 Trades Corrected

**What was done:**
1. User spotted trade #1331 (SB2 Absorption) wrongly scored as LOSS when T1 was clearly hit at +10.
2. Found **Bug 1 (forming-bar):** `_check_setup_outcomes` scanned forming bars (partial high), set `_es_last_bar_idx`, then skipped the completed bar (with real high) because `bidx <= last_scanned`. Fixed: `<=` → `<` with entry bar guard. 17 false LOSSes → WIN, +281.4 pts.
3. Ran S2 (SB2 Absorption data check). Found **Bug 2 (batch-scan):** Scanner processed ALL new bars in bulk, accumulating `_seen_high`/`_seen_low` before trail/stop check. Broke temporal order — favorable move on bar N+5 could mask stop hit on bar N. Fixed: per-bar trail advancement + stop check with early exit.
4. Full audit corrected **42 trades total**: 17 false LOSSes→WIN (+281.4), 21 false WINs→LOSS (-184.2), 4 wrong P&L values. Net impact: +97.2 pts.
5. **SB2 verdict:** 35t, 40% WR, -46.2 pts = LOSER. Shorts decent (76% WR) but longs toxic (39%). Keep collecting, consider shorts-only.
6. User manually verified 3 sample trades on TradingView — all confirmed correct.
7. Both fixes committed, pushed, deployed to Railway.

**Final corrected stats:**
- Overall: 1,200t, +1,082.3 pts
- SC +458, AG +365, DD +210, ES Abs +152, SB2 -46, GEX Long -74

**Key insight:** Two separate bugs in `_check_setup_outcomes` were silently corrupting outcomes in both directions. The forming-bar bug missed fast T1 hits; the batch-scan bug created false wins by advancing trail before checking stop in temporal order.

## 2026-03-29 (session 58) — VIX Divergence: New Setup from User Insight

**What was done:**
1. User spotted VIX-SPX divergence on Mar 27: price dropping while VIX flat (09:36-09:58), then VIX dropping while SPX flat (09:58-11:22), then SPX exploded +28 pts.
2. Researched Discord backing — this is Apollo's core "vol seller/worm" framework. Multiple experts validate.
3. Built full backtest across 24 days (Feb 24 - Mar 27):
   - Two-phase detection: Phase 1 (VIX suppression) + Phase 2 (VIX compression)
   - Both LONG and SHORT directions (old VIX Compression was long-only)
   - Combined: +131 pts March, PF 2.11, 58% green days
4. Exit strategy study: trail-only beats fixed TP massively (PnL doubled)
   - SHORT: BE@8, trail activation=10, gap=5
   - LONG: IMM trail gap=8
5. Refinement study: identified longs at VIX<26 as main DD source, but user decided to keep all signals for data collection
6. **Entry timing breakthrough:** "Strong bar 1.5pt" — wait for first 1-min bar with body >= 1.5pt in signal direction
   - WR: 50% → 68%, PnL: +149 → +262, MaxDD: 29 → 11, avg MAE: 5.3 → 2.3
   - 97% fill rate (keeps data collection intact)
   - Implemented as stop-entry confirmation with 30-min timeout
7. Deployed both commits: VIX Divergence setup + stop-entry confirmation. LOG-ONLY mode.

**Key insight:** The entry timing optimization was the user's idea ("find the exact right time before they explode") — better than filtering. Reduces drawdown naturally without losing any signal data.

**Deployed:** Commits `b00f3ff` + `86e23a6`, pushed to Railway. Market closed.

## 2026-03-29 (session 57) — V12-fix: Gap Filter Study & Correction

**What was done:**
1. User reported Mar 27 SC long loss at 09:42 — gap filter didn't block it
2. Investigated: code computed gap=-28.5 (first-cycle spot 6450.2 vs prev close 6478.8), actual SPX gap was ~-23. Both under the 30-pt threshold.
3. Ran gap filter study on 585 V12-passing trades (Feb 4 – Mar 27). Key findings:
   - **Rule A (all-day gap-up block) was HARMFUL:** blocked 32 longs with 72% WR, +58.9 pts. Original study used unfiltered data (112t, 38% WR). V12 base filter already removes bad longs.
   - **Rule B was blocking profitable shorts:** 11 shorts before 10:00 on gap days = 55% WR. All shorts before 10:00 = 71% WR, +47 pts.
   - Early longs on big gap days (|gap|>30) = 29% WR, -68.8 pts — correctly blocked.
4. Implemented V12-fix: removed Rule A, made Rule B longs-only before 10:00 when |gap|>30.
5. Updated all 4 filter locations (Python + 3 JS portal filters), CLAUDE.md, MEMORY.md.

**Key lesson:** Filter studies must be done on FILTERED data matching the current live filter. The V12 gap study was done on raw unfiltered trades, making Rule A appear beneficial when it was actually redundant.

**Deployed:** Already live on Railway (auto-deploy on push). Market closed.

## 2026-03-28/29 (session 56) — Delta Absorption Setup: Full Research → Implementation

**What was done:**
1. User reported ES Absorption missed signals at 10:45 and 13:00 on Mar 27 — volume gate silently dropped signals, bar aggregation mismatch (10-pt chart vs 5-pt detector)
2. Volume rate (vol/sec) research — user insight that raw volume is broken for range bars. Built full March vol-rate dataset (12,738 bars)
3. User manually identified ~28 absorption signals from Sierra Chart across Mar 27 and Mar 19
4. Human vs Machine comparison: user 64% WR vs machine 32% WR — gap is context judgment
5. Iterative development V4→V7: delta-vs-price divergence with data-driven tiers and grading
6. **V7 FINAL: 91 signals, 62.6% WR, +292.5 pts/month, PF 3.01, 95% green days, MaxDD -16.2**
7. Grading system perfectly monotonic: A+=86% WR, A=79%, B=45%, C=29%, LOG=0%
8. Implemented in setup_detector.py + main.py as LOG-ONLY, deployed to Railway

**Key insights:**
- Delta opposing bar color is the core signal — not volume, not CVD slope
- Doji (body 0.5-1.0) = strongest predictor of winners
- Afternoon 12:30-15:00 (skip 14:00-14:30) = cleanest time window
- IMM trail `stop = max(maxProfit-8, -8)` beats all other exit strategies
- Peak ratio ≥ 2.5 is toxic (14% WR) — cap at <2.5
- Setup is direction-neutral (no bull/bear bias in criteria)
- Grading: delta magnitude + body size + signal freshness + time of day

**Files:** `_v7_final.py`, `_v7_grading.py`, `exports/v7_final_signals.csv`, multiple backtest scripts

## 2026-03-29 (session 55) — VPS Cloud Migration: Kamatera Setup

**What was done:**
1. **VIX data provider research** — Rithmic confirmed NO CBOE/CFE (Cameron Growney email Mar 27). Evaluated Databento (CFE not normalized yet, PCAPs only $750/mo), IBKR (needs local TWS), dxFeed, Sierra on cloud VPS.
2. **Architecture decision** — Sierra Chart on cloud VPS replaces local PC + Rithmic. Provides ES + VIX tick data. NT8 stays for E2T execution. eval_trader runs on VPS.
3. **Full migration plan designed** — 5 phases: (1) VPS setup, (2) ES data bridge, (3) VIX pipeline, (4) monitoring, (5) cutover. Plan file saved.
4. **Kamatera VPS provisioned** — Free 30-day trial. 2 vCPU, 4GB RAM, 50GB SSD, Windows Server 2022, US-NY2 datacenter. IP: 103.54.56.210. Name: 0dte-vps. $62/mo after trial.
5. **VPS initial setup completed** — RDP connected, Sierra Chart installed + Denali downloading data, Chrome installed, Git Bash installed, Claude Code installed on VPS.
6. **Claude Code on VPS completed steps 1-3** — Python 3.12.9 installed, repo cloned to C:\0dtealpha\, pip packages installed (requests, websocket-client, pytz).

**Still to do (next session):**
- Sierra DTC Protocol Server config (port 11099, JSON, Allow Trading)
- Add ES + VX symbols in Sierra
- Install NT8 + Rithmic on VPS
- Install IBKR TWS on VPS
- Create eval_trader_config_vps.json
- Test eval_trader on VPS
- Windows auto-start + auto-logon
- Build vps_data_bridge.py + Railway endpoints

**Decisions:**
- Kamatera over Vultr (free trial, $62/mo vs $72/mo)
- Windows Server 2022 Standard (not Core — need GUI for Sierra/NT8)
- NT8Bridge for execution (not SierraBridge — proven path, less risk)
- VPS left running 24/7

**Cost analysis:**
- Current: Rithmic $122 + Sierra $71.50 = $193.50/mo
- After migration: Sierra $71.50 + Kamatera $62 = $133.50/mo
- Savings: $60/mo + VIX data + 24/7 reliability

---

## 2026-03-28 (session 54) — Telegram Research Channel + HTML Reports

**What was done:**
1. **Telegram Research Channel setup** — "0DTE Alpha Researchs" channel connected (chat ID: -1003792574755). Same bot token. Tested message + document sending.
2. **HTML report template established** — dark-themed (vol_event_guide style), with KPI cards, color-coded tables, bar charts, callout boxes, decision tree flowcharts.
3. **ES Absorption Deep Study** sent as first HTML report — 305 trades, anti-predictive grading finding, best filters, direction/VIX analysis.
4. **V12 Gap-Up Longs Filter** sent as second HTML report — decision tree flowchart, impact KPIs, FOMC rejection, filter evolution timeline, full V12 ruleset.

**Bugs/mistakes:**
- **ASSUMPTION IN REPORT:** Added "Gap Down → Normal Trading" branch to flowchart without any data backing it. Gap-down was never studied. User caught it and was rightfully upset. Fixed to only show what was actually researched (gap-up > +30 → block longs). Critical feedback saved.

**Decisions:**
- Flowcharts/decision trees are the standard for complex filter logic in HTML reports
- Reports sent proactively when content is intensive (user doesn't need to ask)
- NEVER add unstudied branches to decision trees — only data-backed conclusions

**Feedback saved:**
- `feedback_self_fetch_railway.md` — fetch Railway env vars yourself, don't ask user
- `feedback_use_flowcharts.md` — use decision tree charts for complex logic
- `feedback_never_assume_in_reports.md` — CRITICAL: only show researched data in reports
- `reference_telegram_research_channel.md` — channel details + workflow

---

## 2026-03-28 (session 53) — Copilot Session 3 + Vol Event Detector + DD/ES Research

**What was done:**
1. **Copilot live market monitoring** — 12 real-time updates across Mar 27 session (09:59-16:15 ET). Tracked SPX 6438→6375 (-63 pts trend day). DD flipped 8+ times. VIX 29.66→31.28. SVB 1.57→2.18.
2. **Discord analysis** — 536 messages from volland-daytrading-central. Key: Apollo vol seller call wrong, LordHelmet "fake rally then more pain" perfect, Wizard/Zack both said "not a vol event" at overvix 2.18.
3. **Copilot session 3 post-market analysis** — 53 signals, V12 filter analysis (4 SC longs passed, 1W/3L -33.6 pts). 10 suggestions drafted, S5 later WITHDRAWN after deeper research.
4. **S5 deep research: "when do we catch the bottom?"** — Analyzed ALL Discord exports (479 msgs), all high-VIX days in DB. Found: SVB level alone doesn't predict long success (Mar 3 SVB 3.82 = best long day, Mar 13 SVB -0.68 = worst). S5 withdrawn. V12 overvix gate is correct as-is. SC catches bottoms via VIX exemption (Mar 9: 11W/1L +152 pts). Cost/reward 1:4.5.
5. **Vol Event Detector BUILT and DEPLOYED** — 3-phase alert (compression/vol_event/vol_release) in main.py. Sends Telegram on phase transitions. Health endpoint shows current phase. Commit `0435192`, pushed.
6. **Educational HTML guide** — `vol_event_guide.html` explaining compression→event→release mechanics, thresholds, expert quotes, ASCII diagrams.
7. **ES Absorption deep study** — 305 trades. KEY: grading is ANTI-PREDICTIVE (C=62% WR, A+=0%). Best filter: C+LOG only + vol<3 = 61% WR +155 PnL. Volume 2-3x sweet spot. 14:30-15:30 dead zone. First 3 signals toxic. PARKED for user review.
8. **DD per-strike research** — Confirmed mechanical flip mechanics from volland_exposure_points data. Strike 6415 = $2.71B flip. 35 DD flips/day within ±10pts. Backtested (V2 timezone-fixed): only extreme flips >$3B are toxic (12t, 25% WR, -37 PnL). Bulk of DD profit is flow-based. PARKED for user DD self-learning.
9. **Copilot rules updated** — Now 37 rules. Added R32-R37 (DD instability, SPX>SPY DD, paradigm stability, SVB revised, macro override, DD whiplash). R35 revised (SVB level not predictive alone). S5 withdrawn.

**Bugs found:**
- **TIMEZONE BUG in DD backtest V1** — Used `+3 hours` to convert ET→UTC instead of raw UTC join. Produced OPPOSITE conclusion (mechanical = +33.7 vs corrected -20.5). User caught it. CRITICAL feedback saved. Rule: ALWAYS join on raw UTC timestamps, never manual hour offsets.

**Decisions:**
- V12 filter unchanged — overvix +2 gate is correct, S5 withdrawn
- ES Absorption improvements parked — user will review later
- DD per-strike filter parked — user will self-learn DD mechanics first
- Hunter Edmonds = Apollo in Discord (Volland co-founder)

**Files created/updated:**
- `copilot_session3_mar27.md` — full day analysis + 10 suggestions
- `copilot_market_rules.md` — 37 rules, session scores
- `research_es_absorption_study.md` — 305-trade deep study
- `research_dd_perstrike_mechanics.md` — flip mechanics + backtest
- `reference_hunter_is_apollo.md` — Hunter = Apollo
- `feedback_timezone_critical.md` — NEVER guess timezone in DB queries
- `vol_event_guide.html` — educational guide

---

## 2026-03-28 (session 52) — Volland Watchdog Thread

**What was done:**
1. Investigated Mar 27 Volland stale (died at 15:39 ET, 21 min before close)
2. Root cause confirmed: vol.land API slowness → route.fetch 30s timeout (already fixed in `e60e645`)
3. Confirmed Stock GEX Scanner is NOT the cause — TS API had zero gaps, Volland is separate service
4. Found Stock GEX Scanner has 0 rows saved (failing silently) — separate issue to investigate
5. Added **watchdog daemon thread** to `volland_worker_v2.py`: checks every 60s, if no save for 10 min during market hours → Telegram alert + `os._exit(1)` → Railway restart
6. Rollback tag: `stable-20260328-pre-watchdog`
7. Committed + pushed (`d7282eb`)

**Decisions:**
- Watchdog is the 5th defense layer (after route.fetch timeout, stale lastModified, 0-point, session expiry)
- Uses `os._exit(1)` not `sys.exit()` to bypass hung Playwright calls
- Timer resets outside market hours to prevent false triggers at open

---

## 2026-03-28 (session 51) — Communication Style in CLAUDE.md

**What was done:**
1. Added `# Communication Style` section to CLAUDE.md (plain English summaries, terse error reporting, short responses).

**Decisions:** None. Quick housekeeping session.

---

## 2026-03-28 (session 50) — S14 Verify + SC/AG Trail Study Confirmation

**What was done:**
1. **S14 completed** — all 4 checks PASS: SC gap=5 on #1290, 10K OHLC rows (27 days Feb 19-Mar 27), trail columns populated on Mar 27 trades, backfill endpoint run successfully.
2. **OHLC-based trail parameter sweep** — wrote `_trail_study.py`, ran full grid sweep for SC (5 SL × 4 ACT × 3 GAP) and AG (5 SL × 5 ACT × 4 GAP) using 1-min OHLC simulation.
3. **SC gap 8→5 CONFIRMED** — +94.4 pts improvement (630 vs 536), same WR (73.4%), PF 2.10 vs 1.93. P2T DD slightly worse (-89.5 vs -85.0) but acceptable.
4. **AG ACT=12 CONFIRMED** — best P2T DD (-46.5) vs ACT=15 (-64.2, 38% worse). Only -24 pts less PnL.
5. **Sim cross-check: 95% outcome match** (19/20 SC trades). Passes Gate 2.
6. **Eval trader AG ACT still =15** — stale, needs sync to 12 (not done this session).

**Decisions:** All current deployed params confirmed optimal. No changes needed.

---

## 2026-03-27 (session 49) — UI Fixes: EOD Review Layout + 0DTE GEX Chart

**What was done:**
1. **EOD Review side-by-side layout** — changed from vertical stack (details → chart) to side-by-side grid (details left 40%, chart right 60%). Responsive fallback to single column at <900px. Outcome boxes flex-wrap for narrower left column.
2. **0DTE SPX GEX chart strike limit** — SPX was showing unlimited strikes (proximity=125), making chart unreadable. Capped `all_levels` to 40 strikes centered on spot (20 below + 20 above) in `_compute_stock_gex()`. Doesn't affect key level computations, only chart display.
3. **Scheduled as S15** — code pushed to GitHub (`a48b3a8`) but NOT deployed. Deploy after 16:10 ET (no market-hours deploys).

---

## 2026-03-28 (session 48) — Volland Stale Data Auto-Recovery

**What was done:**
1. **Volland down (2nd time today)** — vol.land API intermittently timing out (Route.fetch 30s timeout). Data captured was stale (lastModified unchanged, pts=4564). Existing 0-points detector didn't trigger because pts > 0.
2. **Restarted Volland** — immediate fix, confirmed fresh save at 19:31 UTC.
3. **Stale lastModified detection (commit `028adf3`)** — new counter tracks unchanged lastModified during market hours. 5 cycles (~10 min) → page reload + Telegram alert. 8 cycles (~16 min) → full browser recreate. Recovery notification on fresh data. All reset paths covered.
4. **Route.fetch timeout + retry (commit `e60e645`)** — root cause was vol.land API slowness, not stale browser. Increased timeout 30s→60s, added 1 automatic retry per exposure fetch. This is the actual fix for today's issue.

**Key insight:** First fix (stale detection) addressed the wrong layer. Browser restart doesn't fix server-side API timeouts. The 60s timeout + retry is what actually prevents the outage pattern seen today.

---

## 2026-03-28 (session 47) — EOD Review Page

**What was done:**
1. **EOD Review page built** (`/eod-review`) — standalone full-page scrollable layout for end-of-day trade analysis. Shows all trades for a date expanded inline: daily summary banner, per-setup breakdown chips, then each trade as a card with info grid, outcome row, Plotly price chart (with entry/stop/target/LIS levels), score breakdown, and editable notes.
2. **Batch API endpoint** (`/api/setup/eod-review?date=YYYY-MM-DD`) — returns all trades for a date with full outcome details, price histories, ES bars, and levels in one response. Includes summary stats and per-setup breakdown.
3. **Strategy filters added** — full V12/V12-LE/V12-NT/V11/V10/V9/V8/V7+AG/SC+AG/SC Only filter dropdown, same logic as dashboard Trade Log. Summary banner recalculates dynamically on filter change. Daily gaps fetched for V12 gap filter.
4. **Dashboard link** — "EOD Review" button added to dashboard sidebar.

**Technical notes:**
- ES Absorption trades get candlestick + CVD overlay chart (same as detail modal)
- Cards are color-coded by result (green=WIN, red=LOSS, amber=EXPIRED, blue=OPEN)
- Expand/collapse all buttons, individual card toggle
- Comments save via existing `/api/setup/log/{id}/comment` API

---

## 2026-03-28 (session 46) — SC Trail Gap 8→5 + Analysis Infrastructure Overhaul

**What was done:**
1. **SC trail gap 8→5 deployed** — R7 task completed. Gap=8 was copy-paste from ES Absorption range-bar bug fix (Mar 2), never SC-studied. 0/13 clean losers touched activation → gap only affects winner capture. SL=14 and ACT=10 confirmed optimal. Changed in main.py (2 dicts), real_trader.py, eval_trader.py.
2. **4 contaminated Mar 26 SC outcomes cleared** — TS API outage trades still had outcomes. First clear didn't persist (Railway session issue), second attempt stuck.
3. **Analysis validation protocol** — CLAUDE.md mandatory 3-gate checklist (data quality → cross-check → presentation). `tools/validate_study_data.py` created with known outages, param change history, MFE outlier checks. Memory rule saved.
4. **`spx_ohlc_1m` table** — real tick-based 1-min SPX OHLC from TS barcharts API, pulls every 2 min. Eliminates TradingView exports, DST issues, and 2-min gaps. Backfill endpoint (`POST /api/spx/ohlc/backfill`) for historical data — scheduled S14 Monday 09:35 ET.
5. **Trail params per trade** — `trail_sl`, `trail_activation`, `trail_gap`, `exit_price` columns added to setup_log. Populated on every new signal. Eliminates era-guessing in analysis.
6. **Full SC trail parameter sweep** — 192 combos tested on OHLC sim + DB MFE-based analysis. Found DST bug in OHLC conversion, mixed SL eras, contaminated data — all caught by user, not Claude. Led to validation protocol.

**Bugs found & fixed:**
- DST timezone: OHLC bars used hardcoded UTC-4 instead of zoneinfo → 1hr shift for pre-Mar 8 data
- Mar 26 SC outcomes (4 trades, +181.8 pts) not cleared in original Mar 27 cleanup
- Railway `railway run` Python scripts may not persist DB commits reliably (first clear didn't stick)
- `setup_log_full.csv` export had truncated timestamps (MM:SS only, no hours)

**Decisions:**
- Gap=5 for SC (not 3 or 4) — 5 pts pullback room is safe margin for 30s cycle checks
- Filter version NOT stored per trade — filters are retroactive analysis, trail params are what change behavior
- Data quality flag NOT added — already handled by `data_ts` freshness gates from session 44

---

## 2026-03-27 (session 45) — Discord Analysis + Stock GEX 4-Bug Fix + Copilot Learning

**What was done:**
1. **Discord Mar 26 analysis** — 344 msgs from daytrading-central + 2 from 0dte-alerts. Apollo: globex long TP@09:48, "vol buyside" at 10:10 (preceded 90pt drop), 6475 target hit. Our system aligned (all shorts, 38 signals, 73.7% WR, +770 pts).
2. **Copilot rules updated** — 3 new rules (R29-31: vol buyside, liquidity pull, vol regime). 4 validated (R1, R2, R22, R25). Session score 6/10.
3. **Real trader review** — 9 trades/2 days, 2W/6L/1E. Mar 26 losses = TS API outage. V12 gap filter saved Mar 25.
4. **Discord Live Monitor (B12)** — researched user-token gateway approach. Read-only = very low detection risk. Saved as task.
5. **Stock GEX live: 4 critical bugs found and fixed:**
   - T2 below spot → instant false exits (BAC, AMAT). Now T2 must be > spot.
