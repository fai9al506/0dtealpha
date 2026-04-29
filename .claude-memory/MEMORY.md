# 0DTE Alpha - Key Notes

## User Preferences
- **Always commit & push after code changes** — don't wait for user to ask. If unsure, ask "want me to commit & push?"
- **NEVER assume values** — always use real data (snapshots, API, DB). If unavailable, confirm with user first. See `feedback_no_assumptions.md`
- **Never hide dashboard rows on null** — always show "n/a" or "error" so issues are visible. See `feedback_show_na_not_hide.md`
- **Always verify ALL SIM fills against theo prices** — TS SIM option exits are stale/fake. Check EVERY trade, not just outliers. See `feedback_verify_all_sim_fills.md`
- **Always use Railway CLI** for logs/status — if session expired, ask user to run `railway login`. See `feedback_railway_login.md`
- **PDF dark theme is default** — use Analysis #15 dark style for ALL PDF reports/charts. See `feedback_pdf_style.md`
- **Drawdown is a MAJOR factor** — when comparing filters, MaxDD and consistency matter as much as PnL. Prefer all-green trades over higher PnL with red streaks. See `feedback_dd_priority.md`
- **Backtest reporting standards** — always include PnL with duration, max DD, risk factor, and full setup details (entry/SL/target/trail). See `feedback_backtest_reporting.md`
- **Exhaustive checks** — when verifying completeness (all setups in dropdown, etc.), grep ALL instances in codebase first. See `feedback_exhaustive_checks.md`
- **Always verify filter from CODE** — never trust MEMORY.md/CLAUDE.md for active filter version. Read `_passes_live_filter()` directly. See `feedback_verify_filter_from_code.md`
- **Alignment is RELATIVE to trade direction** — +3 = all Greeks WITH the trade, -3 = all AGAINST. NOT fixed bullish/bearish. See `feedback_alignment_semantics.md`
- **Telegram only for live setups** — portal/logging setups collect data silently. Don't suggest enabling Telegram as "fix". See `feedback_telegram_live_only.md`
- **Always verify numbers from DB** — never present P&L/trade counts from manual calculations. Query DB first. See `feedback_verify_from_db.md`
- **User timezone is KSA (UTC+3)** — laptop, Discord exports all in KSA. KSA → ET = subtract 7h. See `user_timezone.md`
- **CRITICAL: Never guess timezone in DB queries** — ALWAYS join on raw UTC timestamps. Never add/subtract hours manually. EDT≠EST≠KSA. Bug produced OPPOSITE backtest results. See `feedback_timezone_critical.md`
- **NEVER deploy during market hours** — each deploy kills Rithmic/ES streams, causes missed signals. Day 1 cost us +$43.50 from a missed WIN. See `feedback_no_deploy_market_hours.md`
- **VX is a LEADING signal, not a filter** — per-tick clusters at key levels, NOT aggregate regime. Do NOT add to `_passes_live_filter()`. See `feedback_vx_leading_not_filter.md`
- **Schedule post-market deploys as Tasks.md entries** — if session dies before deploy, next session picks it up. See `feedback_schedule_deploys_as_tasks.md`
- **Use PowerShell for ET time, not Git Bash** — `TZ=America/New_York date` is broken on Windows (shows GMT). See `feedback_use_powershell_for_et.md`
- **MANDATORY analysis validation** — before ANY study: validate data quality (staleness, param changes, DST), cross-check sim vs DB (>10% = broken), state clean sample size + caveats. See `feedback_analysis_validation.md`
- **Self-fetch Railway values** — never ask user for env vars/tokens available via `railway variables`. Fetch yourself. See `feedback_self_fetch_railway.md`
- **Use flowcharts for complex logic** — decision trees/hierarchy charts in HTML reports, not just text or ASCII. See `feedback_use_flowcharts.md`
- **NEVER assume in reports** — only show branches/conclusions backed by actual data. Gap-down incident: added "normal trading" without any study. See `feedback_never_assume_in_reports.md`
- **KNOW THE SYSTEM before diagnosing** — 3 traders (SIM/real/eval), 3 brokers, different SLs. Verify from CODE not memory. See `feedback_know_the_system.md`
- **SYSTEM CHEAT SHEET** — complete config matrix for all components, setups, SLs, trails, filters, DB tables. READ THIS before any diagnosis. See `system_cheat_sheet.md`
- **LIVE TRADING STATUS** — Real+Eval ACTIVE, SIM+Options OFF. Verified Apr 2. See `project_live_trading_status.md`

## Copilot Self-Improving Playbook
- **`copilot_market_rules.md`** — 50 tactical rules (R1-R50, incl macro override, pre-speech vol, mean reversion, JPM Q2 collar, DD+ default bias). Pre-market checklist. 4 session scores (avg 5.1/10). Read EVERY session start.
- **`copilot_session3_mar27.md`** — Mar 27 deep analysis: 53 signals, V12 filter gaps, 7 concrete suggestions (SVB filter, ES Abs shorts whitelist, DD instability).
- **`copilot_reference_knowledge.md`** — deep mechanics, paradigm details, expert profiles (Apollo/Wizard/LordHelmet), validated calls. Read for unfamiliar patterns.
- **Trigger word "Copilot"** — activates market analysis mode. Pull data, apply rules, give conviction 1-5. See `feedback_copilot_trigger.md`.

## Volland Worker: v2 is ACTIVE, v1 is SUSPENDED
- `volland_worker_v2.py` = production (route-based capture, single workspace)
- `volland_worker.py` = legacy/suspended (JS injection hooks, two workspaces)
- When user says "volland" they mean v2. NEVER confuse the two.

## Telegram Research Channel
- **"0DTE Alpha Researchs"** — channel for HTML research reports/deep analysis. Chat ID: `-1003792574755`. Same bot token. See `reference_telegram_research_channel.md`
- Workflow: user says "send to Tel Res" → generate dark-themed HTML report with charts/tables → send as document via Telegram Bot API

## Railway Services & Plan
- **User is on Railway Pro plan** — do NOT suggest free/hobby limitations
- `0dtealpha` — web + delta worker (via Procfile)
- `Volland` (capital V) — separate service running `python volland_worker_v2.py` (NOT in Procfile)
- Use `railway logs -s Volland` and `railway restart -s Volland --yes`
- PostgreSQL database has scheduled backup capability (Pro feature)

## Common Issues
- **Volland stale after overnight**: sync phase hangs because page went stale. Fixed 2026-02-13 with page reload + 2min timeout.
- **Volland stale after long weekend**: route handlers stop intercepting. Auto-restart added 2026-02-17 (5 cycles → browser recreate).
- **Pipeline alerts not firing (v1)**: freshness query default is "closed", which was silently skipped during market hours. Fixed 2026-02-13.
- **Pipeline alerts not firing (v2)**: freshness query OR with `statistics IS NOT NULL` let empty `{}` pass as "fresh". Fixed 2026-02-17 — now requires `exposure_points_saved > 0`.
- **SQL cast vulnerability**: `(payload->>'exposure_points_saved')::int` hardened with CASE/regex.

## Volland Auto-Restart (added 2026-02-17)
- `ZERO_POINTS_THRESHOLD = 3` → Telegram alert at ~6 min
- `AUTO_RESTART_THRESHOLD = 5` → browser recreate at ~10 min
- Tracks `_total_pts == 0` (not `_zero_exps`) to catch empty exposure lists

## ACTIVE FILTER — V12-fix (deployed Mar 29 2026)

**V12-fix = V11 + longs-only gap block before 10:00. DEPLOYED ON SIM + E2T + REAL.**

**V12-fix changes (over V12):**
- **Rule A REMOVED:** V12 blocked longs all day on gap-up >+30. Study on V12-filtered data: those 32 longs had 72% WR, +58.9 pts. The original V12 study was on unfiltered data (112t, 38% WR) — V12 base filter already removes the bad longs.
- **Rule B fixed (longs-only):** Block LONGS only before 10:00 ET when |gap| > 30. Was blocking all trades (longs+shorts). Shorts before 10:00 = 71% WR, +47 pts — should not be blocked. Blocked longs = 29% WR, -68.8 pts.
- Gap computed once/day from chain_snapshots (yesterday's last price vs first-cycle spot).

**V11 rules (unchanged):**
- SC/DD blocked 14:30-15:00 ET (charm dead zone: 35% WR, -114 pts)
- SC/DD blocked 15:30-16:00 ET (too little time, mostly EXPIRED)
- BofA Scalp blocked after 14:30 ET (0% WR in 10 trades)
- **SC grade gate: only A+/A/B pass (C/LOG blocked).** Backtest: same PnL, 37% less MaxDD.

**V10 base rules (unchanged):**
- **Longs:** alignment >= +2 AND (Skew Charm OR VIX <= 22 OR overvix >= +2)
- **Shorts whitelist:** SC (all), AG (all), DD (align!=0). No VIX gate on shorts.
- **SC/DD shorts blocked when paradigm = GEX-LIS** (24t, 43% WR — LIS = support floor)
- **Overvix** = VIX - VIX3M. When >= +2: mean reversion signal (allow longs even at high VIX)

**Filter naming history:** R1 → Option B → V7 → V7+AG → V8 → V9-SC → V10 → V11 → V12 → **V12-fix (current)**

**VIX3M BUG FIXED (Mar 17):** Missing `global _vix3m_last, _overvix` in run_market_job(). Bug present since Mar 14.

## General Telegram Channel Cleaned (2026-03-24)
- Disabled: vol spikes, LIS changes, target changes, +/-gamma changes, paradigm change alerts
- Method: `alert_settings` DB table (toggle per alert type)
- Before: ~160 msgs/day of noise. After: ~7-9/day (summaries + pipeline + auth)
- All actionable alerts remain in Setups channel

## Discord Comparison #2 (Apr 1) — Rules Only, No Filter Change
- **Last sync: Mar 31.** Compared 700 msgs (Mar 27-31) vs 155 signals. See `project_discord_analysis.md`.
- V12-fix turns -29 → +79 on 7 trades. Already very efficient.
- VIX direction modifier REJECTED (EOD-based, unreliable in real-time).
- ES Abs whitelist REJECTED (16 trades/day too noisy, -95 MaxDD, grading inverted).
- **8 new copilot rules R38-R45** added (vanna vacuum, DD extremes, JPM collar change, etc).
- **ES Abs grading v2** needed — same inverted bug SC had (C=60% WR, A+=0%).

## Gamma & DD Per-Strike Research (Mar 20) — NO FILTER CHANGE
- Deep study: gamma stacked S/R, DD per-strike, EOD butterfly targeting
- **Result: Nothing strong enough to change V9-SC.** All findings are VIX confounds or too small sample.
- DD alignment boost (V4) showed +70.6 pts (15 new trades, 73% WR) but user decided to wait for more data.
- EOD butterfly: 50% direction accuracy, mechanical approach loses money. Needs discretionary timing.
- See `research_gamma_dd_perstrike.md` for full details and what to revisit later.

**Filter naming history:** R1 → Option B → V7 → V7+AG → V8 → V9-SC → V10 → V11

## Skew Charm — ENABLED (2026-03-08, grading v2 2026-03-22)
- 210 trades total. 140W/70L, 66.7% WR, +521.8 pts
- **Enabled on SIM auto-trader + both eval instances** (Mar 8)
- **SL changed 20->14 (Mar 18):** Same PnL, 27% less MaxDD. SL=14 optimal.
- Trail params: BE@10, activation=10, gap=8, initial_sl=14 (SIM), 12 (eval)
- Flow B split-target: T1=5@+10pts, T2=5 trail-only
- **Grading v2 (Mar 22):** Old grading was ANTI-PREDICTIVE (r=-0.19, A+=25% WR, LOG=78% WR). Root cause: charm and time scores were INVERTED. New scoring r=+0.32. Components: paradigm subtype (0-30), time INVERTED (0-25, morning=best), VIX (0-20), charm INVERTED (0-15, low=best), skew mag (0-10). New A=87% WR, A+=78% WR.
- **Paradigm subtype is #1 predictor:** GOOD (SIDIAL, GEX-PURE, AG-TARGET, BofA-LIS, AG-PURE)=84% WR. BAD (GEX-LIS, AG-LIS)=45% WR.
- **Per-strike charm near spot:** strongest differentiator (winners -8.3M, losers +10.2M). Needs work to convert to filter.

## SC Trail Gap 8→5 (deployed Mar 28)
- Gap=8 was copy-paste from ES Absorption range-bar bug fix, never SC-studied
- 0/13 clean losers ever touched activation → gap only affects winner capture
- SL=14 and ACT=10 confirmed optimal (unchanged)
- Expected improvement: capture 80% of MFE (was 65%)

## Analysis Data Infrastructure (deployed Mar 28)
- **`spx_ohlc_1m` table** — real 1-min SPX OHLC from TS barcharts API every 2 min. Backfill via `POST /api/spx/ohlc/backfill` (S14 Monday).
- **`trail_sl/trail_activation/trail_gap/exit_price`** columns on setup_log — snapshot config at signal time. `WHERE trail_gap = 5` instead of date guessing.
- **`tools/validate_study_data.py`** — run before any study. Checks outages, param changes, MFE outliers.
- **CLAUDE.md validation protocol** — mandatory 3-gate checklist before presenting analysis.

## Exit Strategy: Opt2 (Trail All) vs Opt3 (Split TP) — Mar 25
- **Opt2 beats Opt3 by +874 pts across 667 trades** — see `research_exit_strategy_comparison.md`
- **SC: SWITCHED TO OPT2** on real trader + portal (Mar 25). Trail-only, no partial TP@10.
- **AG: STAYS OPT3** for now. User prioritizes safety. Opt2 is +143 pts better but +5.4 MaxDD worse. Will switch later when ready for more risk.
- DD/GEX Long: still Opt3. Not yet analyzed for real money.

## Real Trader Config (Mar 25, updated Apr 6)
- **MAX_CONCURRENT_PER_DIR = 1** (accounts ~$1,880 after $1K top-up each, margin $687/MES)
- **MARGIN_PER_MES = $700** (TS intraday $686.75, rounded up)
- **SC only** on real trader. AG not enabled yet.
- **Opt2 for SC**: entry + stop only, no target limit. Trail advances stop via update_stop().
- **CRITICAL: NEVER fire-and-forget real money broker calls.** `_broker_submit` is SIM-only. Real trader `place_trade`, `update_stop`, `close_trade` MUST be synchronous — return results, handle errors inline. Silent failures = blocked slots + missed trades. Apr 6 bug cost $183 net (9 SC trades missed). See `feedback_never_fire_forget_real.md`.
- **Stale order cleanup:** `cleanup_stale_orders()` runs daily 09:28 ET, removes unfilled orders from previous days. Prevents weekend carry-over slot blocking.
- **force_release():** On outcome resolution, slot is freed synchronously BEFORE broker flatten attempt. Ensures slot availability even if broker call fails.

## Data Glitch Guard (Mar 25)
- Added ±20pt guard on spx_cycle_low/high in `_check_setup_outcomes`
- Rejects cycle extremes that diverge >20pts from spot (TS API glitch protection)
- Trade #1189 was the only affected trade (cleared from DB, backfilled correctly)

## Charm S/R Limit Entry — DISABLED (2026-03-24)
- **DISABLED:** 5-min OHLC backtest showed market orders beat all limit thresholds (+551 vs +85 pts best). 44% fill rate killed the edge.
- **All shorts now use MARKET orders** (same as longs).
- Code still exists in codebase but charm_limit_entry is no longer computed/passed.
- May revisit as a FILTER (not entry method) — e.g., block entries when charm S/R range is unfavorable.

## Same-Direction Stacking — CRITICAL BUG FIXED (2026-03-05)
- Deployed Mar 4. First live day: massive SIM loss ($2000+) while system showed +120 pts
- **ROOT CAUSE:** `_flatten_position()` used broker TOTAL qty (all stacked trades) instead of trade's own qty
  - Closing 1 stacked trade → over-closed ALL positions → orphaned stops created SHORT ghost positions
  - Ghost positions had no stops → bled money until EOD
- **5 bugs fixed:** (1) flatten uses trade's own qty, (2) integrity check after each close, (3) periodic orphan detects direction mismatch with active trades, (4) EOD cancels ALL orders first then closes position once, (5) EOD nuclear order sweep
- **`_alert()` is still a no-op** — SIM alerts suppressed, only `_alert_critical` sends Telegram

## 10pt Level Bug (fixed 2026-03-04)
- Trailing setups used `tp.get("activation")` for ten_pt_level instead of fixed 10
- DD Exhaustion showed 10pt at 20 pts away (activation=20), causing false MISS
- Fixed: `ten_pt_level = spot ± 10` always. Commit `b9f6e1e`.

## Setup Outcome Tracking (1,200 trades as of 2026-03-29, post-full-audit)
- **Unfiltered: +1,082.3 pts** across 1,200 resolved trades
- Skew Charm: 256t, 69% WR, +458.4 pts (MVP)
- AG Short: 80t, 69% WR, +365.2 pts (consistent)
- DD Exhaustion: 334t, 48.5% WR, +210.4 pts (volume workhorse)
- ES Absorption: 305t, 54.5% WR, +152.0 pts (#4 — was inflated to +280 before batch-scan correction)
- SB Absorption: 9t, 77.8% WR, +82.3 pts
- SB2 Absorption: 35t, 40% WR, **-46.2 pts** (LOSER — shorts 76% WR but longs 39% toxic)
- GEX Long: 63t, 36.7% WR, -73.5 pts. Blocked by VIX>22. Needs bull regime.
- **Mar 29 full audit:** 42 trades corrected across TWO bugs. See `project_bar_scanner_bug.md`.
  - Bug 1 (forming-bar): 17 false LOSSes → WIN (+281.4 pts)
  - Bug 2 (batch-scan): 21 false WINs → LOSS (-184.2 pts), 4 wrong P&L
  - Both bugs fixed and deployed.
- **Mar 26 cleanup (2026-03-27):** 42 outcomes cleared (37 TS outage + 5 SB2 abs_details bug).

## GEX Long — Force Alignment Rewrite (2026-03-08)
- **Old code was broken:** gap 20 (too loose), blocked below-LIS, 8pt stop (hit 83%), no alignment gate → 29% WR, -103 pts
- **New framework:** every level is SUPPORT (below spot) or MAGNET (above spot). All forces pointing UP = A+
- **Gate conditions:** GEX paradigm + |gap to LIS| ≤ 5 + +GEX ≥ 10 above + target ≥ 10 above
- **Force scoring (6 components, max 100):** LIS proximity (0-25), -GEX force (0-20), +GEX magnet (0-20), target magnet (0-15), LIS type bonus (0-10), time (0-10)
- **Key: below-LIS now ALLOWED** — LIS above = magnet pulling up (72% reach rate confirmed)
- **Stop: 8 pts**. Trail: BE@8, activation=10, gap=5 (backtest optimal: +127.7 pts, PF 7.14 vs fixed +67.4)
- **Grade thresholds:** A+ ≥ 85, A ≥ 70, A-Entry ≥ 50
- **CRITICAL: ES-SPX contamination bug** — original backtest (94% WR, +251 pts) mixed SPX spot (entry) with ES bars (forward sim). ES trades ~15-20 pts above SPX, inflating MFE and causing false instant target hits.
- **Corrected SPX-only backtest:** 17 trades, 50% WR (SL=12/T=15). Best combo SL=8/T=10 = 60% WR, PF 1.86, +45.4 pts
- **Paradigm subtypes (2026-03-08):** GEX-LIS=100% WR, GEX-PURE=67% WR, GEX-TARGET=25% WR (toxic), GEX-MESSY=0% WR
- **Blocked subtypes:** GEX-TARGET and GEX-MESSY filtered out in setup_detector.py (commit `fd37853`)
- **With Greek filter (charm>0/unknown + alignment≥+1):** ~70% WR, PF 2.80 on remaining GEX-LIS/PURE trades
- **Status:** Disabled on Eval Real, monitoring on SIM. Need 15+ live signals to validate.
- **ES Absorption RESTORED (2026-03-11)** — CVD Divergence rewrite was a failure (39% WR, -140 pts). Original code restored with alignment filter.

## GEX Velocity — Separate Setup (2026-03-19)
- **Separate from GEX Long** — does NOT modify original GEX Long (gap <= 5 unchanged)
- Fires when LIS surges +25 pts AND gap is 5-10 (range GEX Long misses)
- **Status:** Monitoring on SIM. See `project_vix_divergence.md` for VIX setup details.

## Double-Up Size Filter — Weekly Review
- [Details](project_double_up_study.md) — weekly check for SC long criteria combos worth 2x sizing. Best so far: BOFA-PURE +3 align = 94% WR (17t, needs 50+).

## Stock GEX Scanner — Data Collection (2026-03-21)
- `app/stock_gex_scanner.py` — completely isolated from 0DTE SPX pipeline
- Scans 23 stocks every 30 min during market hours (9:30-16:00 ET)
- Two expirations per stock: `weekly` (this Friday) + `opex` (nearest 3rd Friday)
- Data collection only — no alerts, no signals, no Telegram
- Uses TS API chain data (real gamma × OI), NOT Unusual Whales scraping
- DB: `stock_gex_scans` (with `exp_label`). API: `/api/stock-gex/*`
- See `project_stock_gex_scanner.md` for full details and next steps.

## Stock GEX Support Bounce — NEW STRATEGY (2026-03-21/22)
- **VALIDATED: 89% WR, +711% avg ROI, every month profitable**
- Entry: OTM weekly call at -GEX strike when stock dips 1% below -GEX
- Filters: ratio>3 + support below + magnet above + spot opened above -GEX + skip 09:30
- Same-day GEX (fresh OI each morning) is CRITICAL (stale Tuesday = 56% WR, same-day = 89%)
- Avg hold: 1-2 hours, capital: $200/trade, 1.2 trades/week
- Separate Telegram channel (not 0DTE SPX)
- Full details in `project_stock_gex_strategy.md`
- Data: `C:\Users\Faisa\stock_gex_data\` (ThetaData, local disk)
- **LIVE SCANNER BUILT (Mar 23-24):** See `project_stock_gex_live.md` for full details
- 56 stocks, streaming TS API, Unusual Whales-inspired page at `/stock-gex-live`
- GEX structure score (CLEAN/MIXED/MESSY), sort/filter on All Levels tab

## Approved Dashboard Style (2026-03-24)
- **Saved in `project_dashboard_style.md`** — full color palette, font specs, component rules
- Outfit body + JetBrains Mono for numbers only, high-contrast dark layers
- User wants to apply same style to main 0DTE dashboard (`/`) later
- Revert tag: `pre-gex-redesign`

## SB2 Absorption — v2 OR Gate (2026-03-25)
- Two-bar flush + recovery pattern on 5-pt Rithmic range bars
- Bar N-1: flush (vol>=1.2x OR delta>=1.3x, delta+price agree)
- Bar N: price reverses >=60% of flush bar range = absorption confirmed
- **v2 deployed Mar 25:** Gate AND→OR, cooldown 10→20, time 9:45-15:00, SVB key fixed
- OR gate catches fast bars with strong delta but low volume (missed 2 WINs on Mar 25)
- SVB<0 blocks (market dislocation) — #1 Volland filter for SB2 (PF 1.07→1.52)
- RM: SL=8, T=12, trail BE@10/act=20/gap=10
- Backtest: 132 sig/22d, **47.7% WR, +260 pts, PF 1.52, MaxDD=-80**
- **Portal/logging only** — collecting data. No Telegram, no auto-trade.
- Live data: 7 signals pre-v2 (6W/1L, 86% WR, +77 pts)

## Sierra Chart VolDetector — IN PROGRESS (2026-03-23)
- `C:\SierraChart\ACS_Source\VolDetector.cpp` — Apollo-style vol seller/buyer dots
- Net delta per bar: green below = vol sellers, red above = vol buyers. One dot per bar.
- Threshold ~150 for VIX works. Needs live market calibration to match Apollo's sparsity.
- **Parked** — user wants to revisit during live market hours with Apollo's real-time posts for comparison.

## VX Futures Research — COMPLETED (2026-03-25)
- 31 days, 164K ticks, 1,013 setups backtested. See `research_vx_futures_analysis.md`
- **VX as trade filter: HARMFUL** — AGAINST trades 55.9% WR (+467 pts), ALIGNED 44.1% (-174 pts)
- **VX price inverse: 72.1%** — real but may be mechanical VIX-SPX correlation
- **VX momentum: marginal** — seller exhaustion 56%, buyer exhaustion 47% (useless)
- **Why inverted:** Our setups are contrarian. They profit when institutional positioning (VX flow) is wrong.
- **Rithmic CFE: permission denied** on Paper Trading. User purchasing CFE data for next month.
- **Sierra SCID reader works** — `C:\SierraChart\Data\VXM26_FUT_CFE.scid`, binary parsing, full aggressor data
- **DO NOT add VX to `_passes_live_filter()`** — it would actively hurt performance

## VIX Data Provider — Databento (2026-03-28)
- [VIX Data Provider Decision](project_vix_data_provider.md) — Databento chosen for cloud VIX ticks, Rithmic/Sierra/TS rejected

## IV Momentum — NEW Setup (2026-03-21)
- Apollo's vol-confirmed momentum SHORT (from Discord analysis)
- Signal: spot drops ≥5pts in 10min AND put IV rises ≥0.05 at ATM strikes
- Marginal (+13 pts/month with proper TZ). LOG-ONLY, collecting data.
- See `project_iv_momentum.md` for full details.

## Vanna Butterfly — Grading v2 (2026-03-23)
- **40pt** call butterfly centered on max abs 0DTE vanna strike, fires ~15:00 ET (was 30pt)
- **GREEN vanna gate:** 72.7% WR (11t), RED=18.8% → grade "LOG". Pin sign is #1 predictor.
- GREEN+gap<=30 40pt: **80% WR**, +$53.68, ~$3,970/mo/contract
- Gap filter widened 20→30 (GREEN pulls price even from 25pts)
- Now logged to setup_log with butterfly-specific outcome (WIN/LOSS at expiry)
- **Portal/logging only** — blocked in `_passes_live_filter()`. NOT auto-traded, NO Telegram.
- See `vanna_pin_setup.md` for full backtest details.

## VIX Divergence — Replaces VIX Compression (2026-03-29)
- Two-phase: Phase 1 (VIX suppression during SPX move) + Phase 2 (VIX compression while SPX flat)
- **Both LONG + SHORT** (old was long-only). SHORT B-grade = 100% WR (5/5)
- SHORT: BE@8, trail@10/g5, VIX<26 gate. LONG: IMM trail gap=8. SL=8 both.
- Combined March: **+131 pts, PF 2.11, 58% green days, 36 signals**
- No SVB/vanna gates (Phase 1 strength IS the quality gate)
- **LOG-ONLY** — blocked in `_passes_live_filter()`. Collecting live signals.
- See `project_vix_divergence.md` for full details.

## SPX+SPY DD Combined Signal — Priority #1 (2026-03-24)
- Combine SPX + SPY DD Hedging for stronger directional signal
- Apollo validated live Mar 23: combined -6B preceded 60pt drop
- See `project_spx_spy_dd_combined.md` for implementation plan

## Discord Research Ideas (2026-03-23)
- Fixed strike vol for vanna, panic vs structural puts, vol spike pause, gap-day charm delay, FOMC filter
- See `research_discord_ideas_mar23.md`

## AI Trading Co-Pilot — DEFERRED (2026-03-22)
- Claude Code skills (zero cost) or Claude API (~$1-3/day) for morning briefs, signal commentary, Discord/news monitoring
- User interested but not ready yet. See `project_ai_copilot.md`

## Sierra Chart Vol Detector — PENDING (2026-03-21)
- `VolDetector.cpp` in `C:\SierraChart\ACS_Source\` — needs building on Monday
- Delta bars, CVD, absorption detection, divergence arrows
- Based on Apollo's "time and sales + LOB shifts" method
- See `project_sierra_vol_detector.md` for setup steps
- See `research_apollo_vol_detection.md` for ALL Discord insights on vol detection

## VIX Compression — REPLACED (2026-03-20, replaced 2026-03-29)
- Replaced by VIX Divergence v2. Old: 2 signals, 2 losses. SVB/vanna gates overfit.
- See `project_vix_compression.md` for why it failed.

## SIM vs Portal Gap (investigated 2026-02-28, FIXED 2026-03-01)
- **Root causes identified:** ghost trades, reversal closings killing winners, SPX/MES spread
- **FIXED:** Single-position mode deployed — no more reversals, no more force-closing winners
- **User's target:** 5 pts/day × 4 ES = $1K/day = $21K/month — achievable (system avg 26.4 pts/day)

## Missed-Stop Bug (fixed 2026-02-20)
- 3 trades had initial stop breached but live tracker missed it (30s polling gap)
- All 3 from before session H/L tracking was added (commit 84348bb)
- #62 GEX Long Feb 5: WIN+10 -> LOSS-8, #80 GEX Long Feb 5: WIN+20 -> LOSS-8, #139 DD Feb 19: WIN+20.3 -> LOSS-12
- Session H/L tracking now prevents this: `_spx_cycle_high/low` catches between-cycle breaches

## DD Exhaustion (updated 2026-03-22, grading v2)
- 289 trades total. 135W/154L, 46.7% WR, +311 pts (trail pushes avg win >> avg loss)
- **Continuous trail**: activation=20, gap=5 (contrarian setups need room)
- **Grading v2 (Mar 22):** Old grading random (r=-0.017). New scoring r=+0.296. Components: paradigm subtype (0-25), greek alignment CONTRARIAN (0-25, anti-alignment=best), VIX sweet spot 21-26 (0-20), DD shift magnitude (0-15), time (0-15). New A+=67% WR, A=58% WR.
- **DD is CONTRARIAN:** anti-alignment (align=-1) is the BEST signal (56% WR, +252 pts). Over-alignment (align=+3) is the WORST (41% WR, -184 pts). This is opposite to all other setups.
- **AG-LIS is toxic:** 28% WR, -94 pts (32t). AG-TARGET is best: 79% WR, +111 pts (19t).

## BofA Trail — Tested & Rejected (2026-02-19)
- Simulation showed extending hold time + adding trail dropped BofA from +51.2 to -24.7
- The 30-min expiry is protective: BofA scalps capture quick moves, holding longer lets them reverse
- Do NOT add trailing stops to BofA

## Eval Trader Phantom Position Fix (2026-02-26)
- **Root cause:** `reverse()` deleted position file before saving new one → crash left orphan NT8 position
- **Fixes:** float() cast for es_price, crash-resilient reverse(), startup Layer 0 recovery, reconcile_with_api() every 60s, reconcile_with_nt8() every 60s
- **NT8 PositionReporter:** `nt8_position_reporter.cs` — NOT YET COMPILING in NT8. Needs direct debugging on NT8 machine.
- **Sierra Chart backup plan:** If NinjaScript fails, try Sierra Chart DTC Protocol (TCP API for position queries)

## E2T Eval Trader (updated 2026-03-08)
- `eval_trader.py` — standalone local script, running on work desktop (24hr machine)
- **Multi-config support (2026-03-03):** `--config` CLI flag, state files derived from config suffix
  - SIM: `python eval_trader.py` → default files (backward compatible)
  - Real: `python eval_trader.py --config eval_trader_config_real.json` → `*_real.json` state files + `eval_trader_real.log`
- **Signal source: API** (`signal_source: "api"`) — polls `/api/eval/signals` on Railway directly
- **Two instances running simultaneously:**
  - SIM (`falde5482-sim`): 10 MES, relaxed limits, Greek filter ON
  - Real E2T (`falde5482tcp50d070084`): **8 MES**, strict compliance, Greek filter ON
- **Sizing (2026-03-08):** Real = 8 MES ($40/pt). Backtest: $488/day avg, pass in ~15 days, can absorb 2 max losses before daily limit. 10 MES too aggressive (1.7 losses to limit).
- **Greek filter:** `greek_filter_enabled: true` on BOTH. SIM=Option B (asymmetric), E2T=Option C (asymmetric + SVB<-0.5)
- **DD blocks REMOVED (2026-03-08):** Both proved to be sample-size artifacts at 476 trades
- E2T 50K TCP rules: daily loss $1,100 (buffer $100), EOD trailing drawdown $2K, max 60 MES, flatten 15:50 CT
- **Enabled setups (real):** AG Short, DD Exhaustion, Paradigm Reversal, Skew Charm, ES Absorption (ALL ENABLED)
- **Deploy dedup (2026-03-11):** 3-layer guard for Railway rolling deploy overlap (DB 90s, auto_trader 90s, eval_trader 120s)

## MES Auto-Rollover (added 2026-02-21)
- Both `auto_trader.py` and `eval_trader.py` auto-calculate front-month MES symbol
- Quarterly cycle: H(Mar), M(Jun), U(Sep), Z(Dec) — rolls ~8 days before 3rd Friday
- `ES_TRADE_SYMBOL=auto` (env var) or `nt8_mes_symbol: "auto"` (config)
- Current: MESH26 / MES 03-26 → auto-switches to MESM26 / MES 06-26 on ~March 12
- No manual rollover needed ever again

## MES Auto-Trader (updated 2026-03-08)
- **10 MES** contracts per trade, symbol auto-rolls
- **SAME-DIRECTION STACKING** (2026-03-04): Multiple positions allowed if same direction, opposite blocked.
- **GREEK ASYMMETRIC FILTER (updated 2026-03-11, Analysis #9):** F1-F6 rules
  - F1: Charm alignment gate (all setups), F2: GEX Long align >= +1, F3: AG Short align != -3
  - F4: DD Exhaustion SVB weak-negative block
  - F5+F6: ASYMMETRIC — Longs: align >= +3. Shorts: block ES Abs (all), BofA (all), DD align=0. No general alignment filter on shorts.
- **Margin pre-check (2026-03-07):** `_get_buying_power()` queries account balance before each trade. Skips if BP < $27,370 (10 MES × $2,737). Prevents cascade of rejected orders.
- **EOD flatten retry (2026-03-07):** 3s initial wait after cancellations, 4 retry attempts (0/3/5/10s waits), re-checks position each retry, detects rejection in response, critical alert if all fail. Prevents naked overnight positions.
- **`flatten_account_positions()` rejection handling (2026-03-07):** Now detects Error=FAILED in TS response instead of silently treating rejected close as success.
- Flow A (BofA/Paradigm): entry + stop + single limit @ +10pts
- Flow B (GEX/AG/DD/Absorption): entry + stop + T1=5@+10 + T2=5@full target (DD/Absorption: trail-only)
- **T1 fill → stop moves to breakeven + commissions** (0.50 pts offset)
- SIM account `SIM2609239F`, balance $50K
- **MESH26 expires ~March 20** → update to MESM26

## SPX 0DTE Options Trader (updated 2026-03-19)
- `app/options_trader.py` — self-contained, same pattern as auto_trader.py
- Equities SIM account `SIM2609238M` (separate from futures SIM `SIM2609239F`)
- **CREDIT SPREAD strategy (2026-03-19, default):** Sells ATM credit spreads instead of buying options
  - Bullish = bull put spread (sell ATM put + buy lower put)
  - Bearish = bear call spread (sell ATM call + buy higher call)
  - Theta works FOR us (no 90-min timeout needed, hold to setup resolution)
  - Two separate orders: SELLTOOPEN + BUYTOOPEN, tracked as single spread in state
  - **Backtest Mar 18:** credit $2-wide +$587 vs single-leg -$79 (26 trades, +132 setup pts)
  - **Root cause of single-leg loss:** theta ate -$742 across 26 trades ($5/SPX-pt at 0.50 delta on SPY)
  - **ATM (0.50 delta) is best for credit spreads** — ITM hurt at 50% WR, OTM too little credit
- Config: `OPTIONS_STRATEGY=credit_spread`, `OPTIONS_SPREAD_WIDTH=2`, `OPTIONS_TARGET_DELTA=0.50`
- State fields: `strategy`, `short_symbol`, `long_symbol`, `theo_credit`, `theo_debit`, `theo_pnl`
- main.py updated: `/api/options/log` uses pre-computed `theo_pnl`, commission 4-leg for spreads
- Single-leg mode still available via `OPTIONS_STRATEGY=single_leg`
- `_get_option_quote()` uses live TS API (`api.tradestation.com`) for both legs
- **TS SIM fills are FAKE** — ALWAYS use theo prices. See `feedback_verify_all_sim_fills.md`

## Real MES Accounts — SC Strategy (2026-03-19)
- **210VYX65** — Account A (SC Longs only), 1 MES, cap=2 concurrent
- **210VYX91** — Account B (SC Shorts only), 1 MES, cap=2 concurrent
- Total capital: $7K ($3,500 each). Scale to 2 MES at $13K.
- See `project_real_mes_accounts.md`

## Real SPY Options Account (2026-03-14)
- **TradeStation account #11697180, funded $4K** — see `project_real_spy_account.md`
- V9-SC filter, 1 SPY per signal, 0DTE at ~0.50 delta (ATM)
- **2-week validation period** (~Mar 14-28) — tracking logs only, no live trades yet
- Backtest: +$3,135/month, 91% ROI, worst day -$432
- Scale to 2 SPY at $6,894 balance

## V2 Dashboard (2026-03-15)
- `app/dashboard_v2.py` at `/v2` — modern trading cockpit, separate from original `/dashboard`
- Signal bar with audio alert, KPI cards, actionable overview
- **TO DELETE:** remove `app/dashboard_v2.py` + 31 lines from `app/main.py`. See `project_v2_dashboard.md`
- Commits: `a549265`, `98febfe`, `82845eb`

## Manual Trading Plan (2026-03-15)
- FundingPips $36/$5K eval (MT5/cTrader) for manual trading
- Sierra Chart via brother's E2T Rithmic credentials (avoids concurrent session conflict)
- See `project_manual_trading_plan.md`

## Rithmic ES Stream (LIVE on Paper Trading)
- **Status: LIVE** — conformance passed, switched to Rithmic Paper Trading
- Railway env vars: `RITHMIC_USER=faisal.a.d@msn.com`, `RITHMIC_SYSTEM_NAME=Rithmic Paper Trading`, `RITHMIC_URL=wss://rprotocol.rithmic.com:443`
- `RITHMIC_CONFORMANCE` removed (was only for test phase)
- Telegram alerts on connect AND disconnect
- Files: `rithmic_es_stream.py`, 4 integration points in `app/main.py`, `async-rithmic` in requirements.txt
- Writes to `es_range_bars` table with `symbol='@ES-R'`, `source='rithmic'`
- Key feature: exchange aggressor field (BUY=1/SELL=2) instead of bid/ask inference
- **Trade aggregation** (added 2026-02-24): `_agg_buf` combines Rithmic sub-fills using `aggressor_exchange_order_id` to match ATAS trade counting
- **ES Delta tab** uses Rithmic as primary, TS quote stream as fallback
- **DB storage**: `es_range_bars` table stores OHLC, volume, buy_vol, sell_vol, delta, CVD OHLC, timestamps per bar

## Rithmic Historical Data
- **Script:** `tmp_rithmic_hist.py` — pulls RTH ticks, aggregates sub-fills, builds 5-pt range bars, saves JSON/CSV
- **Aggressor mapping (historical):** `bid_volume = buyer aggressor`, `ask_volume = seller aggressor` (confirmed vs ATAS)
- **Aggressor mapping (live):** `aggressor=1=BUY` (exchange aggressor enum) — consistent with historical
- CME MDP 3.0 unbundling: Rithmic splits per-fill, reaggregate by (timestamp, price, side) for historical, by `aggressor_exchange_order_id` for live
- ATAS CVD doesn't reset at RTH open — carries overnight delta. Our RTH-only data starts at 0 (both correct, different baseline)
- `async_rithmic` tip: use `client.connect(plants=[SysInfraType.HISTORY_PLANT])` for history-only pulls (avoids multi-plant conflicts)
- Rithmic Paper Trading may be unstable during off-peak hours — pull during market hours
- **Pending:** Pull Feb 23, verify vs ATAS, then pull all dates for backtest alongside Volland data

## EOD Report (added 2026-03-03)
- `app/eod_report.py` — self-contained, no imports from main.py
- Sends at 16:05 ET via `_send_setup_eod_summary()` in main.py
- **Chart:** ES candlesticks + all setup entries + stats sidebar + PnL curve. Sent as `sendDocument` for zoom.
- **PDF:** Dark-themed A4. Page 1 = KPI cards + chart. Page 2+ = trade log with Reason column.
- Dependencies: `fpdf2`, `matplotlib`
- Gotcha: fpdf2 Helvetica = latin-1 only (`_sanitize()`). Compare bar/trade timestamps in UTC (not mixed ET/UTC).

## Economic Calendar (added 2026-02-24)
- `economic_events` table: ts, title, country, impact, forecast, previous, actual
- Source: `nfs.faireconomy.media/ff_calendar_thisweek.json` (free, no API key)
- Cron: Monday 8 AM ET + on startup. Upsert logic (no duplicates)
- API: `/api/economic-calendar?country=USD&impact=High`
- Purpose: correlate trade outcomes with CPI/FOMC/NFP events

## ES Absorption Rewrite (2026-02-25) + Zone-Revisit (2026-02-26) + Split-Target (2026-02-27)
- **Swing-to-swing CVD divergence** replaces trigger-vs-swing (was firing opposite signals)
- 4 swing patterns: sell_exhaustion (dominant 64% WR), sell_absorption, buy_exhaustion, buy_absorption (toxic 23% WR)
- **Zone-revisit detection** (added 2026-02-26): tracks CVD at each 5-pt price zone, fires when price returns after 5+ bars with significant CVD change. 2 patterns: zone_accumulation (bullish), zone_distribution (bearish)
- `abs_max_trigger_dist: 40` setting (trigger bar within 40 bars of recent swing pair)
- **Grading: 7-factor composite** (updated 2026-02-26): div 25%, vol 25%, DD 10%, paradigm 10%, LIS proximity 10%, LIS side 10%, target direction 10%
- **Split-target (2026-02-27):** T1=+10pt fixed, T2=trailing (BE@+10, gap=5). Both tracked independently. Trail params: `{"mode": "hybrid", "be_trigger": 10, "activation": 10, "gap": 5}`
- **Performance:** 83.3% WR, +35.8 pts (7 trades on Feb 26). Trail analysis: +111.5 pts vs fixed +60.0
- **Auto-trader:** ES Absorption now in Flow B (split target, trail-only T2). Was Flow A (single target).
- **Eval trader:** ES Absorption target changed to `null` (trail-only), added to `_TRAIL_PARAMS`
- **Pattern priority tiers** (added 2026-02-27): Exhaustion=T2 beats Absorption=T1 when both fire on same bar. Prevents misreads (e.g. trade #275). Score tiebreak for same tier. Rejected divergence logged.
- **abs_details JSONB** column on setup_log: stores all divergences, swing pairs, tier resolution for analysis
- **Signal frequency too high** (~10/day). User wants 1-3/day with 70%+ WR. Iterative refinement.
- **CRITICAL: es_range_bars data contamination** — Feb 24+ has overlapping bar_idx from live+rithmic sources. ALL queries MUST filter by source
- Rithmic historical pull blocked by concurrent session limit.

## Vanna Pin Setup — NEW RESEARCH (2026-03-05)
- **Concept:** Max absolute 0DTE vanna near spot = pin/magnet strike
- **Backtest:** 93% within 10 pts at EOD (15 days), beats charm (71%)
- **Best strategy:** 20pt butterfly centered on pin, 60% WR, PF 6.6x, +$4.64/trade
- **Data changes deployed:** Theta/Vega now saved to chain_snapshots, strikes widened 125→200, saved 40→60
- **Status:** Collecting data. Need 2-4 months before live implementation
- See `memory/vanna_pin_setup.md` for full research details

## Chain Snapshots — Enhanced (2026-03-05)
- Now saves **Theta + Vega** per strike (were fetched but dropped before)
- Strike range widened: 200 pts proximity (was 125), 60 strikes saved (was 40)
- Old snapshots keep 21 cols, new have 25 — backwards compatible (JSONB per row)
- Enables future butterfly/IC pricing from historical data

## GEX Long Vanna Filter (discovered 2026-02-27, DEPLOYED 2026-03-01)
- Negative aggregated vanna ALL = 0% WR for GEX Long. Deployed on SIM.

## Railway Database (checked 2026-03-04)
- 250 GB volume, 3.13 GB used, ~71 MB/day growth
- `volland_exposure_points` = 86% of DB

## TS SIM Account Verification (STANDARD METHOD)
- **Always use TS API** to check account P&L — NOT Railway logs (logs can miss trades or lack detail)
- Script: `tmp_sim_pnl.py` — run via `railway run -s 0dtealpha python tmp_sim_pnl.py`
- Pulls `/v3/brokerage/accounts/{id}/orders` with `since` param, `/balances`, `/positions`
- FIFO position tracking to compute trade-by-trade realized P&L
- Fields: `Legs[0].BuyOrSell`, `Legs[0].ExecQuantity`, `FilledPrice`, `CommissionFee`
- Futures SIM: `SIM2609239F`, Options SIM: `SIM2609238M`
- SIM API base: `https://sim-api.tradestation.com/v3`

## SPY Option Chain Integration (2026-03-10)
- **Completely isolated** from SPX — separate table, globals, scheduler job, lock
- DB table: `spy_chain_snapshots` (same schema as `chain_snapshots`, NO shared columns)
- Globals: `latest_spy_df`, `_spy_df_lock`, `_last_spy_run_status`, `_last_spy_saved_at`
- Scheduler: `run_spy_market_job()` runs at same interval as SPX, own thread
- SPY params: symbol=`SPY`, strike_interval=1, strike_proximity=25
- `get_0dte_exp(symbol)` and `get_chain_rows(symbol, strike_interval, strike_proximity)` parameterized with backward-compatible defaults (SPX unchanged)
- API: `/api/snapshot?symbol=SPY`, `/api/history?symbol=SPY`, `/download/history.csv?symbol=SPY`
- Portal: `/table` has SPXW/SPY toggle buttons
- NOT used by setup detection, auto-trader, or eval trader — analysis/portal only
- Rollback tag: `stable-20260310-spy-before-push`

## Telegram Message Format (updated 2026-03-10)
- All 7 format functions: 3-line concise format with alignment display
- Format functions accept `alignment=None` param, rebuilt in main.py via format function mapper after alignment computed
- GEX/AG show +GEX/-GEX levels, CVD shows CVD gap
- Grade upgrades: 1-line format
- EOD summary: compact with alignment per trade, direction[:1] for L/S/B

## Outcome Stop Level Fix (2026-03-10)
- `outcome_stop_level` now stores INITIAL stop (never mutated), not the trailed stop
- Trail exit price stored in `outcome_target_level` for trailing WINs (as T2)
- `initial_stop_level` key added to trade dicts at creation time
- Commit `8024ebc`

## URGENT: V8 VIX3M Verification (next market open)
- **MUST CHECK** `/api/health` for `vix3m` field when market opens
- If null → `$VIX3M.X` not valid on TradeStation → find alternative symbol
- If overvix stays null, V8 gate still works (blocks all VIX>26 longs) but overvix override disabled
- See `project_v8_vix3m_verify.md` for full checklist

## Feedback
- [Credit spreads must be atomic multi-leg orders](feedback_credit_spread_atomic.md)

## Deployment
- Railway auto-deploys on push to main — do NOT ask "want me to deploy?" Just commit and push.
