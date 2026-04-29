# Session Log

## 2026-04-07 (session 69) — Full Setup Assessment: Mar 1 - Apr 2

**What was done:**
1. Comprehensive setup assessment for all setups, March + first week of April (24 trading days, 1,164 trades).
2. Queried DB for both unfiltered AND V12-fix filtered numbers for every setup — side-by-side comparison.
3. Generated dark-themed HTML report sent to Telegram Research channel (twice — first unfiltered only, then with V12 comparison per user request).

**Key findings:**
- **V12-fix filter effectiveness confirmed:** cuts 73% of trades, ADDS +313 pts (+28%), WR 55.7% → 70.2%
- **SC V12:** +768.5 pts, 74.5% WR, -82.3 MaxDD (anchor setup, keep on Real+Eval)
- **AG V12:** +345.0 pts, 82.1% WR, -60.0 MaxDD (recommended for Real Money)
- **DD V12:** -45 UF → +276.3 V12 (filter transforms loser into winner, keep on Eval, fix grading)
- **ES Abs:** +390 UF but only +9 V12 (99% blocked — not in shorts whitelist, biggest opportunity)
- **Paradigm Reversal:** 0 trades pass V12 (dead weight on Eval, should remove)

4. SC reliability deep-dive at user's request — user concerned about declining weekly PnL (363→35).
   - Week 1 was not just big wins: base WR was 90.5% (38W/4L), even without >20pt winners = +243.9 pts
   - BUT 82% of total SC PnL comes from big winners (>15pts): +805 full vs +148 without
   - Decline correlates with VIX: VIX<25 = 87-90% WR, VIX>25 = 42-52% WR
   - Week 3 counter-proof: +208.7 pts with only 1 big win, 87.5% WR (proves base works)
   - Conclusion: SC is regime-dependent (thrives at VIX<25), not broken. Keep on Real.

**Action items identified (not implemented this session):**
1. Enable AG Short on Real Money
2. Remove Paradigm Reversal from Eval config
3. ES Absorption grading V2 rewrite + add to V12 shorts whitelist
4. DD grading V2 rewrite

**No code changes this session — analysis only.**

---

## 2026-04-06 (session 68) — Real Trader Missed Trades: _broker_submit Fire-and-Forget Bug

**What was done:**
1. Real trader wasn't placing trades on Apr 6. Investigation found TWO bugs:
   - **Bug 1 (stale order):** Order #1540 from Apr 2 (unfilled SC short) stayed in `_active_orders` over the weekend. Blocked all shorts via MAX_CONCURRENT_PER_DIR=1 until deploy at 10:30 ET.
   - **Bug 2 (silent close_trade failure):** Trade #1559 placed at 10:50, but `close_trade` via fire-and-forget `_broker_submit` silently failed, keeping the slot blocked for the rest of the day. 9 SC trades missed (+$253 potential, $183 net).

2. **Root cause:** Commit `70339dd` (Apr 1, session 63 market job timeout fix) moved ALL broker calls to fire-and-forget `_broker_submit` thread pool. This was correct for SIM auto-trader (non-critical) but catastrophic for real_trader (real money). Full audit found 6 operations affected, 3 HIGH risk.

3. **6 fixes deployed:**
   - `2082536` — Daily stale-order cleanup at 09:28 ET (catches orders from previous days that never resolved)
   - `f763f5f` — `force_release()` synchronous slot free on outcome resolution (don't wait for broker)
   - `caf2201` — `close_trade` still flattens position after force_release
   - `f91c043` — **Revert ALL real_trader calls to synchronous** (place_trade, update_stop, close_trade). NEVER fire-and-forget real money operations.
   - `f23aa9d` — Fix deadlock in pre-market cleanup and stale-order cleanup (_persist_order inside _lock)
   - `c8f8a80` — Fix 2 more deadlock sites (ghost_reconcile + periodic_orphan)

4. Also deployed fixes from session 67 that were pending: entry_base undefined bug, misfire_grace_time on scheduler jobs.

5. Eval trader found offline since Mar 30 — user said ignore for now.

6. User transferred $1K to each TS account (210VYX65 + 210VYX91).

7. Final audit: all 7 checks PASS, real trader clean and ready for Apr 7.

**Key architectural rule established:** `_broker_submit` (fire-and-forget) is ONLY for SIM auto-trader. Real trader (`real_trader.py`) MUST use synchronous broker calls — `place_trade`, `update_stop`, `close_trade` all return results and handle errors inline. Silent failures in real money = missed trades and blocked slots.

**Commits:** `2082536`, `f763f5f`, `caf2201`, `f91c043`, `f23aa9d`, `c8f8a80`

---

## 2026-04-06 (session 67) — V12-LE Filter Fix + Bug Investigation

**What was done:**
1. User noticed 5 V12-LE trades in portal that didn't fire on TS Real or Eval. Investigated thoroughly.
2. **V12-LE portal filter fix:** Grade gate was too strict — only allowed A+/A/B but real `_passes_live_filter()` only blocks C/LOG. Fixed in both dashboard and review page. A-Entry now passes through. Committed + pushed (`7497dd1`).
3. **Root cause of non-fires:** 3 of 5 SC shorts blocked by GEX-LIS paradigm (correct behavior), 1 passed filter but likely hit MAX_CONCURRENT_PER_DIR=1 cap, 1 passed and may have fired. All 5 were -14.0 LOSS — filter saved money.
4. **S22 Stock GEX verification: FAIL.** Zero rows in `stock_gex_scans` table EVER. Scanner initializes fine but cron jobs never execute.
5. **Two bugs found:**
   - `entry_base` undefined in `_compute_setup_levels()` for Vanna Pivot Bounce (line 3856). Fix: change to `spot`.
   - Stock GEX cron jobs silently misfiring — APScheduler default `misfire_grace_time=1s` drops jobs when scheduler thread pool is busy. Fix: add `misfire_grace_time=300`.
6. **Fixes prepared but NOT deployed** — user said done in another session.

**Committed:** V12-LE filter fix (portal-only, no trading logic impact).
**Pending:** entry_base fix + stock GEX misfire fix (done in other session per user).

---

## 2026-04-06 (session 66) — Copilot Session: Apr 2 Liberation Day

**What was done:**
1. Copilot mode activated for Apr 2 market. Full pre-market checklist applied.
2. **Morning brief (09:38 ET):** Called SHORT bias conviction 3/5. Gap -78.2 pts, SIDIAL-EXTREME paradigm, DD -$2.65B, vol event triggered (overvix +2.78), VIX ~29.5. Warned about headline reversal risk (Liberation Day tariffs).
3. **Midday update (13:27 ET):** Massive V-reversal — SPX rallied +89 pts from LOD 6,483 to 6,572. DD flipped from -$2.65B to +$16.4B (beach ball R3). Paradigm flipped 4 times (SIDIAL→GEX-PURE→GEX-LIS cycling). Shifted to LONG conviction 2/5 with level-to-level caution.
4. **Portal outcomes (15 resolved):** 6W/9L, -14.3 pts unfiltered. V12-filtered: SC B short LOSS -14, DD A short LOSS -12, DD A+ short WIN +15.5, DD A+ short LOSS -1 (trail saved). Real trader had no fills in logs.
5. **Self-score: 5.5/10.** Morning short correct, warned beach ball, but didn't lead with "expect V-reversal on gap-down headline day." CVD stayed negative (-4,162) despite +89 rally = absorption confirmed.

**New validations:**
- R3 beach ball: RE-VALIDATED. DD -$2.65B → +$16.4B. Extreme DD + gap = snap-back.
- R34 paradigm instability: VALIDATED. 4 flips = unstable, no paradigm call reliable.
- R40 extreme DD: DD +$16.4B = forced buying pressure into close (validated directionally).

**No code changes. No memory updates needed beyond session log + copilot score.**

---

## 2026-04-06 (session 65) — Quick Health Check

**What was done:**
1. User received two Telegram alerts after deploy: Rithmic reconnect + "TS API data 0 min old — not updating".
2. Checked Railway logs for both 0dtealpha and Volland — all healthy. Chain pulls OK, Rithmic streaming, Volland capturing all 10 exposures.
3. Health endpoint confirmed `healthy`.
4. Investigated DD SPY = n/a on dashboard. Found it's a timing race — SPY paradigm response occasionally arrives after Volland save_cycle(). Self-corrects next cycle. Combined DD still works (uses cached SPY value).
5. No code changes needed. Both issues were transient from deploy restart.

**No changes to code, config, or memory.**

---

## 2026-04-02 (session 64) — S19 Investigation + System Knowledge Fix

**What was done:**
1. Investigated S19 (trade #1352 — portal WIN +36.3 vs real LOSS -$70). Initially misdiagnosed as trail resilience bug.
2. User corrected 3 times: (a) eval SL=12 ≠ real SL=14, (b) SPX-MES basis slippage explains the 2pt difference, (c) verified from TS statements — SL hit at 6:51 KSA (11:51 ET), legitimate stop.
3. **S19 CLOSED** — not a bug. Normal ~2pt basis slippage at VIX 30. MES hit 14pt SL while SPX max adverse was only 12.5.
4. **S21 CLOSED** — already deployed (commit `e29711e`, Mar 31). ES price REST fallback live.
5. Built `system_cheat_sheet.md` — comprehensive reference: all components, SLs, trails, filters, price spaces, DB tables, API details, scheduler jobs, env vars.
6. Added Diagnostic Protocol to CLAUDE.md — mandatory "rule out basics before investigating" checklist.
7. Saved `feedback_know_the_system.md` — 4-step pre-diagnosis process.
8. Added `system_cheat_sheet.md` as mandatory Step 5 in Session Start protocol (CLAUDE.md).
9. Verified live trading status from Railway: Real (ACTIVE, SC only) + Eval (ACTIVE, 6 setups). SIM + Options both OFF.
10. Saved `project_live_trading_status.md` with current system status.

**Key lesson:** Simple explanations first (basis slippage, config diff), investigation second. Reading the cheat sheet before diagnosing would have saved 30 min.

**Tasks updated:** S19 closed, S21 closed, Tasks.md updated.

**Pending for next session:** S22 (verify Stock GEX at 10:00 ET Apr 2), S3-S7 periodic data checks.

---

## 2026-04-01 (session 63) — Market Job Timeout Crisis Fix

**What was done:**
1. Diagnosed market job 90s watchdog timeouts causing Telegram error→recovered spam every 2 min.
2. Deployed 3 rounds of timing instrumentation to isolate bottleneck.
3. **Root cause:** Broker API calls (close_trade, update_stop, place_trade) ran synchronously inside market job — up to 150s blocking. Stock GEX (59 stocks) also saturated TS API.
4. **Fix:** All broker ops now fire-and-forget via `_broker_executor` (3-thread pool). HTTP session refresh every 30min + explicit connect timeouts.
5. Stock GEX was fully broken (all 404s, zero DB rows ever saved). Fixed: 59→14 stocks, Friday-only expirations for non-0DTE symbols.
6. Stock GEX opex scan staggered 10:00→10:30 ET.
7. Result: cycles dropped from 90s+ to 6-8s, zero timeouts rest of day.

**Commits:** `ceb5be3` (timing), `f9889d3` (HTTP session), `32ec1ca` (debug timing), `70339dd` (async broker), `2273e24` (stock GEX fix)

**Pending:** S22 — verify Stock GEX fix at 10:00 ET Apr 2.

---

## 2026-04-01 (session 62) — Discord Comparison #2 (Mar 27-31)

**What was done:**
1. Extracted ~700 Discord messages (Mar 27-31) from Volland daytrading channel. Cross-referenced expert calls vs our 155 system signals.
2. Applied V12-fix manually to all 155 trades: V12-fix turns -29 pts → +79 pts on just 7 trades. Very efficient filter.
3. Tested **VIX Direction Modifier** (allow longs when VIX crushing): +173 pts on 97 trades across 9 CRUSH days in March. **REJECTED** — based on EOD VIX measurement, useless in real-time. Even "VIX open vs current" is unreliable (VIX can reverse midday).
4. Tested **ES Absorption both directions whitelist**: +182.8 pts (319 trades, 54% WR) in March. **REJECTED** — 16 trades/day too noisy, -95 MaxDD unacceptable, grading inverted (C=60% WR, A+=0%).
5. Added **8 new copilot rules (R38-R45)**: vanna vacuum (Dark Matter), charm zero-crossing (Dark Matter), DD extremes (Apollo), vol event near-miss (Wizard), CTA exhaustion (Yahya Z), vanna cascade (BigBill), multi-day vol (jk23), JPM collar CME change (Zack).
6. Identified ES Abs grading is inverted (same bug SC had) — needs v2 rewrite as future task.
7. Updated `project_discord_analysis.md`, `copilot_market_rules.md`, `MEMORY.md`. Synced to Google Drive.

**Key finding:** V12-fix already closes ~90% of the gap with Discord experts. The remaining 10% is regime-reading (DD, vanna, VIX direction) that experts do holistically — can't reduce to a simple filter yet. Best captured as copilot rules for discretionary use.

**Next session:** ES Abs grading v2 study. Next Discord sync due ~Apr 7.

---

## 2026-03-30 (session 61) — Environment Setup (New Machine/Context)

**What was done:**
1. Created Claude Code memory directory, copied 85 memory files from Google Drive.
2. Added bash aliases (`0dte`, `claude`).
3. Read full session state — no code changes, no new decisions.

**Next session:** VPS setup completion (S17/S18). Market opens 6 PM ET Sun.

---

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
