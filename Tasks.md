# Tasks

Pending tasks, research, and implementation ideas for 0DTE Alpha.
Two types: **Scheduled** (time-based, checked every session) and **Backlog** (do when free).

Last updated: 2026-03-28

---

## SCHEDULED TASKS (Check Every Session)

These tasks are time-sensitive. Claude checks them at session start and alerts if due.

| # | Task | Trigger | Action | Status |
|---|------|---------|--------|--------|
| S1 | **Mar 25 deployment verification** | First market day after Mar 25 | All 5 checks PASS. SPY DD flowing, gap=+64.2 (longs blocked), combined DD working. Two bugs found+fixed: (1) gap SQL used wrong column, (2) SPY DD not reaching setup detector. | DONE 2026-03-25 |
| S2 | **SB2 Absorption data check** | Every 5 trading days | 35 trades audited Mar 29: 14W/21L, 40% WR, -46.2 pts = LOSER. Shorts decent (76% WR, +78.5) but longs toxic (39% WR, -69). Two outcome bugs found+fixed (forming-bar + batch-scan). Only 4 days of data — keep collecting, consider shorts-only. | DONE 2026-03-29 |
| S3 | **IV Momentum data check** | Every 5 trading days | 0 signals. Gate conditions nearly impossible with noisy 0DTE IV. May need redesign or removal. | PENDING (0 signals) |
| S4 | **Vanna Butterfly data check** | Every 5 trading days | 1 signal. 15-min daily window (14:55-15:10) too narrow + vanna pin data dependency. | PENDING (1 signal) |
| S5 | **VIX Divergence data check** | Every 5 trading days | REPLACED VIX Compression (session 58). Two-phase detector, LONG+SHORT, stop-entry confirmation. Backtest: +262 pts, 68% WR, MaxDD 11. Collecting live signals from Mar 31. | PENDING (0 live signals) |
| S6 | **GEX Long live signal check** | Every 5 trading days | 62 signals, 34% WR, -83 pts. Last fired Mar 20. Blocked by VIX>22 + alignment<2. Needs bull regime. | PENDING (blocked by VIX) |
| S7 | **GEX Velocity live signal check** | Every 5 trading days | 0 signals. No LIS surges detected. Needs bull trend. | PENDING (0 signals) |
| S8 | **Options circuit breaker analysis** | When 30+ days of V11 option data | Re-run circuit breaker study: stop trading after 4 consecutive option losses. Backtest showed +48% improvement. Needs 30+ days V8+ data. | WAITING (need data) |
| S9 | **Stock GEX Support Bounce — live alerts** | Each trading day 10:00-14:00 ET | Monitor `/stock-gex-live` for stocks dipping 1% below -GEX with CLEAN structure. Telegram channel connected. | ACTIVE |
| S10 | **Real money daily P&L check** | Each trading day after 16:05 ET | Check Telegram for real_trader EOD summary. Verify no bugs, no missed trades, no ghost positions. Accounts: 210VYX65 (longs), 210VYX91 (shorts). | ACTIVE |
| S11 | **SB2 Absorption v2 tuning deploy** | 2026-03-25 after 16:10 ET | Deployed: OR gate (vol>=1.2x OR dlt>=1.3x), cd=20, time 9:45-15:00, SVB key fixed. +260 pts, 47.7% WR, PF 1.52. | DONE 2026-03-25 |
| S12 | **Push 0DTE GEX improvements** | 2026-03-25 after 16:10 ET | Deployed: last-scan timestamp, 2-min spot refresh, history viewer (date/time picker). Commit `cc213a3`. | DONE 2026-03-25 |
| S13 | **Push AG Short 15-min cooldown** | 2026-03-25 after 16:10 ET | Commit+push AG Short cooldown fix: 15-min time floor prevents flicker re-fires. Data: <15min signals = 63% WR (weak), 15-30min = 85% WR (best). Changes in `setup_detector.py`. | DONE 2026-03-25 |
| S14 | **Verify deploy + OHLC backfill** | 2026-03-28 at 09:35 ET | All 4 PASS: (1) SC GAP=5.0 on #1290. (2) 10,000 rows, 27 trading days (Feb 19-Mar 27). (3) trail_sl/activation/gap populated on all Mar 27 trades. (4) Backfill 10K bars saved. | DONE 2026-03-27 |
| S15 | **Deploy UI fixes: EOD Review + 0DTE GEX chart** | 2026-03-27 after 16:10 ET | Commit `a48b3a8`: EOD Review side-by-side layout + 0DTE GEX 40-strike limit. Deployed. | DONE 2026-03-28 |
| S16 | **Vol Event Detector verification** | First day overvix > +1.0 | Verify Telegram fires "compression building". Check `/api/health` vol_event phase. Monitor daily reset + no spam. Deployed commit `0435192`. | PENDING |
| S17 | **VPS setup completion** | 2026-03-29 (next session) | Kamatera VPS (103.54.56.210). **Installed:** Python 3.12.9, Git 2.53.0, pip 24.3.1, VS Code 1.113.0, GitHub CLI (logged in), Railway CLI (logged in), Sierra Chart, Google Drive, Git Bash. **Remaining:** DTC server config, ES+VX symbols, NT8+Rithmic install, eval_trader config, auto-start, **NT8 daily restart (Task Scheduler 9:15 ET)**, test trade. | IN PROGRESS |
| S18 | **Build vps_data_bridge.py** | After S17 complete | Sierra DTC → ES range bars + VIX ticks → Railway POST endpoints. New Railway endpoints needed: /api/vps/es/bar, /api/vps/vix/ticks, /api/vps/heartbeat. DB table: vix_futures_ticks. | PENDING |
| S19 | **Fix real trader trail resilience** | ASAP (before next market open) | Railway restart loses trail management → stops stay at initial SL. Fix: (1) make real_trader trail self-contained (restore max_favorable from DB, poll ES price independently), (2) compute pnl on close. Mar 30 cost: -$70 real vs +$181.50 dashboard on trade #1352. | PENDING |
| S20 | **Double-up size filter — weekly review** | Every Friday after close (or Sunday) | Re-run SC long alignment × paradigm × VIX breakdown. Check if any bucket crosses 50-trade threshold with WR > 75%. Best candidate: BOFA-PURE +3 align (94% WR, 17t as of Mar 31). Goal: find high-conviction combo to justify 2x MES sizing. See `project_double_up_study.md`. | PENDING |
| S21 | **Deploy ES price REST fallback** | 2026-03-31 after 16:10 ET | Commit+push `_get_es_price_fallback()`: REST `GET /marketdata/quotes/@ES` when TS quote stream `last_price` is None. Wired into SIM auto-trader, real_trader, and ES Absorption paths. Fixes silent trade skips (0 real trades Mar 31, missed V-shape #1386). | PENDING |

---

## BACKLOG TASKS

### Implementation (Build)

| # | Task | Priority | Details | Source |
|---|------|----------|---------|--------|
| ~~B1~~ | ~~SPY DD into setup detector~~ | ~~HIGH~~ | DONE — Combined DD (SPX+SPY) feeds DD Exhaustion. Deployed Mar 25. See completion log. | DONE |
| B2 | **Charm S/R as FILTER (not entry)** | HIGH | Limit entry disabled (44% fill rate), but charm S/R range could work as a quality filter — block entries when range is unfavorable. Needs research first. | PROJECT_BRAIN |
| B3 | **0DTE GEX tab on Stock GEX Live page** | HIGH | Add SPX/SPY/QQQ/IWM 0DTE chains to `/stock-gex-live`. Different from stocks: same-day exp, SPXW symbol, wider strikes. Verify with live market. | `project_0dte_gex_tab.md` |
| B4 | **DD alignment boost (V4) for filter** | MEDIUM | SC + DD_aligned + no toxic paradigm = 85.7% WR, PF 6.44. Three options: (A) SC DD-Aligned Only, (B) Hybrid V10 + DD gate on SC, (C) keep V10. User interested but undecided. Needs 50+ days data. | PROJECT_BRAIN |
| B5 | **Dashboard restyle (main 0DTE page)** | MEDIUM | Apply approved dark style from `project_dashboard_style.md` to main dashboard (`/`). Outfit body + JetBrains Mono for numbers. Revert tag: `pre-gex-redesign`. | `project_dashboard_style.md` |
| B6 | **AI Copilot — Claude Code skills** | MEDIUM | `/morning-brief` (10 AM market read), `/review-trades` (EOD analysis), `/check-discord` (filter actionable info). Zero extra cost, uses existing Claude Code subscription. | `project_ai_copilot.md` |
| B7 | **IBKR Cash Account connector** | LOW | Build IB Gateway + ibapi connector for IBKR cash account (U10235312). Starting capital $5,000. No PDT rule. Infrastructure not yet built. | `project_cash_account_plan.md` |
| B11 | **Autonomous Copilot Worker** | MEDIUM | `copilot_worker.py` Railway service: collects market data (DD, paradigm, charm, gap, signals) + Discord messages, calls Claude API for analysis, sends bias/actionable calls to Telegram every 30 min during market hours. Cost ~$0.40-4/day. Needs: Claude API key, Telegram channel, periodic Discord export or bot. Design in `copilot_market_rules.md`. | Session 36, Mar 25 |
| B12 | **Discord Live Monitor (self-bot)** | HIGH | Read-only Discord gateway listener using user token. Monitors `#volland-daytrading-central` + `#0dte-alerts`. Parses Apollo/LordHelmet/Wizard messages for vol flow, DD calls, levels, bias. Feeds into Copilot Worker (B11). Raw WebSocket approach (no library), proper Identify with auto-fetched build number, residential IP. Detection risk: very low (read-only = identical to browser tab). Token from browser DevTools. Run locally or Railway. | Session 44, Mar 26 |
| B8 | **SB10 Absorption recalibration** | LOW | Only 10 signals in 56 days — needs multiplier recalibration (1.3x-1.5x) for 10-pt range bars. | `project_sb_absorption.md` |
| B9 | **FOMC Event Day Filter** | LOW | Known FOMC dates = no trading or reduced sizing. Low effort (date list check). | `research_discord_ideas_mar23.md` |
| B10 | **SPX GEX Bounce — full study** | LOW | Complete SPY/QQQ/IWM downloads, run dip study on all 4, test 10:00-13:00 time filter, calculate actual options P&L (not just pts). | `project_spx_gex_bounce.md` |

### Research (Study / Analyze)

| # | Task | Priority | Details | Source |
|---|------|----------|---------|--------|
| R1 | ~~Gap-day filter~~ | ~~HIGH~~ | DONE — Implemented as gap-up longs block (gap > +30 pts). See completion log. | DONE |
| ~~R2~~ | ~~Per-strike charm near spot as filter~~ | ~~HIGH~~ | DONE — Studied 228 SC + 276 ES Abs trades. SC: charm redundant with V12 (blocked trades = 69% WR winners). ES Abs: bearish charm<-20M = 27% WR toxic but V12 already blocks all bearish. Short whitelist (align<=-2, charm>=0) = 58.3% WR, too thin (48t). No filter change. | DONE 2026-03-29 |
| R3 | **ES Absorption redesign** | MEDIUM | Current design flaw: fires up to 40 bars after swing. User's correct model: high-volume bar IS the comparison point, fire immediately. Neither approach clearly superior in backtest. Deferred. | PROJECT_BRAIN |
| R4 | **Fixed strike vol for vanna interpretation** | MEDIUM | Discord idea: vanna support only holds when fixed-strike vol is declining. Needs investigation. | `research_discord_ideas_mar23.md` |
| R5 | **Panic vs structural put buying** | MEDIUM | Distinguish geopolitical panic from institutional structural put buying. Different trading responses. | `research_discord_ideas_mar23.md` |
| R6 | **Volatility spike pause** | MEDIUM | If ES range bar volatility exceeds 3x normal, pause entries 15-30 min. | `research_discord_ideas_mar23.md` |
| ~~R7~~ | ~~SC Trail Optimization~~ | ~~HIGH~~ | DONE — Gap 8->5 deployed Mar 28. Gap=8 was copy-paste from ES Absorption bug fix (range bars), never SC-studied. 0/13 losers ever touched activation, so gap only affects winner capture. SL=14 and ACT=10 confirmed optimal. 4 contaminated Mar 26 SC outcomes also cleared. | DONE 2026-03-28 |
| R8 | **DD per-strike for ES Absorption stacking** | LOW | Revisit when ES Absorption trade count grows. Not enough data yet. | `research_gamma_dd_perstrike.md` |
| R9 | **Gamma per-strike on dashboard** | LOW | Visual awareness only. No filter impact expected. | `research_gamma_dd_perstrike.md` |
| R10 | **EOD DD trajectory for manual butterflies** | LOW | Display DD direction into close for discretionary butterfly entries. 50% direction accuracy — needs timing skill. | `research_gamma_dd_perstrike.md` |
| R11 | **ThetaData — OpEx pinning study** | LOW | Data already downloaded. GEX pins monthly expiry strikes. | `project_thetadata_ideas.md` |
| R12 | **ThetaData — IV crush around events** | LOW | Needs more data collection. Pre-event IV spike → post-event collapse. | `project_thetadata_ideas.md` |
| R13 | **Options strategy expansion** | LOW | Non-directional setups: butterfly, IC, iron fly. Pin criteria: charm concentration, DD neutrality, low paradigm conviction, VIX term structure. | PROJECT_BRAIN |

### Parked (Revisit Later)

| # | Task | Details | Why Parked |
|---|------|---------|------------|
| P1 | **Sierra Chart VolDetector** | Apollo-style vol seller/buyer dots on ES chart. `VolDetector.cpp` built, threshold ~150 for VIX. | Needs live market calibration with Apollo's real-time posts for comparison. |
| P2 | **FundingPips manual trading** | Trade manually on FundingPips ($36 eval) using Sierra Chart. | Needs separate data solution (Denali feed or brother's Rithmic). |
| P3 | **SPY real options account** | TradeStation #11697180, $4,000 funded. V8 filter, 1 SPY contract, 0DTE at ~0.30 delta. | 2-week validation period (~Mar 14-28). Need to check status. |

---

## Completion Log

| Date | Task | Result |
|------|------|--------|
| 2026-03-29 | R2: Per-strike charm near spot filter | DONE — Redundant with V12 for both SC and ES Abs. No filter change. ES Abs short whitelist parked (58.3% WR, 48t too thin). |
| 2026-03-27 | Data staleness protection | DONE — data_ts column + freshness gates on all 4 snapshot tables. Prevents saving stale data during API outages. |
| 2026-03-27 | SB2 abs_details + cooldown fix | DONE — abs_details now saved for all absorption variants (was ES-only). Cooldown reads settings (20 bars, was hardcoded 10). |
| 2026-03-27 | Mar 26 outcome cleanup | DONE — Cleared 42 contaminated outcomes (37 Mar 26 outage + 5 Mar 24 SB2 bug). Grand total corrected: +776.6 unfiltered, +1,278.5 V12. |
| 2026-03-25 | Gap-up longs filter (gap > +30 pts) | DONE — blocks longs all day on gap-up. Backtest: +290.9 pts saved, 112 trades. FOMC filter rejected (FOMC day = best day). |
| 2026-03-25 | Combined DD into setup detector | DONE — SPX+SPY DD feeds DD Exhaustion. Boundary: Mar 25 (SPX-only before). |
| 2026-03-25 | SPY DD capture (v2 worker + dashboard) | DONE — deployed, verify at market open (S1) |
| 2026-03-24 | Charm S/R limit entry disabled | DONE — market orders beat all limit thresholds |
| 2026-03-24 | V11 SC grade gate deployed | DONE — A+/A/B pass, C/LOG blocked |
| 2026-03-24 | General Telegram channel cleaned | DONE — ~160 msgs/day reduced to ~7-9/day |
| 2026-03-23 | SB2 Absorption setup created | DONE — LOG-ONLY, collecting data |
| 2026-03-23 | Vanna Butterfly grading v2 | DONE — GREEN vanna gate, 80% WR |
| 2026-03-23 | VIX Compression tuning v2 | DONE — 100% WR with Volland gate |
| 2026-03-22 | SC grading v2 recomputed | DONE — old grades were anti-predictive, fixed |
| 2026-03-21 | Stock GEX Scanner deployed | DONE — 23 stocks, every 30 min |
| 2026-03-21 | Stock GEX Live page built | DONE — 56 stocks, streaming, `/stock-gex-live` |
