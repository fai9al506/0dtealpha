# Tasks

Pending tasks, research, and implementation ideas for 0DTE Alpha.
Two types: **Scheduled** (time-based, checked every session) and **Backlog** (do when free).

Last updated: 2026-04-17

---

## SCHEDULED TASKS (Check Every Session)

These tasks are time-sensitive. Claude checks them at session start and alerts if due.

| # | Task | Trigger | Action | Status |
|---|------|---------|--------|--------|
| S1 | **Mar 25 deployment verification** | First market day after Mar 25 | All 5 checks PASS. SPY DD flowing, gap=+64.2 (longs blocked), combined DD working. Two bugs found+fixed: (1) gap SQL used wrong column, (2) SPY DD not reaching setup detector. | DONE 2026-03-25 |
| S2 | **SB2 Absorption data check** | Every 5 trading days | **2026-04-09 re-check (79t lifetime, 47% WR, +29.1 pts):** TURNAROUND from Mar 29 (-46 pts). New 44 signals: 52% WR, +75 pts. **Direction flipped** — bullish now BETTER (55% WR, +64.5) vs bearish (50%, +10.8). Mar 29 shorts-only call is now wrong. Don't act on direction filter — could be noise either way. Re-check at 100t. Keep LOG-only. | DONE 2026-04-09 |
| S3 | **IV Momentum data check** | Every 5 trading days | 0 signals. Gate conditions nearly impossible with noisy 0DTE IV. May need redesign or removal. | PENDING (0 signals) |
| S4 | **Vanna Butterfly data check** | Every 5 trading days | 1 signal. 15-min daily window (14:55-15:10) too narrow + vanna pin data dependency. | PENDING (1 signal) |
| S5 | **VIX Divergence data check** | Every 5 trading days | **2026-04-09 re-check (6 trades total, all post-Mar 31):** STRONG EARLY. 5W/1L, **83.3% WR**, **+46.9 pts** ($234.50 at 1 MES), **PF 6.86**, **MaxDD only 8 pts** (the single loss, trade 1). Both directions: 3L/3S balanced. Avg MFE on wins +18.81 (trail leaving 11pts/winner on table). Avg MAE on wins -4.35 (entry timing very clean — stop-entry confirmation working). The 1 loss was instant clean stop (mfe +0.1). All 6 trades in VIX 21-26 regime. **Promotion criteria for SIM auto-trade: 30+ trades, WR ≥65%, MaxDD ≤25, PF ≥2, ≥5 trades in VIX <20.** See S31 tracker. | PENDING — STRONG EARLY |
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
| S17 | **VPS setup completion** | 2026-03-29 | VPS fully operational: Sierra DTC connected, ES+VX symbols live, NT8+Rithmic, eval_trader+watchdog+auto-start. All infra done. | DONE 2026-04-06 |
| S18 | **VPS data bridge live** | After S17 | `vps_data_bridge.py` running on VPS. Sierra DTC → Railway `/api/vps/es/bar`, `/api/vps/vix/ticks`, `/api/vps/heartbeat`. DB tables: `vps_es_range_bars`, `vps_vix_ticks`, `vps_heartbeats`. 14 days of data (Mar 23 → Apr 9). **Issue:** Apr 7-9 bridge captured only 4-21% of bars (heartbeats OK, DTC data dropped). User investigating Sierra/DTC side. | DONE (monitoring) |
| S33 | **Rithmic → Sierra cutover** | After 5 clean Sierra days | Comparison done Apr 9: CVD direction 95.5% match, RTH bar counts ±3-8, 10.7% avg CVD magnitude diff — good enough. **Root cause found:** (1) Sierra DTC "Allow Market Data" was disabled, (2) symbol format `ESM6.CME` → `ESM26-CME`. VPS Claude fixed code, user needs to enable DTC setting in Sierra GUI + restart bridge. After 5 clean trading days, cutover = swap ~10 `es_range_bars` queries to `vps_es_range_bars` in main.py + eod_report.py, disable Rithmic on Railway, cancel sub. **Saves $114/mo.** | BLOCKED (waiting 5 clean days post-fix) |
| S19 | ~~Fix real trader trail resilience~~ | ~~ASAP~~ | **CLOSED (session 64).** Trade #1352 was NOT a bug. MES stop at 6452 hit at 11:51 ET (verified from TS statements). SPX max adverse was -12.5 (didn't hit 14pt SL), but MES hit its 14pt SL due to ~2pt basis slippage — normal at VIX 30. Portal showed WIN because SPX didn't reach SL; real trader correctly stopped. Not a trail bug, not a restart bug — just SPX-MES basis risk. | CLOSED 2026-04-02 |
| S20 | **Double-up size filter — weekly review** | Every Friday after close (or Sunday) | Re-run SC long alignment × paradigm × VIX breakdown. Check if any bucket crosses 50-trade threshold with WR > 75%. Best candidate: BOFA-PURE +3 align (94% WR, 17t as of Mar 31). Goal: find high-conviction combo to justify 2x MES sizing. See `project_double_up_study.md`. | PENDING |
| S21 | ~~Deploy ES price REST fallback~~ | ~~2026-03-31 after 16:10 ET~~ | Commit `e29711e` pushed Mar 31. `_get_es_price_fallback()` wired into SIM, real_trader, ES Absorption. Deployed and live. | DONE 2026-03-31 |
| S22 | **Verify Stock GEX fix** | 2026-04-02 at 10:00 ET | Stock GEX was fully broken: 59 stocks, all 404s, zero DB rows saved, wasting API quota. Fixed: (1) trimmed to 14 stocks (4 0DTE ETFs + 10 top stocks), (2) stocks use Friday-only expirations (was using today=Tuesday→404). **Verify at 10:00 ET:** scans complete without 404s, rows appear in `stock_gex_scans` table. If still failing, check expiration format. If timeouts return, Stock GEX is still overloading API — increase delay or reduce stocks further. | PENDING |
| S23 | **Deploy stale-order cleanup fix** | 2026-04-06 after 16:10 ET | Bug: #1540 (Apr 2, unfilled) stayed in `_active_orders` over weekend, blocked shorts via MAX_CONCURRENT=1. **Full investigation found deeper issue:** commit `70339dd` moved ALL broker calls to fire-and-forget, silently breaking close_trade on real trader. 6 fixes deployed: stale-order cleanup, force_release, close_trade after release, revert real_trader to sync, 2 deadlock fixes. All 7 audit checks PASS. | DONE 2026-04-06 |
| S24 | **Eval trader offline since Mar 30** | Next session | Eval trader has been offline since Mar 30. User said to ignore for now. Check status and decide whether to restart or decommission. | PENDING |
| S25 | **Verify Apr 6 fixes clean before AG Short** | 2026-04-07 EOD | Apr 7 audit found ALL 5 placed shorts ghost-reconciled. Root cause: `_get_broker_position` filtered with `qty > 0` but TS API returns SIGNED Quantity for futures shorts (`-1`). Fix in commit `1a98ec1` patched 3 sites in real_trader.py with `abs(int(Quantity))`. Apr 8 verified: 7 SC trades placed, 6 closed cleanly via `stop_filled`, 1 was user manual save. **PASS.** | DONE 2026-04-08 |
| S26 | **Plan & implement AG Short on SHORT account** | After S25 PASS | Implemented in commit `e7a0f40` — 2 surgical changes: `real_trader.py:266` (defense-in-depth filter accepts AG Short) + `main.py:4764` (dispatch gate accepts AG Short). All other plumbing already in place: trail params, stop level, live filter, account routing, LONG isolation. Live for Apr 9 open. | DONE 2026-04-08 |
| S27 | **Harden remaining Volland format_statistics() fields** | Next session | Session 63 fixed LIS NaN-string crash (commit `78dcca5`). Three sibling formatters at `volland_worker_v2.py:294,313,317` (`target`, `delta_decay_hedging`, `opt_volume`) still check `is not None` but assume numeric — same crash class if vol.land sends `'NaN'` for any of those. 5-line defensive coercion, same risk profile as today's fix. User was offered but ended session before accepting. | PENDING |
| S28 | **Bot-down watchdog (no-signal-in-2h alert)** | Next session | 9-day P&L audit (session 66) found Mar 25 (5 SC signals, 0 placed) and Mar 31 (12 SC signals, 0 placed) — bot was completely down those trading days, no Telegram alert ever fired. Operational risk separate from qty-sign bug. Add watchdog: if `setup_log` has 0 placed real trades AND >5 filtered signals fired in any rolling 2h window during market hours → Telegram alert. Cost of those 2 days: ~$200 missed P&L. | PENDING |
| S29 | **Verify AG Short first fire on Apr 9** | 2026-04-09 market open | AG Short #1703 fired Apr 9 13:21 ET on AG-LIS paradigm. Entry 6871.25, WIN +11.2 pts ($62.50 real). Closed via stop_filled. No ghost_reconcile. SC + AG shared slot correctly on 210VYX91. **PASS.** | DONE 2026-04-12 |
| S30 | **Real-vs-portal daily drift tracking (5-10 days)** | Each trading day Apr 9 → Apr 18 | Continue monitoring started in session 66. Each day pull true broker P&L from TS historicalorders, compare to portal P&L for placed trades. Goal: confirm post-fix drift stays small (Apr 8 showed real beat portal by $36 — first clean day). After 5-10 clean days, the qty-sign fix is fully validated. | ACTIVE |
| S31 | **VIX Divergence promotion tracker** | Each S5 re-check (every 5 trading days) | **2026-04-12: 8t, 75% WR, +55.6 pts.** SL study done: SL=10 outperforms SL=8 (+21.9 simulated). Portal display bug FIXED (commit `bc39a00`). **At 15t:** re-run SL study, change 8→10 if confirmed. **At 30t:** full promotion to TS RT if criteria pass. User excited about this setup. See `research_vix_divergence_sl.md`. Promotion criteria: (1) ≥30 trades, (2) WR ≥65%, (3) MaxDD ≤25 pts, (4) PF ≥2.0, (5) ≥5 trades in VIX <20. | PENDING — 8/30 trades |
| S34 | **VIX < 20 regime recalibration study** | When 30+ SC trades at VIX < 20 (~2-3 weeks after VIX normalizes) | Full recalibration for low-VIX regime. Study: (1) SC SL optimization (14 may be too wide — test 10-12), (2) Trail params (activation/gap for tighter ranges), (3) Target distance (10 pts vs 8), (4) SC grade weight shifts at low VIX, (5) ES Absorption volume gates (lower vol = different thresholds), (6) DD reliability at low VIX (should be cleaner without headline noise), (7) GEX Long/Velocity — likely to shine at VIX 15-20 (currently blocked by VIX>22 gate, needs bull regime data). **Thesis:** VIX < 20 = Greek-driven market, less macro noise, paradigms more stable. Our system should be BETTER, just needs parameter tuning. User believes GEX setups will finally activate in this regime. Track SC trade count at VIX < 20 each session. | WAITING (need VIX < 20 data) |
| S35 | **Verify SIDIAL-EXTREME longs block** | 2026-04-13 (first market day) | Deployed commit `94f1327` (Apr 12). Block all longs when paradigm = SIDIAL-EXTREME. Study: 34t, 29% WR, -182.5 pts. Apr 10 real trader: 3 SIDIAL-EXT SC longs all lost (-$140). Verify: (1) next SIDIAL-EXTREME paradigm appearance, check that longs are skipped in logs, (2) shorts still fire normally, (3) portal shows "SIDIAL-EXTREME longs blocked" reason. | PENDING |
| S36 | **Vanna Pivot Bounce promotion tracker** | At 30 trades (~mid-May) | 21t as of Apr 12, 76% WR, +120 pts. Shorts 90% WR (10t). Low-frequency setup (~1/day). Fires and logs correctly, excluded from V12 shorts whitelist. **At 30t:** full assessment for TS RT promotion — add to shorts whitelist + real_trader dispatch. Need clean data, WR ≥65%, MaxDD ≤25. Currently across 6 trading days over 23 calendar days. | PENDING — 21/30 trades |
| S37 | **SC Grade C re-assessment** | When 30+ SC grade C trades with SL=14 | Currently blocked. 68t total at 63% WR, +96.3 pts BUT MaxDD=-67.2 (worse than A/B at -44). Early trades used SL=20, inflating DD. When enough SL=14 C-grade trades accumulate, re-check MaxDD. If MaxDD improves to ≤-50 → consider unblocking. | WAITING (need SL=14 data) |
| S38 | **Copilot rules R53-R57 from Discord** | Next session | R53 CPF (mindset, not filter), R54 DD+gamma=PIN, R55 vol buyer=bearish, R56 spot up+VIX up, R57 VIX<20 puts cheap. All 5 saved to `copilot_market_rules.md`. | DONE 2026-04-12 |
| S32 | **DD Hedging Magnet re-test** | After 30 more trading days (~2026-05-22) | Discord Idea #4 deferred Apr 9. Initial backtest Mar 18-Apr 8: +104.5 pts incremental over V12-fix, but ~70% from Apr 2 alone, OOS train half showed -46.2 (block hurt), threshold sensitive. Re-run `_discord_idea4_v12fix.py` with extended date range. **Deploy criteria:** (a) incremental positive on BOTH halves, (b) sample ≥250 SC/DD shorts post V12-fix, (c) Apr 2-style outliers excluded → still ≥+50 pts/30 days. Side check: SC longs on GEX-LIS (31% WR / -44 pts on 14t in initial sample) — verify if growing. See `research_dd_magnet_idea.md`. | WAITING (need data) |
| S39 | **Deploy cap=2 for SHORTS account (weekend)** | 2026-04-18 Saturday or Sunday (before Mon 09:30 ET) | Split MAX_CONCURRENT into per-direction: `MAX_CONCURRENT_LONG=1` (unchanged), `MAX_CONCURRENT_SHORT=2` (was 1). Both configurable via env vars. Cap check now reads direction-specific constant. Startup log + status API expose both values. User funded +$1k each account for margin headroom (landing pre-Monday). | DONE 2026-04-18 |
| S44 | **Verify +$1k funding landed + cap=2 active** | 2026-04-21 Monday 09:00 ET (pre-market) | Three checks: (1) both accounts show +$1k funding landed — long acct ~$3,170, short acct ~$2,558. (2) Railway logs show `max_long=1 max_short=2` in real-trader init line. (3) First short signal Monday shows `(1/2)` or `(2/2)` in cap log instead of `(1/1)`. If funding not landed, revert SHORT cap to 1 via `REAL_TRADE_MAX_CONCURRENT_SHORT=1` env var (no redeploy needed). | PENDING |
| S40 | **V13 5-day OOS validation (first re-assess)** | 2026-04-24 (Friday after 5 V13-live trading days) | V13 deployed Apr 17. After 5 clean trading days, re-run backtest on Apr 17-24 window to validate edge holds OOS. Compare: (a) actual TSRT PnL vs theoretical V13 PnL, (b) vanna cliff/peak flags populated correctly on new trades, (c) no ghost reconcile events, (d) no unexpected blocks/allows. If OOS PnL is within 30% of backtest expectation, V13 validated. If degraded >50%, investigate vanna rules for overfit. See latest audit report. | PENDING |
| S41 | **V13 + cap=2 combined re-assess** | 2026-05-01 (~2 weeks post-deploy) | After both V13 and cap=2 are live for ~2 weeks, full re-assessment vs 4-scenario projections. Check: (1) actual monthly rate vs scenario 5 projection ($1,842/mo), (2) short account slot usage distribution (is cap=2 actually used or mostly 1-slot?), (3) per-setup PnL attribution, (4) any new SC long loss patterns emerging, (5) SIDIAL-EXTREME block effectiveness, (6) FI ladder progress (target: 2 MES at +20 clean days). | PENDING |
| S42 | **Bug-era damage quantified — track recovery** | Each EOD through end of April | Current balance: $3,728 / $4,000 = -$272 (building cost from Apr 8 and earlier bugs). Track daily: when does combined account balance cross back above $4,000? At post-fix rate (~$1,800/mo = $72/day), expect ~4 clean trading days to recover. Reconciliation: gross SPX pnl -$119 + fees -$153 = -$272 matches statement. See Tel Res msg 20. | ACTIVE |
| S43 | **Re-assess SC long strategy** | When 20+ V13-era SC long trades complete (~3 weeks) | V13 vanna rule blocks 5/13 (38%) of historical SC long losers. Remaining leak was mostly SIDIAL-EXTREME (now blocked by V12-fix) + 3 Mar 24 AG-PURE/high-VIX (stochastic bad luck per deeper study — NOT a systematic pattern). Re-verify: after 20 new V13-era SC longs, is WR back to expected ~67%? If still <50%, deeper investigation needed. If ≥60%, SC longs are healthy on V13. See `_sc_long_agpure_study.py`. | PENDING |

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
