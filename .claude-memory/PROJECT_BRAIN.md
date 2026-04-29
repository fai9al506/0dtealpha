# Project Brain — 0DTE Alpha

## Current State
- System running in production on Railway (web + Volland services)
- Volland worker has auto-restart capability now (added 2026-02-17)
- Pipeline health alerts properly detect 0-point Volland snapshots

## HIGH PRIORITY — Next Session
- **Real trader deployed clean (Apr 6)** — 6 fixes for fire-and-forget bug + stale orders + deadlocks. All 7 audit checks PASS. Monitor Apr 7 trading.
- **Eval trader offline since Mar 30** — user said ignore for now. S24 task added.
- **VPS Setup Completion** — Kamatera VPS (0dte-vps, 103.54.56.210) provisioned. Sierra installed. Need: DTC server config, ES+VX symbols, NT8+Rithmic, IBKR TWS, eval_trader config, auto-start. Then build vps_data_bridge.py + Railway endpoints. Market opens Sun 6 PM ET for validation.
- **VX Futures via Sierra DTC on VPS** — Rithmic has NO CFE. Sierra Denali has CFE ($12/mo, already subscribed). VX ticks from Sierra DTC → vps_data_bridge.py → Railway API → vix_futures_ticks table. Apollo-style vol analysis later.
- **Test 0DTE GEX tab with live market** — verify SPX/SPY/QQQ/IWM chains load, GEX levels compute, dip triggers fire

## Pending / Ideas
- **DD Hedging Alignment Filter (Analysis #15, 2026-03-20):** SC + DD_aligned + no toxic paradigm = 85.7% WR, PF 6.44, MaxDD -10.1 pts. Three implementation options: (A) SC DD-Aligned Only ($19K/mo, -$404 DD), (B) Hybrid V10 + DD gate on SC ($28K/mo, -$2K DD), (C) keep V10 unchanged. User interested but hasn't decided yet.
- **Real MES trading GO-LIVE Mar 25:** `app/real_trader.py` fully audited (session 23). SC-only V11 A+/A/B, market orders, 1 MES, fixed +10, SL=14. Direction-routed (210VYX65 longs, 210VYX91 shorts). Env vars on Railway (both `false`). User flips to `true` when at desk. Circuit breaker $300/day. NY timezone bug FIXED (commit 4f6683e).
- **Charm S/R as FILTER (research):** Limit entry disabled (44% fill rate), but charm S/R range could work as a quality filter — block entries when S/R range is unfavorable. Needs research.
- **Gap-day charm delay (research):** Discord idea — charm behaves differently on gap days. Needs investigation.
- **Hybrid strategy validated:** SC on MES futures (directional edge, 72% WR), DD on SPXW credit spreads (theta + mean reversion). Bearish sample bias on AG — needs more data.
- **Credit spread direction bug FIXED:** `api_debug_options_sim` was building spreads backwards. Corrected in session 7.
- **`_passes_live_filter()` is single source of truth:** Controls both Telegram notifications and auto-trade eligibility. Any filter change applies everywhere automatically.
- Holiday calendar: `market_open_now()` only checks weekday, not US market holidays. Presidents' Day caused unnecessary cycles. Could add a holiday check.
- References library: `references/` folder has Volland PDFs + README + INDEX.md but INDEX.md not yet populated with summaries.
- **Auto-trade: LIVE on SIM** — 10 MES split-target, all setups ON. **Same-direction stacking** (2026-03-04): critical over-close bug caused >$2K SIM loss + overnight ghost short. **FIXED (2026-03-05, commit `d16c88d`):** flatten uses trade's own qty, integrity check after every close, direction-mismatch detection in periodic orphan check, EOD cancels all orders first then closes once. **Still pending:** cap 1 active per setup name + grade gate B+ for ES Absorption. **Greek Optimal Filter deployed (2026-03-04).** SIM account SIM2609239F ($50K balance).
- **Skew Charm:** ENABLED on SIM auto-trader + both eval instances (2026-03-08). **SPX 0DTE options trader:** Limit orders, NO stop-loss, 90min time exit. `reconcile_with_broker()` added (2026-03-12). **TS SIM fills are FAKE** — stale per-strike prices, always use `theo_exit`. **3 EOD bugs fixed (2026-03-17, `ef3e0e9`):** (1) EOD summary closes options, (2) new `_options_trade_eod_flatten()` at 15:55, (3) poll errors logged not swallowed. **SIM balance is NOT real** — inflated by fake fills. Mar 16 real (theo) P&L: -$57 (normal loss day, within backtest range). Needs TS symbol format verification on first signal.
- **F7 Charm Support Gate — REJECTED for V8 (2026-03-14):** Tested F7 on V8-filtered shorts. Blocks 13 trades at 76.9% WR, losing 61.1 pts. V8 already filters the losers F7 targeted. F7 was valuable pre-V8 (+136 pts on unfiltered data) but harmful on V8.
- **V8 optimization PROVEN COMPLETE (2026-03-14):** Tested 11 improvement candidates (VIX floors, remove ES Abs, lean V8, DD threshold, tighter alignment, etc). Every single one makes V8 worse. V8 is the optimal filter configuration.
- **Paradigm gate REJECTED (Analysis #10):** Blocking LONG in AG/BOFA paradigm would remove +732 pts of winners (contrarian setups). Do NOT implement.
- **TS SIM intraday margin:** Does NOT apply day trade margin via API — always overnight ($2,735/MES). User may contact TS trade desk to investigate for LIVE accounts.
- **Rithmic:** LIVE on Paper Trading. ES Delta tab switched to Rithmic (TS fallback). Live stream aggregates sub-fills. Data saved to `es_range_bars` DB table.
- **Rithmic historical backfill:** `tmp_rithmic_batch_pull.py` ready for bulk pull. Blocked by concurrent session limit (Railway holds Rithmic session → ForcedLogout). Needs Railway Rithmic temporarily disabled + full redeploy. Only 6 days of TS live data available for backtesting.
- **SB Absorption GRADED (2026-03-19):** Real grades enabled (A+/A/B/C based on vol/delta/CVD/Volland confluence scoring). NOT auto-traded yet — monitoring. First 2 trades: #951 LOSS -8 (fired 15:59 ET, now blocked by time gate), #955 WIN +10. 15:55 ET cutoff added. All ES-price-space bugs fixed (outcome tracking, detail view, backfill). Next: collect 10+ graded signals to evaluate grade vs outcome correlation, then enable auto-trading.
- **ES Absorption refinement (iterative):** Signal frequency too high (~10/day). User wants 1-3/day with 70%+ WR. Approach: user spots setups visually in real-time, checks with me whether system caught it, we refine detection over time. NO automated filters yet.
- **ES Absorption design flaw (identified 2026-03-04):** Current system decouples swing detection from signal trigger — fires up to 40 bars after swing, entry price far from absorption zone. User's correct model: high-volume bar IS the comparison point, fire immediately. Backtest script `tmp_abs_backtest_compare.py` compares both approaches (neither clearly superior — A better trending, B better choppy). User deferred redesign.
- **VANNA PIN SETUP (researched 2026-03-05):** Max absolute 0DTE vanna near spot predicts close within 10 pts 93% of days. 20pt butterfly centered on pin strike: 60% WR, PF 6.6x. Data collection phase — need 2-4 months. Chain snapshots enhanced with Theta/Vega/wider strikes. See `memory/vanna_pin_setup.md`.
- **OPTIONS STRATEGY EXPANSION:** Vanna pin opens the door to non-directional setups (butterfly, IC, iron fly). Other pin criteria to explore: charm concentration, DD neutrality, low paradigm conviction, VIX term structure. Options allow trading flat/range setups that directional MES trades can't capture.
- **Vol Spike setup (researched 2026-03-18, PARKED):** Whale detection via OTM volume spikes in chain_snapshots. Extensively backtested (118 dates, 6 script versions). Best combo: vol>=3K, OTM<=0.3%, 45min hold = 48% WR, +$186/trade. Too weak for standalone setup (compare SC 66% WR). Core problem: aggregate volume can't distinguish directional bets from MM hedging/spreads. Would need trade-level aggressor data. Could revisit as confluence filter if better data source found. **Mar 24 deep backtest confirmed:** 5,495 events, call spikes ANTI-predictive (30% at 30min), put spikes = market bias only. User wants to revisit as proper setup later with trade-level data.
- **General Telegram channel cleaned up (2026-03-24):** Disabled vol spikes, LIS, target, +/-gamma, paradigm change alerts via `alert_settings` DB table. Channel was ~160 msgs/day of pure noise. Now ~7-9/day (summaries + pipeline + auth). All actionable alerts live in Setups channel.
- **Discord Analysis COMPLETE (2026-03-19):** Deep analysis of daytrading chat (338 msgs) + 0DTE alerts (67 alerts, 30 days). Backtested 5 Discord recommendations — ALL REJECTED. FOMC gate (-130.9), Sidial filter (-270.3), overvix regime (-71.9), Friday longs (no value on V9-SC), OPEX gate (-526.9). Key finding: our contrarian system thrives in conditions Discord traders avoid (Sidial, FOMC, chop). Bonus: BOFA-MESSY+GEX-LIS paradigms worst (43% WR, +106 if blocked) — needs more data. Bullish regime needed to properly test GEX Long.
- **GEX Velocity setup DEPLOYED (2026-03-19):** Separate setup catching rapid LIS convergence (gap 5-10, velocity 25+). Key discovery: LIS velocity as quality filter = 69% WR vs 38% baseline. Needs bull trend data to validate. Future: if velocity proves strong enough, could become a required condition for ALL GEX Long signals (not just wider gaps).
- **ES Absorption range bar mismatch with ATAS:** Our bars start at 6 PM (overnight session), ATAS likely starts at RTH. Different starting price shifts ALL bar boundaries. Prices confirmed correct by cross-checking Rithmic vs TS 1-min bars.
- **ES Absorption RM optimization:** User wants to test deeper. SL=5/T=5 best fixed (+34 pts), buy_absorption pattern toxic (23% WR, block candidate). Current deployed: SL=12/T=10 (from before rewrite).
- **DD Exhaustion tuning:** Analysis #5 time/paradigm blocks PROVEN WRONG at 476 trades (sample-size artifacts from 49 trades). Both removed 2026-03-08. Remaining ideas (threshold $500M, charm ceiling, paradigm cooldown) also suspect — collect more data before revisiting.
- **Economic calendar:** Live in DB (`economic_events` table). Fetches weekly from Fair Economy API. Can correlate trade outcomes with CPI/FOMC/NFP events. API: `/api/economic-calendar`.
- **Survival mode analysis complete:** BE@5+SL=12 on filtered trades = 75% WR, +655 PnL, 0 wins killed. Implemented on eval trader only (portal keeps original params for data collection).
- **DD Concentration filter (F1):** Block DD Exhaustion when concentration > 75%. Backtested: kills 1 win, saves 8 losses, +92 pts improvement, PF 1.55→1.87. User declined implementation — wants more data first.
- **DD Concentration + Bullish DD filter (F4):** F1 + block ALL setups when DD total > +$2B. PF 2.02, +93 pts improvement. Strongest combo but more aggressive.
- **GEX Magnet detection:** When one strike has dominant call+put GEX simultaneously, price gravitation effect confirmed within 5-30 pts (48% touch rate). Could be used as a price target or filter. Not implemented — research only.

## Completed
- **V2 Dashboard (2026-03-15):** Modern trading cockpit at `/v2`. Signal bar with audio alert, KPI cards (Spot+LIS, Paradigm, DD+SVB, Charm, VIX+Overvix, Today P&L), overview with price+levels+exposure, horizontal exposure bars with synced Y-axis. Design: Plus Jakarta Sans + JetBrains Mono, navy grain bg, pill tabs. File: `app/dashboard_v2.py`, 31 lines in main.py. Commits `a549265`, `98febfe`, `82845eb`. Easy to delete — see `project_v2_dashboard.md`.
- **SPY Option Chain Integration (2026-03-10):** Completely isolated — separate `spy_chain_snapshots` table, own scheduler job, own globals/lock. Parameterized `get_0dte_exp()` and `get_chain_rows()` with backward-compatible defaults. Portal toggle buttons on `/table`. API endpoints support `?symbol=SPY`. Not used by setups/auto-trader. Rollback tag `stable-20260310-spy-before-push`. Commit `6554827`.
- **Outcome Stop Level Fix (2026-03-10):** `outcome_stop_level` now stores INITIAL stop (was being overwritten by trail). Trail exit in `outcome_target_level` for trailing WINs. `initial_stop_level` key in trade dicts. Commit `8024ebc`.
- **Telegram Concise Format (2026-03-10):** All 7 format functions rewritten to 3-line max. Added alignment display. +GEX/-GEX on GEX/AG, CVD gap on CVD. Format function mapper in main.py rebuilds messages after alignment computed. Commit `54bf5a1`.
- **PDF Trading Guide (2026-03-10):** `0DTE_Alpha_Trading_Setups_Guide.py` — 16-page dark-themed PDF explaining all setups for beginners. Covers core concepts, all 7 setups, Greek filter, risk management.
- **EOD PDF + Trades Chart (2026-03-03):** Daily report at 16:05 ET via Telegram. PDF with PnL chart + trade log + "Why" explanations. Dark-themed ES candlestick chart with all setup entries marked by shape/color. `app/eod_report.py` (self-contained). Commits `66175b8`, `997508f`.
- **ES Absorption rewrite (2026-02-25):** Swing-to-swing CVD divergence with 4 patterns. d=40 optimal. Deployed commit `949273b`.
- **ES Absorption zone-revisit + grading (2026-02-26):** Zone-revisit detection (same price zone, CVD change after 5+ bars). 7-factor grading: div, vol, DD, paradigm, LIS proximity, LIS side, target direction. Commit `6ad1267` (zone), `e4f843f` (grading). GEX Long hybrid trail enabled (`f9a5ad7`).
- **ES Absorption LIS bug (2026-02-24):** LIS distance now uses SPX spot instead of ES price.
- **Outcome price source:** SOLVED — TS API 30s pulls + session H/L tracking (`_spx_cycle_high/low`) catches between-cycle breaches. No longer depends on 2-min playback snapshots.
- **MESH26 rollover:** SOLVED — auto-rollover in both auto_trader.py and eval_trader.py.
- **E2T Eval Trader:** DEPLOYED on work desktop, polling Railway API directly (`signal_source: "api"`). Account `falde5482-sim`, $200 max risk, 4 setups enabled.
- **NT8 PositionReporter:** NinjaScript strategy that writes position state to `position_state.json` for eval_trader reconciliation. File created (`nt8_position_reporter.cs`) but NOT YET COMPILING in NT8 — needs debugging on the NT8 machine directly. Once working, eliminates phantom position problem entirely.
- **Rithmic conformance:** PASSED — switched to Paper Trading, live stream running with trade aggregation.

## V8 Filter — DEPLOYED (Analysis #12, Mar 14 2026)

**V8 = V7+AG + Smart VIX Gate. THE ACTIVE FILTER on SIM + Eval + Real.**

**Rules:**
- **Longs:** alignment >= +2 AND (VIX <= 26 OR overvix >= +2)
- **Shorts whitelist:** Skew Charm (all), AG Short (all), DD Exhaustion (align != 0)
- **Blocked shorts:** ES Absorption, BofA Scalp, Paradigm Rev, DD align=0

**SPX-point backtest (431 trades, 24 days, Feb 5 - Mar 13):**

| Metric | V7+AG | V8 | Delta |
|--------|-------|-----|-------|
| Trades | 431 | 364 | -67 |
| Win Rate | 55.5% | 60.7% | +5.2% |
| PnL | +657 | +1,140 | +483 |
| PF | 1.30 | 1.71 | +0.41 |
| MaxDD | 472 | 50 | -422 |
| Sharpe | 0.29 | 0.77 | +0.48 |

**Real option prices backtest (255 trades, 10 days, Mar 1-13):**
- V8: $14,930 total, $1,493/day, PF 1.33, MaxDD $8,615
- Skew Charm: +$9,450 (MVP), DD Exhaustion: +$4,080, AG Short: +$2,670

**Overvix indicator (VIX - VIX3M):** Logged to setup_log, Telegram, health endpoint, eval signals, dashboard. MUST verify $VIX3M.X on Monday.

## Real Money Accounts (Mar 24 updated — GO-LIVE Mar 25)

### MES Futures (Primary — go-live Mar 25)
- **Two TS accounts:** 210VYX65 (longs), 210VYX91 (shorts) — direction-routed
- **Strategy:** SC-only, V11 filter, A+/A/B grade gate, MARKET orders only (charm-limit disabled), 1 MES, fixed +10 target, SL=14
- **Module:** `app/real_trader.py` (1,629 lines), 7 safety layers, 3s fast poll, 30s reconciliation
- **Option 1 validated:** +589.5 pts / $2,622 net for March (130 trades). Simpler than split-target, 90% of portal PnL.
- **Status:** Config finalized, user flips env vars Mar 25 when at desk. Circuit breaker $300/day.

### SPY Options (Paused — pending MES validation)
- **TradeStation #11697180, funding to $7,000**
- **Strategy:** V9-SC filter, 1 SPY 0DTE per signal at 0.50 delta (ATM)
- **SIM validation first:** Track theo P&L via Options Log for 1-2 weeks
- **Max DD expected:** ~$600-800 (9% of $7K), recovers in ~4 days
- **T+1 cash account:** Max daily capital ~$6,200 (worst day), most days $2-4K
- **Monthly projection (conservative):** +$2,800-3,500 (40-50% ROI on $7K)

## Monthly Income Projections (Mar 18 updated — V9-SC + 0.50 delta)
- **SPY options (1 contract):** +$208/day = +$4,166/month (theoretical)
- **SPY options conservative (70%):** +$146/day = +$2,916/month
- **MES futures (10 contracts):** +67.4 pts/day × $5/pt = +$6,740/month (V9-SC)
- **Circuit breaker (pending):** "Stop after 4 consecutive losses" showed +48% P&L improvement. Need 30+ days to validate.

## Monthly Infrastructure Costs (~$599/mo)
| Service | Monthly | Notes |
|---------|---------|-------|
| Volland (vol.land) | $380 | Charm/vanna/gamma exposure data |
| Rithmic | $114 | Paper Trading + CME Market Depth + API |
| E2T Eval | $75 | Temporary — stops once eval passed |
| Railway Pro | ~$30 | 2 services + PostgreSQL |
| TradeStation | Free | $10K capital parked (minimum) |
| Telegram | Free | Bot API |
- Post-E2T: ~$524/mo (~$6,300/yr)
- Breakeven: ~40 pts/month at 3 MES average

## Design Decisions
- Volland auto-restart threshold: 5 cycles (~10 min) chosen to avoid false restarts during brief data gaps while still recovering within a reasonable window.
- Pipeline freshness requires `exposure_points_saved > 0` — intentionally strict. Better to alert on a false positive than miss a real outage.
- **Broker call synchronicity rule (Apr 6):** SIM auto-trader can use fire-and-forget (`_broker_submit` thread pool) for speed. Real trader MUST use synchronous calls — `place_trade`, `update_stop`, `close_trade` return results and handle errors inline. Learned the hard way: fire-and-forget `close_trade` failed silently, blocked slots all day, 9 trades missed ($183 net loss). `force_release()` added as safety net — frees slot synchronously before broker flatten attempt.
- **Stale order cleanup (Apr 6):** `cleanup_stale_orders()` cron at 09:28 ET removes unfilled orders from previous days. Prevents carry-over blocking (e.g., weekend stale orders blocking Monday trading).
