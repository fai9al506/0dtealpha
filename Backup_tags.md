# Backup Tags

Full backup tags for the 0DTE Alpha project. Use `git checkout <tag>` to revert to any backup.

| # | Date & Time | Tag | Notes |
|---|-------------|-----|-------|
| 1 | 2026-02-11 20:06 | `stable-20260211-200636` | Stable working version before volland_worker_v2 deployment |
| 2 | 2026-02-11 21:13 | `stable-20260211-211338` | Stable working version before Delta Decay chart |
| 3 | 2026-02-11 21:44 | `stable-20260211-214447` | Delta Decay chart added to Charts & Exposure views, query robustness fix |
| 4 | 2026-02-11 22:05 | `stable-20260211-220557` | Charts HT tab with high-tenor Vanna & Gamma 3x2 grid |
| 5 | 2026-02-12 01:00 | `stable-20260212-010000` | Regime Map with real candles and 1m/5m toggle before 2-min interval change |
| 6 | 2026-02-13 16:44 | `stable-20260213-164448` | Pre-Rithmic ES cumulative delta integration |
| 7 | 2026-02-13 15:24 | `stable-20260213-152400` | Rithmic delta worker + Volland v2 sync fix + pipeline alert fix |
| 8 | 2026-02-13 18:27 | `stable-20260213-182718` | Pre-TradeStation ES cumulative delta refactor (replacing Rithmic) |
| 9 | 2026-02-13 20:08 | `stable-20260213-200831` | ES Delta dashboard tab with 5-pt range bars, volume, delta, CVD candles |
| 10 | 2026-02-13 22:07 | `stable-20260213-220739` | Pre-ES quote stream (bid/ask delta range bars) |
| 11 | 2026-02-13 23:48 | `stable-20260213-234842` | ES quote stream, absorption detector, SPX key levels on ES Delta chart |
| 12 | 2026-02-14 19:00 | `stable-20260214-190010` | 401 Telegram alert, /api/health upgrade, Volland 0-pts per-exposure alert |
| 13 | 2026-02-15 00:05 | `stable-20260215-000500` | Pre-swing-based absorption detector rewrite (thread safety, auto-refresh, cooldown persistence) |
| 14 | 2026-02-16 18:00 | `stable-20260216-180000` | Session continuity system, DB cleanup (585 MB reclaimed), Volland weekend fix |
| 15 | 2026-02-17 22:30 | `stable-20260217-223000` | Pre-paradigm reversal setup, BofA relaxation, ES range bars investigation |
| 16 | 2026-02-18 21:54 | `stable-20260218-215457` | Trade Log tab, DD Exhaustion detector, outcome tracking with backfill |
| 17 | 2026-02-19 23:45 | `stable-20260219-234500` | DD continuous trail (activation=20, gap=5), GEX SL=8+trail, high/low tracking, outcome fixes, trade study |
| 18 | 2026-02-23 17:00 | `stable-20260223-170000` | Eval trader + auto_trader + tmp scripts + reports + eval API plan saved |
| 19 | 2026-02-24 19:00 | `stable-20260224-190000` | Pre-tab reorganization (Charts merge, Historical merge, tab reorder) |
| 20 | 2026-02-24 22:35 | `stable-20260224-223500` | Survival mode (BE@5+SL=12), economic calendar, LIS fix, analysis scripts |
| 21 | 2026-02-25 pre | `stable-20260225-pre-playback-redesign` | Pre-playback tab redesign (rollback point) |
| 22 | 2026-02-25 20:56 | `stable-20260225-205654` | ES Absorption swing-to-swing rewrite (4 patterns, d=40, +161pts backtest) |
| 23 | 2026-03-01 12:00 | `stable-20260301-120000` | Single-position mode + vanna filter + DD filters for auto-trader |
| 24 | 2026-03-10 18:45 | `stable-20260310-184500` | Alignment +3 gate live (81% WR backtest). Day 1: futures +$1,242, options +$4,724. Tick trade for E2T. Options on all setups. |
| 25 | 2026-03-10 pre-SPY | `stable-20260310-spy-before-push` | Pre-SPY integration. Rollback point if SPY chain fetch causes any issues. |
| 26 | 2026-03-11 pre-asym | `stable-20260311-asymmetric-filter-pre` | Pre-asymmetric short filter. Analysis #9 saved. Rollback if new filter causes issues. |
| 27 | 2026-03-12 pre-v7ag | `stable-20260312-pre-v7ag` | Pre-V7+AG filter upgrade. Charm S/R implemented. Analysis #11 saved. Rollback if V7+AG causes issues. |
| 28 | 2026-03-21 | `stable-20260321-stock-gex-scanner` | Stock GEX Scanner added (59 stocks, data collection, independent from 0DTE). Two expirations: weekly + opex. |
| 29 | 2026-03-25 | `stable-20260325-spy-dd-pre-impl` | Pre SPY DD implementation. Rollback to this if SPY DD capture breaks Volland pipeline. |
| 30 | 2026-03-28 | `stable-20260328-pre-watchdog` | Pre Volland watchdog thread. Rollback if watchdog causes false exits. |
| 31 | 2026-04-29 17:52 | `stable-20260429-175213` | Pre real-trader skip Telegram alerts + close_trade format fix + V14 SC long alignment rule. |
| 32 | 2026-05-06 15:22 | `stable-20260506-pre-s86` | Pre S86 4-fix deploy bundle (slippage fix + V14 ES Abs PURE filter + audit-trail fix + ES Abs C6 trail params). Rollback if any of the 4 changes misbehaves post-deploy. |
| 33 | 2026-05-18 21:00 | `stable-20260518-2100-v16-1-bug-fixes` | After day-long ship: V16.1 DD long align carve-out + 5 critical bug fixes (Bug 1/2/3a/3b/5) + DD dispatch gap closed + VIX Div disabled + GEX Long v3.1/v3.1.1. 8 commits today. Projection: $1,000-1,400/mo at 1 MES. Rollback point if June reveals issues with the new code. |
| 34 | 2026-05-19 23:00 | `stable-20260519-2300-v13.2-margin-removal-atomic-v16-mirror-s149-deferred` | After day-2 multi-fix ship: V13.2 vanna refinement (SC LONG cliff=A peak=B admit, DD SHORT narrowed) + margin pre-check REMOVED (TS is source of truth) + atomic NORMAL ordergroup (SIM-tested, code live, flag OFF) + V16 portal = exact TSRT mirror + S149 SC long BOFA-PURE 2x MES code shipped (flag default OFF, validate at 1x first). 6 commits today. Real broker +$230 (first DD long fire ever +$116). Projection at 1 MES @ 65% capture: ~$2,300/mo mean. Rollback point if V13.2 vanna admit decays in June. |
| 35 | 2026-06-13 14:06 | `stable-20260613-140610` | Pre-S217 June-drawdown fix (trail-bug #2 + basket-gate #1). Rollback point before real-money trade-logic changes. Root cause of −$1,338 6-day bleed = execution/capture in high-vol reversal regime (portal June −$12 vs broker −$1,338). Rollback here if #2 (S131 trail SPX-gate) or #1 (basket gate Scheme B) misbehave. |
