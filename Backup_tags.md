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
