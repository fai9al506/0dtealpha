---
name: ES Absorption Rebuild — User Visual Signals
description: Rebuilding ES Absorption from scratch using ~20 manually identified signals from user's Sierra Chart. Volume intensity (vol/sec) replaces raw volume gate.
type: project
---

## ES Absorption Rebuild (started Mar 28 2026)

**Approach:** User manually identifies ~20 absorption signals on Sierra Chart (5-pt range bars), provides date+time. We pull full bar data from DB, extract common traits, build new detection criteria from user's visual pattern recognition.

**Key insight (user):** Raw volume gate is broken for range bars. A bar forming in 10 sec with 1K vol is more intense than a 3-min bar with 5K vol. Volume RATE (vol/sec) is the correct metric.

**Data infrastructure:** `exports/es_range_bars_march_volrate.csv` — 12,738 bars, 38 columns, all March 2026.

**Backtest findings (raw vs rate gate):**
- Rate gate alone is worse than raw (-9.5 vs +50.5 PnL)
- Both gates combined: 49 signals, 46.9% WR, +22.5 PnL, MaxDD 38 (74% less DD)
- div_raw is INVERTED: mild divergence (1) = 49% WR, extreme (4) = 23% WR
- Medium-volume shorts (1.0-1.4x rate) = best bucket: 52.5% WR, +105 PnL

**User expectations:** ~5 signals/day max, normally 1-2. ~100/month.

**Signals collected so far:**
1. Mar 27, 11:17 (bar 278) — RED climax bar, vol=13,089 (2.67x), delta=-887, price reversed +32 pts after, max DD -0.5

**Status:** IMPLEMENTED as "Delta Absorption" (LOG-ONLY). Deployed Mar 29.

**V7 Final Results (March 2026 backtest):**
- 91 signals, 4.5/day, 62.6% WR, +292.5 pts, PF 3.01
- MaxDD -16.2, 95% green days, avg +14.6/day
- Grading: A+=86% WR, A=79% WR (monotonic, r=0.44)

**Signal rules:**
- Core: delta opposes bar color (or doji with delta opposing prior trend)
- Min |delta| >= 100, trend precondition (3/5 bars opposite), cooldown 5 bars
- T1 (Doji): body < 1.0, any time 9:30-15:00
- T3 (Afternoon): 12:30-15:00, |delta| >= 200, skip 14:00-14:30
- Filter: peak ratio < 2.5
- Trail: IMM stop = max(maxProfit - 8, -8)

**Grading (4 components, max 100):**
- Delta magnitude (0-30): 200-500=30, 500-700=20, 100-200=15, 1000+=10, 700-1000=3
- Body size (0-25): 0.5-1.0=25, 3.0-4.0=22, 2.0-3.0=18, 1.0-2.0=14, 4.0+=10, <0.5=2
- Signal freshness (0-20): #1-2=20, #3=8, #4+=2
- Time of day (0-25): 12:30-13:00=25, 14:30-15:00=20, 10-11=15, 9:30-10=15, 11-12=12, 13:30-14=12, 13-13:30=5, else=0
- Thresholds: A+>=85, A>=70, B>=55, C>=40, LOG<40

**Next:** Collect live LOG signals, compare against user's Sierra picks. When ready: enable A+&A for live trading (77% WR, +314 pts backtest).
