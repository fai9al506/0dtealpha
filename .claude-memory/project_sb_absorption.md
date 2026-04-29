---
name: SB Absorption Research
description: SB (Single-Bar) and SB10 (10-pt) Absorption setup status, backtest results, and implementation details
type: project
---

## SB Absorption (5-pt bars) — LOG-ONLY, validated

**3-month backtest (Sierra Chart, Jan 2-Mar 20, 56 days):**
- 70 signals, 72.9% WR, +174.3 pts, PF 2.15
- MaxDD 39.5, MaxConsecLoss 2, PnL/day +3.1
- Monthly: Jan +25.4 | Feb +85.0 | Mar +63.9
- Shorts 69.2% WR (+98.1), Longs 77.4% WR (+76.2)
- Grade anti-predictive: C = most PnL (+68.3)
- SVB filter critical: blocks -154 pts of bad signals (from Rithmic study)
- Avg MFE = 32.1 pts — justifies wide trail

**Trail params:** T1=+10 fixed, T2=trail (BE@10, activation=20, gap=10)
- PnL = average of T1 and T2
- Gap=5 too tight (killed by first pullback), gap=10 optimal

**Status:** LOG-ONLY. 3 live signals, all WINs (+25.9 pts). Collecting more before enabling.

**Why:** User wants clean Telegram for real money trades only. SB stays silent in background.

**How to apply:** When ready to promote: uncomment Telegram send in `_run_single_bar_absorption()` line 4605, add to `_passes_live_filter()`.

## SB10 Absorption (10-pt bars) — LOG-ONLY, needs work

**Implementation (Mar 21):**
- Parallel 10-pt bar builder in rithmic_es_stream.py
- Same es_range_bars table with range_pts=10.0
- Separate cooldown (_cooldown_sb10_abs), 5-bar cooldown

**Backtest:** Only 10 signals in 56 days — 2x vol/delta gates too strict for 10-pt bars.
- Needs recalibration to 1.3x-1.5x multipliers for larger bars
- Concept is sound (naturally aggregates multi-bar patterns) but not yet viable

## Sierra Chart SCID Reader

Script `tmp_sb_sierra_backtest.py` reads Sierra's .scid binary files:
- SCDateTimeMS = int64 microseconds since Dec 30, 1899
- ES prices stored as price × 100
- Files: ESH6.CME.scid (240MB), ESM6.CME.scid (222MB)
- Can be reused for future backtests on any setup
