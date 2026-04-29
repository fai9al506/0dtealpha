---
name: DD Combined SPX+SPY — Tracking Start Date
description: Combined DD (SPX+SPY) feeds setup detector since Mar 25 2026. Historical data before this date is SPX-only. Critical for future backtesting.
type: project
---

## DD Combined SPX+SPY — Data Boundary

**Date combined DD deployed:** 2026-03-25

**Before Mar 25:** DD Exhaustion setup detector used SPX DD only. All historical setup_log entries, backtests, and WR/PnL numbers are based on SPX-only DD.

**After Mar 25:** Setup detector uses SPX + SPY DD combined. If SPY DD is missing (old snapshots), falls back to SPX-only (backward compatible).

**Why this matters for backtesting:**
- Any future backtest of DD Exhaustion comparing "before vs after" must account for this change
- Improved DD signal should show up as better DD Exhaustion WR/PnL after Mar 25
- Cannot retroactively apply combined DD to old trades (no historical SPY DD in DB before Mar 25)

**How to apply:** When backtesting DD Exhaustion, split analysis at Mar 25 boundary. Compare SPX-only period vs combined period.
