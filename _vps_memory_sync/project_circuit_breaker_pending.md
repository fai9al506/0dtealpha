---
name: Circuit breaker — pending validation
description: "Stop after 4 consecutive losses" circuit breaker showed +$7,105 improvement (50% of total PnL) — needs more data before deploying
type: project
---

**Rule:** Stop trading for the day after 4 consecutive option losses.

**Backtest (Mar 1-13, 10 days, V8 filter, real option prices):**
- Baseline (no limit): $14,930
- Stop after 4 consecutive losses: $22,035 (+$7,105 / +48%)
- Stop after daily P&L < -$2,000: $18,200 (+$3,270)
- Max 20 trades/day: $20,670 (+$5,740)
- Stop after 3 losses: $9,160 (-$5,770 — too tight)

**Why not deployed yet:**
- Only 10 trading days — any daily circuit-breaker is potentially curve-fitting to 5 losing days
- The "4 consecutive losses" rule works here but might cut winners on another dataset
- Need 30+ trading days to validate reliably

**Why it matters:**
- Saving 50% of total PnL is massive
- Directly addresses worst days (Mar 4: -$4,325, Mar 9: -$2,795)
- Risk management priority — aligns with user's philosophy

**How to apply:** Re-run this analysis when we have 30+ days of V8 option data. If still shows significant improvement, implement as a configurable daily circuit breaker.

**Added:** 2026-03-14
