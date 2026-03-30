---
name: Backtest reporting standards
description: Always include PnL duration, max drawdown, risk factor, and full setup details (entry/SL/target/trail) in any backtest or analysis
type: feedback
---

When presenting backtest or analysis results, ALWAYS include:
1. **PnL with duration** — e.g., "+336 pts over 22 days" not just "+336 pts"
2. **Max drawdown** — worst peak-to-trough intraday or multi-day DD
3. **Risk factor** — profit factor, Sharpe, or similar risk-adjusted metric
4. **Setup details** — full entry logic, stop loss, target, trailing parameters

**Why:** User needs the full picture to evaluate a setup. Raw PnL without context is misleading — a +300 setup with -200 MaxDD is very different from +300 with -50 MaxDD.

**How to apply:** Every backtest summary table and recommendation must include these fields. Don't present PnL in isolation.
