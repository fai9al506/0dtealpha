---
name: Real MES Trading Accounts
description: Two TradeStation real money accounts for SC strategy — one longs, one shorts, 1 MES each, cap=2 concurrent
type: project
---

Two real TradeStation accounts for SC on MES (1 MES per trade, cap=2 concurrent per direction):

- **210VYX65** — Account A (SC Longs only)
- **210VYX91** — Account B (SC Shorts only)

**Why:** Total capital $7K ($3,500 per account). Start with 1 MES ($5/pt). Scale to 2 MES when balance reaches ~$13K.

**How to apply:** Configure two eval_trader instances, each locked to one direction. Same V9-SC filter, SL=14, trail params as SIM.

**Backtest basis:** 13 days Mar 2-18, 147 trades, 72% WR, PF 1.62, +$3,265/month at 1 MES. Worst case $140 per event.

Created: 2026-03-19
