---
name: SC Trail Optimization Opportunity
description: Skew Charm trail params may be suboptimal — backtest shows activation=12/gap=6 could yield +1036 vs actual +424 pts (240 trades, Feb-Mar 2026)
type: project
---

Skew Charm trail optimization is a high-priority investigation.

**Current SC RM:** hybrid trail, BE@+10, activation=10, gap=8, initial SL=14
**Backtest finding (Mar 27 analysis):**
- SC actual: 240 trades, 63.3% WR, +423.9 pts
- Alt (act=12, gap=6): simulated +1,036.6 pts, 49.2% WR, PF 1.99x
- SC winners avg MFE=20.6, avg capture=10.5 (51% efficiency), 28% of winners leave 10+ pts on table

**Why:** SC's gap=8 is very loose — trail gives back too much. Tightening to gap=6 with slightly higher activation=12 captures significantly more per winner.

**How to apply:** Run comprehensive SC trail parameter sweep (activation 8-16, gap 4-10) against the 240-trade dataset. Then validate with fresh data before deploying. This could be the single biggest PnL improvement available.

**Caveat:** MFE-based simulation is conservative (assumes MAE before MFE). Real improvement may be different. Need price-path simulation from chain_snapshots for accuracy.
