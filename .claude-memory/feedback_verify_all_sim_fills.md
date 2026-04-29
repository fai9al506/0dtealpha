---
name: Always verify ALL SIM fills against theo prices
description: TS SIM options fills are unreliable - EVERY exit must be checked against theo_exit, not just obvious outliers
type: feedback
---

ALWAYS compare sim_exit vs theo_exit for EVERY option trade, not just the obvious outliers.

**Why:** On Mar 16, I only caught 2 trades with $7.87 fake fills and declared the rest "corrected at +$1,133". In reality, 17 out of 20 exits were fake (stale per-strike prices). Real P&L was -$71, not +$1,133. User had to call it out.

**How to apply:** When analyzing options SIM data, NEVER trust sim_exit. Always use theo_exit (the live API bid price at exit time). Compare every single trade, not just outliers. If sim_exit != theo_exit by more than $0.05, flag it as fake.
