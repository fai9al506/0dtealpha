---
name: Always verify numbers from DB
description: Never present P&L or trade counts from manual calculations — always query the DB directly
type: feedback
---

Always verify P&L numbers, trade counts, and filter results by querying the actual database. Do NOT present numbers from manual calculations or hardcoded simulations.

**Why:** In the Mar 23 deploy analysis, manual simulation said V11 P&L = +132.3 but actual DB showed +234.6. The V11 filter uses `_passes_live_filter()` (alignment + VIX + paradigm), not just grade. Manual estimates were wrong multiple times in the same session, eroding trust.

**How to apply:** When computing P&L, trade counts, or filter results — ALWAYS query the DB first. If a DB query isn't possible, explicitly state "this is an estimate" and flag the uncertainty. Never present estimates as facts.
