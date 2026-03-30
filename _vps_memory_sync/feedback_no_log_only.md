---
name: Never suggest LOG-ONLY for setups
description: User corrected that all setups are logged equally - the live filter controls what auto-trades, not the setup grade
type: feedback
---

Don't suggest making setups "LOG-ONLY" with special LOG grades. All setups fire normally with real grades (A+/A/B). The live filter (`_passes_live_filter`) controls which setups auto-trade — a setup NOT in the live filter simply fires, logs to DB, and shows on portal without auto-trading.

**Why:** The user's architecture separates detection (all setups fire equally) from execution (live filter whitelist). Suggesting "LOG-ONLY" implies the setup is second-class, when it's really just "not yet approved for auto-trade."

**How to apply:** When adding new setups, implement them with normal grading. Block in `_passes_live_filter()` by returning False for that setup name. Don't use special LOG grades or mention "log-only" — just say "portal/monitoring only, blocked from live filter."
