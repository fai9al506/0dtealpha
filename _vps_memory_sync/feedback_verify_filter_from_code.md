---
name: Always verify active filter from code, not memory
description: When writing backtests or analysis scripts, read _passes_live_filter() from the actual codebase — never trust MEMORY.md or CLAUDE.md for the current filter version
type: feedback
---

Always verify the active filter version by reading `_passes_live_filter()` in `app/main.py` before writing any backtest or analysis script. Do NOT rely on MEMORY.md or CLAUDE.md — these docs can be stale.

**Why:** Used V10 rules from memory while V11 was already deployed (V11 added time-of-day gates). The study ran with wrong filter, user caught the error. Memory/docs lag behind code changes.

**How to apply:** At the start of any backtest or filter-related analysis, `grep _passes_live_filter app/main.py` and read the actual function. Replicate its exact logic in the script. Never hardcode filter rules from memory alone.
