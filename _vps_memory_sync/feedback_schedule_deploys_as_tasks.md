---
name: Schedule post-market deploys as Tasks.md entries
description: Always write pending deploys to Tasks.md with date/time so they survive session crashes
type: feedback
---

Always schedule post-market code deploys as Tasks.md entries (with date, time, and exact steps) in case the session dies before deploy time.

**Why:** User explicitly asked for this after SB2 v2 backtest — if session crashes between backtest completion and market close, the next session needs to know what to deploy and exactly how.

**How to apply:** When a code change is validated but can't be deployed yet (market hours), immediately add a scheduled task with: (1) exact trigger time, (2) all changes to make, (3) backtest results for reference. Use format `S##` in the scheduled tasks table.
