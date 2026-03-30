---
name: Show n/a instead of hiding dashboard rows
description: Never hide dashboard stats rows when data is null — always show "n/a" or "error" so issues are visible
type: feedback
---

Never hide dashboard stat rows when data is null/missing. Always show "n/a" or "error" instead.

**Why:** If a row is hidden, the user won't notice something is broken. If it shows "n/a" or "error", they can spot and fix the issue immediately.

**How to apply:** When adding any new stat/indicator to the dashboard, always include an `else` branch that renders the row with "n/a" in muted text. Same applies to any existing rows that currently hide on null.
