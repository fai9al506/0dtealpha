---
name: Never assume — always use real data
description: User strictly forbids assumptions in calculations. Must use actual data (snapshots, API, DB). If real data unavailable, confirm with user before proceeding.
type: feedback
---

**NEVER assume values in calculations.** Always use real data from snapshots, API, or DB.

Examples:
- Options P&L: use actual option prices from chain_snapshots, NOT a delta approximation
- If data is unavailable (e.g., SPY snapshots don't exist for a date range), tell the user and ask how to proceed
- If there's truly no alternative but to estimate, explicitly confirm with user FIRST before using any approximation

The user said: "plz, never assume!!! mark this in ur brain, if u have no choice only to assume, confirm with me first."
