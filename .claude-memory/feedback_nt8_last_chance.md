---
name: NT8 last chance — Sierra Chart migration if any more bugs
description: User will switch from NinjaTrader 8 to Sierra Chart for eval_trader if any more NT8/OIF integration bugs occur. Decision made Mar 17, 2026 after losing E2T account ($75) due to flatten time bug.
type: feedback
---

NT8 is on its LAST CHANCE for eval_trader integration. If any more NT8 OIF bugs cause real losses, migrate to Sierra Chart.

**Why:** Three E2T accounts lost to NT8-related issues:
- Mar 11: 6-instance bug → -$1,540, account failed ($65 reset)
- Mar 16-17: CHANGE command missing action field (silent stop failure) + stale ES price (wrong stop levels) + flatten_time_ct at deadline not before it → account failed ($75 reset)
- User is frustrated with "endless bugs" in NT8 OIF integration

**How to apply:**
- Triple-check any NT8 OIF format changes before deploying
- Test all OIF commands (PLACE, CHANGE, CANCEL) with actual NT8 before live trading
- If a new NT8 bug surfaces, immediately start Sierra Chart migration (DTC protocol)
- Keep Sierra migration plan in mind as fallback
