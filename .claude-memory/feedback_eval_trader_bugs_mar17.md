---
name: Eval trader bugs fixed Mar 17 — CHANGE action, stale ES price, flatten time
description: Three critical eval_trader bugs fixed Mar 17 that caused E2T account loss. OIF CHANGE missing action field, stale ES price guard, flatten time buffer before E2T deadline.
type: feedback
---

**Bug 1: NT8 OIF CHANGE missing action field**
- `change_stop()` wrote CHANGE without SELL/BUY action → NT8 silently rejected → stops disappeared
- Fix: Added `direction` param, includes exit_side (SELL for longs, BUY for shorts)
- Rule: ALL NT8 OIF commands must include the action field

**Bug 2: Stale ES price from Railway**
- Railway ES quote stream reported Friday's close on Sunday open (48pt stale spread)
- Stop/target calculated from stale price → stop placed ABOVE entry for longs
- Fix: `_MAX_ES_SPX_SPREAD = 25` — reject es_price if `|es_price - spot| > 25`, fall back to SPX spot
- Rule: Always validate Railway es_price against spot before using for order calculations

**Bug 3: Flatten time at E2T deadline**
- `flatten_time_ct: "15:50"` was exactly E2T's 3:50 PM CT cutoff
- Code uses `>=` so fires at 15:50:01 — past the deadline
- Fix: Set to "15:44" (6 min buffer). No new trades after "15:20"
- Rule: E2T cutoff is 3:50 PM CT. Always have buffer. User is in UTC+3 (Saudi).

**How to apply:** When touching eval_trader order logic, verify: (1) OIF format matches PLACE format, (2) prices validated against spread, (3) time configs have buffer before deadlines.
