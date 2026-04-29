# Feedback: NEVER Fire-and-Forget Real Money Broker Calls

**Date:** 2026-04-06 (session 68)
**Severity:** CRITICAL — cost $183 net (9 missed SC trades, $253 potential)

## What Happened
Commit `70339dd` (Apr 1) moved ALL broker calls to fire-and-forget `_broker_submit` thread pool to fix 90s market job timeouts. This was correct for SIM auto-trader (non-critical) but broke real_trader silently:
- `close_trade` failed silently → slot stayed blocked → 9 SC trades missed all day
- `place_trade`/`update_stop` were also affected (3 HIGH risk operations)

## The Rule
**`_broker_submit` (fire-and-forget) is ONLY for SIM auto-trader.**

Real trader (`app/real_trader.py`) MUST use synchronous broker calls:
- `place_trade()` — returns order ID, handles rejection inline
- `update_stop()` — returns success/failure, retries if needed
- `close_trade()` — returns confirmation, force_release runs first

Silent failures in real money = blocked slots + missed trades + no error visibility.

## How to Avoid
- When optimizing for speed (timeouts, async), NEVER blanket-convert all callers
- Audit each caller: SIM = fire-and-forget OK, REAL = synchronous mandatory
- The 90s timeout fix should have been scoped to SIM auto_trader only
