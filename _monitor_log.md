# System Monitor Log — 2026-04-01

## Issue #1: Recurring Watchdog Timeouts — FIXED
- **Root cause:** Broker API calls (close_trade, update_stop, place_trade) ran synchronously inside the market job. Up to 150s of blocking TS API calls in a 90s watchdog.
- **Compounding factor:** Stock GEX scanner (43 stocks) saturated TS API at scan times, throttling all requests.
- **Fix deployed (commit 70339dd):**
  1. All broker operations now fire-and-forget via `_broker_executor` (3-thread pool)
  2. Stock GEX opex scan staggered 10:00→10:30 ET
  3. HTTP session refresh every 30 min + explicit connect timeout (5s)
- **Result:** Cycles dropped from 90s+ (timeout) to 6-8s. Zero watchdog timeouts since fix.

## Issue #2: Volland Paradigm Intermittently N/A — SELF-RESOLVED
- Intermittent N/A on paradigm/LIS/charm (3 out of 9 saves early morning)
- Self-recovered, no action needed. Monitor if recurs.

## ⚠️ PENDING VERIFICATION: Stock GEX Scanner (Apr 2)
- **Stock GEX weekly scans (10:00, 12:00, 15:00 ET) have NOT fired since the fix was deployed.**
- **First test: Apr 2 at 10:00 ET** — 43 stocks × chain fetch = 87 API calls over ~4 min.
- **If watchdog timeouts return at 10:00/12:00/15:00 ET, Stock GEX is the cause.**
- **Fallback plan:** Reduce stock list from 43 to 20, or increase inter-stock delay from 5s to 10s.
- Opex scan now at 10:30 ET (was 10:00, colliding with weekly).

## Timeline
- 09:10 ET — Volland re-login successful
- 09:23 ET — Market pre-open, all systems nominal
- 09:26 ET — First watchdog timeout
- 10:27-14:00 ET — Continuous watchdog timeout loop (~every 2 min)
- 14:10 ET — Timing instrumentation deployed, identified 6-8s cycles (not the bottleneck)
- 14:30 ET — HTTP session fix deployed, timeouts persisted → not stale connections
- 14:48 ET — Debug timing deployed, found hang in _check_setup_outcomes (broker calls)
- ~15:05 ET — Full audit: broker close_trade = 50s blocking, Stock GEX = 279 calls at 10:00
- ~15:15 ET — Async broker fix + Stock GEX stagger deployed
- 15:15+ ET — All cycles 6-8s, zero errors, system fully healthy
