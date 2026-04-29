---
name: CRITICAL — Never guess timezone conversions in DB queries
description: Timezone bug in DD backtest (used +3h instead of UTC) produced OPPOSITE results. ALWAYS use raw UTC timestamps when joining tables. Never manually add hours. This error can destroy all research.
type: feedback
---

## RULE: NEVER manually convert timezones in DB queries

**Incident:** Mar 28 DD per-strike backtest. Used `ts_et + interval '3 hours'` to convert ET back to UTC for joining with `volland_exposure_points.ts_utc`. This was WRONG:
- EDT (Mar-Nov) = UTC-4, not UTC-3
- EST (Nov-Mar) = UTC-5
- The 1-2 hour error caused exposure data to mismatch with signal timestamps
- Result: V1 backtest showed mechanical = +33.7 PnL (good). V2 (fixed) showed mechanical = -20.5 PnL (bad). OPPOSITE conclusion.

**Why:** The user caught this. A timezone bug silently produced convincing but WRONG results. No error, no crash — just wrong data joins that flip the entire conclusion.

**How to apply:**
1. **ALWAYS use raw UTC timestamps** when joining tables. Both `setup_log.ts` and `volland_exposure_points.ts_utc` are timestamptz — compare them directly. NEVER convert to ET and back.
2. **If you MUST display in ET:** Use `ts AT TIME ZONE 'America/New_York'` for display ONLY. Never use the ET result in WHERE clauses against UTC columns.
3. **Never hardcode UTC offsets** (+3, +4, +5). Use `zoneinfo.ZoneInfo("America/New_York")` in Python or `AT TIME ZONE` in SQL.
4. **Cross-check:** After any timezone-sensitive query, verify by checking a known timestamp manually (e.g., "this trade at 10:15 ET should match exposure data from ~14:15 UTC in March").

**DB timezone reference:**
- `setup_log.ts` — timestamptz (stored UTC)
- `chain_snapshots.ts` — timestamptz (stored UTC)
- `volland_snapshots.ts` — timestamptz (stored UTC)
- `volland_exposure_points.ts_utc` — timestamptz (stored UTC)
- `es_range_bars.ts_start/ts_end` — timestamptz (stored UTC)
- `spx_ohlc_1m.ts` — timestamptz (stored UTC)
- All tables store UTC. Join on raw timestamps. Display in ET only for user-facing output.
