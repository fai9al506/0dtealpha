---
name: ES Price REST Fallback
description: TS WebSocket quote stream intermittently drops last_price=None, causing real trader to silently skip trades. REST fallback added.
type: project
---

**Problem (discovered Mar 31):** TS WebSocket quote stream `_es_quote["last_price"]` intermittently returns None. When this happens at setup fire time, real_trader silently skips the trade. Mar 31: zero real trades all day, missed V-shape SC #1386 (+43pt bounce). Also affected previous days (Mar 24: 1 miss, Mar 26: 2 misses including A+, Mar 30: 1 miss).

**Why:** TS WebSocket is an HTTP chunked stream, not true WebSocket. Drops due to DualLogon (3rd session boots existing), GoAway (server rotation), no read timeout (hangs silently). Mar 31: only 8K trades vs Rithmic 175K — stream was severely degraded all day.

**Fix (commit 4a272b6, pending push):** `_get_es_price_fallback()` — REST `GET /marketdata/quotes/@ES` called only when stream price is None. Added to 3 paths: SIM auto-trader, real_trader, ES Absorption. Rate limit safe (250/5min, we use ~1-2/day).

**How to apply:** If `[es-price] REST fallback` appears frequently in logs, investigate the TS WebSocket stream health — the fallback masks the problem but doesn't fix the root cause (stream reliability).
