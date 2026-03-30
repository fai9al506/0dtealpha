---
name: V8 VIX3M Symbol Verification
description: URGENT Monday check - verify $VIX3M.X works on TradeStation, otherwise V8 gate blocks ALL longs at VIX>26
type: project
---

**MUST CHECK when market opens (next trading day):**

1. Check `/api/health` for `vix3m` field — if null, `$VIX3M.X` is not a valid TS symbol
2. If null, try alternatives: `$VIX3M`, `VIX3M.X`, or use CBOE data/yfinance fallback
3. If overvix stays null, the V8 gate defaults to `_ov = -99` which means ALL longs at VIX>26 get blocked (no overvix override possible)
4. This is safe (same as plain A5) but means the Apollo mean-reversion override won't work

**How to check:**
```
curl -s https://0dtealpha.com/api/health | python -c "import json,sys; d=json.load(sys.stdin); print(f'VIX={d.get(\"vix\")} VIX3M={d.get(\"vix3m\")} OV={d.get(\"overvix\")}')"
```

**If VIX3M is null:** Check Railway logs for the quote response:
```
railway logs -s 0dtealpha --lines 50 --filter "VIX3M"
```

**Why:** line 2193 in main.py: `api_get("/marketdata/quotes/%24SPX.X,%24VIX.X,%24VIX3M.X")`
If TS doesn't recognize `$VIX3M.X`, it may return fewer Quotes or error silently.

**Added:** 2026-03-14
