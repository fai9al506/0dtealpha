# Monday Morning Monitoring Playbook

Open this file and tell Claude: **"Monitor the deploy per monitor.md"**

## Timeline

### Pre-market (~9:15-9:25 ET)

1. Check both Railway services are alive:
   ```bash
   railway logs -s 0dtealpha --lines 20
   railway logs -s Volland --lines 20
   ```

2. Hit `/api/health` — verify all components reporting

3. Check DB for stale cooldown state — ensure new `swing_tracker` key doesn't cause import errors on startup

4. Verify Volland is logged in and not stuck in sync

### Market Open Watch (~9:30-9:45 ET)

5. Tail Volland logs — look for `saved ... exposures=10 points=XXXX`

6. Tail 0dtealpha logs — look for:
   - `[chain]` — options chain pulling
   - `[es-quote]` — quote stream connected, range bars building
   - `[absorption]` — swing tracker updating (no signals expected before 10:00 AM)
   - `[pipeline]` — health checks running

7. Watch for any `KeyError`, `TypeError`, or crash from the new swing code

8. Hit `/api/es/delta/rangebars` — confirm range bars are building

9. Watch for 401 errors from TradeStation token

### Post-10:00 AM

10. Watch for first absorption signals — confirm swing-based format in logs:
    ```
    [absorption] BULLISH C (XX/100) price=XXXX.XX cvd=+XXX vol=XXX(X.Xx) best_swing: L@XXXX.XX cvd_z=X.XX price_atr=X.Xx swings=X
      div#1: L@XXXX.XX idx=XX cvd=+XXX -> z=X.XX atr=X.Xx score=XX
    ```

11. Verify Telegram alerts arrive for qualifying signals

## Quick Fixes

| Problem | Fix |
|---------|-----|
| Absorption crashes | Disable via API: `POST /api/setup/settings?absorption_enabled=false` |
| Volland stuck | `railway restart -s Volland --yes` |
| 401 errors | Check TS_REFRESH_TOKEN env var, may need manual refresh |
| Web service crash | Check `railway logs -s 0dtealpha --lines 50`, restart if needed |
| Old cooldown JSONB error | Safe — `import_cooldowns` checks keys with `if "swing_tracker" in data` |
| New settings not in DB | Safe — all use `.get()` with defaults |

## Weekend Changes Risk Assessment

| Change | Risk | Status |
|--------|------|--------|
| Swing tracker in cooldown DB | Old JSONB missing `swing_tracker` key | Safe — guarded with `if` check |
| New settings keys (`abs_pivot_left` etc.) | Not in saved DB settings | Safe — `.get()` with defaults |
| Volume ratio 1.5 -> 1.4 | More frequent volume triggers | Expected behavior |
| Grade "C" fallback | Signals that were suppressed now fire | Expected — detection-first mode |
| No absorption before 10:00 AM | Opening bars skipped | By design — volume unreliable pre-10AM |
| Charm thresholds recalibrated | [50M, 100M, 250M, 500M] vs old [500-10K] | Correct — matches actual data |
| Dashboard no-reload | Tabs refresh in-place | Tested, tab persistence via sessionStorage |
| Thread locks on ES state | `_es_delta_lock`, `_es_quote_lock` | Standard pattern, matches existing code |

## Commands Reference

```bash
# Service logs
railway logs -s 0dtealpha --lines 30
railway logs -s Volland --lines 30

# Filter specific component
railway logs -s 0dtealpha --filter "absorption"
railway logs -s 0dtealpha --filter "es-quote"
railway logs -s 0dtealpha --filter "pipeline"
railway logs -s Volland --filter "saved"

# Restart
railway restart -s Volland --yes
railway restart -s 0dtealpha --yes

# Quick disable absorption if crashing
# POST /api/setup/settings?absorption_enabled=false
```
