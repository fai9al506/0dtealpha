---
name: Bar Scanner Forming-Bar Bug Fix
description: Outcome tracker missed T1 hits on ES-based trades due to forming-bar idx collision — 17 trades corrected, +281.4 pts impact
type: project
---

## Forming-Bar Bug (discovered & fixed 2026-03-29)

**Bug:** `_check_setup_outcomes()` scans ES range bars to track `_seen_high`/`_seen_low`. The Rithmic `get_rithmic_bars()` function returns completed bars + the current forming bar. The forming bar gets `idx = len(completed)` — the same idx it will have when completed.

**Mechanism:**
1. Outcome checker runs, scans forming bar (e.g., idx=366, high=6432.25 — partial)
2. Sets `_es_last_bar_idx = 366`
3. Bar completes with real high (6435.25) — above target
4. Next outcome check: completed bar 366 has `bidx <= last_scanned (366)` → **SKIPPED**
5. T1 (+10pt) never registered, trail never activated, original stop hit → false LOSS

**Fix:** Changed `bidx <= last_scanned` to `bidx < last_scanned` (re-scans last bar to catch forming→completed high update). Special case: `bidx == entry_bar_idx` still skipped to avoid scanning entry bar.

**Why:** Fast bars (11 seconds for bar 366) complete between outcome checker polls (~30s). The partial high from the forming snapshot was below target, but the completed high exceeded it.

**How to apply:** Code fix in `app/main.py:_check_setup_outcomes()` around line 3785. Deployed with the fix. No further action needed unless bar scanning logic is modified.

## Trades Corrected (17 total, +281.4 pts)

| ID | Date | Entry | Was | Corrected | Impact |
|----|------|-------|-----|-----------|--------|
| 387 | Mar 3 | 6745.0 | LOSS -12 | WIN +19.9 | +31.9 |
| 395 | Mar 3 | 6802.75 | LOSS -12 | WIN +5.0 | +17.0 |
| 401 | Mar 3 | 6795.75 | LOSS -12 | WIN +5.0 | +17.0 |
| 415 | Mar 3 | 6836.75 | LOSS -12 | WIN +5.0 | +17.0 |
| 421 | Mar 3 | 6832.75 | LOSS -12 | WIN +5.0 | +17.0 |
| 432 | Mar 4 | 6842.5 | LOSS -12 | WIN +5.0 | +17.0 |
| 434 | Mar 4 | 6857.25 | LOSS -12 | WIN +14.5 | +26.5 |
| 573 | Mar 6 | 6767.0 | LOSS -12 | WIN +5.0 | +17.0 |
| 575 | Mar 6 | 6758.0 | LOSS -12 | WIN +5.0 | +17.0 |
| 682 | Mar 11 | 6791.0 | LOSS -8 | WIN +5.0 | +13.0 |
| 732 | Mar 12 | 6708.5 | LOSS -8 | WIN +5.0 | +13.0 |
| 737 | Mar 12 | 6709.5 | LOSS -8 | WIN +5.0 | +13.0 |
| 918 | Mar 18 | 6738.0 | LOSS -8 | WIN +5.0 | +13.0 |
| 939 | Mar 18 | 6707.75 | LOSS -8 | WIN +5.0 | +13.0 |
| 1013 | Mar 20 | 6598.75 | LOSS -8 | WIN +5.0 | +13.0 |
| 1199 | Mar 25 | 6656.75 | LOSS -8 | WIN +5.0 | +13.0 |
| 1331 | Mar 27 | 6424.25 | LOSS -8 | WIN +5.0 | +13.0 |

## Bug 2: Batch-Scan Temporal Order (discovered same session)

**Bug:** Bar scanner accumulated `_seen_high`/`_seen_low` across ALL new bars in one pass, then ran trail + stop check. This broke temporal order: a favorable move on bar N+5 could advance the trail, masking a stop hit on bar N.

**Mechanism:** In one 30s cycle, bars N through N+10 all scanned → `_seen_high` includes favorable bar N+5 → trail advances to BE → stop check uses new BE level → old stop hit on bar N is masked → false WIN.

**Fix:** Restructured ES-based path to process bars ONE AT A TIME: update extremes → advance trail → check stop per bar. If stop hit on bar N, break immediately. Broker stop updates run after the loop (final level only).

**Trades corrected:** 21 false WINs → LOSS, 4 wrong P&L. Impact: -184.2 pts.
- 11 SB2 Absorption trades
- 10 ES Absorption trades (including 7 false WINs with MAE > -10, impossible with 8pt stop)

**User verified 3 samples on TradingView (Mar 29) — all confirmed correct.**

## Final Impact (both bugs combined)

| Bug | Direction | Trades | P&L Impact |
|-----|-----------|--------|------------|
| Forming-bar | false LOSS → WIN | 17 | +281.4 |
| Batch-scan | false WIN → LOSS | 21 | -184.2 |
| Batch-scan | wrong P&L | 4 | varies |
| **Total** | | **42** | **+97.2 net** |

**ES Absorption corrected:** +152.0 pts (305t, 54.5% WR). #4 earner.
**SB2 Absorption corrected:** -46.2 pts (35t, 40% WR). LOSER — shorts 76% WR, longs 39% toxic.
