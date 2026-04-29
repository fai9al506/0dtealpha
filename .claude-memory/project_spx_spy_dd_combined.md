---
name: SPX+SPY DD Combined Signal — Priority #1
description: Combine SPX and SPY DD Hedging from Volland for stronger directional signal. Apollo validated live Mar 23: combined -6B preceded 60pt SPX drop.
type: project
---

## SPX + SPY DD Combined Signal

**Priority:** #1 — implement tomorrow (Mar 25) when market is open.

**Evidence (Mar 23 at 11:34 ET / 18:34 KSA):** Apollo shared Volland screenshot showing SPX DD -3.9B and SPY DD -2.2B. Said "well this is quite bearish, almost -6B." SPX then dropped from ~6630 to 6570 (60 pts).

**Why:** Currently we only use SPX DD Hedging. Apollo sums SPX+SPY for net imbalance. When they offset (SPX bearish, SPY bullish) → neutral. When combined → -6B → strong directional signal. Single-source DD misses the full picture.

**How to apply:**
1. User will add SPY statistics workspace/URL to Volland
2. Amend volland_worker_v2.py to also scrape SPY DD Hedging
3. Dashboard: add "Total DD Hedging" row below existing "DD Hedging" (SPX+SPY sum)
4. Setup detector: use combined DD for DD Exhaustion signals
5. Implementation requires seeing live data first to confirm SPY DD format matches SPX

**Safe implementation plan (agreed Mar 24):**
- Step 1: ✅ User created NEW Volland workspace (copy of existing + SPY statistics widget)
  - URL: `https://vol.land/app/workspace/69c2d38cce2143e384a8cfa1`
  - SPY statistics is a SEPARATE widget on the same page
- Step 2: Create `volland_worker_v3_test.py` — exact v2 copy with:
  - Separate test tables (`volland_v3_test_snapshots`, `volland_v3_test_exposure_points`)
  - Multi-paradigm capture (SPY widget fires its own paradigm API call)
  - Enhanced diagnostic logging for all API patterns
  - No Telegram (console only)
  - New workspace URL
- Step 3: Run locally to test — if works, merge SPY DD into v2. If fails, delete test file, zero impact.
- Step 4: Dashboard display (Total DD = SPX + SPY)
- Step 5: Setup detector integration

**Status:** v2 code fully read. Ready to write v3_test. Resume Mar 25.
