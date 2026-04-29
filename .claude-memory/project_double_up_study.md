---
name: Double-Up Size Filter — Weekly Review
description: Weekly review of SC long criteria to find high-conviction combos worth doubling position size
type: project
---

**Goal:** Identify SC long (and possibly other setup) criteria combinations with high enough WR and sample size to justify doubling position size (2x MES qty).

**Why:** Trade #1386 (Mar 31) caught a 43-pt V-shape reversal — SC long, +3 align, B grade, VIX 28.31, GEX-LIS paradigm. User wants to know if such setups can be predicted for size-up.

**Current findings (Mar 31, 72 filtered trades):**
- SC Long + Align +3 + Grade A+/A/B overall: 69% WR, +140.5 pts
- Same + VIX>25: 59% WR, -40.5 pts (drops hard)
- Same + VIX>28: 43% WR, -26.0 pts (danger zone)
- **BOFA-PURE paradigm: 94% WR (17t, +114.7 pts)** — best candidate but needs 50+ trades
- AG-TARGET: 100% WR (5t) — too small
- GEX-LIS: 33% WR — worst, avoid sizing up
- Near-bottom entries on big range days: 100% WR (15t) — but hindsight only

**Candidate double-up filters to track:**
1. SC long + align +3 + BOFA-PURE → 94% WR (need 50+ trades)
2. SC long + align +3 + AG-TARGET → 100% WR (need 30+ trades)
3. SC long + align +2 → 80% WR (15t) — watch for growth
4. Any setup combo with WR > 80% AND sample > 50 AND positive PnL

**How to apply:** Every week (Sundays or after Friday close), re-run the alignment × paradigm × VIX breakdown for SC longs. Check if any bucket has crossed the 50-trade threshold with WR > 75%. When one does, propose a concrete size-up rule to the user. Add as a scheduled task in Tasks.md.

**Threshold for action:** WR >= 75% with 50+ trades AND MaxDD acceptable for doubled size.
