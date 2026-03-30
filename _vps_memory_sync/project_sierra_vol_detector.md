---
name: Sierra Chart Vol Detector Study
description: VX order flow absorption study — green dot=buy VIX (bearish SPX), red dot=sell VIX (bullish SPX). No price filter (momentum vs absorption not distinguished).
type: project
---

## Sierra Vol Detector — Ready for Monday

**File:** `C:\SierraChart\ACS_Source\VolDetector.cpp` (already in place, needs to be built)

### Step 1: Build the DLL
- Open Sierra Chart
- **Analysis > Build Custom Studies DLL** (remote build, 64-bit)
- Should compile `VolDetector.cpp` → `VolDetector_64.dll`

### Step 2: Add to ES chart
- Right-click ES chart > **Studies > Add Custom Study**
- Add **"Vol Detector"** — goes in panel below price (delta bars + CVD + absorption signals)
- Add **"Vol Detector Bar Color"** — colors main chart bars by delta imbalance

### Step 3: What to look for (Apollo's method)
The study shows:
1. **Delta bars** (green/red) — who's more aggressive (buyers vs sellers)
2. **CVD line** (gold) — cumulative delta, shows overall buyer/seller dominance
3. **Absorption diamonds:**
   - **Cyan** = bullish absorption (high volume, sellers aggressing, but price holds = vol sellers providing support)
   - **Magenta** = bearish absorption (high volume, buyers aggressing, but price drops = vol buyers creating resistance)
4. **CVD divergence arrows:**
   - **Green up arrow** = price making new low but CVD holding (bullish divergence)
   - **Pink down arrow** = price making new high but CVD failing (bearish divergence)

### Step 4: Compare with our system
- When absorption cyan diamonds appear near our LIS levels → strong confirmation of support
- When absorption magenta diamonds appear near our charm target → resistance confirmation
- CVD divergence arrows should align with our ES Absorption / SB Absorption signals
- Track: does the Vol Detector spot setups BEFORE our automated system fires?

### Step 5: Tune parameters
Default settings:
- Divergence lookback: 8 bars
- Absorption vol threshold: 1.5x average (20-bar)
- Absorption max price move: 2.0 pts
- Imbalance threshold: 55%

Adjust based on chart type (range bars vs time bars). For 5-pt range bars, the 2.0pt price threshold means "price moved less than half a bar" = strong absorption.

### Future: VIX chart
- VIX symbol in Sierra via Rithmic — test on Monday during market hours
- Weekend = no data download (expected)
- Once VIX loads, can overlay with ES for spot-vol correlation visual

### What this is NOT
- This is NOT automated trading — it's a visual tool for manual discretion
- Apollo's full method also requires tape reading (Time & Sales) which is built into Sierra natively
- The study automates the DETECTION part; the trader confirms with tape
