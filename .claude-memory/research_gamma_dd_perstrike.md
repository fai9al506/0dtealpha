---
name: Gamma & DD Per-Strike Research (Mar 20, 2026)
description: Deep study of Volland gamma and delta decay per-strike data for filtering/scoring — no actionable filter found after V9-SC
type: project
---

## Gamma Per-Strike Study (772 trades, 24 days, Feb 13 - Mar 19)

**Triggered by:** Discord traders calling "stacked 6600" on Mar 19 — gamma + charm converging at same strike.

### Key Findings

**Stacked S/R (charm + gamma at same strike):**
- Stacked: 61% WR, +957.7 pts (453 trades)
- Single: 52% WR, -44.9 pts (312 trades)
- BUT after V9-SC applied: no incremental edge. V9-SC already captures the quality.
- Cannot hard-block singles — loses +1,707 pts of good winners.

**Gamma per-strike after V9-SC (V10 attempt):**
- V10 (4 gamma filters): +35.2 pts over V9-SC. Modest, unreliable.
- F1 (short gamma resist): -6.2 pts. V9-SC already handles bad shorts.
- F2 (long gamma pin >20M): +60.8 pts BUT confounded with VIX. Early dates (VIX 19-20) = winners, late dates (VIX 26-27) = losers. Not gamma, it's VIX.
- F3 (BofA stacked): -20 pts (blocks 2 winners). Too few trades.
- F4 (TF agreement): 0 impact (93% of trades already 4/4 agreement).

**Volland Whitepaper on gamma:** "The high level of focus on gamma as a market-moving force is misguided." Gamma hedging ~$5-10B/pt vs vega hedging in trillions. Gamma's correlation to SPX is "likely an externality of vega exposure."

**Discord usage of gamma:** Awareness/context (seeing stacked levels), NOT mechanical filter. Pin concept = butterfly at gamma-heavy strike, not directional signal.

### Conclusion
Gamma per-strike is valuable for dashboard awareness (like Discord uses it) but not as a filter on top of V9-SC.

---

## Delta Decay Per-Strike Study (766 trades, 25 days)

**DD = net hedging consequence of charm + vanna + gamma combined. Wizard: "All the 0DTE vanna, charm, and gamma is captured in the delta decay widget."**

### Key Findings

**DD direction as long filter:**
- LONG + DD bullish: 58% WR, +332 pts
- LONG + DD bearish: 53% WR, -39 pts
- BUT blocking DD bearish longs within V9-SC HURTS (-147 pts) — those trades are still 70% WR.

**DD alignment boost (V4 — best variant):**
- Add +1 alignment for DD bullish longs → 15 new trades pass V9-SC gate
- 11W/4L (73% WR), +70.6 pts, PF 2.96
- Small but real. Not strong enough to implement yet.

**DD+Charm stacking:**
- Helps LONGS: 59% vs 52% WR (stacked vs not)
- HURTS SHORTS: 52% vs 63% WR — stacking creates magnet that pulls price up before rejecting.
- ES Absorption benefits most from stacking (57% vs 48% WR).

**DD resistance sweet spot for shorts:** 50-200M = 79% WR (33 trades). Small sample.

### Conclusion
DD per-strike has modest value for longs (alignment boost +70 pts) but nothing strong enough to change V9-SC filter.

---

## EOD Butterfly Target (Aggregate DD, 26 days)

**DD@14:00 direction accuracy: 50% — coin flip.** Neither convention (positive=bullish or positive=bearish) predicts afternoon direction.

**Butterfly simulation results:**
- 14:00 entry: -$218/day ($5-wide), -$285/day ($10-wide)
- 15:00 entry: -$41/day ($5-wide), -$7/day ($10-wide, near breakeven)
- Only 3/26 days close within 5 pts of spot@14 (12%)
- Average afternoon move: 20.7 pts — too wide for mechanical butterflies

**Why Wizard's butterfly works but ours doesn't:**
1. He enters 15:30-15:45 (much later, SPX already settling)
2. He picks specific pinning days (discretionary, not every day)
3. He uses per-strike DD bars to find exact pin level
4. Mechanical "every day" approach loses money

### Conclusion
EOD butterfly targeting requires discretionary timing. Not suitable for automation. The data (DD trajectory + pin identification) could be shown on dashboard for manual trading.

---

## What To Revisit Later
- DD alignment boost (V4) if we accumulate 50+ days of data
- DD per-strike for ES Absorption stacking if ES Abs trade count grows
- Gamma per-strike on dashboard for visual awareness
- EOD DD trajectory display for manual butterfly opportunities
