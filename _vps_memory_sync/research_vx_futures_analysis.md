---
name: VX Futures Research (Mar 25 2026)
description: 31-day backtest of VIX futures (VXM26) tick data vs setup outcomes. VX as filter is HARMFUL. 72% price inverse is real but not actionable as filter.
type: project
---

## VX Futures Analysis — 31 Days, 1,013 Setups

**Data source:** Sierra Chart SCID file (`C:\SierraChart\Data\VXM26_FUT_CFE.scid`), 164K ticks, Feb 18 - Mar 25 2026.

**Why:** User asked if VX futures data (like Apollo uses in Discord) could boost our setups.

### Key Findings

**1. VX price direction inversely predicts SPX: 72.1% (562/780)**
- When VX drops in a 5-min window, SPX rises within 30 min (and vice versa)
- Held up from 7-day (71.1%) to 31-day (72.1%)
- BUT: may just be the mechanical VIX-SPX inverse relationship, not a leading signal

**2. VX flow as trade FILTER: ACTIVELY HARMFUL**
- ALIGNED trades (setup agrees with VX): 44.1% WR, -174 pts (WORST)
- AGAINST trades (setup opposes VX): 55.9% WR, +467 pts (BEST)
- Every filter simulation made performance WORSE
- Blocking AGAINST trades removes +467 pts of winners

**3. Per-setup VX alignment:**
- Skew Charm AGAINST VX: 72% WR, +170 pts (best of any bucket)
- DD Exhaustion AGAINST VX: 54% WR, +249 pts
- ES Absorption: only setup where ALIGNED > AGAINST (55% vs 42%)
- GEX Long ALIGNED: 17% WR, -108 pts (catastrophic)

**4. VX momentum/acceleration: MARGINAL**
- Seller exhaustion predicts SPX drop: 56.1% (barely above coin flip)
- Buyer exhaustion predicts SPX rise: 47.4% (BELOW coin flip)
- Delta spikes: 47.1% accuracy (coin flip)

**5. VX sell regime was persistent:**
- 19 of 26 days were SELL-DAY (vol sellers 56-67%)
- Only 7 MIXED days, ZERO BUY days
- Structural bias: VIX futures contango = natural sellers

### Why AGAINST Works Best

Our setups are CONTRARIAN by design. DD Exhaustion catches over-hedging reversals. Skew Charm catches charm-driven bounces against positioning. VX flow = institutional positioning. Our setups PROFIT when that positioning is wrong. Filtering WITH VX would kill the contrarian edge.

### Apollo's Use

Apollo uses VX discretionary — spotting flow shifts in real-time. This is a skill-based pattern recognition that doesn't translate to a mechanical filter. Our system already captures the relevant signals through Volland Greeks + ES delta.

### Rithmic CFE Access

- Tested locally: Rithmic Paper Trading returns "permission denied" for VX on CFE
- User needs to purchase CFE market data separately (~$4-14/month)
- Sierra Chart DTC server: connected but "Market data request not allowed" (trading-only service)
- Sierra SCID file reading works perfectly (used for this analysis)
- User decided to purchase CFE for next month regardless (72% price signal + Apollo's usage)

### How to Apply (Once CFE Active)

NOT as a filter. Potential uses:
- Entry timing signal (VX price direction = 72% SPX prediction)
- Regime context on portal (vol seller/buyer visualization)
- Real-time flow shift alerts (for discretionary overlay)
- ES Absorption only: ALIGNED VX improves (55% vs 42%)

**How to apply:** Do NOT add VX as a filter to `_passes_live_filter()`. If integrated, use as informational context on portal only, possibly as ES Absorption-specific confluence.
