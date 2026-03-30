---
name: ThetaData Subscription Ideas
description: Ideas to maximize $40/month ThetaData subscription beyond stock GEX support bounce
type: project
---

## ThetaData Subscription — Utilization Ideas

**Subscription:** ThetaData Value Plan ($40/month). Local Terminal API at `http://127.0.0.1:25510`.
**Already built:** Stock GEX Support Bounce (67 stocks, 94% WR Mon-Wed, +$2,354/month).
**Data on disk:** 59 stocks × 12 months (Mar 2025 → Mar 2026), options chains + daily + intraday prices.

---

### Idea #1: SPX 0DTE Historical Chains (PRIORITY — IN PROGRESS)
**What:** Download 12 months of SPX/SPXW 0DTE chains (gamma, OI, IV per strike) from ThetaData.
**Why:** Most directly useful — backtest our live setups (GEX Long, AG Short, DD Exhaustion, ES Absorption, Skew Charm) with REAL per-strike Greeks. Validate charm S/R limit entry. Currently we only have Volland snapshots (hard to scrape historically). GEX-only analysis also has edge (pre-Volland era).
**Status:** Planning phase. See SPX 0DTE download plan.

### Idea #2: GEX Resistance Short / Put Spread
**What:** Mirror of Support Bounce — stock rallies INTO +GEX resistance, buy OTM puts or bear put spreads.
**Why:** Same data, same infrastructure, opposite direction. Completes the picture.
**Effort:** Low — reuse backtest engine, flip entry logic.
**Status:** Not started. Run after Support Bounce is live and validated.

### Idea #3: IV Crush Plays Around GEX Levels
**What:** When OI is extremely concentrated at one strike, IV at that strike is elevated. Sell straddles/strangles there.
**Why:** Pure vol-selling edge, separate from directional GEX. ThetaData has IV per strike.
**Effort:** Medium — need IV tracking over time, different backtest logic.
**Status:** Not started. Needs IV time-series download (multiple snapshots per day).

### Idea #4: OpEx Week Pinning Strategy
**What:** Stocks pin near max-pain / highest OI strikes during OpEx week. Sell iron condors centered on max-OI strike Mon-Wed of OpEx week.
**Why:** Theta harvest — IV inflated into OpEx but price goes nowhere.
**Effort:** Medium — need max-pain calculation, iron condor P&L sim.
**Status:** Not started. We have opex chain data already downloaded.

### Idea #5: Weekly GEX Breakout (Neg-GEX Regime)
**What:** When total GEX flips negative, market makers are short gamma → volatility amplifies. Buy straddles or trade breakouts.
**Why:** Regime indicator — neg-GEX days have 2x average range.
**Effort:** Medium — aggregate GEX across all strikes per day, correlate with realized vol.
**Status:** Not started. Data available.

### Idea #6: Earnings GEX Setup
**What:** Compare GEX structure pre-earnings vs post-earnings. High OI at specific strikes = dealer positioning = predictable post-earnings magnet.
**Why:** Well-known institutional edge, rarely backtested at retail level.
**Effort:** High — need earnings dates, pre/post chain comparison.
**Status:** Not started. Need to identify which stocks had earnings in our data range.

---

## Priority Order
1. **SPX 0DTE Chains** — directly improves our $$ system
2. **GEX Resistance Short** — low effort, reuses everything
3. **OpEx Pinning** — data already downloaded
4. **IV Crush** — needs more data collection
5. **GEX Breakout** — interesting but less actionable
6. **Earnings GEX** — high effort, uncertain edge
