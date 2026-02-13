# Trade Analyses

Running log of setup performance analysis, pattern observations, and tuning decisions.

---

## Analysis #1 — Feb 13, 2026

### Dataset: 9 trades, Feb 9-12

| # | Date (ET) | Setup | Dir | Grade | Score | Entry | P&L | Result | Outcome |
|---|-----------|-------|-----|-------|-------|-------|-----|--------|---------|
| 96 | Feb 9 ~9:53 | GEX Long | Long | A+ | 90 | 6923 | +15.9 | WIN | Target hit |
| 97 | Feb 9 ~10:26 | GEX Long | Long | A | 75 | 6942 | +11.5 | WIN | Target hit |
| 98 | Feb 11 ~10:20 | BofA Scalp | Long | A-Entry | 60 | 6925 | +18.5 | WIN | Target hit |
| 99 | Feb 11 ~10:36 | AG Short | Short | A | 75 | 6931 | -1.3 | LOSS | Stop hit |
| 100 | Feb 11 ~14:30 | BofA Scalp | Long | A-Entry | 65 | 6954 | -3.0 | LOSS | Timeout |
| 101 | Feb 12 ~12:14 | BofA Scalp | Long | A-Entry | 65 | 6860 | +6.1 | WIN | Timeout |
| 102 | Feb 12 ~12:29 | BofA Scalp | Long | A-Entry | 65 | 6860 | -11.7 | LOSS | Timeout |
| 103 | Feb 12 ~14:24 | BofA Scalp | Short | A | 80 | 6874 | -2.6 | LOSS | Timeout |
| 104 | Feb 12 ~14:59 | BofA Scalp | Long | A | 85 | 6869 | -16.0 | LOSS | Stop hit |

**Actual results: 4W / 5L = 44% win rate, +17.4 pts total**

### Volland Metrics at Detection

| # | DD Hedging | Total Charm | Paradigm After | Notes |
|---|-----------|-------------|----------------|-------|
| 96 | +$1.4B | -48.6M | GEX-PURE (held) | Bullish DD + bullish charm = correct long |
| 97 | +$790M | -29.9M | BOFA-PURE (shifted) | DD still positive at entry |
| 98 | +$7.7B | -45.7M | AG-LIS (shifted) | Very strong DD but paradigm flipped after |
| 99 | **+$6.7B** | **-71.1M** | **BofA-LIS (shifted)** | **Shorted into bullish DD + bullish charm** |
| 100 | **-$721M** | +6.6M | BOFA-PURE (held) | **Went long into bearish DD** |
| 101 | -$5.6B | +114M | BOFA-PURE (held) | Bearish DD but trade worked short-term |
| 102 | -$1.8B -> -$4.5B | +137.6M | BOFA-PURE (held) | DD accelerating bearish during trade |
| 103 | +$36M (near zero) | +156.7M | BOFA-PURE (held) | Neutral DD, very bullish charm, went short |
| 104 | **-$2.7B** | +166M | BOFA-PURE (held) | **Went long into strongly bearish DD** |

### Key Findings

#### 1. Charm score component is dead (always 0)
Every single setup scored `floor_cluster_score = 0` for charm. The thresholds are <=500/2000/5000/10000 but actual charm values are in the **tens of millions**. The component contributes nothing to composite scores. Needs recalibration.

**Suggested thresholds (not yet implemented, waiting for more data):**
- <=5M -> 100
- <=20M -> 75
- <=50M -> 50
- <=100M -> 25
- >100M -> 0

#### 2. DD Hedging sign contradicts direction on every loss
- #99: DD +$6.7B (bullish) but shorted -> LOSS
- #100: DD -$721M (bearish) but went long -> LOSS
- #104: DD -$2.7B (bearish) but went long -> LOSS

**Proposed filter (not yet implemented):** Block trade when DD hedging sign opposes direction. Negative DD + long = blocked. Positive DD + short = blocked.

#### 3. BofA 15pt target too aggressive, 10pt would capture more wins
- #102: max profit reached +12.3 pts (missed 15pt target, timed out at -11.7)
- #103: max profit reached +13.7 pts (missed 15pt target, timed out at -2.6)
- Both would be wins with a 10pt target

**IMPLEMENTED: Target changed from 15 -> 10 pts (Feb 13, 2026)**

#### 4. Duplicate entries on same LIS
- #101 and #102 entered at ~6860 on same LIS, 15 min apart
- #101 won, #102 lost
- Cooldown didn't prevent re-entry because grade/gap changed

### Scenario A: Hypothetical with all changes applied

Changes: 10pt target + DD hedging directional filter (charm fix doesn't change outcomes)

Blocked: #99 (AG Short, DD bullish), #100 (BofA Long, DD bearish), #104 (BofA Long, DD bearish)

| # | Setup | Result | P&L |
|---|-------|--------|-----|
| 96 | GEX Long | WIN | +15.9 |
| 97 | GEX Long | WIN | +11.5 |
| 98 | BofA Scalp | WIN | +10.0 |
| 101 | BofA Scalp | WIN | +10.0 |
| 102 | BofA Scalp | WIN | +10.0 |
| 103 | BofA Scalp | WIN | +10.0 |

**Hypothetical: 6W / 0L = 100% win rate, +67.4 pts total**

### Comparison

| Metric | Actual | Scenario A |
|--------|--------|------------|
| Trades | 9 | 6 |
| Win Rate | 44% | 100% |
| Total P&L | +17.4 pts | +67.4 pts |
| Avg P&L/trade | +1.9 pts | +11.2 pts |
| Max Drawdown | -16.0 pts | 0 |
| Profit Factor | 1.3x | infinite |

### Status of Proposed Changes

| Change | Status | Date |
|--------|--------|------|
| BofA target 15 -> 10 pts | IMPLEMENTED | Feb 13, 2026 |
| DD hedging directional filter | PENDING — need more data | — |
| Charm threshold recalibration | PENDING — need more data | — |
| Duplicate entry prevention | PENDING — need more data | — |

### Important Caveat

9 trades is a very small sample. These hypothetical numbers (100% WR) are almost certainly inflated by small sample size. The DD hedging filter and charm recalibration need validation over 50+ trades before implementation. The 10pt target change is lower risk and was implemented immediately.

---

## Next Review

Re-run this analysis after accumulating 20+ more trades (target: ~2 weeks). Check:
1. Does the 10pt target maintain win rate vs old 15pt?
2. Do the DD hedging filter observations hold on new data?
3. What is the actual distribution of charm values? (for threshold calibration)
4. Are there new patterns emerging?

---

## Milestone: Deep Volland Factor Analysis (Target: May 2026)

### Objective

After 3 months of continuous data collection (~90 trading days), run a comprehensive quantitative analysis across all Volland metrics to discover new edges and validate existing ones.

### Available Data (collecting since ~Feb 2026)

- **Playback snapshots** every 2 minutes: spot, net_gex by strike, charm by strike, call/put volume, stats (paradigm, LIS, target, DD hedging, opt volume)
- **Volland snapshots** every ~60 seconds: raw payloads with statistics, exposure captures
- **Volland exposure points**: charm, vanna, gamma broken down by strike AND expiration
- **Chain snapshots**: full options chain with Greeks (gamma, delta, OI, volume)

At 2-min intervals over 90 days: ~19,500 playback snapshots + millions of strike-level exposure points.

### Analysis Plan

#### Phase 1: Single-Factor Predictive Power
Test each factor independently against future price movement (T+10min, T+30min, T+1hr, T+close):
- Aggregate charm (sum across strikes)
- Aggregate vanna
- Aggregate gamma
- DD hedging (sign and magnitude)
- Net GEX (sum)
- Max +GEX strike location relative to spot
- Max -GEX strike location relative to spot
- Vol / Beta from Volland stats
- Options volume (total, put/call ratio)

#### Phase 2: Expiration Breakdown
- Does 0DTE charm predict differently than weekly/monthly charm?
- Which expiration's Greeks have the strongest price correlation?
- Does the mix of expirations signal anything (e.g., heavy 0DTE gamma = pinning)?

#### Phase 3: Cross-Factor Interactions
Test factor combinations for stronger signals:
- Charm direction + vanna sign + gamma profile
- DD hedging sign + charm direction
- Charm concentrated at strike X + spot distance from X
- Vanna exposure + VIX/vol regime
- GEX profile shape (clustered vs distributed) + price behavior

#### Phase 4: Level Gravity & Strike Magnetism
- When charm/gamma concentrates at a strike, does price gravitate to it?
- How reliably? How fast? Does it depend on distance?
- Can we predict intraday support/resistance from exposure concentration?

#### Phase 5: Regime Fingerprinting
- What do early-day (9:30-10:30) factor profiles look like on trending vs. range-bound days?
- Can we classify the day's regime within the first hour using Volland data?
- Which paradigm transitions predict directional moves?

#### Phase 6: Time-of-Day Effects
- Charm predictive power by hour (morning vs. afternoon vs. Dealer O'Clock)
- Vanna impact during high-vol vs. low-vol periods
- Does gamma's influence change as 0DTE options decay?

### Methodology

- **Split data**: 60 days training / 30 days out-of-sample validation
- **Statistical rigor**: require significance (p < 0.05), not just backtested P&L
- **Multiple comparison correction**: adjust for testing many factors (Bonferroni or similar)
- **Regime awareness**: test whether findings hold across different market conditions
- **Microstructure logic**: only pursue correlations that have a theoretical reason to exist (avoid data mining artifacts)

### Output

- Ranked factor list by predictive power (with confidence intervals)
- Top 3-5 factor combinations for potential new setups
- New scored setup definitions (same architecture as GEX Long / AG Short / BofA Scalp)
- Calibrated thresholds based on actual data distributions (fixes current charm threshold issue)

### Prerequisite

Keep the data pipeline running uninterrupted. Every gap day is lost signal. The system is currently collecting everything needed — no code changes required until analysis time.

---

## Feature Log — Feb 13, 2026 (Evening Session)

### Changes Since Backup #10 (stable-20260213-220739)

#### 1. Real-Time ES Quote Stream with Bid/Ask Delta Range Bars
- TradeStation streaming quotes endpoint feeds live ES bid/ask prices
- Delta calculated from bid/ask trade classification (tick rule)
- 5-point range bars built from streaming data (same as existing 1-min bar approach but higher fidelity)
- Falls back to 1-min bars if quote stream has <10 bars (requires minimum data before switching)
- Stored in `es_delta_bars` table alongside 1-min bar data

#### 2. ES Absorption Detector (Price vs CVD Divergence + Volland Confluence)
- Detects absorption signals: price makes new high/low but CVD diverges (institutional absorption)
- Grades signals A+/A/B/C based on divergence strength, volume confirmation, and Volland confluence
- Volland confluence: checks if DD hedging, charm direction, and paradigm align with the signal
- A/A+ signals displayed as chart markers on the ES Delta price panel
- Signal data included in `/api/es/delta/rangebars` response

#### 3. SPX Key Levels on ES Delta Chart
- Fetches `/api/statistics_levels` in parallel with range bar data (no extra latency)
- Converts SPX levels to ES prices using live spread: `offset = ES_last - SPX_spot`
- Draws 5 dashed horizontal lines on the price panel:
  - Target (blue #3b82f6) — Volland target price
  - LIS Low/High (amber #f59e0b) — Volland LIS bounds
  - Max +Gamma (green #22c55e) — largest positive GEX strike
  - Max -Gamma (red #ef4444) — largest negative GEX strike
- Labels show rounded ES-converted prices (e.g., "Tgt 6064")
- Graceful degradation: no lines if SPX spot unavailable (pre-market)
- Lines refresh every 5s with the chart auto-update cycle

#### 4. Minor Fixes
- Removed mock/test button from ES Delta tab
- Default ES Delta view zoomed to last 50% of bars for better readability
- Quote stream priority fix: require 10+ bars before switching from 1-min fallback

### Trading Relevance

The SPX key levels on the ES Delta chart are significant for execution:
- Traders watching ES futures can now see the same Volland-derived levels without switching tabs
- The SPX→ES offset auto-adjusts as the spread changes intraday
- Combined with absorption signals, this creates a complete ES execution view: price action + delta flow + key levels + institutional absorption markers
