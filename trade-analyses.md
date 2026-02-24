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

## Analysis #2 — Feb 17, 2026

### Dataset: 2 AG Short setups, same day

| # | Time (ET) | Setup | Grade | Score | Entry | LIS | Target | Result (actual) | Result (dashboard) |
|---|-----------|-------|-------|-------|-------|-----|--------|-----------------|-------------------|
| 112 | 10:04 | AG Short | A+ | 90 | 6797.56 | 6799 | 6772 | WIN | WIN |
| 113 | 10:16 | AG Short | A | 85 | 6794.18 | 6795 | 6778 | LOSS (27pt adverse) | **WIN (BUG)** |

### Price Action Timeline

```
09:40  SPX=6844.69  ← High of day
09:52  SPX=6800.02  ← Selling accelerates
10:00  SPX=6783.65  ← Local low
10:04  SPX=6792.43  ← Setup #112 fires (A+ AG Short, spot 6797)
10:06  SPX=6801.01  ← Brief bounce (+3 pts)
10:12  SPX=6787.49  ← Drops, 10pt target hit
10:16  SPX=6787.55  ← Setup #113 fires (A AG Short, spot 6794)
10:18  SPX=6800.32  ← Bounce starts
10:20  SPX=6814.31  ← VIOLENT SQUEEZE (+27 pts against #113!)
10:24  SPX=6796.90  ← Fades back
10:34  SPX=6777.18  ← Low of day, near both targets
```

### Volland Metrics — DD Hedging Flip

| Time | DD Hedging | Charm | Note |
|------|-----------|-------|------|
| 10:05 | **-$828M** | 58M | #112 fires into deepening bearish DD |
| 10:12 | -$301M | 70M | DD weakening |
| 10:15 | -$284M | 68M | DD fading toward zero |
| 10:17 | **-$808M** | 53M | #113 fires, looks bearish... |
| 10:19 | **+$636M** | 32M | DD FLIPS BULLISH ($1.4B swing in 2 min!) |
| 10:22 | +$826M | 26M | Dealers hedging bullish → 27pt squeeze |
| 10:24 | -$440M | 48M | DD flips back bearish, price drops again |

### Why #112 Worked and #113 Didn't

**1. DD Hedging alignment (#112) vs DD flip (#113)**
- Setup #112 fired at 10:04 when DD was deepening bearish (-$490M → -$828M). Fully aligned with short.
- Setup #113 fired at 10:17, literally 2 minutes before DD swung from -$808M to +$636M. The $1.4B flip caused the 27-pt squeeze.
- This is the same pattern as Analysis #1: every loss has DD hedging opposing the direction.

**2. Charm collapse**
- Charm halved from 53M to 32M at the exact moment of the bounce (10:17→10:19), removing bearish pressure.

**3. "Second bite" trap**
- Setup #112 was the primary short near LIS at 6797. By the time #113 fired, SPX was already 10 pts lower at 6787 — chasing a mostly-done move right before mean reversion.

### BUG FOUND: Outcome Calculation Marks #113 as "Win"

**Root cause:** The stop level for AG Short is calculated as:
```python
stop_level = lis + 5          # = 6795 + 5 = 6800
if max_plus_gex > stop_level:
    stop_level = max_plus_gex  # = 6885 (!)
```

This sets the stop at **6885 — a full 91 points above entry**. No realistic stop. Price bounced to 6814 (20 pts against), but that's still below 6885, so the stop was never triggered. Price then dropped to 6777, crossing the 10pt level (6784) → `first_event = "10pt"` → dashboard shows **WIN**.

**The problem:** Using `max_plus_gex` as the stop level is wrong when it's far from spot. Today's max +GEX was at 6885 (88 pts above LIS). This makes the stop unreachable and inflates the win rate by ignoring massive adverse moves.

**Proposed fix:** Cap the AG Short stop at `max(lis + 5, spot + 15)` or similar. A 15-pt stop is realistic for a 0DTE setup. Using `max_plus_gex` only when it's within ~20 pts of LIS.

### Status of Issues Found

| Issue | Status | Priority |
|-------|--------|----------|
| Stop level uses distant max_plus_gex (inflates wins) | **BUG — needs fix** | HIGH |
| DD hedging momentum filter (fading toward zero) | PROPOSED | MEDIUM |
| Re-entry cooldown for same setup within 15 min | PROPOSED | LOW |

---

## Analysis #3 — Feb 17, 2026: DD Hedging Deep Dive

### Objective

Understand DD hedging as a signal, not just a filter. Analyzed 4,526 DD change observations across 28 trading days (Jan 20 - Feb 17) and simulated a standalone "DD Exhaustion" strategy.

### Part 1: DD Alignment vs Trade Outcome (30 deduped setups)

| | Count | Avg P&L @10m | WR @10m | Avg P&L @30m | WR @30m |
|---|---|---|---|---|---|
| DD ALIGNED | 18 | +3.4 | 61% | +8.3 | 67% |
| DD OPPOSED | 12 | -2.3 | 58% | -6.0 | 33% |
| **Edge** | | **+5.7/trade** | | **+14.2/trade** | |

By setup type:
- **GEX Long**: ALIGN +0.7 @10m (56% WR) vs OPPOSE -15.4 @10m (**0% WR, never went green**)
- **AG Short**: ALIGN +8.3 @10m (71% WR) vs OPPOSE -0.8 @10m (50% WR)
- **BofA Scalp**: DD filter NOT useful — BofA is mean-reversion, works against DD

DD flip within 5 min: avg -6.2 pts (33% WR) vs DD stable: +3.0 pts (67% WR).

### Part 2: DD as Leading Indicator (4,526 observations)

**Key finding: DD is LAGGING, not leading.**

| DD Change Bucket | N | Avg ret @10m | % Up |
|---|---|---|---|
| Big bull flip (>+$500M) | 806 | -0.26 | 48% |
| Flat (-$50 to +$50M) | 548 | +0.26 | 50% |
| Big bear flip (<-$500M) | 813 | +0.03 | 46% |

Big DD shifts have zero predictive power. The move already happened.

DD momentum (3+ consecutive shifts) is noise:
- Bullish momentum: 53% correct direction
- Bearish momentum: **38% correct** (actually a contrarian bullish signal)

### Part 3: DD + Charm Confluence (THE DISCOVERY)

Charm alone: positive charm = +0.85 @10m, negative charm = -0.97 @10m.

| Scenario | N | Avg @10m | Avg @30m | WR @10m |
|----------|---|---------|---------|---------|
| DD bear shift + pos charm (both "bearish") | 88 | **+1.38** | **+2.12** | **56%** |
| DD bull shift + pos charm | 89 | +0.29 | +0.14 | 48% |
| DD bull shift + neg charm (both "bullish") | 29 | **-1.37** | **-4.43** | **34%** |
| DD bear shift + neg charm | 37 | -1.23 | -5.68 | 35% |

**Interpretation: DD-Charm divergence is a contrarian/exhaustion signal.**
- DD goes bearish while charm stays positive = dealers over-hedged bearish, price bounces (LONG)
- DD goes bullish while charm stays negative = dealers over-positioned bullish, price fades (SHORT)
- DD and charm aligned in same direction = noise, negative EV

### Part 4: DD Exhaustion Strategy Simulation

**Rules:**
- LONG: DD shift < -$200M + charm > 0 (bearish exhaustion bounce)
- SHORT: DD shift > +$200M + charm < 0 (bullish exhaustion fade)
- Target: 10 pts, Stop: 20 pts, Max hold: 60 min, Cooldown: 30 min
- Market hours: 10:00 - 15:30 ET

**Results: 24 trades over 5 days (Feb 11-17)**

| Metric | Value |
|--------|-------|
| Win rate (target hit) | 58% (14/24) |
| Total P&L | +54.2 pts |
| Avg per trade | +2.3 pts |
| Profit factor | 1.55x |
| Max drawdown | 30.0 pts |

| Direction | Trades | WR | P&L | Avg/trade |
|-----------|--------|-----|------|-----------|
| LONG (bearish DD exhaust) | 17 | 59% | +21.6 | +1.3 |
| SHORT (bullish DD exhaust) | 7 | 57% | +32.6 | +4.7 |

Equity curve consistently upward, no extended losing streaks. Shorts more profitable per trade (+4.7 vs +1.3).

### Trade Log (Full)

| # | Date | Time | Dir | Entry | DD chg | Charm | Result | P&L | MaxFav | MaxAdv | Hold |
|---|------|------|-----|-------|--------|-------|--------|-----|--------|--------|------|
| 1 | 02/11 | 12:22 | S | 6950.9 | +1473M | -193M | WIN | +10.0 | +14.8 | -6.9 | 52m |
| 2 | 02/11 | 13:03 | S | 6948.0 | +4941M | -190M | WIN | +10.0 | +11.8 | -0.5 | 12m |
| 3 | 02/11 | 13:42 | S | 6944.5 | +1886M | -231M | T-LOSS | -12.9 | +0.0 | -13.8 | 57m |
| 4 | 02/11 | 14:16 | S | 6954.4 | +311M | -119M | T-WIN | +5.3 | +5.8 | -3.9 | 59m |
| 5 | 02/11 | 14:58 | S | 6949.6 | +267M | -274M | T-WIN | +0.2 | +9.6 | -1.9 | 52m |
| 6 | 02/12 | 12:11 | L | 6860.5 | -2697M | 137M | WIN | +10.0 | +12.0 | -11.9 | 26m |
| 7 | 02/12 | 12:50 | L | 6863.9 | -3877M | 163M | T-WIN | +3.4 | +4.4 | -18.9 | 59m |
| 8 | 02/12 | 13:22 | L | 6849.1 | -234M | 231M | WIN | +10.0 | +23.1 | -4.1 | 17m |
| 9 | 02/12 | 13:57 | L | 6867.7 | -385M | 136M | T-WIN | +4.4 | +9.1 | -9.5 | 59m |
| 10 | 02/12 | 14:29 | L | 6860.4 | -2293M | 252M | WIN | +10.0 | +16.4 | -5.5 | 9m |
| 11 | 02/12 | 15:03 | L | 6870.6 | -804M | 238M | LOSS | -20.0 | +2.9 | -45.4 | 31m |
| 12 | 02/13 | 10:04 | L | 6829.4 | -893M | 48M | WIN | +10.0 | +32.9 | -8.6 | 5m |
| 13 | 02/13 | 10:36 | L | 6862.3 | -245M | 67M | LOSS | -20.0 | +0.0 | -34.4 | 9m |
| 14 | 02/13 | 11:14 | L | 6853.2 | -319M | 103M | WIN | +10.0 | +22.4 | -13.4 | 30m |
| 15 | 02/13 | 11:46 | L | 6865.8 | -330M | 64M | WIN | +10.0 | +14.5 | -1.3 | 30m |
| 16 | 02/13 | 12:21 | L | 6869.7 | -1318M | 39M | WIN | +10.0 | +10.6 | -8.9 | 21m |
| 17 | 02/13 | 13:03 | L | 6863.2 | -1512M | 54M | WIN | +10.0 | +16.1 | -2.4 | 14m |
| 18 | 02/13 | 13:45 | L | 6869.7 | -943M | 75M | T-LOSS | -14.6 | +4.7 | -14.6 | 58m |
| 19 | 02/13 | 14:21 | S | 6869.9 | +1454M | -40M | WIN | +10.0 | +29.2 | -1.8 | 18m |
| 20 | 02/13 | 14:52 | S | 6855.2 | +1241M | -95M | WIN | +10.0 | +37.3 | +0.0 | 7m |
| 21 | 02/13 | 15:24 | L | 6839.3 | -3205M | 259M | LOSS | -20.0 | +0.0 | -21.4 | 20m |
| 22 | 02/17 | 10:05 | L | 6801.0 | -338M | 58M | WIN | +10.0 | +13.3 | -23.8 | 15m |
| 23 | 02/17 | 10:49 | L | 6793.6 | -240M | 39M | WIN | +10.0 | +55.3 | -1.9 | 13m |
| 24 | 02/17 | 11:21 | L | 6842.6 | -2061M | 3M | T-LOSS | -11.6 | +6.3 | -14.1 | 12m |

### Caveats & Next Steps

- Only 5 days with charm data available (charm capture started ~Feb 11). Need 20+ days minimum.
- 24 trades is a small sample. Results could be inflated by favorable period.
- The $200M DD change threshold and charm sign are initial parameters — not optimized.
- Shorts outperformed longs per trade; consider asymmetric targets.
- Consider adding: paradigm filter, LIS proximity, time-of-day weighting.

**REVIEW AFTER: 50+ trades (~3 more weeks of data collection). Re-run simulation with larger dataset.**

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

---

## Analysis #4 — Feb 22, 2026: GEX Long Deep Dive (Why 17.6% Win Rate?)

### Dataset: 17 GEX Long trades, Feb 3 - Feb 20

| # | Date | Time | Grade | Score | Paradigm | Spot | +GEX | -GEX | LIS | Gap | Result | PnL |
|---|------|------|-------|-------|----------|------|------|------|-----|-----|--------|-----|
| 1 | 02/03 | 09:30 | A | 80 | GEX-PURE | 6986.4 | 7010 | 6950 | 6984 | +2.4 | LOSS | -8.0 |
| 7 | 02/03 | 12:26 | A-Entry | 65 | GEX-LIS | 6919.6 | 6980 | 6900 | 6912 | +7.6 | LOSS | -8.0 |
| 13 | 02/03 | 15:12 | A | 80 | GEX-LIS | 6889.6 | 6930 | 6900 | 6886 | +3.6 | WIN | +20.0 |
| 62 | 02/05 | 11:26 | A-Entry | 70 | GEX-LIS | 6810.8 | 6890 | 6800 | 6809 | +1.8 | LOSS | -8.0 |
| 79 | 02/05 | 13:05 | A | 75 | GEX-LIS | 6826.9 | 6890 | 6800 | 6808 | +18.9 | LOSS | -8.0 |
| 80 | 02/05 | 13:41 | A-Entry | 65 | GEX-PURE | 6809.3 | 6890 | 6800 | 6808 | +1.3 | LOSS | -8.0 |
| 96 | 02/09 | 09:53 | A+ | 90 | GEX-PURE | 6923.2 | 7015 | 6850 | 6922 | +0.2 | WIN | +20.0 |
| 97 | 02/09 | 10:26 | A | 75 | GEX-LIS | 6942.1 | 6975 | 6875 | 6939 | +3.1 | WIN | +25.0 |
| 109 | 02/13 | 14:59 | A | 75 | GEX-LIS | 6843.8 | 6935 | 6850 | 6835 | +9.8 | LOSS | -8.0 |
| 110 | 02/13 | 15:16 | A-Entry | 65 | GEX-LIS | 6840.4 | 6935 | 6850 | 6827 | +13.3 | LOSS | -8.0 |
| 111 | 02/13 | 15:31 | A | 80 | GEX-LIS | 6824.3 | 6860 | 6810 | 6823 | +1.3 | EXPIRED | +6.0 |
| 117 | 02/17 | 15:40 | A+ | 95 | GEX-LIS | 6840.6 | 6935 | 6840 | 6840 | +0.6 | EXPIRED | -0.4 |
| 123 | 02/18 | 13:00 | A-Entry | 60 | GEX-TARGET | 6902.9 | 6915 | 6830 | 6898 | +5.0 | LOSS | -8.0 |
| 153 | 02/20 | 11:01 | A | 80 | GEX-MESSY | 6903.1 | 7000 | 6845 | 6900 | +3.1 | LOSS | -8.0 |
| 156 | 02/20 | 11:11 | A | 80 | GEX-MESSY | 6898.5 | 6920 | 6845 | 6897 | +1.5 | LOSS | -8.0 |
| 161 | 02/20 | 11:50 | A-Entry | 65 | GEX-MESSY | 6904.7 | 6920 | 6845 | 6897 | +7.7 | LOSS | -8.0 |
| 162 | 02/20 | 12:02 | A-Entry | 70 | GEX-MESSY | 6902.6 | 7000 | 6845 | 6897 | +5.6 | LOSS | -8.0 |

**Actual: 3W/12L/2E = 17.6% WR, -25.4 pts**

### Finding 1: GEX Alignment Is NOT the Problem

14 of 17 trades had correct alignment (+GEX above spot, -GEX below). Those 14 went 2W/10L/2E = 14% WR. The GEX structure thesis is sound — the detector is firing at wrong times/paradigms.

### Finding 2: Paradigm Quality Matters

| Paradigm | Trades | W/L/E | WR | PnL | Notes |
|----------|--------|-------|-----|-----|-------|
| GEX-PURE | 4 | 2/2/0 | 50% | +14.0 | Clean GEX structure |
| GEX-LIS | 9 | 1/5/3 | 11% | -28.0 | Mixed signals |
| GEX-MESSY | 4 | 0/4/0 | 0% | -32.0 | All losses, maxP near zero |
| GEX-TARGET | 1 | 0/1/0 | 0% | -8.0 | |

**GEX-MESSY was 0/4 with near-zero max profit.** "MESSY" literally means the GEX structure is unclear.

### Finding 3: Time of Day

| Period | Trades | W/L/E | WR | PnL |
|--------|--------|-------|-----|-----|
| Morning (9:30-11:00) | 3 | 2/1/0 | 67% | +37.0 |
| Midday (11:00-14:00) | 9 | 0/7/2 | 0% | -72.0 |
| Afternoon (14:00-16:00) | 5 | 1/4/0 | 20% | +9.6 |

**Every midday trade was a loss.** Mean reversion dominates 11-14:00, overwhelming the GEX directional signal.

### Finding 4: Max Profit on Losers — Stop Too Tight

| # | Max Profit | Stop (PnL) | Notes |
|---|-----------|------------|-------|
| 62 | +26.1 | -8.0 | Was right, shaken out |
| 80 | +24.2 | -8.0 | Was right, shaken out |
| 109 | +7.2 | -8.0 | Close, not enough room |
| 123 | +5.1 | -8.0 | Close, not enough room |

Trades #62 and #80 went +24-26 pts in favor before being stopped at -8. Correct directional call killed by tight stop.

### Finding 5: Re-fires on Broken Levels

- Feb 5: #62, #79, #80 all on same LIS 6808 — 3 trades, 0W at the time
- Feb 13: #109, #110 back-to-back 17 min apart
- Feb 20: #153, #156, #161, #162 — four trades in 1 hour on same LIS 6897

Cooldown isn't preventing repeated entries on a level that already failed.

### Simulation: Filter + Trail Optimization

Replayed all 17 trades through chain_snapshots price data with different configurations.

**Scenario comparison (8 filtered trades: no MESSY, before 14:00):**

| Config | W/L/E | WR | PnL | Avg/trade |
|--------|-------|-----|-----|-----------|
| Fixed tgt=10, stop=12 | 4/4/0 | 50% | -8.0 | -1.0 |
| Fixed tgt=15, stop=12 | 4/4/0 | 50% | +12.0 | +1.5 |
| Trail act=5, gap=3, stop=12 | **5/3/0** | **62%** | **+53.6** | **+6.7** |
| Trail act=5, gap=5, stop=12 | 5/3/0 | 62% | +43.6 | +5.5 |
| Trail act=10, gap=5, stop=12 | 4/4/0 | 50% | +31.5 | +3.9 |
| Trail act=15, gap=5, stop=12 | 4/4/0 | 50% | +39.6 | +4.9 |
| Trail act=15, gap=7, stop=12 | 4/4/0 | 50% | +61.3 | +7.7 |
| Trail act=20, gap=5, stop=12 | 4/4/0 | 50% | +39.6 | +4.9 |

**Key insight on trail activation levels:**

- **act=5** catches one extra winner (#123 peaked at +5.1, trail locks +2) but risks premature lock on choppy action
- **act=7 through act=20** produce identical W/L splits — all 4 winners ran past +20, so activation level doesn't matter for them
- **act=25** loses a winner (#80 peaked at +24.2, never activates, hits -12 stop)
- **gap=7** outperforms gap=3 and gap=5 because trade #97 ran to +36 — wider gap lets it breathe and captures +48 vs +31

Trail activation 10-15 with gap 5 is the safe sweet spot. Act=5/gap=3 is highest raw PnL but aggressive for 0DTE noise.

### Individual trade replay (best scenario: act=5, gap=3, stop=12):

| # | Date | Paradigm | DB Result | Sim Result | Sim PnL | MaxP |
|---|------|----------|-----------|------------|---------|------|
| 1 | 02/03 | GEX-PURE | LOSS | LOSS | -12.0 | +0.0 |
| 7 | 02/03 | GEX-LIS | LOSS | LOSS | -12.0 | +0.0 |
| 62 | 02/05 | GEX-LIS | LOSS | **WIN** | **+12.0** | +22.7 |
| 79 | 02/05 | GEX-LIS | LOSS | LOSS | -12.0 | +0.0 |
| 80 | 02/05 | GEX-PURE | LOSS | **WIN** | **+21.0** | +24.2 |
| 96 | 02/09 | GEX-PURE | WIN | WIN | +22.0 | +25.0 |
| 97 | 02/09 | GEX-LIS | WIN | WIN | +33.0 | +35.7 |
| 123 | 02/18 | GEX-TARGET | LOSS | **WIN** | **+2.0** | +5.1 |

### Proposed Changes (NOT YET IMPLEMENTED — pending more data)

| Change | Impact | Confidence |
|--------|--------|------------|
| **Exclude MESSY paradigm** | Removes 4 guaranteed losses (-32 pts) | HIGH — 0/4 with ~zero max profit |
| **Exclude after 14:00** | Removes 5 afternoon losers (-9.4 pts) | MEDIUM — small sample |
| **Widen stop 8 -> 12 pts** | Saves #62 and #80 from shakeout | MEDIUM — bigger losses when wrong |
| **Add continuous trail (act=10-15, gap=5)** | Lets winners run (+31 to +40 vs +10) | HIGH — proven with DD Exhaustion |
| **Re-entry cooldown on same LIS** | Prevents 3-4x entries on broken level | LOW — needs more study |

### Estimated Impact if All Applied

| Metric | Current | With Changes |
|--------|---------|-------------|
| Trades | 17 | ~8 (filtered) |
| Win Rate | 18% | ~50-62% |
| Total PnL | -25.4 | +32 to +54 |
| Avg/trade | -1.5 | +4 to +7 |

### Caveats

- 17 trades is a very small sample; 8 after filtering is even smaller
- Trail activation optimization on 4 winners is curve-fitting risk
- The MESSY filter is the only high-confidence change (clear 0/4 pattern)
- **REVIEW AFTER: 15+ new GEX Long trades with these filters. Re-run simulation to validate.**

---

## Analysis #5 — Feb 24, 2026: DD Exhaustion Deep Dive (49 Trades)

### Objective

Deep analysis of DD Exhaustion setup performance to identify improvement levers: when to enter, when to avoid, which Volland metrics (charm, paradigm, vanna) make signals stronger.

### Dataset: 49 DD Exhaustion trades, Feb 18-23

**Overall: 22W / 18L / 9E = 44.9% WR, +283.9 pts**

| Direction | Trades | WR | P&L |
|-----------|--------|-----|-----|
| SHORT | 29 | 48.3% | +219.0 |
| LONG | 20 | 40.0% | +64.9 |

### Finding 1: Afternoon Is a Dead Zone (BIGGEST EDGE)

| Time (ET) | Trades | WR | Total P&L | Avg P&L |
|-----------|--------|-----|-----------|---------|
| 10:00-11:00 | 6 | 50% | +45.5 | +7.6 |
| **11:00-12:00** | **13** | **76.9%** | **+221.2** | **+17.0** |
| 12:00-13:00 | 7 | 71.4% | +85.3 | +12.2 |
| 13:00-14:00 | 10 | 50% | +14.0 | +1.4 |
| **14:00-16:00** | **13** | **0%** | **-82.1** | **-6.3** |

Every single trade after 14:00 ET lost or expired. Zero wins in 13 attempts. The scoring model awards maximum points (15/15) for "dealer o'clock" 14:00+ — this is backwards. Best window is 11:00-13:00 (76% WR, +15.3 avg/trade).

### Finding 2: Paradigm Determines Fate

| Paradigm | Trades | WR | P&L | Notes |
|----------|--------|-----|-----|-------|
| **SIDIAL-MESSY** | 4 | **100%** | **+77.9** | All shorts, perfect |
| **GEX-MESSY** | 4 | **100%** | **+122.9** | All shorts, massive P&L |
| SIDIAL-EXTREME | 3 | 67% | +56.3 | Good |
| AG-TARGET | 4 | 50% | +32.2 | Mixed |
| AG-LIS | 14 | 36% | +22.9 | High volume, mediocre |
| AG-PURE | 5 | 40% | +7.3 | Mediocre |
| **BOFA-PURE** | **11** | **18.2%** | **-21.5** | **Worst — avoid** |
| GEX-LIS/PURE/TARGET | 3 | 0% | -36.0 | Small sample, 0/3 |

MESSY paradigms work brilliantly (8/8 wins, +200.8 pts). When GEX/SIDIAL structure is unclear, the DD-Charm divergence becomes the dominant signal. In clean paradigms (BOFA-PURE, GEX-PURE), the existing regime fights the DD exhaustion signal.

### Finding 3: Score Is NOT Predictive

| Grade | Trades | WR | P&L | Avg P&L |
|-------|--------|-----|-----|---------|
| LOG (score=0) | 5 | **80%** | +65.9 | +13.2 |
| A-Entry (<55) | 12 | 41.7% | +40.2 | +3.4 |
| A (55-74) | 23 | 47.8% | +157.8 | +6.9 |
| **A+ (75+)** | **9** | **22.2%** | **+20.1** | **+2.2** |

Correlation between score and P&L: -0.033 (essentially zero). A+ has the worst WR. The 5-component scoring formula does not predict outcomes.

### Finding 4: Direction + Time Interaction

| Period | Short WR | Short Avg P&L | Long WR | Long Avg P&L |
|--------|----------|---------------|---------|--------------|
| **Morning (10-12)** | **84.6%** | **+21.7** | 33.3% | -1.0 |
| Midday (12-14) | 50.0% | +0.5 | **60.0%** | +7.5 |
| Afternoon (14-16) | 0% | -7.5 | 0% | -4.0 |

Shorts work in the morning, longs work at midday. Aligns with market microstructure — morning selling exhaustion bounces back (short DD fade), midday mean-reversion supports long signals.

### Finding 5: Charm Sweet Spot

| |Charm| Bucket | Trades | WR | Avg P&L |
|-----------------|--------|-----|---------|
| < $20M | 17 | 41.2% | +4.5 |
| $20-50M | 6 | 50% | +9.8 |
| **$50-100M** | **14** | **57.1%** | **+13.7** |
| $100-250M | 9 | 33.3% | -1.3 |
| **$250M+** | **3** | **0%** | **-8.5** |

Sweet spot is $50-100M. Too weak (<$20M) = no structural conviction. Too strong ($250M+) = charm regime dominates and DD divergence gets overwhelmed.

### Finding 6: DD Shift Magnitude

| DD Shift Bucket | Trades | WR | Avg P&L |
|-----------------|--------|-----|---------|
| $200-500M | 14 | 35.7% | +2.2 |
| $500M-1B | 9 | 22.2% | -0.7 |
| **$1B-2B** | **15** | **46.7%** | **+7.0** |
| **$2B-3B** | **4** | **75%** | **+19.8** |
| $3B+ | 7 | 57.1% | +9.4 |

Sweet spot is $1B-3B. Smaller shifts ($200-500M) barely clear the threshold and have poor WR.

### Finding 7: Clustering Disaster (Feb 19 Afternoon)

6 consecutive BOFA-PURE short entries in 4 hours, net -33.3 pts:

| Time ET | Paradigm | Result | P&L |
|---------|----------|--------|-----|
| 12:12 | BOFA-PURE | EXPIRED | -6.9 |
| 12:41 | BOFA-PURE | **WIN** | +15.5 |
| 13:25 | BOFA-PURE | LOSS | -12.0 |
| 14:02 | BOFA-PURE | EXPIRED | -5.9 |
| 14:43 | BOFA-PURE | LOSS | -12.0 |
| 15:17 | BOFA-PURE | LOSS | -12.0 |

DD signal kept re-firing as DD shifted, but price was range-bound in BOFA-PURE. Only the first clean entry worked.

### Finding 8: Losers That Never Went Green

Of 18 losses:
- **7 (39%) had max_profit = 0** — signal fundamentally wrong from start
- 6 had max_profit 1-5 pts — marginal
- 5 had max_profit > 5 pts — setup was right, exit was the problem
- Trade #7 had 16.6 pts max profit but lost -12 (GEX-LIS long, 14:00+ ET)

### Finding 9: Win vs Loss Patterns

| Metric | Winners | Losers |
|--------|---------|--------|
| Avg |DD Hedging| | $2,151M | $1,218M |
| Avg |charm| | $43M | $64M |
| Avg time (ET) | 11:59 | 13:02 |
| Avg elapsed | 89 min | 39 min |
| Avg score | 49.9 | 57.1 |

Winners: bigger DD shifts, moderate charm, earlier in the day, held longer. Losers: higher scores (ironic), later times, resolved quickly (hit stop fast).

### Proposed Filter Stack (Cumulative Impact)

| Filter | Removes | Remaining | WR | P&L |
|--------|---------|-----------|-----|-----|
| **Baseline** | — | 49 | 44.9% | +283.9 |
| **Cut after 14:00 ET** | 13 | 36 | 61.1% | +366.0 |
| **Block BOFA-PURE** | 7 more | 29 | 62.1% | +365.5 |
| **Raise DD threshold to $500M** | ~5 more | ~24 | ~67% | ~+340 |
| **Block |charm| > $200M** | ~2 more | ~22 | ~68% | ~+345 |

### Recommended Changes (Priority Order)

| # | Change | Confidence | Impact |
|---|--------|------------|--------|
| 1 | `dd_market_end`: "15:30" → "14:00" | VERY HIGH — 0/13 after 14:00 | +82 pts saved |
| 2 | Block BOFA-PURE paradigm | HIGH — 2/11, -21.5 pts | +21 pts saved |
| 3 | Raise `dd_shift_threshold` to $500M | MEDIUM — 5/14 WR at $200-500M | Removes weak signals |
| 4 | Add charm ceiling $200M | MEDIUM — 0/3 at $250M+ | Safety filter |
| 5 | Paradigm-level cooldown (60 min) | MEDIUM — prevents clustering | Reduces overtrading |
| 6 | Rewrite scoring formula | LOW — score doesn't gate trades | Cosmetic |

### Proposed Scoring Rewrite

Current 5-component score doesn't predict outcomes. Simpler model based on what actually works:

| Factor | Weight | Best Values |
|--------|--------|-------------|
| Time of day | 30 | 11:00-13:00 = max, 10:00-11:00 = medium |
| Paradigm fit | 25 | MESSY/SIDIAL = max, AG-LIS = medium, BOFA-PURE = 0 |
| DD shift magnitude | 20 | $1B-3B = max, $500M-1B = medium |
| Charm strength | 15 | $50-100M = max, $20-50M = medium |
| Direction-time match | 10 | Short morning OR Long midday = bonus |

### Caveats

- 49 trades over 4 trading days is still a small sample (especially per-paradigm buckets of 3-4 trades)
- The Feb 20 monster day (19 trades, +203 pts) heavily skews aggregate numbers
- GEX-MESSY and SIDIAL-MESSY perfection may be sample luck (only 4 trades each)
- The afternoon dead zone is robust (13 trades, 0 wins) but could be a regime artifact of this specific week
- **REVIEW AFTER: 30+ more trades (~2 weeks). Validate time filter and paradigm filter on new data before implementing.**

### Status

| Change | Status | Date |
|--------|--------|------|
| All proposed changes | PENDING — saved for implementation | Feb 24, 2026 |
