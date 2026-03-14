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

---

## Analysis #6 — Feb 26, 2026: GEX Long Vanna Regime Filter

### Objective

Investigate whether aggregated vanna (all expirations) can serve as a filter for GEX Long setups. Hypothesis: when aggregated vanna is negative in higher-tenor expirations, GEX Long doesn't work because the vanna regime suppresses bullish gamma effects.

### Data Source

Vanna data from `volland_exposure_points` table, `greek = 'vanna'`, `exposure_option = 'ALL'` (all expirations combined). Available from Feb 11 onwards. Per-trade vanna computed from nearest snapshot within 5 minutes of setup detection.

### Dataset: 32 GEX Long trades, Feb 3 - Feb 26

| ID | Date | Time ET | Grade | Result | PnL | Vanna ALL | Filter |
|----|------|---------|-------|--------|-----|-----------|--------|
| 1 | Feb 03 | 09:30 | A | LOSS | -8.0 | N/A | NO_DATA |
| 7 | Feb 03 | 12:26 | A-Entry | LOSS | -8.0 | N/A | NO_DATA |
| 13 | Feb 03 | 15:12 | A | WIN | +20.0 | N/A | NO_DATA |
| 62 | Feb 05 | 11:26 | A-Entry | LOSS | -8.0 | N/A | NO_DATA |
| 79 | Feb 05 | 13:05 | A | LOSS | -8.0 | N/A | NO_DATA |
| 80 | Feb 05 | 13:41 | A-Entry | LOSS | -8.0 | N/A | NO_DATA |
| 96 | Feb 09 | 09:53 | A+ | WIN | +20.0 | N/A | NO_DATA |
| 97 | Feb 09 | 10:26 | A | WIN | +25.0 | N/A | NO_DATA |
| 109 | Feb 13 | 14:59 | A | LOSS | -8.0 | -1.73B | BLOCKED |
| 110 | Feb 13 | 15:16 | A-Entry | LOSS | -8.0 | -1.52B | BLOCKED |
| 111 | Feb 13 | 15:31 | A | EXPIRED | +6.0 | -1.25B | BLOCKED |
| 117 | Feb 17 | 15:40 | A+ | EXPIRED | -0.4 | -608M | BLOCKED |
| 123 | Feb 18 | 13:00 | A-Entry | LOSS | -8.0 | -1.04B | BLOCKED |
| 153 | Feb 20 | 11:01 | A | LOSS | -8.0 | -202M | BLOCKED |
| 156 | Feb 20 | 11:11 | A | LOSS | -8.0 | -231M | BLOCKED |
| 161 | Feb 20 | 11:50 | A-Entry | LOSS | -8.0 | -185M | BLOCKED |
| 162 | Feb 20 | 12:02 | A-Entry | LOSS | -8.0 | -209M | BLOCKED |
| 183 | Feb 23 | 09:42 | A | LOSS | -8.0 | -472M | BLOCKED |
| 200 | Feb 24 | 09:51 | A+ | LOSS | -8.0 | +1.36B | ALLOWED |
| 227 | Feb 25 | 10:06 | A-Entry | WIN | +15.0 | +935M | ALLOWED |
| 230 | Feb 25 | 10:28 | A-Entry | WIN | +10.0 | +660M | ALLOWED |
| 250 | Feb 26 | 09:49 | A-Entry | LOSS | -8.0 | -214M | BLOCKED |
| 251 | Feb 26 | 10:01 | A-Entry | LOSS | -8.0 | -194M | BLOCKED |
| 253 | Feb 26 | 10:05 | A-Entry | LOSS | -8.0 | -46M | BLOCKED |
| 254 | Feb 26 | 10:08 | A-Entry | LOSS | -8.0 | -357M | BLOCKED |
| 255 | Feb 26 | 10:09 | A-Entry | LOSS | -8.0 | -357M | BLOCKED |
| 256 | Feb 26 | 10:15 | A+ | LOSS | -8.0 | -146M | BLOCKED |
| 260 | Feb 26 | 10:52 | A+ | WIN | +10.3 | +23M | ALLOWED |
| 261 | Feb 26 | 11:19 | A | LOSS | -8.0 | -12M | BLOCKED |
| 263 | Feb 26 | 11:38 | A | LOSS | -8.0 | +27M | ALLOWED |
| 264 | Feb 26 | 11:40 | A | LOSS | -8.0 | +102M | ALLOWED |
| 268 | Feb 26 | 11:51 | A+ | LOSS | -8.0 | +219M | ALLOWED |

### Finding 1: Negative Vanna ALL = 0% Win Rate

| Vanna ALL Sign | Trades | Wins | Losses | Expired | WR | Total PnL | Avg PnL |
|----------------|--------|------|--------|---------|-----|-----------|---------|
| **NEGATIVE** | **17** | **0** | **15** | **2** | **0.0%** | **-114.4** | **-6.7** |
| POSITIVE/ZERO | 7 | 3 | 4 | 0 | 42.9% | +3.3 | +0.5 |
| NO_DATA | 8 | 3 | 5 | 0 | 37.5% | +25.0 | +3.1 |

When aggregated vanna across all expirations is negative, **not a single GEX Long trade has won** out of 17 attempts. Zero. Every trade was a loss or expired at breakeven.

### Finding 2: Higher-Tenor Vanna (ALL minus TODAY) Also Strong

| HT Vanna Sign | Trades | Wins | WR | PnL |
|----------------|--------|------|----|-----|
| NEGATIVE | 20 | 1 | 5.0% | -120.1 |
| POSITIVE | 12 | 5 | 41.7% | +34.0 |

### Finding 3: Vanna Magnitude Matters

| Vanna ALL Bucket | Trades | Wins | WR | PnL |
|------------------|--------|------|----|-----|
| Very negative (<-5B) | 3 | 0 | 0% | -10.0 |
| Moderate negative (-5B to -1B) | 4 | 0 | 0% | -24.4 |
| Slight negative (-1B to 0) | 9 | 0 | 0% | -72.0 |
| Slight positive (0 to +1B) | 5 | 1 | 20% | -21.7 |
| Moderate positive (+1B to +5B) | 2 | 2 | 100% | +25.0 |
| Very positive (>+5B) | 1 | 0 | 0% | -8.0 |

All negative buckets: 0% WR across 16 trades. The sweet spot is moderate positive (+1B to +5B): 100% WR on 2 trades.

### Finding 4: Cross-Setup Vanna Impact

| Setup | Neg Vanna WR | Pos Vanna WR | Notes |
|-------|-------------|-------------|-------|
| GEX Long | 0% (17 trades) | 42.9% (7) | Perfect filter |
| DD Exhaustion | 45.9% (37) | 22.6% (53) | Opposite — DD loves neg vanna |
| AG Short | 42.9% (7) | 40.0% (15) | Neutral |
| BofA Scalp | 25.0% (16) | 33.3% (6) | Slight neg preference |
| ES Absorption | 50% (2) | 70% (10) | Too small |

DD Exhaustion performs BETTER in negative vanna (contrarian signal works when dealers are heavily hedged). GEX Long is directional and gets crushed when vanna regime is bearish.

### Finding 5: Daily Vanna Regime Context

| Date | Vanna ALL | GEX Long Results |
|------|-----------|-----------------|
| Feb 13 | Negative | 0/3 (all blocked correctly) |
| Feb 17 | Negative | 0/1 (blocked correctly) |
| Feb 18 | Negative | 0/1 (blocked correctly) |
| Feb 20 | Negative | 0/5 (all blocked correctly) |
| Feb 23 | Negative | 0/1 (blocked correctly) |
| Feb 24 | Positive | 0/1 (allowed — still lost) |
| Feb 25 | Mixed | 2/2 (positive snapshots won) |
| Feb 26 | Mixed | 1/11 (neg morning blocked, pos midday mixed) |

### Filter Impact Summary

| Metric | Without Filter | With Vanna Filter |
|--------|---------------|-------------------|
| Trades | 32 | 15 (ALLOWED + NO_DATA) |
| Wins | 6 | 6 (zero wins lost) |
| Losses | 24 | 9 |
| Win Rate | 20.0% | 40.0% |
| **Total PnL** | **-86.1 pts** | **+28.3 pts** |
| **Improvement** | — | **+114.4 pts** |

### Proposed Filter

**Block GEX Long when the most recent `vanna ALL` aggregated sum (across all strikes) is negative.**

Implementation: In `_run_setup_check()`, before calling the GEX Long detector, query the latest `volland_exposure_points` snapshot where `greek = 'vanna'` and `exposure_option = 'ALL'`, sum all strike values. If sum < 0, skip GEX Long evaluation.

### Caveats

- 32 trades is a moderate sample; 17 in the BLOCKED bucket is reasonable but could still be coincidental with a bearish market period (Feb 13-26)
- Vanna data only available from Feb 11 — the 8 NO_DATA trades (Feb 3-9) can't be verified
- The ALLOWED bucket (7 trades, 42.9% WR) is still mediocre — vanna filter alone doesn't make GEX Long a strong setup
- Additional filters (paradigm, time-of-day from Analysis #4) could further improve the ALLOWED bucket
- **REVIEW AFTER: 15+ more GEX Long trades with positive vanna. Validate that positive vanna truly enables GEX Long.**

### Status

| Change | Status | Date |
|--------|--------|------|
| Block GEX Long when vanna ALL < 0 | PENDING — saved for implementation | Feb 26, 2026 |

---

## Analysis #7 — Feb 26, 2026: EXPIRED Trade Bug Fix & PnL Correction

### Bug Found

EXPIRED trades from Feb 24 onwards had `outcome_pnl = 0.0` instead of their actual P&L at market close.

### Root Cause

Two bugs colliding:
1. `run_market_job()` stops calling `_check_setup_outcomes()` after 16:00 (market_open_now() returns False)
2. EOD summary at 16:05 tries to parse spot from `last_run_status["msg"]` which is already overwritten to `"outside market hours"` → `spot = None` → `pnl = 0.0`

### Fix Applied

Changed `market_closed` threshold from `dtime(16, 0)` to `dtime(15, 57)` — all open trades now close 3 minutes before market end, while spot price is still available from the live tracker. Added `_last_known_spot` cache as safety net for EOD summary fallback.

### Backfill Results

31 trades corrected using playback_snapshots closing prices:
- Feb 24 close: SPX 6892.29
- Feb 25 close: SPX 6948.65
- Feb 26 close: SPX 6906.73

Grand total PnL correction: +429.9 (inflated) → **+389.9 pts** (accurate). The 31 expired trades were mostly losers hidden behind pnl=0.

### Status

| Change | Status | Date |
|--------|--------|------|
| Auto-close at 15:57 ET | IMPLEMENTED | Feb 26, 2026 |
| Backfill 31 EXPIRED trades | COMPLETED | Feb 26, 2026 |

---

## Analysis #8 -- Greek Context Filter Analysis, March 4, 2026

### Dataset: 266 trades (WIN/LOSS only, excluding EXPIRED and LOG), 17 trading days (Feb 3 - Mar 3)

### New Fields Added to setup_log

5 new columns for per-signal Greek context logging (no filtering, pure data capture):

| Field | Type | Source |
|-------|------|--------|
| `vanna_all` | DOUBLE PRECISION | Sum of vanna ALL expiration from volland_exposure_points |
| `vanna_weekly` | DOUBLE PRECISION | Sum of vanna THIS_WEEK expiration |
| `vanna_monthly` | DOUBLE PRECISION | Sum of vanna THIRTY_NEXT_DAYS expiration |
| `spot_vol_beta` | DOUBLE PRECISION | SVB correlation from volland statistics |
| `greek_alignment` | INTEGER (-3 to +3) | Charm + Vanna + GEX direction vs signal direction |

Data coverage: Vanna 96%, SVB 96%, Charm 95%, full context 252/266 trades.

### Key Finding #1: Greek Alignment is Strongly Predictive

| Alignment | N | WR | PnL | Avg/trade |
|-----------|---|-----|------|-----------|
| -3 | 28 | 42.9% | -78.0 | -2.8 |
| -2 | 5 | 40.0% | -21.9 | -4.4 |
| -1 | 41 | 36.6% | -69.7 | -1.7 |
| 0 | 54 | 40.7% | -25.2 | -0.5 |
| **+1** | 39 | **53.8%** | **+44.4** | +1.1 |
| **+2** | 78 | **69.2%** | **+454.3** | +5.8 |
| **+3** | 21 | **71.4%** | **+69.8** | +3.3 |

**Alignment >= +1: 138 trades, 65.2% WR, +568.5 pts. Alignment <= 0: 128 trades, 39.8% WR, -194.8 pts.**

### Key Finding #2: Charm Alignment is the Single Best Filter

| Charm vs Direction | N | WR | PnL | Avg |
|--------------------|---|-----|------|-----|
| Aligned | 172 | 58.1% | +514.4 | +3.0 |
| Opposed | 80 | 38.8% | -205.9 | -2.6 |

Per-setup highlights:
- AG Short: aligned 87.5% WR (+101.3) vs opposed 35.7% (-43.3)
- BofA Scalp: aligned 63.6% (+27.3) vs opposed 33.3% (-38.3)
- GEX Long: opposed 20.0% WR (-111.3) -- charm opposition drives all GEX Long losses
- Paradigm Reversal: aligned 100% WR (6/6)

### Key Finding #3: Vanna Weekly/Monthly Divergence

| State | N | WR | PnL | Avg |
|-------|---|-----|------|-----|
| Divergent (weekly vs monthly opposite) | 117 | 65.0% | +393.3 | +3.4 |
| Aligned | 138 | 42.0% | -48.3 | -0.4 |

Divergence captures regime transitions where dealer positioning is shifting -- high signal quality.

### Key Finding #4: SVB Setup-Specific Insights

- DD Exhaustion @ strong negative SVB (<-0.5): **73.9% WR, +169.7 pts** (23 trades)
- DD Exhaustion @ strong positive SVB (>0.5): 42.1% WR, +10.0 pts (57 trades)
- Paradigm Reversal @ strong negative SVB: 100% WR, +50.0 pts (5 trades)
- DD is contrarian -- thrives in stressed markets (negative SVB)

### Key Finding #5: GEX Long at Alignment -1 is Toxic

- GEX Long alignment -1: **0% WR** (13 trades, -104.0 pts)
- GEX Long alignment +1: 45.5% WR (+2.2 pts)
- The existing vanna filter blocks negative vanna, but charm opposition (-1 alignment) is the real killer

### Filter Simulation Results

Three filter levels tested:

#### OPTIMAL Filter (charm aligned + setup-specific guards)
Rules: (1) Charm must align with trade direction, (2) GEX Long blocked at alignment < +1, (3) AG Short blocked at alignment -3, (4) DD blocked at SVB weak-negative.

| Metric | Baseline | Optimal | Change |
|--------|----------|---------|--------|
| Trades | 266 | 176 | -90 |
| Win Rate | 53.0% | 60.8% | +7.8% |
| Total PnL | +373.7 | +602.4 | **+228.7** |
| Avg Daily PnL | +22.0 | +35.4 | **+13.5** |
| Profit Factor | 1.28 | 1.80 | +0.52 |
| Max Drawdown | 86.2 | 35.0 | **+51.2 improvement** |
| Sharpe (daily) | 0.368 | 0.610 | +0.242 |
| % Winning Days | 41% | 71% | **+30%** |
| Max Loss Streak | 7 | 5 | -2 |
| Worst Day | -31.7 | -26.0 | +5.7 |
| Monthly (4 ES) | $92,328 | $148,838 | **+$56,510** |
| Monthly (10 MES) | $23,082 | $37,210 | **+$14,127** |

Blocked 90 trades: 34W/56L, -228.7 pts (all losers net).

#### AGGRESSIVE Filter (universal alignment >= +1 gate)

| Metric | Baseline | Aggressive | Change |
|--------|----------|------------|--------|
| Trades | 266 | 138 | -128 |
| Win Rate | 53.0% | 65.2% | **+12.2%** |
| Total PnL | +373.7 | +568.5 | +194.8 |
| Avg Daily PnL | 22.0 | 40.6 | **+18.6** |
| Profit Factor | 1.28 | 2.06 | **+0.78** |
| Max Drawdown | 86.2 | 20.2 | **+66.0 improvement** |
| Sharpe (daily) | 0.368 | 0.836 | **+0.467** |
| % Winning Days | 41% | 64% | +23% |
| Monthly (4 ES) | $92,328 | $170,556 | **+$78,228** |
| Monthly (10 MES) | $23,082 | $42,639 | **+$19,557** |

Blocked 128 trades: 51W/77L, -194.8 pts. BUT blocks 22/25 AG Short trades (contrarian setup hurt by directional filter) and 13/17 BofA trades. Higher PnL per trade but fewer opportunities.

#### Per-Setup Impact (Optimal Filter)

| Setup | Baseline N/WR/PnL/PF | Filtered N/WR/PnL/PF | Blocked PnL |
|-------|----------------------|----------------------|-------------|
| AG Short | 25 / 56.0% / +61.2 / 1.47 | 11 / 81.8% / +104.5 / 4.42 | -43.3 |
| BofA Scalp | 17 / 52.9% / -11.0 / 0.89 | 11 / 63.6% / +27.3 / 1.64 | -38.3 |
| DD Exhaustion | 99 / 50.5% / +224.5 / 1.45 | 89 / 52.8% / +247.3 / 1.56 | -22.8 |
| ES Absorption | 70 / 57.1% / +12.8 / 1.04 | 38 / 60.5% / +30.8 / 1.17 | -18.0 |
| GEX Long | 35 / 28.6% / -101.8 / 0.49 | 10 / 50.0% / +9.5 / 1.24 | -111.3 |
| Paradigm Reversal | 9 / 88.9% / +65.0 / 5.33 | 6 / 100% / +60.0 / inf | +5.0 |
| Skew Charm | 11 / 90.9% / +123.0 / 7.15 | 11 / 90.9% / +123.0 / 7.15 | 0 |

### Daily Equity Curve

| Date | Baseline Cum | Optimal Cum | Aggressive Cum |
|------|-------------|-------------|----------------|
| Feb 3 | -1.0 | -1.0 | -1.0 |
| Feb 5 | +16.2 | +16.2 | -7.0 |
| Feb 9 | +48.7 | +48.7 | +25.5 |
| Feb 13 | +23.7 | +13.7 | +9.5 |
| Feb 19 | +135.4 | +140.4 | +58.2 |
| Feb 20 | +256.2 | +278.4 | +169.6 |
| Feb 24 | +214.6 | +288.0 | +215.6 |
| Feb 25 | +182.9 | +293.8 | +215.5 |
| Feb 26 | +170.0 | +335.2 | +301.9 |
| Feb 27 | +232.7 | +412.7 | +379.4 |
| Mar 2 | +211.3 | +434.2 | +440.1 |
| Mar 3 | +373.7 | +602.4 | +568.5 |

Key: Optimal filter **never dips below zero** after Feb 5. Baseline drops from +256 to +170 (Feb 20-26). Optimal stays above +252 through same period.

### Recommendation

**Deploy the OPTIMAL filter** (charm aligned + setup-specific guards):
1. **Charm alignment gate**: block trades where charm opposes direction (pass if charm unknown)
2. **GEX Long alignment >= +1**: blocks when vanna AND/OR charm are opposed
3. **AG Short alignment != -3**: blocks total Greek misalignment
4. **DD Exhaustion SVB filter**: block weak-negative SVB (-0.5 to 0)

Why OPTIMAL over AGGRESSIVE:
- Aggressive kills AG Short (3/25 trades survive) and most BofA
- Optimal is surgical: keeps 176/266 trades, blocks only the truly toxic combos
- Optimal has better max drawdown recovery ratio (17.2x vs 28.1x) and higher total PnL (+602 vs +568)
- Aggressive's higher Sharpe (0.836) is partly from fewer trade days (14 vs 17)

**Income projections (Optimal):**
- E2T 50K (10 MES): ~$29,768/mo ($357K/yr) -- conservative with compliance drag
- 4 ES ($50/pt): ~$148,838/mo ($1.79M/yr)
- User's $21K/mo target: achievable at just 3 MES contracts

### Implementation Status

| Change | Status | Date |
|--------|--------|------|
| Add 5 Greek context columns to setup_log | DEPLOYED | Mar 4, 2026 |
| Generalize vanna cache (ALL/weekly/monthly) | DEPLOYED | Mar 4, 2026 |
| Greek alignment computation per signal | DEPLOYED | Mar 4, 2026 |
| SVB extraction per signal | DEPLOYED | Mar 4, 2026 |
| /api/eval/signals returns Greek fields | DEPLOYED | Mar 4, 2026 |
| Backfill all 334 historical rows | COMPLETED | Mar 4, 2026 |
| Charm alignment gate (auto-trade level) | PENDING | -- |

### Financial Projections (with Greek Optimal Filter)

Based on 17 trading days, 176 filtered trades, 60.8% WR, PF 1.80, Sharpe 0.610.

| Scale | Daily | Monthly | Yearly | Max DD | DD % Acct | Acct Size |
|-------|-------|---------|--------|--------|-----------|-----------|
| **10 MES** | $1,772 | $37,210 | $446,514 | $1,751 | 7.0% | $25K |
| **2 ES** | $3,544 | $74,419 | $893,029 | $3,502 | 7.0% | $50K |
| **4 ES** | $7,088 | $148,838 | $1,786,057 | $7,004 | 7.0% | $100K |
| **6 ES** | $10,631 | $223,257 | $2,679,086 | $10,506 | 7.0% | $150K |

Risk metrics (all sizes): Recovery factor 17.2x | Kelly 27.1% | Max loss streak 5 | 71% winning days

**E2T 50K Sizing Analysis:**

| MES Qty | Daily $ | Max DD $ | Worst Day $ | vs $2K DD Limit | vs $1.1K Daily Limit | Status |
|---------|---------|----------|-------------|-----------------|---------------------|--------|
| 6 MES | $1,063 | $1,051 | -$780 | SAFE | SAFE | **SAFE** |
| 8 MES | $1,418 | $1,401 | -$1,040 | SAFE | SAFE | **SAFE** |
| 10 MES | $1,772 | $1,751 | -$1,300 | SAFE | RISKY | RISKY |
| 15 MES | $2,658 | $2,626 | -$1,950 | DANGER | DANGER | DANGER |

**Recommendation: 8 MES for E2T** -- max safe size, $1,418/day, passes eval in ~2 days ($3K target).

**Equity Curve (4 ES = $200/pt):**
- Baseline: $+74,742 final, max DD $17,246 (Feb 24-26 drop)
- **Filtered: $+120,488 final, max DD $7,004 (Feb 13 only)**
- Filter turns the Feb 24-26 drawdown from -$14,660 into a **flat/positive stretch**
- After Feb 19, filtered equity curve **never draws down** -- pure upward trajectory

---

## Analysis #9 — Mar 11, 2026: Asymmetric Short Filter

### Problem: Greek Alignment is Structurally Biased Against Shorts

**Trigger:** Mar 11 was a -39.8 pt day (33 trades). With alignment +3 filter, PnL was WORSE (-54.3 pts) because the filter concentrated ALL trades into longs on a down day.

**Root Cause Discovery:**

The `_compute_greek_alignment()` function scores 3 components:
1. **Charm**: positive = bullish (+1 for longs, -1 for shorts)
2. **Vanna_all**: positive = bullish (+1 for longs, -1 for shorts)
3. **GEX position**: spot below +GEX = bullish (+1 for longs, -1 for shorts)

**Critical finding:** Since Feb 24 (10 consecutive trading days), ALL THREE components have been permanently bullish:
- Vanna: positive every day (971M to 6.6B range, never negative)
- GEX position: spot almost always below +GEX
- Result: max possible short alignment = -1 (only if charm opposes)

**Impact on alignment distribution:**

| Alignment | Long trades | Short trades | Notes |
|-----------|------------|-------------|-------|
| +3 | 85 (100% long) | 0 | All 3 bullish = impossible for shorts |
| +2 | 88 (100% long) | 0 | 2/3 bullish = impossible for shorts |
| +1 | 60 (79%) | 16 (21%) | Rare for shorts |
| 0 | 26 (36%) | 47 (64%) | First alignment where shorts appear |
| -1 | 11 (5%) | 197 (95%) | **Where most shorts live** |
| -2 | 1 (2%) | 47 (98%) | |
| -3 | 0 | 42 (100%) | All 3 oppose = current filter passes these |

**98.8% of alignment +3 trades are long.** The filter `abs(align) >= 3` effectively means:
- Longs: Keep only +3 (73% WR, +444.2 pts) — GREAT
- Shorts: Keep only -3 (48% WR, -47.6 pts) — TERRIBLE (selects worst shorts)

### Winning Short Trade Analysis (144 wins analyzed)

**Alignment of winning shorts:**
- align -1: 80 wins (55.6%) — **majority of winners**
- align -2: 22 wins (15.3%)
- align -3: 16 wins (11.1%)
- align 0: 22 wins (15.3%)
- align +1: 4 wins (2.8%)

**Per-setup short performance (all-time):**

| Setup | Trades | WR% | PnL | Verdict |
|-------|--------|-----|-----|---------|
| Skew Charm shorts | 66 | 75% | +153.8 | MVP — allow all |
| DD Exhaustion shorts | 116 | 49% | +72.1 | Workhorse — block align=0 |
| AG Short | 34 | 57% | +50.2 | Good — block align=-3 |
| Paradigm Reversal shorts | 16 | 70% | -2.2 | Small sample, allow |
| ES Absorption shorts | 69 | 42% | **-175.6** | TOXIC — block all |
| BofA Scalp shorts | 21 | 42% | **-26.3** | TOXIC — block all |

**ES Absorption shorts by alignment (all negative):**
- align -3: 22t, -44.4 pts, 47% WR
- align -2: 16t, -11.0 pts, 50% WR
- align -1: 30t, -124.0 pts, 36% WR
- align +1: 1t, +3.8 pts
- **Verdict: negative at EVERY alignment level. Unredeemable.**

**DD Exhaustion shorts by alignment:**
- align -1: ~70 trades, positive (bulk of DD shorts)
- align 0: 28% WR, -97 pts — **specific toxic combo**
- align +1/+2: positive (contrarian edge — more Greek opposition = better for DD)

**AG Short by alignment:**
- align -3: 14t, 46% WR, -13.2 pts — worst bucket
- align -1: bulk of AG shorts, positive
- align +1/+3: small sample but positive (contrarian)

### Paradigm Impact on Shorts

| Paradigm | Short trades | WR% | PnL | Notes |
|----------|------------|-----|-----|-------|
| SIDIAL-EXTREME | ~20 | 65% | positive | Best paradigm for shorts |
| BofA-LIS | ~15 | 68% | positive | Good for shorts |
| AG-PURE | ~30 | 55% | positive | Natural short paradigm |
| GEX-LIS | ~10 | 30% | -73 pts | Toxic — bullish paradigm |
| BOFA-PURE | 63 | 48% | -120 pts | Worst paradigm for shorts |

### SVB (Spot-Vol-Beta) Impact on Shorts

| SVB Range | Short WR% | Notes |
|-----------|----------|-------|
| < -1.5 | 58% | Decent |
| -1.5 to -0.5 | 64% | **Best** — strong inverse correlation |
| -0.5 to 0 | 38% | **Worst** — weak signal zone |
| 0 to +0.5 | 55% | OK |
| > +1.5 | 47% | Bad |

### Discord/Reference Insights for Shorts

**Apollo:** "Bearish charm won't effect much with elevated skew. IF skew comes down then you can realize the bearishness."
- Charm alone isn't enough for shorts — need skew compression

**Wizard of Ops:** "Play extremes for reversion."
- Best shorts are at daily extremes, not mid-range

**Dark Matter:** "Break past level, break below it — new 30 min candle retests it and you enter on that retest."
- Retest entries, not chase entries

**Key Discord rule:** "Post-2 PM is dealer o'clock where charm moves come in" — afternoon shorts can work for Skew Charm

**Messy paradigm insight:** SIDIAL-MESSY (100% WR) and GEX-MESSY (100% WR) are goldmine for shorts. When structure breaks, ALL short setups fire and win.

### Strategy Comparison (24 trading days, 620 total trades)

| Strategy | N | PnL | WR% | Max DD | PnL/day | Sharpe |
|----------|---|-----|-----|--------|---------|--------|
| Baseline (unfiltered) | 620 | +847 | 58% | 239 | +35.3 | 0.47 |
| **Current PROD (abs≥3)** | **122** | **+397** | **66%** | **113** | **+28.3** | **0.44** |
| +3L + all shorts (no filter) | 407 | +516 | 59% | 344 | +23.5 | 0.30 |
| +3L + SVB<-0.5 shorts | 190 | +572 | 68% | 135 | +40.9 | 0.57 |
| +3L + vanna<0 shorts | 136 | +635 | 74% | 130 | +37.4 | 0.69 |
| **Option B: +3L + per-setup shorts** | **253** | **+852** | **69%** | **128** | **+44.8** | **0.63** |
| Option B + SVB<-0.5 (Option C) | 169 | +613 | 70% | 96 | +40.9 | — |
| +3L + SC+DD(a!=0) only | 232 | +768 | 70% | 128 | +51.2 | 0.64 |
| Long+2 + V7 shorts | 320 | +1063 | 69% | 128 | +66.4 | 0.83 |

### Critical Risk Case: March 9 (Rally Day)

March 9 had 32 trades. Under current filter: +132.2 pts (10 trades, 1 short). Under wide-open shorts: -56.0 pts (28 trades, 19 shorts all lost).

This shows that opening shorts completely can have brutal drawdown days when market rallies. The per-setup blocks (Option B) reduce this risk by blocking the worst combos while still capturing the winning shorts.

### Decision: Asymmetric Filter (Option B for SIM, Option C for E2T)

**Option B — Per-Setup Toxic Combo Blocks (SIM auto-trader + options trader):**
- Longs: alignment +3 (unchanged)
- Shorts:
  - Block ES Absorption shorts (ALL) — toxic at every alignment, -175.6 pts
  - Block BofA Scalp shorts (ALL) — net negative, -26.3 pts
  - Block DD Exhaustion shorts at align=0 — 28% WR, -97 pts toxic combo
  - Block AG Short at align=-3 — 46% WR, -13.2 pts toxic combo
  - Allow everything else (Skew Charm, DD at any other align, AG at any other align, Paradigm Rev)
- Expected: 253 trades, +852 pts, 69% WR, DD=128, Sharpe=0.63

**Option C — Option B + SVB < -0.5 (E2T eval trader):**
- Same per-setup rules as Option B
- Additional gate: shorts only when SVB < -0.5
- Expected: 169 trades, +613 pts, 70% WR, DD=96
- Safest for funded account — DD well within E2T $2K limit

**Why NOT use alignment as general short filter:**
- Alignment is structurally biased bullish (vanna+GEX permanently positive)
- Best winning shorts cluster at alignment -1 (55.6% of wins)
- -3 filter selects the WORST shorts (48% WR, -47.6 pts)
- Per-setup blocks target specific toxic combos, not alignment as a concept

**Net improvement over current:**
- Option B: +455 pts more, DD only +15 pts more, Sharpe +0.19
- Option C: +216 pts more, DD actually -17 pts LESS, even safer

---

## Analysis #10 — Mar 12, 2026

### Investigation: Alignment +3 During Bearish Regime (Mar 11 trades)

**Trigger:** User noticed LONG signals firing with alignment +3 around 13:00 ET on Mar 11, but market was clearly in a bearish regime. How did alignment score +3?

### Mar 11 Paradigm Timeline

| Time (ET) | Paradigm | Charm | SVB | DD Hedging |
|-----------|----------|-------|-----|------------|
| 12:00-12:40 | AG-PURE | +45.8M | +0.39 | -$763M |
| ~12:42 | AG-LIS → BOFA-PURE | +41.9M | +0.39 | -$2.08B |
| 12:50-14:00 | **BOFA-PURE** | +26-30M | +0.10 to -0.16 | -$1.6B to -$1.7B |

Paradigm was bearish (AG then BOFA) the entire afternoon. DD hedging deeply negative.

### Alignment +3 Breakdown

The `_compute_greek_alignment()` function scores 3 components (±1 each):

| Component | Value | Score for LONG |
|-----------|-------|----------------|
| Charm (aggregatedCharm) | +26-45M (positive) | **+1** |
| Vanna ALL | +5.3B (positive) | **+1** |
| GEX (spot vs max +GEX) | Spot 6770 < max +GEX ~6850 | **+1** |
| **Total** | | **+3** |

**Problem:** Paradigm is NOT a component of alignment. All 3 components can read "bullish" during a bearish regime because:
1. **Charm** measures net time-decay exposure across ALL strikes — can be positive in AG/BOFA
2. **Vanna ALL** is dominated by absolute vol levels, almost always positive (~+5B all day) — structurally biased bullish
3. **GEX position** (spot < max +GEX) — in AG regime, GEX ceiling is resistance not support

### Eval Trader Impact (Mar 11)

Only 1 trade filled: Skew Charm LONG [C] @ 6774.40 → stopped at -12 pts (-$497).

Four more LONG signals at alignment +3 around 12:54-13:33 ET were attempted but ALL rejected by NT8:
- Skew Charm LONG [B] @ 6770.08 → REJECTED
- DD Exhaustion LONG [A] @ 6770.60 → REJECTED
- Skew Charm LONG [B] @ 6771.51 → REJECTED
- DD Exhaustion LONG [A] @ 6775.26 → REJECTED

### Proposed Fix #1: Paradigm Direction Gate

**Hypothesis:** Block LONG trades in AG/BOFA paradigms, block SHORT trades in GEX paradigms.

**Result: HARMFUL — do NOT implement.**

| Scenario | Trades | WR | PnL | PF |
|----------|--------|-----|------|-----|
| No gate (baseline) | 620 | 50.3% | +847 | 1.29 |
| Strict gate (passed) | 328 | 45.7% | +115 | 1.07 |
| **Strict gate (blocked)** | 292 | **55.5%** | **+732** | **1.60** |

The blocked trades are the **best performers** (55.5% WR, PF 1.60). Reason: DD Exhaustion and Skew Charm are **contrarian** — they fire in bearish paradigms and profit from reversals.

**Paradigm direction affinity (counterintuitive):**
- AG paradigm: LONG = 56% WR, +155 pts / SHORT = 41% WR, -68 pts → **favors LONG**
- BOFA paradigm: LONG = 62% WR, +497 pts / SHORT = 42% WR, -52 pts → **favors LONG**
- GEX paradigm: LONG = 44% WR, +41 pts / SHORT = 40% WR, +6 pts → neutral

### Proposed Fix #2: Vanna ALL Sign Gate

**User observation:** "When cumulative vanna (weekly/monthly/all) is negative, bullish & GEX setups always fail."

**Result: CONFIRMED — strong signal but already captured by existing alignment filter.**

| Direction | Vanna ALL | Trades | WR | PnL | PF |
|-----------|-----------|--------|-----|------|-----|
| LONG | Positive | 253 | **60.9%** | **+770** | **1.77** |
| LONG | **Negative** | 36 | **25.0%** | **-28** | **0.84** |
| SHORT | Negative | 51 | 39.2% | **+191** | **2.18** |
| SHORT | Positive | 268 | 45.5% | -122 | 0.92 |

LONG with negative vanna_all = **25% WR** (devastating). However, negative vanna drops alignment by 2 points, which the existing F5 filter (alignment >= +3 for longs) already catches in most cases.

### Proposed Fix #3: Charm Exposure Resistance/Support Gate

**Hypothesis:** For LONG trades, big positive charm bars above spot (within 20 pts) = resistance, block longs. For SHORT trades, big negative charm bars below spot = support, block shorts.

#### LONG Side — Does NOT Work

Blocking LONGs when charm sum above spot > threshold actually **removes winners**:

| Threshold (sum above) | Passed WR | Passed PnL | Blocked WR | Blocked PnL |
|----------------------|-----------|-----------|------------|------------|
| >50M | 54.9% | +447 | **60.8%** | **+328** |
| >100M | 56.1% | +565 | **57.4%** | **+210** |
| >200M | 55.7% | +623 | **64.0%** | **+152** |

Blocked LONGs have higher WR at every threshold. Positive charm above spot is a **magnet** (dealers unwind into close, price drifts up), not resistance.

#### SHORT Side — Works Extremely Well

Blocking SHORTs when charm sum below spot < threshold (big negative = support):

| Threshold (sum below) | Passed WR | Passed PnL | Blocked WR | Blocked PnL |
|----------------------|-----------|-----------|------------|------------|
| <-50M | **49.8%** | **+149** | 22.0% | -77 |
| <-100M | **49.1%** | **+208** | **16.3%** | **-136** |
| <-200M | 47.6% | +113 | 16.7% | -41 |
| <-500M | 46.6% | +129 | 6.7% | -57 |

**Best threshold: -100M** — blocks 43 shorts at **16.3% WR** (terrible), saves +136 pts.

SHORT losers have **7x more negative charm below spot** than SHORT winners.

#### Time-of-Day Effect

Charm grows exponentially through the day:
- 10:00 AM: sums ~20-50M
- 12:00 PM: sums ~100-200M
- 2:00 PM: sums ~500M-1B
- 3:30 PM: sums ~2-5B

Late-day SHORT with charm_sum < -100M = **8.3% WR** (nearly all losers).

### Summary & Recommendations

| Filter | Direction | Impact | Verdict |
|--------|-----------|--------|---------|
| Paradigm gate | Both | Loses +732 pts | **REJECT** |
| Vanna ALL sign | LONG | 25% WR when negative | **Already captured by F5** |
| Charm resistance (above spot) | LONG | Removes winners | **REJECT** |
| Charm support (below spot) | SHORT | Blocks 16% WR losers, saves +136 | **IMPLEMENT as F7** |

**Recommended F7:** Block SHORT when sum of charm exposure points at strikes within 20 pts below spot < -100M.

**NOT recommended for LONG:** Positive charm above spot acts as a magnet (price drifts toward it as charm decays), not resistance. This is the opposite of the initial hypothesis.

---

## Analysis #11 — Mar 12, 2026: V7+AG Filter Upgrade

### Trigger

Mar 12 was a -13.2 pt SPX down day. Unfiltered PnL = +47 pts (shorts carried). Filtered PnL (current deployed Option B) = -36 pts. The filter concentrated 100% of trades into longs on a sell-off day, missing +83 pts in profitable shorts.

### Root Cause: Current Filter Has Two Problems

1. **Longs too strict (align >= 3):** Alignment 2 longs have 66% WR and +316 pts historically — solid edge being left on the table.
2. **Missing AG Short:** Currently blocked at alignment = -3 (F3 rule). But AG Short is the only short setup that fires on pure sell-offs without skew/DD signals. Today AG Short went 5W/1L, +51 pts — all blocked.

### Filter Naming Conventions (for future reference)

| Name | Long Rule | Short Rule | Origin |
|------|-----------|-----------|--------|
| **R1** | Basic Greek filter (GEX>=1, AG!=-3, DD SVB block, ESA<0 block) | Same basic blocks | Mar 8, Analysis #5 |
| **Option B** | align >= 3 | Block ES Abs (all), BofA (all), DD (align=0), AG (align=-3) | Mar 11, Analysis #9 (deployed) |
| **Option C** | align >= 3 | Option B + SVB < -0.5 gate | Mar 11, Analysis #9 (E2T) |
| **V7** | align >= 2 | Only Skew Charm + DD Exhaustion (align!=0) | Mar 11, Analysis #9 (backtest only) |
| **V7+AG** | align >= 2 | Skew Charm + DD Exhaustion (align!=0) + AG Short (all) | **Mar 12, Analysis #11 (new)** |

### Full Comparison (all data, Feb 5 → Mar 12, 373+ trades)

| Filter | Trades | WR | PnL | PnL/day | PF | Max DD | Sharpe | Losing Days |
|--------|--------|-----|------|---------|------|--------|--------|-------------|
| Current (Option B) | 281 | 59% | +809 | +40.5 | 1.64 | 92.5 | 0.56 | 7/20 |
| V7 (L2 + SC+DD) | 332 | 61% | +1023 | +60.2 | 1.71 | 50.1 | 0.76 | 5/17 |
| V7-L3 (L3 + SC+DD) | 244 | 61% | +728 | +45.5 | 1.67 | 92.5 | 0.58 | 6/16 |
| **V7+AG (L2 + SC+DD+AG)** | **373** | **60%** | **+1104** | **+52.6** | **1.66** | **50.1** | **0.73** | **7/21** |

### Drawdown Deep Dive

| Risk Metric | Current (Option B) | V7+AG |
|-------------|-------------------|-------|
| Max trailing DD | 92.5 pts ($3,698 @ 8 MES) | **50.1 pts ($2,005)** |
| Worst single day | -80.5 pts | **-50.1 pts** |
| Worst 2-day streak | -92.5 pts | **-35.0 pts** |
| Avg losing day | -34.8 pts | **-24.9 pts** |
| Avg winning day | +81.0 pts | **+91.3 pts** |
| Days worse than -27.5 | 4 | **3** |
| Worst intraday DD | 143 pts | **128 pts** |
| Max consecutive losing days | 2 | 2 |

**V7+AG is safer on every single DD metric.** Max trailing DD nearly halved (50 vs 93 pts).

### Why V7+AG Has LESS Drawdown Than Current

Current's worst day was Feb 25: -80.5 pts (13 shorts including toxic ES Absorption and BofA). V7+AG only took 8 shorts that day (SC+DD+AG) = -50.1 pts. The toxic setups it blocks (ES Absorption -176 pts all-time, BofA -26 pts) were causing the big DD spikes.

### Worst Days Analysis

| Date | SPX Move | Current | V7+AG | What Happened |
|------|----------|---------|-------|---------------|
| Feb 25 | +22.3 | -80.5 | **-50.1** | Rally day, shorts hammered — V7+AG took fewer toxic shorts |
| Mar 9 | +157.3 | -56.0 | **-41.0** | Huge rally, shorts lost — V7+AG had same exposure |
| Mar 12 | -13.2 | -40.1 | **-8.9** | Sell-off, longs lost — AG Short hedged in V7+AG |
| Mar 11 | -16.5 | -34.4 | **-29.4** | Sell-off — V7+AG had more shorts to hedge |

### What V7+AG Changes vs Current Deployed

| Component | Current (Option B) | V7+AG |
|-----------|-------------------|-------|
| **Longs** | alignment >= 3 | **alignment >= 2** (adds 66% WR longs) |
| **Skew Charm shorts** | Allow (except BofA/ES blocks) | Allow all (unchanged) |
| **DD Exhaustion shorts** | Allow (block align=0) | Allow (block align=0) (unchanged) |
| **AG Short** | Block at align=-3 | **Allow all** (removes F3 block) |
| **ES Absorption shorts** | Block all | Block all (unchanged) |
| **BofA Scalp shorts** | Block all | Block all (unchanged) |
| **Paradigm Reversal shorts** | Allow | **Block** (net -2.2 pts, not worth including) |

### Charm S/R Interaction

Charm S/R limit entry (implemented same day) is **separate and stacks on top**:
1. V7+AG decides WHICH trades to take (filter)
2. Charm S/R improves HOW shorts are entered (limit vs market order)

The charm S/R backtest showed +822 pts improvement on Option B shorts. Same improvement applies to V7+AG shorts since it uses the same short setups (SC, DD, AG).

### E2T Safety

At 8 MES ($40/pt), V7+AG max trailing DD = $2,005 — right at the $2K E2T limit. Options:
- Reduce to 7 MES ($35/pt): DD = $1,754, safely within limit
- Add SVB < -0.5 gate on shorts (Option C style) for extra safety

### Decision: Deploy V7+AG

**SIM auto-trader:** V7+AG (full)
**Eval trader:** V7+AG (consider 7 MES for E2T safety)

**Why AG Short should never be alignment-filtered:**
- AG Short is the ONLY short setup that fires on pure sell-off days (no skew/DD signals)
- Alignment is structurally biased bullish — AG Short at align=-3 is normal, not bad
- Historical: 53% WR at align=-3, +13.7 pts. Feb 24 disaster was early data anomaly
- Today (Mar 12): 5W/1L, +51.2 pts at align=-3 — all blocked by current filter

---

## Analysis #12 — Mar 14, 2026

### VIX Gate & Overvix Indicator — V8 Filter

**Context:** Mar 12-13 saw -443 pts on V7+AG filtered trades (VIX 27.3 and 27.2). Iran/oil crisis pushed VIX above 26 — all long setups lost. Investigated whether VIX level, VIX direction, SVB, and/or Apollo's overvix indicator (VIX - VIX3M) could be used to gate trades.

### Dataset: 431 V7+AG filtered trades, Feb 5 - Mar 13 (24 trading days)

### Key Findings

**1. VIX Level Analysis:**

| VIX Range | Trades | WR | P&L | PF |
|-----------|--------|-----|------|-----|
| < 18 | 9 | 0% | -70 | 0.00 |
| 18-20 | 60 | 53% | +49 | 1.13 |
| 20-22 | 132 | 64% | +336 | 2.49 |
| 22-24 | 95 | 66% | +314 | 1.93 |
| 24-26 | 81 | 56% | +150 | 1.40 |
| 26+ | 54 | 43% | -326 | 0.57 |

**Sweet spot:** VIX 20-24 (66% WR, PF 2.16). **Toxic:** VIX 26+ (43% WR, -326 pts).
Longs at VIX 26+: 34% WR, -360 pts. Shorts at VIX 26+: 56% WR, break-even.

**2. SVB (Spot Vol Beta) Analysis:**
- SVB 97.5% correlated with raw VIX in this period — adds little beyond VIX level.
- SVB > +0.50 (positive) is BEST: 63% WR, +480 pts.
- SVB -0.50 to 0 (weak negative) is WORST: 41% WR, -86 pts.
- Apollo's overvix is NOT the same as SVB — SVB measures vol-spot sensitivity, overvix measures term structure.

**3. Apollo's Overvix Indicator (VIX - VIX3M):**
- Formula: `overvix = VIX - VIX3M` (VIX3M = CBOE 3-month volatility index)
- Overvix > +2: "signal territory" (short-term fear overpriced, mean reversion bullish)
- Overvix > +3: "heavily overvixed" (strongest bullish signal)
- Since Jan 2024: 92% of days undervixed, only 1.8% above +2
- Standalone swing signal: 4 entries since Jan 2024, 50% WR, +24 pts total (small sample)
- In our data: 0 trades on overvix > +2 days. Max overvix was +1.93 (Mar 6).

**4. VIX Gate Backtest (16 variants tested):**

| Gate | Trades | WR | P&L | PF | MaxDD | Sharpe |
|------|--------|-----|------|-----|-------|--------|
| **Baseline (V7+AG)** | 431 | 55.5% | +657 | 1.30 | 472 | 0.29 |
| **A5: Block longs VIX>26** | 364 | 60.7% | +1,140 | 1.71 | 50 | 0.77 |
| A7: VIX 18-25 only | 294 | 62.6% | +1,094 | 1.95 | 29 | 1.08 |
| A1: Block all VIX>25 | 303 | 60.7% | +1,024 | 1.84 | 50 | 0.83 |
| D-Full regime | 276 | 62.3% | +804 | 1.63 | 163 | 0.61 |

**A5 wins on PnL (+483 pts improvement)** while only removing 67 trades.

**5. Smart VIX Gate (overvix-aware A5):**
Rule: Block longs when VIX > 26 AND overvix < +2 (allow mean-reversion longs when overvixed).
In current data: identical to plain A5 (VIX > 26 + overvix >= +2 never occurred).
But future-proofs for Apollo-type overvix signals at high VIX.

**Daily impact (V8 vs V7+AG on worst days):**

| Date | VIX | Overvix | V7+AG P&L | V8 P&L | Saved |
|------|-----|---------|-----------|--------|-------|
| Mar 6 | 29.5 | +1.93 | +117 (all shorts) | +117 | -- |
| Mar 12 | 27.3 | +0.34 | -170 | +31 | +201 |
| Mar 13 | 27.2 | -0.09 | -273 | +9 | +281 |

### V8 Filter Rules

**V8 = V7+AG + Smart VIX Gate**

All V7+AG rules remain unchanged, PLUS:
- **Longs:** When VIX > 26 AND overvix (VIX - VIX3M) < +2 → BLOCK
- **Longs:** When VIX > 26 AND overvix >= +2 → ALLOW (mean reversion signal)
- **Shorts:** No VIX gate (shorts profitable at high VIX)

**Data source:** VIX and VIX3M fetched from TradeStation API ($VIX.X, $VIX3M.X) every 30s alongside SPX.

### Performance

| Filter | Trades | WR | PnL | PF | MaxDD | Sharpe |
|--------|--------|-----|------|-----|-------|--------|
| V7+AG | 431 | 55.5% | +657 | 1.30 | 472 | 0.29 |
| **V8** | **364** | **60.7%** | **+1,140** | **1.71** | **50** | **0.77** |
| Delta | -67 | +5.2% | **+483** | +0.41 | **-422** | **+0.48** |

### Decision: Deploy V8

**SIM auto-trader:** V8 (V7+AG + Smart VIX Gate)
**Eval trader:** V8 (via vix/overvix fields in /api/eval/signals)
**Portal:** V8 added as default strategy filter, V7+AG retained for comparison

### Overvix Tracking

Overvix (VIX - VIX3M) now logged to:
- setup_log DB table (`overvix` column)
- Telegram setup alerts (VIX=X.X OV=+X.X)
- /api/health endpoint (vix, vix3m, overvix fields)
- /api/eval/signals (overvix and vix per signal)

### Future: Overvix Swing Setup

Apollo's overvix signal (entry when overvix > +2, exit when < 0) is a swing trade, not 0DTE. Only 4 signals since Jan 2024 — need more data. Tracked via Telegram for manual swing trades.

---

## Analysis #13 — Mar 14, 2026

### V8 Options Backtest — Real Option Prices (Mar 1-13)

**Context:** Validated V8 filter using actual option chain snapshots instead of SPX point-based P&L. For each V8 trade, found the ~0.30 delta option (call for longs, put for shorts) at entry, priced it from the chain snapshot, then found the same strike at exit time and got the exit bid. This is the most accurate backtest possible — only ~30 seconds of divergence from real fills.

**Dataset:** 255 V8 trades with matched option prices (1 skipped, no snapshot), Mar 1-13 (10 trading days).

### V8 vs V7+AG (Real Option Prices)

| Metric | V8 | V7+AG |
|--------|-----|-------|
| Trades | 255 | 327 |
| Win Rate | 42.0% | 39.8% |
| Total P&L | $14,930 | $17,850 |
| P&L/day | $1,493 | $1,785 |
| Avg Winner | $560 | $600 |
| Avg Loser | -$304 | -$305 |
| PF | 1.33 | 1.30 |
| **Max Drawdown** | **$8,615** | **$19,945** |

**Note:** V7+AG shows slightly more total P&L because a few lucky longs at VIX>26 on Mar 9 had massive option gains (gamma acceleration: +$3,900, +$3,290, +$3,280). But V8's MaxDD is less than half — $8.6K vs $19.9K. Risk management wins.

**Why WR is lower than SPX-point WR (42% vs 61%):** Options have asymmetric payoffs — small option losses (-$50 to -$200) count as losses but represent tiny premium decay, while big winners ($1,000-$2,400) dramatically outweigh them. PF 1.33 confirms profitability despite lower WR.

### By Setup (V8, Real Option Prices)

| Setup | Trades | WR | P&L | Notes |
|-------|--------|-----|-----|-------|
| Skew Charm | 106 | 48.1% | +$9,450 | MVP — dominates |
| DD Exhaustion | 90 | 31.1% | +$4,080 | Low WR but high avg winner |
| AG Short | 16 | 56.2% | +$2,670 | Small sample, strong |
| GEX Long | 5 | 60.0% | +$290 | Tiny sample |
| ES Absorption | 33 | 48.5% | -$305 | Break-even |
| BofA Scalp | 3 | 0.0% | -$700 | Filtered out mostly |
| Paradigm Rev | 2 | 0.0% | -$555 | Filtered out mostly |

### Daily P&L (V8, 1 SPX contract per signal)

| Date | Trades | P&L | Cumulative | Capital Needed | VIX |
|------|--------|-----|-----------|---------------|-----|
| Mar 2 | 15 | +$2,440 | +$2,440 | $639 | 21.2 |
| Mar 3 | 32 | +$3,135 | +$5,575 | $2,156 | 24.1 |
| Mar 4 | 34 | -$4,325 | +$1,250 | $1,978 | 21.1 |
| Mar 5 | 41 | +$5,450 | +$6,700 | $3,014 | 23.7 |
| Mar 6 | 23 | +$3,300 | +$10,000 | $2,154 | 26.7 |
| Mar 9 | 18 | -$2,795 | +$7,205 | $1,339 | 27.4 |
| Mar 10 | 37 | +$13,030 | +$20,235 | $2,542 | 23.8 |
| Mar 11 | 31 | -$2,320 | +$17,915 | $2,294 | 25.0 |
| Mar 12 | 12 | -$480 | +$17,435 | $1,103 | 26.0 |
| Mar 13 | 12 | -$2,505 | +$14,930 | $1,046 | 26.0 |

**Capital needed = trades × option premium × $100 multiplier (actual cost to buy all options that day).**

### SPY Account Sizing (1 SPY per signal, SPX/10)

| Level | Amount |
|-------|--------|
| Max daily capital | $3,014 |
| Worst day loss | -$432 |
| **Account needed (comfortable)** | **$3,447** |
| Avg daily P&L | +$149 |
| **Monthly P&L** | **+$3,135** |
| **Monthly ROI** | **+91%** |

### Scaling Table

| SPY Qty | Account Needed | Monthly P&L | Monthly ROI |
|---------|---------------|-------------|-------------|
| 1 | $3,447 | +$3,135 | 91% |
| 2 | $6,894 | +$6,271 | 91% |
| 5 | $17,235 | +$15,676 | 91% |
| 10 | $34,470 | +$31,353 | 91% |

### Circuit Breaker Analysis (Pending Validation)

| Risk Control | Total P&L | vs Baseline |
|---|---|---|
| No limit (baseline) | $14,930 | -- |
| **Stop after 4 consecutive losses** | **$22,035** | **+$7,105 (+48%)** |
| Stop after daily P&L < -$2,000 | $18,200 | +$3,270 |
| Max 20 trades/day | $20,670 | +$5,740 |
| Stop after 3 consecutive losses | $9,160 | -$5,770 (too tight) |

**Not deployed yet — only 10 trading days. Need 30+ days to validate. Saving 48% of P&L is significant if it holds.**

### Decision

- **TradeStation account #11697180 funded $4,000**
- **2-week validation period (Mar 14-28):** tracking logs only, no live trades
- **Go live after validation with 1 SPY per signal**
- **Scale to 2 SPY when balance reaches $6,894**
- **Options_trader.py updated:** live TS API quotes for both entry and exit (not stale snapshots)

---

## Analysis #14 — Mar 14, 2026

### Full-Period Options Backtest — Feb 5 to Mar 13 (V8, Real Prices)

**Context:** Extended Analysis #13 to cover the full data period. Feb lacked Skew Charm (added Mar) and had fewer refined setups. Purpose: assess regime stability and per-setup option performance.

**Dataset:** 358 V8 trades with matched option prices, 21 trading days.

### Overall (V8, Real Option Prices)

| Period | Trades | WR | P&L | PF | MaxDD | P&L/day |
|--------|--------|-----|------|-----|-------|---------|
| Feb (11 days) | 103 | 38.8% | +$3,060 | 1.16 | $11,560 | +$278 |
| Mar (10 days) | 255 | 42.0% | +$14,930 | 1.33 | $8,615 | +$1,493 |
| **Full period** | **358** | **41.1%** | **+$17,990** | **1.28** | **$11,560** | **+$857** |

### Per-Setup (V8, Full Period)

| Setup | Trades | WR | P&L | Avg Winner | Avg Loser | P&L/day |
|-------|--------|-----|-----|-----------|-----------|---------|
| **Skew Charm** | 106 | 48% | +$9,450 | $505 | -$296 | +$945 |
| **DD Exhaustion** | 154 | 32% | +$9,390 | $889 | -$325 | +$522 |
| AG Short | 40 | 50% | +$85 | $372 | -$367 | +$6 |
| GEX Long | 5 | 60% | +$290 | $280 | -$275 | +$97 |
| ES Absorption | 42 | 50% | -$130 | $186 | -$192 | -$16 |
| BofA Scalp | 6 | 33% | -$420 | $190 | -$200 | -$84 |
| Paradigm Rev | 5 | 20% | -$675 | $50 | -$181 | -$135 |

**Key: DD Exhaustion has 32% WR but avg winner is 2.7x avg loser ($889 vs $325) — gamma acceleration on options makes low-WR setups profitable.**

### Feb vs Mar Per-Setup

| Setup | Feb P&L | Mar P&L | Notes |
|-------|---------|---------|-------|
| Skew Charm | n/a | +$9,450 | Only existed in March |
| DD Exhaustion | +$5,310 | +$4,080 | Consistent both months |
| AG Short | -$2,585 | +$2,670 | Regime-dependent |
| ES Absorption | +$175 | -$305 | Neutral both months |

### VIX Regime (V8, Real Option Prices)

| VIX Range | Trades | WR | P&L | Avg Premium |
|-----------|--------|-----|------|------------|
| 18-20 | 39 | 21% | -$3,400 | $3.89 |
| **20-22** | **67** | **48%** | **+$1,710** | $5.56 |
| **22-24** | **57** | **51%** | **+$12,185** | $6.57 |
| 24-26 | 102 | 39% | +$4,280 | $7.70 |
| 26+ | 46 | 33% | -$1,710 | $8.67 |

**Sweet spot: VIX 22-24.** VIX 18-20 is toxic even with V8. VIX 26+ still negative (shorts only after V8 gate).

### DD Exhaustion Grade Surprise

| Grade | Trades | WR | P&L |
|-------|--------|-----|------|
| A | 62 | 34% | +$8,805 |
| A+ | 35 | 26% | +$1,640 |
| A-Entry | 53 | 28% | -$2,255 |
| LOG | 4 | 100% | +$1,200 |

**LOG grade (lowest confidence) = 100% WR.** A-Entry (gate-level) is the worst. Confirms Analysis #5: DD score is NOT predictive of outcome.

### Time-of-Day Insights

**Skew Charm:** Best 13:00-16:00 (60%+ WR). Weakens after 17:00.
**DD Exhaustion:** Best at 16:00 (47% WR, +$7,775). Terrible at 19:00 (9% WR, -$3,660).
**AG Short:** Best at 14:00 (80% WR, +$2,590). Terrible at 16:00 (12% WR, -$2,645).

### Capital & Projections (Full Period, 1 SPY)

| Metric | Full Period | Mar Only |
|--------|-----------|---------|
| Daily P&L | +$86 | +$149 |
| Monthly projection | +$1,799 | +$3,135 |
| Account needed | $3,558 | $3,447 |

**Mar-only is more representative of current capability** (Skew Charm enabled, all setups refined).

### 12-Month Growth (starting $4K, 75% of Mar performance)

Month 1: $5,349 → Month 6: $20,191 → Month 12: $129,480
