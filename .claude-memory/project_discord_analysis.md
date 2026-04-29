---
name: Discord Analysis Results & Comparison History
description: Volland Discord vs 0DTE Alpha system comparisons. Filter recommendations tested, expert insights extracted. Track sync dates here.
type: project
---

## Discord Filter Backtesting (Mar 19 2026, 736 trades)

Analyzed Volland Discord daytrading central (338 msgs, Mar 17-18) and 0DTE alerts (67 alerts, Feb 2 - Mar 18). Tested 5 recommendations from community wisdom.

**ALL 5 REJECTED:**

| Recommendation | PnL Delta | Reason |
|----------------|-----------|--------|
| FOMC day gate | -130.9 | FOMC day was +130.9 pts (64% WR) |
| Sidial paradigm block | -270.3 | Sidial is BEST paradigm (SC 91% WR, +190.7) |
| Overvix regime tracking | -71.9 | Can't separate good/bad VIX-rising days |
| Friday longs block | +173 unfiltered, +10.1 on V9-SC | V9-SC already blocks 59/89 bad Friday longs |
| OPEX week gate | -526.9 | OPEX weeks are our BEST weeks |

**Why:** Discord community trades discretionary structures (butterflies, verticals). Their caution about FOMC/Sidial/chop is valid for their style. Our automated contrarian setups (SC, DD) thrive in exact conditions they avoid.

---

## Discord Comparison #2 (Apr 1 2026, Mar 27-31 data)

**Last Discord sync: Mar 31, 2026**

Analyzed ~700 messages from #volland-daytrading-central (Mar 27-31). Cross-referenced expert calls against our 155 signals + V12-fix filter.

### V12-fix vs Discord Experts (Mar 27-31)

| Day | Unfiltered | V12-fix | Discord Bias |
|-----|-----------|---------|--------------|
| Mar 27 (selloff) | -181 | -19.6 (3 trades) | BEARISH (correct) |
| Mar 30 (selloff) | +118 | +66.5 (3 trades) | Mixed/bearish |
| Mar 31 (rally) | +34 | +32.2 (1 trade) | BULLISH (correct) |
| **Total** | **-29** | **+79 (7 trades)** | — |

V12-fix turns 155 signals → 7 trades, -29 → +79. Very efficient.

### Two Filter Ideas Tested — Both Rejected

**VIX Direction Modifier (allow longs when VIX crushing):**
- March data: CRUSH days +173 pts (97t, 60% WR) vs rise/flat -428 pts
- **REJECTED:** Based on EOD VIX (useless in real-time). Even "VIX open vs current" is unreliable — VIX can reverse midday. Mar 25 was a perfect example.

**ES Absorption both directions whitelist:**
- March data: +182.8 pts (319t, 54% WR) combined bullish+bearish
- **REJECTED:** 16 trades/day is too noisy. -95 MaxDD. Grading is inverted (C=60% WR, A+=0%). Needs grading v2 rewrite before practical. Bearish bias in March inflates bearish side.

### What WAS Actionable — 8 New Copilot Rules (R38-R45)

Added to `copilot_market_rules.md`:
- R38: Vanna vacuum = acceleration (Dark Matter)
- R39: Charm zero-crossing = afternoon pivot (Dark Matter)
- R40: DD sum extremes ($10B+ or -$5B) for macro bias (Apollo/Yahya Z)
- R41: Vol event near-miss still valid (Wizard)
- R42: CTA max positioning = selling exhaustion (Yahya Z)
- R43: Negative vanna cascade on breakout (BigBill)
- R44: Multi-day vol assessment > single-day SVB (jk23)
- R45: JPM collar now on CME ES options, invisible in SPX OI (Zack)

### ES Abs Grading Bug Identified

ES Absorption grading is INVERTED (same bug SC had before v2 rewrite):
- Grade C = 60% WR, +52.7 pts (BEST)
- Grade A+ = 0% WR, -33.5 pts (WORST)
**Action:** ES Abs grading v2 rewrite needed (future research task).

### Key Expert Profiles Updated

- **Dark Matter (Edge Extractor):** Publishes full pre-market trade plans with levels/setups. Nailed Mar 27 bearish, Mar 31 6525 ES target. Tracks vanna vacuum zones.
- **Apollo:** DD sum for bias, "don't fight government", called 6510 fade on Mar 31. 87% stated win rate on his fade strategy.
- **Yahya Z:** Real-time DD tracking ($17.5B on Mar 31), SqzMe analysis, CTA positioning, VIX term structure.
- **jk23:** Vol multi-day thesis, "vibes > data" close prediction (6530 on Mar 31 — correct).
- **BigBill:** Vanna cascade mechanics, VIX/3M/6M composite indicator (Apollo's, at 0.08 approaching 0.10 bullish threshold).
- **Wizard:** Vol event creator, "near-miss still valid", always hedged both ways with calendars.
