---
name: Charm S/R limit entry distance analysis
description: Backtest of 81 charm S/R trades (Mar 12-19) — distance from market predicts outcome. Sweet spot 3-15 pts. Near-market (<3pt) and far (>15pt) entries are losers.
type: project
---

## Charm S/R Limit Entry — Distance Analysis (Mar 12-19, 2026)

**Dataset:** 81 resolved short trades with charm_limit_entry set.
**Overall:** 45W / 25L / 11EXP = 55.6% WR, +189.1 pts total.

### Distance from Market = Quality Filter

| Distance Bucket | Trades | WR | PnL | Avg PnL | Verdict |
|---|---|---|---|---|---|
| 0-3 pts (near-market) | 13 | 30.8% | +13.9 | +1.1 | Terrible — fills too easily, no edge |
| 3-5 pts | 10 | 70.0% | +42.4 | +4.2 | Strong |
| 5-10 pts | 28 | 71.4% | +106.6 | +3.8 | Best bucket — bulk of profits |
| 10-15 pts | 19 | 63.2% | +41.4 | +2.2 | Still positive |
| >15 pts | 7 | 14.3% | -30.0 | -4.3 | Negative — skip |

### Sweet Spot: 3-15 pts distance + morning (before 13:00 ET)
**35 trades | 74.3% WR | +183.0 pts**

### Time of Day
| Window | Trades | WR | PnL |
|---|---|---|---|
| 9:30-11:00 | 21 | 81.0% | +144.0 |
| 11:00-13:00 | 29 | 48.3% | +26.8 |
| 13:00-14:30 | 15 | 40.0% | -48.7 |
| 14:30-16:00 | 16 | 50.0% | +67.0 |

### Paradigm Breakdown
- **GEX-LIS**: 37.5% WR, -41.6 pts (counter-trend shorts in bullish paradigm — block?)
- **AG-TARGET**: 80.0% WR, +58.9 pts (best)
- **BOFA-PURE**: 41.7% WR, +6.1 pts (marginal)

### Pending Implementation
1. **Min distance 3 pts** — skip charm limits within 3 pts of market
2. **Max distance 15 pts** — skip charm limits >15 pts from market
3. **Block GEX-LIS paradigm** on short charm entries
4. **Morning bias** — consider blocking 13:00-14:30 window
5. **Timeout stays 30 min** — expired trades wouldn't have been profitable anyway
6. **Re-validation on fill** — for entries >15 min old, re-check paradigm/charm (deferred)

**Why:** Distance acts as a quality filter. Near-market fills (0-3pt) have no edge because charm resistance isn't meaningful at that proximity. Far fills (>15pt) indicate conditions shifted during the long wait.

**How to apply:** Implement distance filter in eval_trader's open_trade() charm S/R logic. Check `abs(charm_limit - es_price)` before placing limit.
