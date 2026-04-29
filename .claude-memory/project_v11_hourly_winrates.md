---
name: V11 Hourly Win Rate Analysis
description: V11 filter win rates by trading hour — 351 trades, best hours 9:30-11AM, shorts decay afternoon, longs improve afternoon
type: project
---

## V11 Filter — Win Rate by Trading Hour (as of Mar 24, 2026)

**Dataset:** 351 trades, 230 wins, 65.5% WR, +1,414.3 pts total.
**Date range:** Feb 4 – Mar 23, 2026.

### Overall Hourly Breakdown

| Hour (ET) | Trades | W | L | E | WR | PnL | Avg PnL |
|---|---|---|---|---|---|---|---|
| 09:30-10:00 | 23 | 17 | 6 | 0 | 73.9% | +84.7 | +3.7 |
| 10:00-11:00 | 84 | 61 | 23 | 0 | 72.6% | +497.1 | +5.9 |
| 11:00-12:00 | 67 | 42 | 22 | 3 | 62.7% | +194.4 | +2.9 |
| 12:00-13:00 | 64 | 41 | 15 | 8 | 64.1% | +197.3 | +3.1 |
| 13:00-14:00 | 54 | 34 | 12 | 8 | 63.0% | +234.7 | +4.3 |
| 14:00-15:00 | 32 | 20 | 7 | 5 | 62.5% | +103.1 | +3.2 |
| 15:00-16:00 | 27 | 15 | 5 | 7 | 55.6% | +103.1 | +3.8 |

### Key Findings

- **Golden hour: 10:00-11:00** — highest volume (84t), best PnL (+497), 72.6% WR. ~35% of all profits.
- **Morning 9:30-11:00 is best window:** 107 trades, 72.9% WR, +582 pts.
- **After 11 AM:** WR drops to low 60s, stays there through 15:00.
- **15:00-16:00 is weakest:** 55.6% WR, highest EXPIRED count.

### By Setup

- **Skew Charm (130t):** Peaks 10-11 (84.4% WR, +254 pts) and 13-14 (81.8%). Solid all hours.
- **DD Exhaustion (141t):** Best 10-12 (~63% WR), sharp decay after 13:00 (41.7%). Supports moving dd_market_end 15:30→14:00.
- **AG Short (61t):** Best first hour (75% WR), weak midday 12-13 (37.5%).
- **ES Absorption (12t):** Small sample, 80% WR at 10-11.

### By Direction

- **Longs (110t):** Improve through the day — 62.5% at open → 100% at 15:00-16:00 (8/8 wins). Strong 13:00+ afternoon.
- **Shorts (229t):** Opposite pattern — 80% at open → 26.7% at 15:00-16:00. Morning is the edge.

**Why:** Informs time-of-day filter tuning and optimal trading windows.
**How to apply:** Prioritize morning signals (9:30-11:00), especially shorts. Afternoon longs are safe. Afternoon shorts (15:00+) are the weakest link — already gated by V11 for SC/DD but AG Short still passes.
