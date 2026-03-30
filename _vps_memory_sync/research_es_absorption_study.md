---
name: ES Absorption Deep Study — Mar 28 2026
description: 305 trades analyzed. Grading is ANTI-PREDICTIVE (C=62% WR, A+=0%). Best filter: C+LOG only + vol<3 = 61% WR +155 PnL. Volume 2-3x sweet spot. 14:30-15:30 dead zone. First 3 signals toxic.
type: project
---

# ES Absorption Deep Study (Mar 28 2026)

305 resolved trades, Feb 19 - Mar 27 (23 trading days).

## Key Finding: Grading v2 is ANTI-PREDICTIVE

| Grade | Trades | WR | PnL | MaxDD |
|-------|--------|-----|-----|-------|
| A+ | 3 | 0% | -33.5 | -33.5 |
| A | 48 | 42% | +7.8 | -55.6 |
| B | 157 | 43% | -109.3 | -147.9 |
| **C** | **71** | **62%** | **+82.3** | **-12.0** |
| LOG | 26 | 50% | +64.8 | -4.2 |

WHY: Absorption is contrarian. Low-grade signals = CVD diverges DESPITE unfavorable Greeks = volume overriding positioning = stronger signal.

## Best Filters

| Filter | Trades | WR | PnL | MaxDD |
|--------|--------|-----|-----|-------|
| C+LOG + vol<3 | 88 | 61.4% | +154.9 | -12.0 |
| Bull C grade only | 45 | 66.7% | +78.5 | 0.0 |
| Vol 2.0-3.0 + 10:30-14:30 | 80 | 57.5% | +113.0 | -36.0 |

## Three Immediate Filters (any option)
1. Volume ratio: raise min 1.5x→2.0x, cap at 3.0x (above 3x = panic, 29% WR)
2. 14:30-15:30 dead zone: block (38.9% WR, -73.2 PnL)
3. Skip first 3 signals/day: warmup needed (first 3 = 38.5% WR, -141 PnL)

## Direction
- Bullish: 150t, 50.7% WR, +40.9 PnL — clearly better
- Bearish: 155t, 43.9% WR, -28.8 PnL — net negative

## VIX Sweet Spot
VIX 25-30 = best range (166t, 51% WR, +105 PnL). VIX 20-25 is worst (-107 PnL). V12 VIX gate (<=22) confines to worst range.

## Recommended Path
Option A (conservative): Enable bullish longs, align>=2, VIX-exempt like SC, block 14:30-15:30.
Option B (data-driven): Reverse grade gate (C+LOG only), both directions, vol<3, block 14:30-15:30.
Start with A on SIM, validate B on fresh data.
