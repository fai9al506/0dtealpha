---
name: Discord Analysis Results
description: Volland Discord daytrading + 0DTE alerts analysis — all 5 filter recommendations rejected by backtest
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

**How to apply:** Do NOT implement any of these filters. V9-SC is well-optimized for our system. The only remaining investigation is BOFA-MESSY + GEX-LIS paradigm blocking (+106 pts, 65 trades at 43% WR) — small sample, needs more data. Also need bullish regime to test GEX Long properly.
