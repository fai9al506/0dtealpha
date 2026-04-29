# Greek Filter — Deep Analysis (Mar 7, 2026)

Comprehensive analysis of Greek alignment filter performance after $605.70 eval_trader loss on Mar 5.

## Why the Loss Happened (3 factors, NOT filter quality)

1. **Serial execution model**: eval_trader captured only 4 of 13 Railway signals. Railway (independent tracking) showed +41 pts with current filter on same day.
2. **max_losses_per_day=3**: Stopped trading after 3 early losses. Missed 9 subsequent signals (many winners).
3. **ES vs SPX price divergence**: Same signal was WIN on Railway (SPX-based) but LOSS on eval_trader (MES-based). 15-20pt variable spread.

## Mar 5 Crash Day — Why Greeks Failed

- Price dropped ~45 pts (5780→5735)
- Greeks stayed bullish ALL DAY: charm positive, vanna +3.5B (never turned negative), GEX floor supporting
- Alignment = +3 (max bullish) for every signal
- Simple filter let ALL longs through, blocked ALL shorts → 100% directional portfolio into crash
- This was UNPRECEDENTED in the backtest sample — every prior selloff day had Greeks flip bearish

## Backtest Results (323 trades, Feb 3 - Mar 5)

| Rank | Filter | PnL | MaxDD | Sharpe | Composite |
|------|--------|-----|-------|--------|-----------|
| 1 | HYBRID | +571.8 | 19.7 | 0.695 | 20.17 |
| 2 | SVB-REGIME | +657.5 | 30.1 | 0.684 | 14.94 |
| 3 | OPTIMAL-v1 | +760.2 | 35.0 | 0.676 | 14.67 |
| 4 | VANNA-ONLY | +693.7 | 26.7 | 0.558 | 14.51 |
| 5 | EXTREME-ONLY | +707.1 | 30.3 | 0.607 | 14.16 |
| 6 | CHARM-ONLY | +621.1 | 29.1 | 0.586 | 12.50 |
| 7 | PARADIGM-v2 | +633.9 | 35.1 | 0.592 | 10.62 |
| 8 | CURRENT | +682.9 | 46.5 | 0.662 | 9.73 |
| 9 | PARADIGM-ONLY | +552.1 | 23.1 | 0.413 | 9.88 |
| 10 | MOMENTUM-ONLY | +530.5 | 77.5 | 0.488 | 3.34 |
| 11 | BASELINE | +520.9 | 86.2 | 0.467 | 2.82 |

Composite = (PnL / MaxDD) * Sharpe — balances return, risk, and consistency.

## SVB Regime Analysis

| SVB Range | Aligned WR | Opposed WR | Delta | Filter Value |
|-----------|-----------|------------|-------|-------------|
| < -0.5 | 72.5% | 25.0% | +47.5% | STRONG |
| -0.5 to 0 | 55.0% | 40.0% | +15.0% | MODERATE |
| 0 to 0.3 | 50.0% | 48.0% | +2.0% | NONE |
| > 0.3 | 58.0% | 35.0% | +23.0% | GOOD |

Key insight: SVB < 0.2 is the "danger zone" where vanna becomes unreliable.

## Vanna Inversion Theory — DISPROVEN

Hypothesis: Positive vanna becomes bearish accelerant during sharp selloffs (dealers sell delta as vol rises).
Data: Longs with vanna > 1B AND price falling > 10pts still won 85.7% in historical data.
Conclusion: Vanna sign remains directionally valid even during selloffs. The theory is wrong.

## HYBRID Filter Design (Deployed)

Three layers applied sequentially to the 3-component alignment score (-3 to +3):

1. **SVB-aware vanna removal**: When SVB < 0.2, compute vanna's contribution to alignment and subtract it. Vanna unreliable in vol-stressed/transitional regimes.

2. **Paradigm cross-check**: If Volland paradigm contradicts trade direction, subtract 1 from effective alignment.
   - Bearish paradigms: AG-PURE, AG-LIS, AG-TARGET, SIDIAL-EXTREME, SIDIAL-MESSY, GEX-LIS, GEX-TARGET
   - Bullish paradigms: GEX-PURE, BOFA-PURE, BOFA-LIS, BOFA-TARGET, SIDIAL-BALANCE

3. **Momentum override**: If spot has moved 15+ pts from day's first spot in the trade's direction, override a negative effective alignment (let the trade through).

Final gate: `effective_alignment < 0 AND NOT momentum_override → BLOCK`

## DD Exhaustion Special Case

Block DD signals when SVB is weak-negative (-0.5 to 0). Separate from main HYBRID filter.

## Alignment Discrepancy (ES Absorption Bug)

`db_volland_stats()` was missing `aggregatedCharm` extraction → ES Absorption computed alignment with only 2 of 3 components (charm vote always 0). Fixed in commit `70cc518`.

There's also a secondary timing discrepancy: Railway uses latest snapshot, eval_trader uses nearest-to-signal-time snapshot. This is inherent and not fixable without refactoring, but typically causes only ~1pt difference.

## Key Takeaways for Future

1. Greek filter works well EXCEPT when Greeks don't flip during crashes (rare but catastrophic)
2. HYBRID's paradigm cross-check catches this: paradigm was AG-TARGET (bearish) while Greeks said bullish
3. Serial execution model is the real ceiling — eval_trader sees 30% of Railway signals
4. max_losses_per_day=3 is too aggressive — consider 5
5. SVB regime awareness is valuable — vanna unreliable when SVB < 0.2
