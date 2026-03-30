---
name: DD Per-Strike Flip Mechanics Research — Mar 28 2026
description: Confirmed DD flips are MECHANICAL (strike crossings flip DD by $2-4B each). Mar 27 had 35 DD flips within ±10pts. Hair trigger gradient >$500M/pt. Data already in volland_exposure_points.
type: project
---

# DD Per-Strike Flip Mechanics

## The Mechanism (confirmed from volland_exposure_points data)
When spot crosses a strike, the DD contribution at that strike FLIPS SIGN. This is a 2x swing.

Example: Strike 6415 on Mar 27:
- Spot ABOVE 6415: DD = -$1.33B
- Spot BELOW 6415: DD = +$1.39B
- Total swing: $2.71 BILLION from ONE strike crossing

## Mar 27 Key Strikes
| Strike | DD Value | Flip Risk (2x) |
|--------|----------|-----------------|
| 6400 | -$1.28B | $2.56B |
| 6415 | -$1.57B | $3.13B |
| 6420 | +$1.92B | $3.85B |

## Flip Count Mar 27
- DD within ±10 pts: **35 sign flips** in one session
- DD within ±20 pts: 24 sign flips
- DD within ±30 pts: more stable

## Hair Trigger Gradient
Gradient = DD change per point of spot movement:
- Normal: <$200M/pt (DD stable)
- High: $200-500M/pt (DD sensitive)
- Hair trigger: >$500M/pt (DD flips on every tick)
Mar 27 hit $0.80B/pt at 15:39 ET — extreme noise zone.

## Backtest Results (V2, timezone-corrected Mar 28)

334 DD Exhaustion signals tested. Classified by whether spot crossed a heavy DD strike:

| Category | Trades | WR | PnL |
|----------|--------|-----|-----|
| Mechanical (flip >$1B) | 60 | 42% | -20.5 |
| Flow-based (no heavy crossing) | 273 | 44% | +214.5 |
| Extreme mechanical (>$3B) | 12 | 25% | -37.1 |
| Extreme mechanical (>$5B) | 4 | 0% | -37.1 |

**Key finding:** Only EXTREME mechanical flips (>$3B, 12 trades) are clearly toxic. Moderate flips are OK. The bulk of DD Exhaustion profit comes from flow-based signals. Blocking extreme flips saves +37 PnL — helpful but modest.

**Original V1 backtest was WRONG** due to timezone bug (used +3h instead of proper UTC). V2 uses raw UTC timestamps throughout.

**Bad DD days are NOT explained by mechanical flips** — they're macro/trend days:
- Mar 13: -90.4 PnL (flow, 31% WR)
- Mar 12: -83.2 PnL (flow, 20% WR)
- Feb 25: -68.5 PnL (flow, 0% WR)

## Status: PARKED
User will self-learn DD mechanics before implementing changes. Per-strike data is useful for extreme flip detection (>$3B) but not a game-changer (+37 PnL). Revisit when user has deeper DD knowledge.

## Implementation (for later)
1. Query volland_exposure_points for deltaDecay/TODAY near spot (±15 pts)
2. Calculate flip_risk = 2 × |DD_value| for each nearby strike
3. Only block DD Exhaustion when crossed strike flip > $3B (12 trades, 0-25% WR)
4. Calculate gradient for Copilot context (hair trigger zones)

## Source
- Hunter/Apollo (Volland video): "0DTE Delta Decay - Intraday Dealer Hedging Quantified"
- Hunter = Apollo in Discord (Volland co-founder)
- TheEdge (Discord Mar 27 13:41): "flipping 638, 639, 640 is massive for DD"
- Johannes (beginners-chatter): "aggregated values matter when they change because a strike is crossed"
- Confirmed by actual volland_exposure_points data analysis
