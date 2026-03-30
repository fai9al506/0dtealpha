---
name: VX is a leading signal, not a filter
description: VX futures flow should be read as per-tick leading indicator (buyer/seller clusters at key levels), NOT as aggregate regime filter. User corrected approach after wrong analysis.
type: feedback
---

VX flow value is in per-tick/per-bar flow shifts (Apollo's "worm"), NOT aggregate delta direction.

**Why:** 31-day backtest showed VX as aggregate filter is ACTIVELY HARMFUL (ALIGNED 44% WR, AGAINST 56% WR). Our setups are contrarian — they profit when institutional VX flow is wrong. But at the micro level, buyer/seller CLUSTERS at key VX price levels DO predict turns (e.g., 9:38 buyer cluster at VX low preceded 52-pt SPX drop).

**How to apply:** When building VX integration, focus on: (1) large print detection per tick, (2) flow shifts (seller->buyer transitions), (3) price reaction to buying/selling pressure at key levels. Do NOT aggregate into 30-min regime buckets. Do NOT add to `_passes_live_filter()`. Present as visual/context signal on portal, not mechanical filter.
