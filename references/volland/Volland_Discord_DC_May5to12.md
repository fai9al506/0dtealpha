# Volland Discord Daytrading Central — Intelligence Extract
### May 5, 2026 – May 12, 2026 (~7 trading days)

---

## Overview

| Metric | Value |
|--------|-------|
| Total messages | 4,641 (Daytrading Central) + 12 (0DTE Alerts) |
| Substantive key-author msgs | 495 |
| Tradeable keyword hits | 27 |
| Key authors | Apollo, Wizard of Ops, LordHelmet, Dark Matter, BigBill, Johannes, Phoenix, Yahya Z |

**Regime narrative (week summary):** Low-vol bull grind continues. Bears keep getting trapped ("never under" meme = lifestyle). 7300/7350/7400 successively defended as resistance THEN broken. Wizard + BigBill flagging caution signals (VIX warning, volatility tsunami) but no trigger yet. Apollo + Dark Matter trading scalp-shorts against magnets that ARE working as resistance in this regime, contrary to what Layer 1's GEX-magnet block assumes. **Mechanism inversion vs Feb-Apr is real for May.**

---

## Daily AM/PM briefings (from Apollo, #0dte-alerts channel)

| Date | Paradigm | Key levels | Vol context |
|---|---|---|---|
| May 4 AM | AG (messy) | 7255 upper / 7220/7200 lower | Vol elevated |
| May 4 PM | Shifted to BofA | 7230 upper / 7180-7150 lower | Vanna supportive |
| May 5 AM | BofA | 7250/7240 support / 7270 (weak) upper | Watch VIX |
| May 5 PM | BofA | 7250/7240 still / 7290-7300 higher order | Undervixing |
| May 6 AM | AG → could shift BofA | 7355 upper / 7255 lower edge | Overvixing, **negative vanna both weeks** |
| May 6 PM | Shifted BofA | 7355 upper / 7300 lower | Overvixing more |
| May 7 AM | DD-hedging positive | 7380/7385 resistance / 7360 core support | Undervixing, negative vanna across tenors |
| May 7 PM | BofA | 7350 upper / 7320/7310 (vanna support) | DD neutral |
| May 8 AM | GEX | 7380/7370 support / 7420 upper | Slightly overvixing, DD supportive |
| May 8 PM | GEX | 7400 big pivot / 7380 lower / 7420 upper | Overvixing |
| May 11 AM | GEX (a bit) | 7400/7390 support / 7425-7430 upper | Overvixing |
| May 11 PM | GEX (target hit) | 7425 pivot / 7420-7380 support / 7450 next | Still overvixing |

---

## Actionable insights (tradeable / system-relevant)

### 1. VIX/VIX3M ratio under 0.83 = "trim longs" signal
- **Apollo:** *"The typical signal is to Trim longs when under 0.83. The long short term Vol when its under 0.8"*
- **BigBill stats (since 12/31/18):** 296 instances of ratio ≤0.83 → avg next-10-day SPX return +0.26%. 18 instances <-3%, 15 instances >+3%. *"At a glance I am not noticing any easy trades."*
- **Implication:** regime warning, not setup. Useful as a context flag rather than an entry trigger.
- **Our system:** we have `overvix = VIX - VIX3M`. Discord uses **ratio (VIX/VIX3M)** — different. Worth adding ratio as secondary regime metric.

### 2. "Never under" paradigm (meme → real)
- Bull regime where 7300, 7350, 7400 all held first test then broke higher.
- Apollo: *"never under my short targets!"* (bearish counter — shorts hitting targets despite trend)
- Dark Matter (May 8): *"lol never under. That 7300 level is no joke. Nicely held. Round trip back."*
- **Mechanism:** key levels in bullish regime act as floor; tactical longs work, swing shorts fail.

### 3. "Backside short" — Apollo definition
- *"backside short being short against past highs"*
- *"if it is the backside short, then looking to break 7330 spx and retrade 7320 spx"*
- LordHelmet (May 11): *"i feel like we are in backside short scalp territory"*
- **System gap:** we don't tag this explicitly. Could be an SC short refinement — only fire SC short when price has retraced FROM a recent high AND is testing back toward it (failed retest).

### 4. JPM Q2 collar — 500 pts ITM on short calls
- Apollo (May 8): *"Heard we are shorting JPM since their collar is 500 pts ITM now on their short calls"*
- Confirms JPM collar still moving markets. Already in our `copilot_market_rules.md`. Worth re-reading rule (R-something) before next OPEX.

### 5. Negative Vanna across tenors (May 6-7)
- Apollo flagged it twice: *"negative vanna on the week now"*, *"we are negative vanna across tenors now so be cautious"*
- Mechanism: negative vanna = bearish risk asymmetry (vol up → spot down).
- **Our system:** we have `vanna_all`, `vanna_weekly`, `vanna_monthly`. Already used by SC long V14 rules. Worth audit: does our negative-vanna setup_log correlation match what Apollo highlights?

### 6. Wizard of Ops: VIX warning + 7350 resistance
- *"VIX is flashing warning signs."* (May 6)
- *"VIX is 5bps away from being green."* (overvixing breach about to happen)
- *"Usually historically (like Covid) it presages an insane drop. Covid happened this way, as did Tariffs."*
- *"This week is the last week anything looks bullish, and 7350 took even that away."*
- **Validated:** 7350 was confirmed resistance May 6-7 multiple tests.

### 7. Dark Matter (May 8): breadth divergence warning
- *"I do not like how breadth is forming - when that sell comes it will be quick and fast. Be ready."*
- *"Various tickers. Seeing more and more stocks selling from their 20day moving avg"*
- Breadth divergence as leading indicator — semis carrying index while broader market weakens.

### 8. BigBill: Volatility Tsunami signal (CAIA paper)
- *"https://caia.org/sites/default/files/forecasting_a_volatility_tsunami.pdf"*
- *"TLDR: stability creates instability."*
- *"Nope... volatility tsunami signal has me cautious."* (May 11)
- **Action:** read paper, evaluate as add-on regime filter.

### 9. Apollo on dealer mechanics (May 8 swing-trading deep dive)
- *"The big turning point in indexes likely come when Dealers hold negative Gamma"*
- *"Negative Gamma coming from customers selling ITM calls"*
- *"Dealers do hold a good Long Vega position where a lot are ITM.... they are well compensated on this Rvol higher"*
- **Implication:** current regime sustained because dealers are net long Vega. Watch for the flip to negative gamma.

### 10. Apollo (May 11): flow divergence warning
- *"Flows didn't shift with this move higher which is sort of odd"*
- *"Q's and RSP havn't made new highs"*
- *"Semis just traded a new high tick"*
- Narrow-breadth bull move — vulnerable to reversal once semis correct.

### 11. Apollo (May 11): "Over 7416 you are back in normal GEX"
- LIS-zone vs normal-GEX paradigm switch at a specific level
- Confirms LIS as paradigm-defining level (already in our system)

### 12. Apollo (May 11): liquidity-vacuum / iceberg reading
- *"Funny how ES just did 25% of the overnight volume in 8 minutes"*
- *"3k limit seller at 7430 es lol"*
- *"108 orders in the queue at 7300"* (May 5)
- Order-book reading critical at key levels (we don't capture this — Sierra Chart MD has the data but we don't extract level-of-book counts)

### 13. "Tops are much harder than bottoms" (Apollo)
- Strategic timing wisdom. Bear-side reversals require multiple confirmations vs single-touch bottom bounces.

---

## Cross-reference to our V14 over-restriction question

Today's V14 audit found that Layer 1 GEX/DD magnet block over-rejects SC shorts in GEX-PURE during May. The Discord chatter independently confirms why:

| Our finding | Discord confirmation |
|---|---|
| May SC shorts at high gex_above had 75% WR | Apollo + Dark Matter explicitly take backside shorts against magnets in this regime |
| Feb-Apr same combo was 14% WR | Discord doesn't go back that far in this extract, but "tops are much harder than bottoms" + "Cant wait for -3% down day" implies normal regime is different |
| Layer 1 mechanism premise (magnet pulls price up) | Wizard says 7350 was strong resistance — magnets DID work as resistance, not magnets |
| Don't ship the carveout yet | "VIX warning" + "volatility tsunami" + "breadth weakening" — Apollo/Wizard/BigBill all flag regime change coming. If/when it does, Layer 1's premise re-activates. |

**Verdict reinforced:** HOLD V14 Layer 1 unchanged. May is a specific regime ("never under" / dealer-long-vega) that Discord pros explicitly identify as anomalous and expected to flip. Shipping the carveout now would over-fit to a regime that's flagged as ending.

---

## New idea backlog (for separate Tasks.md entries)

1. **VIX/VIX3M ratio (0.83 threshold)** — add as secondary regime metric alongside existing `overvix` (which is the difference, not ratio).
2. **Volatility Tsunami signal** — read CAIA paper, evaluate as regime filter.
3. **"Backside short" SC refinement** — only fire SC short when price has retraced from a recent high and is testing back toward failed-retest.
4. **Breadth divergence detector** — semis-vs-broader-market RS gap as bearish regime warning.
5. **Dealer gamma sign flip detector** — current regime sustained by long-vega dealers; track this for top warning.

---

## Process note

This is the second incremental Discord study post-master-extract (Nov-Feb). Coverage ledger maintained in **Tasks.md S103**. Next export window: **May 12+ → next session**.
