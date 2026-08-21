# S312 — How should "the previous day fell" be measured?

**Date:** 2026-08-19 · **Verdict: KEEP V21 EXACTLY AS IT IS. Every proposed alternative is worse.**

Raised by the user after three Skew Charm shorts stopped out into a bounce (−$420).

## The question

V21 blocks shorts when **the previous session's OPEN→CLOSE was worse than −0.8%** and VIX < 24.

The user's objection is logically sound: *a market can gap down 5%, sit flat all day, and close
where it opened.* Open→close reads ≈0% and the rule sees "quiet day", even though the market fell
hard. The move every chart quotes — TradingView and everyone else — is **CLOSE→CLOSE**.

For 2026-08-18 the two measures disagree by more than a factor of ten:

| measure | value |
|---|---|
| open→close (what V21 uses) | **−0.06%** |
| **close→close (what a chart shows)** | **−0.69%** |
| overnight gap | −0.63% |

## Method

Same replay that shipped V21 (`_tmp_s301_relief_rule.py`): **V20 + cap 2 long / 3 short + 90s
dedup + S203 underwater guard + $300 daily breaker + basket sizing `max(qty,2)`**, haircut
0.6 pt/contract and $1.92/contract round-turn charged **inside** the sim. `spx_ohlc_1m` only.
Window 2026-03-01 → 2026-08-18, **119 sessions, 868 baseline trades**. Today excluded (partial).
Scripts: `_tmp_s312_closeclose.py`, `_tmp_s312b_verify.py`, `_tmp_s312c_cumulative.py`.

## Result 1 — every alternative measure is worse

| rule | trades | $/mo | min month | MaxDD | **LOMO** |
|---|---|---|---|---|---|
| V20 (no rule at all) | 868 | +2,151 | −225 | −1,585 | — |
| **V21 SHIPPED — open→close < −0.8%** | **861** | **+2,332** | **+530** | **−906** | **6/6** ✅ |
| close→close < −0.60% | 820 | +2,149 | +412 | −1,033 | 3/6 ❌ |
| close→close < −0.80% | 831 | +2,146 | +327 | −1,033 | — |
| close→close < −1.20% | 847 | +2,252 | +327 | −1,033 | 5/6 ❌ |
| open→close < −0.8% **OR** close→close < −0.6% | 820 | +2,149 | +412 | −1,033 | 3/6 ❌ |
| 2-day cumulative < −1.0% | 824 | +2,118 | +111 | −1,249 | 3/6 ❌ |
| 3-day cumulative < −1.5% | 852 | +2,239 | +194 | −1,167 | 4/6 ❌ |
| close vs 5-day average < −1.5% | 872 | +2,284 | +530 | −906 | 6/6 ✅ |

**V21 as shipped wins on every column.** The only variant that also passes leave-one-month-out
(distance below a 5-day average) earns **$48/mo less** for the same drawdown.

The close→close versions do not merely underperform — they **lose money in specific months**
(July −$514, May −$198, March −$62). A rule that hurts 3 months in 6 does not survive forward.

## Result 2 — the mechanism: open→close is not measuring size, it is measuring *unfinished selling*

This is why the "wrong" measure wins. Raw short performance, VIX < 24, no cap (354 shorts):

**By previous session OPEN→CLOSE — separates cleanly:**

| previous open→close | n | pt/trade | WR |
|---|---|---|---|
| **worse than −1.0%** | 24 | **−5.18** | 38% |
| −1.0% .. −0.8% | 3 | **−7.50** | 33% |
| −0.8% .. −0.5% | 34 | +1.15 | 68% |
| −0.5% .. 0% | 73 | +4.89 | 73% |
| 0% .. +0.5% | 154 | +4.57 | 72% |

**By previous session CLOSE→CLOSE — does not separate:**

| previous close→close | n | pt/trade | WR |
|---|---|---|---|
| worse than −1.0% | 45 | **+0.41** | 53% |
| −1.0% .. −0.8% | 10 | **+4.41** | 90% |
| −0.8% .. −0.6% | 11 | +0.71 | 64% |
| −0.3% .. 0% | 58 | +3.65 | 66% |
| above +0.5% | 114 | +4.47 | 70% |

Shorts after a −1% close→close day still **make** money (+0.41), and the −1.0..−0.8 bucket is one
of the best in the whole table (+4.41 at 90% WR). Close→close simply does not sort the outcomes.

**The reason:** open→close does not measure *how far* the market fell. It measures **whether the
selling was still going on at the close**. A session that grinds down all day ends with sellers
still in control, and the next day is a relief bounce that runs over our fade shorts. **An
overnight gap is already priced by the time we trade.**

## Result 3 — the "gap down then flat" idea is refuted, and it is backwards

The user's second idea: *a deep gap down followed by a flat session signals a bullish next day, so
block shorts.* Tested directly:

| previous session | n | pt/trade | WR | total pt |
|---|---|---|---|---|
| **gapped down >0.5% then went FLAT (\|open→close\|<0.3%)** | 31 | **+5.75** | **77%** | +178 |
| every other short | 323 | +3.10 | 67% | +1,001 |

**Those are our single best shorts.** Blocking them costs money — as a rule it produced
**$1,995/mo with min month −$427 and MaxDD −$1,787**, worse than doing nothing on all three.

Same story in the overnight gap alone: after a gap **below −0.5%**, shorts run **+4.57 pt at 75% WR**.

**A gap down that then holds is a market that has finished selling. That is a good tape for a
fade book, not a warning.**

## Result 4 — random control

V21 blocks only 7 trades in 119 sessions, so its edge must beat chance.

| | $/mo |
|---|---|
| V20 baseline | +2,151 |
| **V21 — the real 7 blocked** | **+2,332** |
| 7 **random** shorts blocked (400 trials) | +2,136 (sd 39) |

**0 of 400 random trials matched it — p = 0.000.**

## Honest weaknesses of V21

1. **It blocks only 7 trades.** Tiny sample.
2. **73% of its benefit is June** (+$754 of ~+$1,029 total). April +$127, July +$148, and it does
   nothing in March, May, August.
3. What saves it is that it **never hurts a single month** (LOMO 6/6) — it is insurance, not income.
   That property is exactly what the close→close variants lack.

## What this says about today (2026-08-19)

Yesterday gapped down 0.63% and then went flat: **that is precisely the +5.75 pt / 77% WR bucket.**
Historically the best possible condition for our shorts. Today all three lost. **One day from a
good bucket is variance, not a broken rule.** No filter change is justified by it.

## Decision

**No change. V21 stays. `LIVE_VER` stays `v21-sb`. No V22.**

Closed lines — do not re-test without new data:
- close→close as the measure (worse, fails LOMO)
- 2-day and 3-day cumulative decline (worse, fails LOMO)
- gap-down-then-flat as a short block (**refuted — it is our best bucket**)
- OR-combinations of open→close with close→close (worse than open→close alone)

Still open: **distance below a 5-day average** is the only alternative that passed LOMO 6/6. It
earns $48/mo less than V21 today, but it is a genuinely different signal and worth re-testing when
the sample is bigger.
