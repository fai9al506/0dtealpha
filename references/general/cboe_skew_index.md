# CBOE SKEW Index — white paper (Jan 2011, 20 pp)

Read 2026-08-14. Source: `C:/Users/Faisa/Downloads/SKEWwhitepaperjan2011.pdf`.

**Why it matters to us:** we are about to sell put spreads on SPX (GEX v7, S251). Natenberg told us
*why* the put side is rich — the left tail is real. **SKEW is the number that says how rich, and how
real, on any given day.** And it is a free public index.

---

## What SKEW actually is

- `SKEW = 100 − 10 × S`, where **S is the risk-neutral skewness of the 30-day SPX log-return**,
  computed from a portfolio of **out-of-the-money SPX options** — same style of construction as VIX.
- **SKEW = 100 means a normal distribution** (no tail bias). Higher = fatter, more negative left tail.
- Historical range 1990–2010: **min 101, max 147.** ~78% of days sit between 110 and 125.

> *"VIX captures the first layer of perceived risk, as it tells how far on average the S&P 500
> log-return is likely to stray on either side of its mean… Once this is gauged, **SKEW catches the
> additional layer of risk implied by the left tail** of the distribution."*

**VIX = how big the move. SKEW = how bad the worst case.** Two different questions.

## 🔑 The table that makes it usable

Risk-adjusted probability that the 30-day SPX log-return falls 2 or 3 standard deviations **below**
the mean:

| SKEW | P(−2 SD) | P(−3 SD) |
|---|---|---|
| 100 | 2.30% | 0.15% |
| 105 | 3.65% | 0.45% |
| 110 | 5.00% | 0.74% |
| 115 | 6.35% | 1.04% |
| 120 | 7.70% | 1.33% |
| 125 | 9.05% | 1.63% |
| 130 | 10.40% | 1.92% |
| 135 | 11.75% | 2.22% |
| 140 | 13.10% | 2.51% |
| **145** | **14.45%** | **2.81%** |

From SKEW 100 → 145 the odds of a 2-SD drop go **2.3% → 14.45%**, a **6× increase**, while VIX may
not move at all. The size of one SD comes from VIX; SKEW tells you how much extra weight sits in the
left tail.

## 🚨 The finding that matters most: SKEW is INDEPENDENT of VIX

> *"high values of SKEW occur in conjunction with **both low or high values of VIX**."*

This is the whole reason to add it. A quiet market is **not** automatically a safe market for a put
seller — tail risk can be elevated while VIX is asleep. We currently have **no measure of this at
all**; every regime input we own (VIX, VIX3M, overvix, spot-vol beta) is about the size of moves,
not the shape of the tail.

There is also a real quirk worth knowing:

> *"the **upper bound of SKEW decreases as VIX rises to extreme values above 40.** The probable
> reason is that VIX surges during periods of crashing stock prices, when a repeat crash may not be
> viewed as that likely."*

So after a crash, tail risk is priced *lower*, not higher. SKEW is not simply "fear".

## 📉 SKEW moves slowly

30-day realised volatility: **SKEW 110 vs VIX 372.** Average absolute daily change is **1.76 points
(1.49%)**. It is a slow, regime-like variable — good as a *gate*, not as a trigger.

---

# 🛑 USER DECISION 2026-08-14 — SWING ONLY, NOT 0DTE

The user cut this correctly on two points, and they were both right:

1. **"I think it helps swing trades, for 0dte it's same."** SKEW is a **30-day** measure. Using it
   to gate a same-day trade is applying a monthly number to a 6-hour hold. Keep it for the swing
   book (S257-B / S258), drop it from the 0DTE and v7 plans.
2. **"We should trade based on risk and gain… a spread of 10 points with 4 credit, it's risk
   limited, no need to complicated."** Correct, and it is the better rule — see the box below.

## ✅ THE ACTUAL DECISION RULE FOR A DEFINED-RISK SPREAD

A credit spread's whole economics are in two numbers: **width** and **credit**.

```
break-even win rate = (width − credit) / width
```

| Width | Credit | Max gain | Max loss | Break-even WR | v7 measured 81.2% |
|---|---|---|---|---|---|
| 10 | **4** | 4 | 6 | **60%** | ✅ 21 pts of margin |
| 10 | 3 | 3 | 7 | 70% | ⚠️ 11 pts |
| 10 | **2** | 2 | 8 | **80%** | 🛑 no margin at all |

**The check before every spread is "is the credit big enough today?", not "what is SKEW?"** If the
credit collapses, the break-even rate climbs to meet our win rate and the edge is gone — regardless
of what any volatility index says.

This is also the cleanest expression of Natenberg's IV rule: **rich options = fat credit = low
break-even**. The credit already contains the information.

---

# For the SWING book only — what SKEW could do

## 1. The 2-D gate ⭐ (swing horizon, not 0DTE)

Combine with Natenberg's rule (`reference_natenberg_credit_spread_rules`, and
`references/general/natenberg_option_volatility_pricing.md`). His rule says *sell the ATM only when
IV is high*. SKEW adds the second axis:

| | **SKEW low** (tail cheap) | **SKEW high** (tail expensive/real) |
|---|---|---|
| **VIX high** (options rich) | ✅ **Best case to sell the put spread** — paid well, tail not unusually fat | ⚠️ Paid well but you are short a genuinely fat tail. Narrow the width or size down. |
| **VIX low** (options cheap) | Poor pay. Natenberg says buy the ATM call instead (debit spread). | 🛑 **WORST CASE — do not sell puts.** Cheap premium *and* elevated crash odds. |

**The bottom-right cell is the one that can hurt us**, and it is invisible with VIX alone. This is a
testable gate, not a theory — we can backfill SKEW and re-run the S251 backtest against it.

## 2. It gives our short strike an actual probability

Our spread has a short put some distance below spot. VIX gives the 1-SD size (and Natenberg's
"gamma rent" says break-even = a 1-SD move). SKEW then gives the **risk-adjusted probability of
breaching 2 or 3 SD** — i.e. the odds of the loss scenario, not just its size.

## 3. Compare "what we get paid" vs "what the tail is worth"

Two numbers we can put side by side per signal:
- premium collected on the spread (we already price this off the chain), and
- the market's own tail-risk reading (SKEW).

If premium is falling while SKEW is rising, we are being paid **less** to take **more** risk. That is
a clean, mechanical stop condition for the options version of v7.

---

## Getting the data

- CBOE publishes SKEW daily, plus a **term structure** of SKEW.
- **To check after the close:** does TradeStation serve it as an index symbol (likely `$SKEW.X`, the
  same pattern as `$VIX.X` / `$VIX3M.X` which we already pull)? If yes it is a one-line addition to
  the existing VIX pull.
- If not, we can compute our own version — we store the full OTM SPX chain every 30 s, which is the
  same input the index uses. The appendix of the white paper has the full portfolio derivation and a
  worked numerical example.
- Related but different: Natenberg's simpler skew measure `IV(25Δ put) − IV(25Δ call)`. That is a
  *slope* at two strikes; SKEW is a *global, strike-independent* measure of the whole curve. Compute
  both — the cheap one today, the index one when we have it.

## Caution

- The probabilities in the table are **risk-neutral**, i.e. what the market is *pricing*, not a
  forecast of what will happen. They include a risk premium.
- The paper is from 2011 and predates 0DTE. SKEW is a **30-day** measure. For a same-day trade it is
  a *regime* input, not a same-day probability.
- 20.47% of history sits in the single bucket 115–117.5, so "normal" is a narrow band. Judge SKEW by
  its percentile, not by whether it is above 100.
