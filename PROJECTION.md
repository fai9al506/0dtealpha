# Monthly Projection Ledger

**Purpose.** One place that answers "what do we expect to earn per month, and what changed it?"
Every setup added, filter refined, or sizing/cap change gets a row here **with its measured
before/after** — so a projection is never a vibe, and we can see whether shipped changes actually
moved the curve.

**Always quote monthly figures in BOTH $ and SAR (peg x3.75).** See memory
`feedback_projections_in_usd_and_sar`.

**Two numbers, always kept apart:**
- **Theoretical** — simulated on the current config over a stated window (chain outcomes).
- **Actual** — broker truth from `tsrt_daily_stmt`. This is the only number that counts.

---

## Rules for updating this file (MANDATORY)

1. **On every change that can move P&L** (new setup, filter edit, cap change, sizing change,
   setup disabled): add a row to *Change Log* with the measured Δ, the window it was measured on,
   and the number of sessions. No row = the change didn't happen.
2. **Monthly, on the 1st trading day**: fill in the previous month's *Actual* row from
   `tsrt_daily_stmt`, and re-state the forward projection. Scheduled as **Tasks S232**.
3. **Never quote a monthly rate from < ~60 sessions.** Concentration makes short windows lie
   (see *Method notes*).
4. **Always apply the broker-capture haircut** (currently **×0.81**, measured) to a chain
   projection before calling it an expectation.
5. **State the drawdown alongside the profit.** A projection without its MaxDD is not a
   projection — it's a wish. Also state MaxDD as **% of current equity**.
6. If Actual misses Theoretical by more than ~40% for two consecutive months, stop and
   investigate before shipping anything new.

---

## Current projection (re-run 2026-08-16, S275)

**Two books, two accounts, no cross-margin. They are projected separately and must be
reported separately.**

| | Main book | GEX Long v7 |
|---|---|---|
| Account | 210VYX65 long / 210VYX91 short | 210XFR64 |
| Capital | $6,000 | $3,000 |
| Filter | V16 + **Friday gate** (`REAL_TRADE_NO_FRIDAY=true`) | GEX Long gated to `gex_state='SUPPORT'` |
| Sizing | 1 MES base, **basket 2× on confirm** | **flat 1 MES**, no basket |
| Cap | **2 long / 3 short** (S249, live since 08-14) | **8**, own pool |
| Breaker | $300 realised | $150 realised |
| Status | live since 2026-08-10 | **arms 2026-08-17** |

### The number

Everything below is **one** simulation: one window, one haircut, one cost model.
Script: `_tmp_s275_projection_rerun.py`.

| | Main | v7 | **Combined** |
|---|---|---|---|
| Simulated (chain, full window) | $2,076 /mo | $298 /mo | **$2,374 /mo · SAR 8,903** |
| × 0.67 live capture | $1,391 /mo | $200 /mo | **$1,591 /mo · SAR 5,966** |
| **Honest floor** (ex-top-3 days, × 0.67) | $1,088 /mo | $13 /mo | **~$1,100 /mo · SAR 4,125** |
| MaxDD (sim $) | **−$1,733** = 29% of $6,000 | **−$170** = 6% of $3,000 | — |
| Worst single day | −$450 | −$150 | — |

**Headline: ~$1,600 /mo (SAR 6,000), floor ~$1,100 (SAR 4,125), ceiling ~$2,400 (SAR 9,000).**
Quote the range, not the point.

**Basis:** 2026-03-01 → 2026-08-15, **119 calendar trading sessions**, chain `outcome_pnl`,
**−0.6 pt/trade/contract** execution haircut (S246) **+ $1.92/contract round-turn** all-in fees
(S266). Main book 903 trades / 64% WR / 65 green vs 30 red. v7 74 trades / 73% WR.

### Post-S217 cross-check (45 sessions, current trail era only)

| | Main | v7 | Combined |
|---|---|---|---|
| Simulated | $1,794 /mo | $814 /mo | $2,608 /mo |
| × 0.67 | $1,202 /mo | $545 /mo | **$1,747 /mo · SAR 6,551** |

Reads slightly **higher** than the full window, so March's high-vol month is not carrying the
result. Main-book MaxDD in this era is only −$896.

### ⚠️ The 0.67 rests on five days

This is the weakest link and it must not be quoted as if it were solid. The **only** live
window on approximately this config is 2026-08-10 → 14:

| day | sim $ | broker net $ | diff |
|---|---|---|---|
| 08-10 | 164 | 53 | +110 |
| 08-11 | 300 | 257 | +44 |
| 08-12 | 20 | −53 | +72 |
| 08-13 | 292 | 286 | +5 |
| 08-14 | −140 | −116 | −24 |
| **total** | **636** | **427** | **+208** |

**Ratio 0.67** — the sim runs ~50% hot *even after* the haircut and fees are charged. It is
high on 4 of 5 days, so it looks systematic rather than noise, but **n=5 sessions and 34
contract round-trips**. The prior figures (×0.81, then ×0.87) came from different windows and
neither charged the newly-found fee. **Re-measure this every month — it is the single number
the whole projection multiplies by.**

Trade selection is *not* the gap: that week produced 27 V16-passing signals, 20 placed, 7
correctly cap-skipped, and the broker booked 34 contract round-trips. The sim and reality agree
on *which* trades. The gap is per-trade capture.

### 🚨 v7 is three days

**v7's top-3 days are 93.4% of its entire profit; ex-top-3 it earns $20/mo.** That is the
clustering the design already assumes (cap 8 exists to catch those days in full), but it means
v7's $298/mo has an enormous variance and **a month with no cluster earns nothing**. Do not
treat it as a steady income line. The main book by contrast is well spread — top-3 days are
21.8% and the ex-top-3 rate is $1,623/mo simulated.

⚠️ **Still never validated forward:** the Friday gate first acts 2026-08-21 and v7 has never
placed an order. Roughly **$700/mo of the $1,600 headline** comes from those two.

⚠️ **S236 contamination:** ES Absorption signals on/after 2026-07-02 were computed from ES
bars ~10 min stale. Immaterial at this level, but it matters for any ES-Absorption-only study.

---

## The ceiling — how much money exists at all (measured 2026-08-08)

> ⚠️ **This whole section predates the 2026-08-16 re-run.** Its dollar figures use the old
> ×0.81 capture, no fee charge, cap 2/2 and no Friday gate, and the V17 rows are **on hold**
> (S234 — V17's lead is mostly a cap artifact). The *shape* of the argument still holds —
> selection is not the bottleneck, capital is — but do not quote these numbers as current.

**All monthly figures in $ and SAR (peg ×3.75) — see memory `feedback_projections_in_usd_and_sar`.**

| | trades | total $ @1 MES | $/mo | SAR/mo | MaxDD | DD % of $5,161 |
|---|---|---|---|---|---|---|
| every signal, **no filter, no cap** | 4,047 | $21,340 | **$4,074** | **SAR 15,278** | **−$1,655** | **32%** |
| same, capped at 3 per side | 2,724 | $14,262 | $2,723 | SAR 10,211 | −$751 | 15% |
| same, capped at 2 per side | 2,246 | $11,659 | $2,226 | SAR 8,348 | −$802 | 16% |
| V16 today | 1,326 | $17,827* | — | — | — | — |
| perfect foresight (winners only) | 2,177 | $112,641 | — | — | — | *(unreachable)* |

\* raw ungated points; the tradeable V16 figure is in the projection table above.
110 sessions (2026-03-01 → 08-06). Monthly = sessions ÷ 21.

**The uncapped row is not achievable** — the busiest session alone had 75 signals, which is
unlimited simultaneous positions and impossible margin. Capping at 2–3 per side, which is what
you can actually carry, brings the true ceiling to **$2,226–2,723/mo (SAR 8,348–10,211)** at
1 MES, and note the drawdown *halves* when you cap it (−$1,655 → −$751).

**V16 already captures 84% of every available point using 33% of the signals.** Selection is
not the bottleneck — the signal set is. No filter can produce $5k/mo at 1–2 MES.

**The path past ~$3k/mo is size, and size is capped by capital:**

| configuration (×0.81) | $/mo | SAR/mo | MaxDD | vs $5,161 | vs $12k |
|---|---|---|---|---|---|
| V16, 1 MES — today | $1,590 | SAR 5,963 | −$1,253 | 24% | — |
| V17 relaxed, 1 MES | $2,436 | SAR 9,135 | −$1,198 | 23% | — |
| V17 relaxed, cap 3/3 | ~$2,900 | SAR 10,875 | −$773 | 15% | — |
| V17 relaxed, cap 3/3, **2 MES base** | **$4,312** | **SAR 16,170** | −$1,712 | 33% | **14%** |

$5,000/mo needs ≈ **2.5 MES**, which needs ≈ **$14–15k equity** to carry its drawdown at the
14% risk level this account runs. That is compounding, measured in quarters.

**Equity at last check:** ~$5,161 (2026-06-23). **Capital, not the edge, is the scaling limit** —
contracts must track *realised* equity, never the projection. 1 ES ≈ $50k equity; 3 ES ≈ $150k.

---

## Change Log

| date | change | Δ / month (theoretical) | measured on | status |
|---|---|---|---|---|
| 2026-08-17 | **GEX Long v7 armed** — GEX Long gated to live `gex_state='SUPPORT'`, own account 210XFR64 ($3,000), flat 1 MES, cap 8, own $150 breaker | **+$298/mo** (post-S217: +$814) — but **93% of it is 3 days** | 119 sessions, 74 trades, 73% WR, MaxDD −$170 | 🟡 **arms 2026-08-17, never placed an order** (Tasks S252) |
| 2026-08-15 | **Friday gate armed** — `REAL_TRADE_NO_FRIDAY=true`, both main accounts, **v7 excluded** | **+$411/mo** and MaxDD −$2,375 → **−$1,733**; red days 47 → 30 | 119 sessions, same basis | 🟡 shipped + armed, **first acts 2026-08-21** |
| 2026-08-15 | **Fee correction** — `FEE_PER_SIDE`; CME+NFA fees appear in **no API field**, only in equity. All-in $1.92/contract round-turn, not the $1.00 previously booked | **−$160/mo** at ~175 RT/mo — a *reporting* fix, not a P&L change; the money was always leaving | 41 filled orders / 68 sides; equity gap 08-10..14 to the cent | ✅ shipped `14c9e59`, era restamped $1,295.75 → $1,035.39 |
| 2026-08-14 | **S249 — short cap 2 → 3** (longs stay 2) | **+$203/mo** and drawdown IMPROVES (−$2,511 → −$2,375) | 119 sessions, same basis. Prior study said +$239 on 114 sessions — agrees | ✅ live, `REAL_TRADE_MAX_CONCURRENT_SHORT=3` |
| 2026-08-08 | **V17 structural relaxation** — relax the filter per SETUP (SC/AG/ES Abs/DD/VIX Div) when the signal's VIX < 22; keep full V16 at VIX ≥ 22; DD shorts still via the V13 stack; VPB never relaxed | **+$846/mo** ($1,590 → $2,436) and MaxDD −$1,253 → −$1,198 | 100 sessions, ex-S236-contaminated | 🟡 **built + verified, MONITORING ONLY** — portal dropdown + `live_filter.passes_v17`, trade path untouched. Ship via Tasks S234 after ~2 weeks of live V16-vs-V17 comparison |
| 2026-08-08 | *rejected* — drop individual V16 rules | +$228 per 100 sessions out-of-sample | leave-one-month-out, 6 folds | ❌ noise-fitting |
| 2026-08-08 | *rejected* — **V18-refit** (a 2026-08-08 experiment; NOT the shipped V18 filter of 2026-08-15), refit the entry filter per setup from scratch (16 numeric + 5 categorical features, thresholds from train data only) | OOS +1,741 pts vs V16 +3,566 and no-filter +4,268. In-sample it read +6,281 | leave-one-month-out | ❌ worse than V16 **and** worse than no filter; rule recurrence across folds ≈ 0 |
| 2026-08-08 | *rejected* — **V19-exits**, re-optimise stop + trail (1,440 parameter sets/setup on clean 1-min SPX, no lookahead) | nothing beat live on points; only a return/DD objective edged +10% at equal risk, by tightening stops, with a much worse Apr/May/Aug | leave-one-month-out, 118 sessions | ❌ **live exit params are already at the risk-adjusted optimum** — they are the output of S224 and the ES-Abs 6/2 change passing an independent re-test |
| 2026-08-08 | *rejected* — add VPB shorts / VIX Div shorts to the book | looked like +$819 and half the drawdown; verification killed it | 100 sessions | ❌ **one day (2026-07-17) was the entire drawdown gain**; cap 4/4 turns it negative; bootstrap lower bound $0 |
| 2026-08-08 | *pending* — add **SB Absorption** (Tasks S235) | +$692–769 per 100 sessions, but 80% of it is March | 45 clean signals (pre-S236) | 🟡 edge is real (+3.59 pts/t, 90% CI [+1.23, +6.05]) and driven by **longs** (24t, 75% WR, +5.20/t); shorts not established. n below this project's 50-signal bar — monitor, revisit once the feed is fixed |
| 2026-08-07 | **GEX Long disabled** (fires longs into tops; Tasks S230) | +$220/mo on the Jul–Aug window; **~break-even over 100 sessions** (69 trades, 43% WR, −$227 total). July alone was −$644 on 33 trades. | 100 sessions Mar 16 – Aug 6 + Jul 1 – Aug 6 | ✅ shipped, env |
| 2026-08-07 | **Basket → sizing-only** (`BASKET_SIZING_MODE=sizeonly`, block removed) | **+$440/mo**; block had cost −$547 (Jul 1 – Aug 6) / −$590 (Jun 11 – Aug 6) and roughly doubled MaxDD | 47 + 26 sessions | ✅ shipped, code |
| 2026-08-07 | **Cap held at 2/2 while 2× sizing is on** | cap 3/3 would add **+$54** for **+$893** of drawdown (28% → 45% of equity) | 47 sessions Jun 1 – Aug 6 | ✅ no change needed |
| 2026-07-27 | ES Absorption SHORT cut from the live filter (S229) | +$60/mo | Apr–Jul, net −$257 for the bucket | ✅ shipped `d596bad` |

### Rejected — measured and did NOT earn a row

| date | idea | why rejected |
|---|---|---|
| 2026-08-07 | Fail the basket block open on tech-vs-SPX divergence | **Lookahead bias** — the 81%/42% split used the day's *close*. At signal time: 57% vs 47%, no edge. |
| 2026-08-07 | Replace the tech basket with SPX %-from-open as confirmer | $3,035 vs $3,905 — worse than the tech basket |
| 2026-08-07 | Cap 2/2 as a general rule | Refuted: over 100 sessions cap 3/3 is +$1,818 and the 3rd stacked position is the *best* position in 5 of 6 months. Only correct **when 2× sizing is on**. |

---

## Actual vs Theoretical

| month | theoretical | **actual (broker, net)** | ratio | notes |
|---|---|---|---|---|
| 2026-05 | — | **+$895.50** | — | 11 sessions, post-V16.1 — **pre-fee-correction figure** |
| 2026-06 | — | **−$933.00** | — | drawdown month: execution bugs, auto-roll incident, macro regime |
| 2026-07 | — | **+$549.50** | — | 1 session only — TSRT disabled 07-01 ~11:57 ET |
| 2026-08 (10–14) | $636 | **+$427.22** | **0.67** | **the only clean live week**; 5 sessions, cap 2/2, no Friday gate |
| 2026-08 (full) | ~$1,600 | *pending* | | first month on the current config |

Era total 2026-05-14 → 2026-07-01, **restamped with the true $1.92/RT cost**: **+$1,035.39 net
/ 34 days / 283 round-turns** (was +$1,295.75 before the fee was found).

**All-time across the two main accounts the deposited money is ~break-even (+$16).** The era
figure measures *strategy* performance from a mid-journey equity anchor ($4,896.99), not return
on capital. Keep the two apart — see memory `project_real_capital_deposited_6000`.

---

## Method notes (why these numbers and not bigger ones)

- 🚨 **A MONTH IS 21 CALENDAR SESSIONS — NEVER "sessions that had a trade".** Both the Friday
  gate and v7 trade on a *subset* of days (v7 fired on 19 of 119). Dividing by the subset and
  multiplying by 21 asks "what if every day were a trading day for this book?", which is not a
  month. Caught during the 2026-08-16 re-run: it read the Friday gate at **+$892/mo instead of
  +$411** and v7 at **+$1,866/mo instead of +$298** — a 6× error on v7. Days with no signal must
  be counted as $0 sessions.
- **Charge BOTH costs, they are additive.** The −0.6 pt/trade haircut is slippage between the
  trail crossing and the market fill; the $1.92/RT is commission + exchange fees. Verified
  independently: the live week's broker gross $492.50 minus 34 × $1.92 = net $427.22 to the cent.
- **Metric = chain (`outcome_pnl`)** for any window after 2026-06-13. See `CLAUDE.md` Gate 0.
  `mes_sim_*` is the wrong model post-S217 and understates by ~2.8 pt/trade.
- **Concentration is a window-length artifact.** Top-3-day share is 56% over 26 sessions but
  **24% over 100**. Quote the *ex-top-3 run rate*, not the share. Ex-top-3 over 100 sessions is
  $1,519/mo at 1 MES flat — that is the honest floor.
- **Why a few days carry the P&L (by design, not fragility):** fixed ~14 pt stop + uncapped
  trailing exit. On the top 3 days the average win is 21.7 pts vs 10.7 on normal days, at the
  *same* trade count. Mean-reversion entries with a trend-following exit = positive skew.
  Big days are not predictable in advance (day-$ vs SPX range r = −0.16, vs net move r = −0.00).
- **Basket sizing has only ~8 weeks of data** — `setup_log.basket_pct` is 0% populated before
  June 2026. It is the largest single contributor to the projection and the least tested. Treat
  the first real month as its validation.
- **The projection is slightly CONSERVATIVE: VIX Divergence was left out of the simulation.**
  I inferred it was disabled (a `whitelist_reject` on 2026-05-21, nothing placed since 05-18),
  but Railway shows `VIX_DIV_REAL_TRADE_ENABLED=true`. It passes V16 base 7× in Jul 1 – Aug 6
  (+11.7 pts = +$58) and 13× since Mar 16 (54% WR, +58.2 pts = +$291 @1 MES). Small and positive
  — fold it into the next re-run rather than restating the headline.
- **Confirmed live env (2026-08-07):** `VPB_REAL_TRADE_ENABLED=true` (the sim depends on this —
  without it `passes_v16` silently drops 21 VPB longs), `SPX_EXIT_ENABLED=true`,
  `GEX_LONG_V3_ENABLED=true` (detector still logs for the S230 rebuild; only the *real-trade*
  flag is off). `BASKET_GATE_ENABLED=true` is a dormant leftover — `basket_gate.evaluate()` is
  no longer called anywhere in the trade path, only the pure `classify()` helper is used.

Detail: memory `research_s231_tsrt_counterfactual_jul_aug.md`.
