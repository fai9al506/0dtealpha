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

## 🔴 CURRENT HEADLINE — V21, re-run 2026-08-17 (S300–S307)

| | $ | SAR |
|---|---|---|
| **AVERAGE month** | **+$2,372** | **8,895** |
| **WORST month** (Jun 2026) | **+$530** | 1,988 |
| **BEST month** | **+$4,906** | 18,398 |

**Every month positive.** 117 calendar sessions, 861 trades, 1 MES, −0.6 pt + $1.92/RT charged.

| month | V20 | **V21** | change |
|---|---|---|---|
| 2026-03 | +$4,906 | +$4,906 | same |
| 2026-04 | +$1,730 | **+$1,857** | +$127 |
| 2026-05 | +$2,122 | +$2,122 | same |
| **2026-06** | **−$225** | **+$530** | **+$754** |
| 2026-07 | +$1,237 | **+$1,385** | +$148 |
| 2026-08 | +$2,417 | +$2,417 | same |

**Risk — the real reason V21 exists:**

| | V20 | **V21** |
|---|---|---|
| max drawdown | −$1,585 | **−$906** (−43%) |
| worst 5-day window | −$1,196 | **−$752** |
| the June 5–12 streak | −$1,219 | **−$465** |

**Distribution (mandatory):** best day 8% of total · best 3 days 23% · **without the best 3 days
~$1,850/mo** · **without the best month ~$1,760/mo** ← honest floor · **6 of 6 months positive**.

**Trade count is risk (mandatory):** V21 **861 trades / 7.4 per day**, against V16's 1,111 / 9.5 —
**250 fewer exposures for $654/month more.**

**⚠️ ASSUMED UPTIME: 21 sessions a month.** We have been trading 6–11. At that rate expect a
third of this. See memory `research_why_capital_is_flat_uptime` — uptime is now the binding
constraint, not the filter.

**Not included:** GEX Long v7 (account never funded), the Friday call spread (log-only), and any
sizing above 1 MES.

---

## Rules for updating this file (MANDATORY)

1. **On every change that can move P&L** (new setup, filter edit, cap change, sizing change,
   setup disabled): add a row to *Change Log* with the measured Δ, the window it was measured on,
   and the number of sessions. No row = the change didn't happen.
2. **Monthly, on the 1st trading day**: fill in the previous month's *Actual* row from
   `tsrt_daily_stmt`, and re-state the forward projection. Scheduled as **Tasks S232**.
3. **Never quote a monthly rate from < ~60 sessions.** Concentration makes short windows lie
   (see *Method notes*).
4. **Always charge execution costs** — **−0.6 pt/trade/contract** (S246 slippage) **plus
   $1.92/contract round-turn** (S266 all-in fees). Charge them *inside* the simulation, per
   contract. **Do NOT then multiply by a capture ratio as well** — measured on 15 post-S217
   live days that double-counts (residual is −0.24 pt, i.e. already slightly too harsh).
   Re-measure the residual monthly with `_tmp_s275_capture_by_day.py`, never on one week.
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

**No further capture multiplier is applied.** The −0.6 pt haircut and the $1.92/RT fees ARE
the capture correction, and measured against 15 post-S217 live days they are already slightly
too harsh (see below). Applying an extra ×0.81/×0.87/×0.67 on top would double-count.

Built up so the untested parts are visible:

| | $/mo | SAR/mo | evidence |
|---|---|---|---|
| **Validated core** — V16 + S249 cap 2/3, basket 2× | **$1,665** | SAR 6,244 | this config, or near it, has traded live |
| + Friday gate | +$411 | +SAR 1,541 | **never blocked a live trade**, first acts 08-21 |
| + v7 | +$298 | +SAR 1,118 | **never placed an order**, arms 08-17 |
| **= Combined projection** | **$2,374** | **SAR 8,903** | |
| **Honest floor** (ex-top-3 days) | **$1,643** | SAR 6,161 | main $1,623 + v7 $20 |
| MaxDD (main) | **−$1,733** | — | 29% of the $6,000 main capital |
| MaxDD (v7) | **−$170** | — | 6% of the $3,000 v7 capital |
| Worst single day | −$450 main / −$150 v7 | — | the two breakers |

**Headline: ~$2,400 /mo (SAR 8,900), floor ~$1,650 (SAR 6,200).** Quote the range, not the
point — and remember **$709/mo of it has never traded**.

**Basis:** 2026-03-01 → 2026-08-15, **119 calendar trading sessions**, chain `outcome_pnl`,
**−0.6 pt/trade/contract** execution haircut (S246) **+ $1.92/contract round-turn** all-in fees
(S266). Main book 903 trades / 64% WR / 65 green vs 30 red. v7 74 trades / 73% WR.

### Post-S217 cross-check (45 sessions, current trail era only)

| | Main | v7 | Combined |
|---|---|---|---|
| Simulated | $1,794 /mo | $814 /mo | **$2,608 /mo · SAR 9,780** |

Reads slightly **higher** than the full window, so March's high-vol month is not carrying the
result. Main-book MaxDD in this era is only −$896.

### Capture: measured on every live day, not on one week

Script: `_tmp_s275_capture_by_day.py`. Method that removes config drift completely — do **not**
re-run the filter. Take the lids TSRT **actually placed** (`real_trade_orders`, using the real
`quantity` field), score them with the chain model + the standard costs, and compare to
**day-level** broker truth (`tsrt_daily_stmt.net` — the S210 rule). Both sides then look at the
same trades, so whatever filter/cap/basket was live that day is irrelevant.

| window | days | contract RT | sim $ | broker $ | gap per contract |
|---|---|---|---|---|---|
| **post-S217, clean** | **15** | **82** | 621 | 718 | **−0.24 pt** |
| all live days, clean | 30 | 217 | 684 | 1,234 | −0.51 pt |
| *(August week alone)* | *5* | *34* | *636* | *427* | *+1.23 pt* |

"Clean" = the sim's contract count equals the broker's that day (30 of 37; the 7 excluded are
mostly May, before `quantity` was stored, plus 06-30's manual stack close).

**The gap post-S217 is −0.24 pt/contract with a standard error of ±0.47 pt (σ=4.26, n=82) —
statistically zero, and it is NEGATIVE, meaning the sim is if anything slightly pessimistic.**
Sign test 9 high / 6 low, a coin flip. The −0.6 pt haircut is doing its job; nothing further is
owed.

**Cross-check:** if the residual after a 0.6 haircut is −0.24, the raw chain bias is about
+0.36 pt/trade. Memory `feedback_chainsim_valid_post_s217` measured +0.18 pt on 43 trades
independently. Same sign, same order — the two agree.

⚠️ **The August week alone reads +1.23 pt/contract (ratio 0.67) and that is what an earlier
version of this file quoted.** It is 1.7 standard errors from zero on 34 round-trips — noise.
**Never calibrate capture on one week.** June's post-S217 days track the broker to within a few
dollars (06-15 +$3, 06-16 +$1, 06-25 +$4, 06-26 −$3).

⚠️ **This validates EXECUTION, not SELECTION.** It only measures trades the bot really placed.
The Friday gate and v7 change *which* trades get placed, and neither has traded.

### 🚨 v7 is three days

**v7's top-3 days are 93.4% of its entire profit; ex-top-3 it earns $20/mo.** That is the
clustering the design already assumes (cap 8 exists to catch those days in full), but it means
v7's $298/mo has an enormous variance and **a month with no cluster earns nothing**. Do not
treat it as a steady income line. The main book by contrast is well spread — top-3 days are
21.8% and the ex-top-3 rate is $1,623/mo simulated.

⚠️ **Still never validated forward:** the Friday gate first acts 2026-08-21 and v7 has never
placed an order. **$709/mo of the $2,374 headline (30%)** comes from those two. Execution
capture *is* now validated on 15 live days — selection is not.

⚠️ **S236 contamination:** ES Absorption signals on/after 2026-07-02 were computed from ES
bars ~10 min stale. Immaterial at this level, but it matters for any ES-Absorption-only study.

---

## The scaling ladder (measured 2026-08-16, S275c) — NOTHING ABOVE RUNG 0 IS ARMED

S250's principle: **scale the PROVEN setup, not the account.** Skew Charm short is the one that
earns it — 131 real broker trades, 60% WR, +$1,418 real money, 5 of 5 months positive, and the
only profitable thing in June–July. Re-priced on the current basis
(`_tmp_s275_scaling_ladder.py`, 119 sessions, V16 + Friday gate, cap 2/3):

| rung | $/mo | of which SC-short | MaxDD | peak short MES | short margin | short acct needs |
|---|---|---|---|---|---|---|
| **0 — today, 1×** | **$2,076** | $829 | −$1,733 | 6 | $1,589 | $2,270 |
| 1 — SC-short **2×** | $2,736 | $1,539 | −$2,276 | 12 | $3,178 | **$4,539** |
| 2 — SC-short **3×** | $3,814 | $2,654 | −$1,934 | 18 | $4,766 | **$6,809** |
| 3 — SC-short **4×** | $4,791 | $3,626 | −$2,677 | 24 | $6,355 | **$9,079** |
| *(compare)* whole book 2× | $3,894 | $1,400 | **−$3,178** | 12 | $3,178 | $4,539 |

"Short acct needs" = peak margin ÷ 0.70 (never commit more than 70% of equity). Long side is
cheap and unchanged throughout: peak 4 MES, $1,059 margin, needs ~$1,513.

**S250's principle is confirmed with numbers.** Scaling SC-short to 3× earns **$3,814** against
whole-book-2×'s $3,894 — the same money for **39% less drawdown** (−$1,934 vs −$3,178) and the
same short-side margin. Concentrating size on the proven setup is strictly better than
levering everything.

**Size does not consume slots** — the cap counts *positions*, not contracts — so P&L scales
almost linearly and **margin, not the cap, is the binding constraint.**

### What actually gates each rung

| | rung 0 | rung 1 | rung 2 | rung 3 |
|---|---|---|---|---|
| MaxDD as % of the $6,000 main capital | 29% | **38%** | 32% | **45%** |
| 40-pt overnight-gap loss (breaker does NOT flatten) | $1,200 = 20% | $2,400 = **40%** | $3,600 = **60%** | $4,800 = **80%** |
| Short account has $2,920.77 + profits; needs | ✅ $2,270 | ❌ $4,539 | ❌ $6,809 | ❌ $9,079 |

**Rung 1 is not fundable today** — the short account holds ~$3,000 and needs ~$4,539. That is
the real schedule-setter, not the calendar. S250's "~2026-09-01" was written before this
margin arithmetic; the account has to earn its way there first.

⚠️ **The MaxDD column is not monotonic** (−1,733 → −2,276 → −1,934 → −2,677). Drawdown depends
on which specific day the peak-to-trough lands on, so these are **noisy to ±$400** and rung 2
is NOT genuinely safer than rung 1. Read the *gap-risk* row instead — that one is arithmetic
and cannot lie.

⚠️ **Two things this ladder does NOT model.** (1) **Fill quality at size** — capture was
measured at 1–2 MES; 18 MES on one side is unvalidated, though MES liquidity makes it likely
fine. (2) The DD is **book-level**, but the accounts are margined separately, so the short
account's own drawdown is what constrains it. The $300 breaker *is* modelled and does not
scale, which correctly penalises the higher rungs.

**Ceiling stays 3–5×, unchanged.** At rung 3 a 40-pt gap costs 80% of the main capital and a
60-pt gap exceeds it. Above that, more equity must come first — it is a survival question, not
a return question.

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
| 2026-08 (10–14) | $636 | **+$427.22** | 0.67 | 5 sessions — **too short to calibrate on**; see the capture section |
| **post-S217, all live days** | **$621** | **+$718** | **1.16** | **15 days / 82 contract RT — this is the real capture check** |
| 2026-08 (full) | ~$2,400 | *pending* | | first month on the current config |

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
