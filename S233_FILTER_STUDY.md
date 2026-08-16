# S233 — Is the V16 filter over-tight? Per-rule study

**Run 2026-08-07. Window 2026-03-16 → 2026-08-06 = 100 sessions, 3,622 resolved signals.**
Outcome model = **CHAIN** (`setup_log.outcome_pnl`) per CLAUDE.md Gate 0.
Sizing = 1 MES base + 2× on basket confirm. Gates re-simulated in `real_trader.place_trade()`
order (dedup 90s → concurrency cap → $300 daily breaker → underwater-stack guard).
GEX Long excluded throughout (real-trade flag is off — Tasks S230).

Scripts: `_tmp_s233_rules.py` (filter decomposed into 34 toggleable rules, **parity-verified
bit-identical to `app/live_filter.passes_v16` on 4,210 rows**), `_tmp_s233_sim.py` (harness),
then `_ablate / _regime / _stability / _combo / _wf2 / _buckets / _robust / _final / _lomo2 /
_vix / _struct / _checks / _brokergap / _stress / _stages / _validate / _modeldiff / _summary`.

---

## 0. CORRECTION added 2026-08-08 (user challenged the VIX gate — they were right)

Two claims in the original write-up were overstated. Both are corrected here; the rest stands.

**(a) The "keep full V16 at VIX ≥ 22" rule rests on ONE DAY.** Direct day-level test on the 19
sessions that contain a VIX ≥ 22 signal: V16 $1,808 vs no-filter $1,255 — the filter is worth
**+$553 in total, and is better on only 8 of those 19 days. 2026-03-23 alone is 95% of the
edge.** By this project's own concentration rule that is not evidence. The VIX gate is cheap
insurance, not a proven regime effect. Keep it because it costs almost nothing and caps the
March-type downside — **do not sell it as validated.**

**(b) The gain is a JUNE–JULY repair, not a universal upgrade.** New trades only (no credit for
displacing V16 losers), by month:

| stage | Apr | May | Jun | Jul | Aug | months positive |
|---|---|---|---|---|---|---|
| relax Skew Charm only | −$122 | −$88 | +$676 | **+$1,478** | +$60 | 3 of 5 |
| + ES Absorption + AG Short | +$70 | −$44 | **+$2,320** | **+$2,229** | −$96 | 3 of 5 |
| full (no VPB) | +$208 | +$724 | +$2,423 | +$1,418 | +$1,020 | **5 of 5** |

The partial stages earn almost everything in the two months V16 broke. **Only the full
relaxation is profitable in every month.** That inverts the original "ship the small stage
first" logic — the small stage is the *more* regime-dependent one, not the safer one.

**(c) Consequence for `SC_GRADE`.** Dropping the Skew Charm grade C/LOG block admits 220
signals at 50% WR worth **−$85.4 pts (−$427)** on raw numbers. It only looks positive inside
the portfolio because those trades displace V16 losers. Do not drop it on its own.

**What still stands unchanged:** the drawdown improvement (24% of the account → 9–15%) is
consistent across caps, sizing modes, eras and both walk-forward directions; rule-picking still
fails out-of-sample; the bucket table in §4 is unchanged; the ES Absorption / S229 finding in §4
is unchanged.

---

## 1. The answer in one line

**The V16 filter is over-tight, but not rule-by-rule — as a whole.** Picking which rules to
drop does **not** survive out-of-sample testing (+$228 over 100 sessions). Relaxing it
*structurally*, per setup, while keeping full V16 on high-VIX signals, does: **+$1,000/month
after the broker haircut, with the drawdown cut from 24% of equity to 9-15%.**

| config (cap 2/2, ×0.81 haircut) | $/mo | MaxDD | DD % of $5,161 | trades/day | beats V16 in |
|---|---|---|---|---|---|
| **V16 — live today** | $1,590 | −$1,253 | 24% | 8.9 | — |
| S1 relax Skew Charm | $2,137 | −$626 | 12% | 11.7 | 5 of 6 months |
| S2 + ES Absorption | $2,407 | −$543 | 11% | 15.5 | 5 of 6 |
| **S3 + AG Short ← ship this first** | **$2,428** | **−$461** | **9%** | 15.7 | 4 of 6 |
| S5 + DD Exhaustion + VIX Div | $2,520 | −$727 | 14% | 16.1 | **6 of 6** |
| S5 at **cap 3/3** | **$2,991** | −$773 | 15% | 18.6 | 6 of 6 |

Every stage earns more **and** draws down less than what runs today. That is the same
"thinning the book is expensive" result as the basket block, one level up.

---

## 2. What the filter is actually doing

Per-rule leave-one-out over 100 sessions (full table in §7). The headline:

| | V16 ON | filter fully OFF |
|---|---|---|
| total (cap 2/2) | $9,816 | **$14,286** |
| trades | 887 | 1,880 |
| win rate | 59% | 55% |
| $ per trade | 11.1 | 7.6 |
| MaxDD | −$1,439 | **−$1,026** |

The filter **does** pick better trades ($11.1 vs $7.6 per trade). It discards ~55% of the book
to do it, and the lost diversification costs more than the selection gains.

### It is regime-dependent, and that part is solid

| session VIX | sessions | V16 ON | filter OFF | filter is worth |
|---|---|---|---|---|
| < 17 | 27 | $4,728 | $7,921 | **−$3,193** |
| 17–19 | 38 | $2,555 | $2,967 | −$412 |
| 19–21 | 18 | $215 | $1,914 | −$1,698 |
| 21–24 | 3 | $14 | −$68 | +$83 |
| **24+** | 14 | $2,304 | $1,553 | **+$751** |

V16 was built in the March/April high-vol regime and still earns its keep there. Below ~VIX 21
it costs money. Sweeping the threshold gives a **broad plateau from 20 to 26** at both caps
(not a spike), so "keep full V16 when VIX ≥ 22" is one parameter with a real mechanism behind it.

---

## 3. The methodological finding that matters most

**Rule-picking is noise-fitting; structure is not.**

Leave-one-month-out — the keeper set is chosen using the other 5 months, then the held-out
month is scored with it:

| held-out month | V16 | rule-picked V17 | no filter (needs no fitting) |
|---|---|---|---|
| 2026-03 | $2,058 | $1,645 | $980 |
| 2026-04 | $2,184 | $1,394 | $2,280 |
| 2026-05 | $2,345 | $2,904 | $2,919 |
| 2026-06 | −$90 | −$63 | $2,115 |
| 2026-07 | $1,683 | $2,652 | $4,150 |
| 2026-08 | $1,636 | $1,512 | $1,842 |
| **TOTAL** | **$9,816** | **$10,044 (+$228)** | **$14,286 (+$4,470)** |

Choosing 24 of 34 rules gains almost nothing out-of-sample. Dropping the filter — a decision
with **zero fitted parameters** — gains $4,470. Greedy selection also walk-forwards positive in
both directions (Mar-May → Jun-Aug +$810; Jun-Aug → Mar-May +$665) but far below its in-sample
promise. **Do not ship a hand-picked rule list.**

---

## 4. What the relaxed book actually re-admits

Every V16-blocked trade the relaxed book takes, by bucket (cap 3/3, chain $):

| bucket | n | WR | 03 | 04 | 05 | 06 | 07 | 08 | TOTAL | months + |
|---|---|---|---|---|---|---|---|---|---|---|
| ES Absorption SHORT | 282 | 55% | +210 | +391 | +68 | +593 | +412 | 0 | **+1,674** | 5/6 |
| DD Exhaustion SHORT | 355 | 47% | +707 | −1,198 | +71 | +1,225 | +866 | −106 | +1,566 | 4/6 |
| Skew Charm SHORT | 197 | 58% | −372 | −328 | +226 | +468 | +1,456 | −10 | +1,440 | 3/6 |
| DD Exhaustion LONG | 202 | 49% | −401 | +108 | +647 | −143 | −700 | +1,323 | +834 | 3/6 |
| ES Absorption LONG | 174 | 51% | −110 | +248 | +401 | −580 | +398 | 0 | +358 | 3/6 |
| Skew Charm LONG | 137 | 55% | −170 | +182 | +70 | +170 | +4 | +80 | +335 | 5/6 |
| VIX Divergence LONG | 30 | 43% | 0 | +28 | −2 | +162 | +40 | +27 | +256 | 4/6 |
| AG Short SHORT | 43 | 58% | −22 | 0 | −233 | +422 | +237 | −155 | +249 | 2/6 |
| **Vanna Pivot Bounce LONG** | 26 | 38% | 0 | −33 | 0 | −25 | −248 | 0 | **−306** | **0/6** |

**Only one bucket is consistently bad — VPB longs.** Everything else the filter blocks is net
positive. That is a decision per *bucket* (9 of them, driven by the data's own sign), not per
rule (34), which is why it holds up where rule-picking does not.

### Two structural exceptions that must be kept

1. **DD shorts need the existing V13 quality stack.** Raw DD shorts lost −$1,198 in April alone.
   Routed through the V13 gates that are already in the code (`V13BULL`, `V13VANNA`, `V13DDQ`,
   `SCDD_SHORT_GEXLIS`), the same bucket is strongly positive and MaxDD improves from −$1,469
   to −$865. This is not a new rule — it is the gate that was already written for DD shorts.
2. **Full V16 when VIX ≥ 22.** Without it, March collapses from $2,058 to $688.

### The S229 ES-Absorption short cut was aimed at different trades

| ES Abs shorts | n | WR | chain pts |
|---|---|---|---|
| the ones V16 **passed** (what S229 measured and cut) | 42 | 45% | +4.2 (≈ $21) |
| the ones **other V16 rules already blocked** | 383 | 54% | **+314.3 (≈ $1,572)** |

The filter was selecting the *worse* half of that setup. Cutting the passing half was defensible;
the blocked half is where the money is.

---

## 5. Robustness — what I tried to break it with

| test | result |
|---|---|
| cap 1 / 2 / 3 / 4 | gain holds at every cap; **survives cap 4/4** (+$3,078 to +$6,174), so it is not a cap-displacement artifact |
| flat 1 MES (removes the basket-sizing confound, which has only 8 weeks of data) | V16 $7,811 → relaxed $10,880 |
| daily loss breaker $200 / $300 / $500 / $1,000 / none | relaxed book is *better* with the $300 breaker than without — leave it alone |
| commission | broker truth: **exactly $1.00 per round trip** (283 round trips, `tsrt_daily_stmt`) = what the sim assumes |
| walk-forward, both directions | positive both ways |
| pre-S217 era (63 sessions) | V16 $6,139 → staged $8,369, DD −$1,400 → −$661 |
| **post-trail-change era, 2026-06-23+ (32 sessions)** | **V16 $3,240 → staged $5,977, WR 59% → 61%, DD −$797 → −$488** |
| MES-walk cross-check | agrees on the sign; ratio 0.87 after removing rows hit by the known `mes_sim` entry-fallback bug |

Peak exposure is **unchanged** — the concurrency cap is untouched, so it is still at most
2 positions per side / 8 MES gross at cap 2/2. What rises is the *number of sequential trades*:
median 8/day → 14/day, p90 16 → 27.

---

## 6. The caveats that could sink it — read before shipping

**(a) Break-even is thin.** The re-admitted trades earn about **1.1 points each**. If they
execute ~1.1 pt worse than the chain sim, the entire gain is gone.

| extra slippage on the new trades | cap 2/2 $/mo | cap 3/3 $/mo |
|---|---|---|
| 0.0 pt (measured bias is 0.18) | $2,503 | $2,934 |
| −0.5 pt | $2,107 | $2,422 |
| −1.0 pt | $1,681 | $1,896 |
| −1.5 pt | $1,217 | $1,390 |
| break-even vs V16 | **−1.06 pt** | **−0.93 pt** |

For context, the measured chain-vs-broker error on 43 executed post-S217 trades is a **+0.18 pt
bias with 1.73 pt MAE**, and the error does not vary much by direction, grade, hour, VIX band or
trade size — including on big winners (+0.24), which is where this book's P&L comes from.

**(b) Two proposed buckets have ZERO broker history.** DD Exhaustion SHORT (52 trades in the
recommended book) and VIX Divergence (24) have never been sent to a broker. ES Absorption SHORT
has only 2. Skew Charm — the biggest re-admitted bucket — has 24.

**(c) DD Exhaustion shows the worst sim bias of any setup** (−2.59 pt/trade, but n=6). Before
re-admitting DD shorts, verify the portal chain sim uses the same trail as
`real_trader._SETUP_TRAIL_OVERRIDE` (continuous, no BE, act 10 / gap 10). Memory
`reference_mes_sim_entry_fallback_bug` flags `setup_log.trail_activation` / `trail_gap` as
stale detector values — a likely source of that bias.

**(d) The trade rate nearly doubles.** 8.9 → 15.7 trades/day, p90 27, worst day 44.

---

## 7. Per-rule table (reference — do NOT ship a hand-picked subset of this)

`dLOO` = P&L change from removing that one rule from full V16, 100 sessions, cap 2/2, basket sizing.
`mo+/mo−` = months where removal helped / hurt.

| rule | dLOO | mo+ | mo− | what it blocks |
|---|---|---|---|---|
| DD_SHORT | +$1,284 | 6 | 0 | DD Exhaustion shorts entirely |
| V13VANNA | +$991 | 4 | 1 | shorts on adverse vanna cliff/peak sides |
| ESABS_ALIGN | +$704 | 4 | 1 | ES Abs long when alignment < 0 |
| SCDD_SHORT_GEXLIS | +$393 | 3 | 2 | SC/DD shorts on GEX-LIS |
| AG_TARGET | +$286 | 2 | 1 | AG Short on AG-TARGET |
| ESABS_GRADE | +$253 | 2 | 1 | ES Abs below grade A |
| SC_GRADE | +$216 | 3 | 2 | Skew Charm grade C / LOG |
| ESABS_PARA | +$154 | 3 | 1 | ES Abs on AG-TARGET / AG-LIS |
| SC_LONG_GEXLIS | +$141 | 1 | 3 | SC long on GEX-LIS |
| V11_LATE | +$122 | 3 | 1 | SC/DD from 15:30 |
| VPB_GRADEB | +$119 | 3 | 0 | VPB below grade B |
| VPB_HOUR11 | +$110 | 1 | 0 | VPB in the 11:00 hour |
| DDLONG_GRADEC | +$108 | 3 | 1 | DD long grade C |
| GAP_LONG | +$108 | 4 | 2 | longs pre-10:00 on \|gap\| > 30 |
| DDLONG_ALIGN_LO | +$101 | 2 | 0 | DD long alignment < 0 |
| V11_DEADZONE | +$88 | 3 | 2 | SC/DD 14:30–15:00 |
| DDLONG_ALIGN_HI | +$75 | 1 | 0 | DD long alignment ≥ 3 |
| DDLONG_VIX22 | +$52 | 1 | 0 | DD long VIX ≥ 22 |
| ESABS_SHORT | +$28 | 2 | 3 | ES Abs shorts (S229) |
| ESABS_LATE | −$13 | 1 | 1 | ES Abs from 15:45 |
| SIDIAL_PM | −$71 | 0 | 1 | longs 14:00–15:00 on SIDIAL-EXTREME |
| SC_LONG_OPEX | −$115 | 1 | 2 | SC long on opex Friday |
| GEXTARGET_PM | −$147 | 0 | 2 | longs after 13:00 on GEX-TARGET |
| AG_OPEX | −$158 | 0 | 1 | AG Short on opex Friday |
| V13BULL | −$176 | 2 | 3 | SC/DD shorts, GEX-above ≥ 75% or DD-near ≥ $3B |
| VIXDIV_GEXPARA | −$271 | 2 | 3 | VIX Div outside GEX-* paradigms |
| SC_LONG_A3PARA | −$276 | 0 | 1 | SC long align=3 on bad paradigms |
| DDLONG_PARA | −$502 | 4 | 1 | DD long on GEX-LIS/AG-LIS/AG-PURE/BofA-LIS/BOFA-MESSY |

The raw bucket each rule discards is in `_tmp_s233_ablate.py` output section A.

---

## 8. Recommendation

**Ship in stages, and shadow before each one.**

| step | change | expected | when |
|---|---|---|---|
| 0 | Arm TSRT Monday on the **current V16** as planned | $1,590/mo, DD 24% | 2026-08-10 |
| 0b | Add a `v17_pass` stamp + daily re-stamp so the portal shows both books forward. **Zero risk** — no trade-path change | — | same week |
| 1 | Relax **Skew Charm + ES Absorption + AG Short** only. Keep full V16 at VIX ≥ 22. Keep cap 2/2 | **$2,428/mo, DD 9%** | after ~2 weeks of measured capture |
| 2 | Add **DD Exhaustion** (routed through the existing V13 stack) + **VIX Divergence** | $2,520/mo, DD 14% | after DD trail parity is verified (§6c) |
| 3 | Consider cap 3/3 | $2,991/mo, DD 15% | only once stage 2 has ~40 sessions of broker data |
| — | **Never** relax Vanna Pivot Bounce | −$306 | — |

**Gate on real data, not on this study:** after each stage, measure realised broker capture on
the *newly admitted* trades specifically. If it is worse than chain by more than **1.0 pt/trade**,
roll that stage back — that is the break-even.

### Implementation surface (4 copies, lockstep — memory `feedback_filter_three_copies_lockstep`)

- `app/main.py:_passes_live_filter` (~4169) — the runtime gate
- `app/main.py` portal JS `passesStrategy` (~13990, `'v16'`)
- `app/main.py` portal JS `_tlPassesStrategy` (~19058, `'v16'`)
- `app/live_filter.py:passes_v16` + bump `LIVE_VER`, then re-run `live_filter_recall.py`

### Also found, unrelated to the filter

`SB Absorption` is outside the real-trade whitelist and is the best per-trade setup in the DB:
**49 trades, +189.4 pts, 3.87 pts/trade, positive in 4 of 5 months.** n is small — worth its own
study before anything else. (`BofA Scalp` +88.6 pts / 0.53 per trade and `Paradigm Reversal`
+0.4 pts are not worth re-admitting.)

---

# V18-refit — "build a better filter from 5 months of data" (2026-08-08, user-requested)
#
# ⚠️ NAME NOTE: this REJECTED experiment predates the shipped **V18** filter
# (S260, 2026-08-15 — V16 minus shorts with a +GEX wall close overhead, in
# app/live_filter.py:passes_v18). They are unrelated. Everything below is the
# dead one.

Scripts: `_tmp_v18_data.py` (full feature set), `_tmp_v18_audit.py` (per-setup baseline),
`_tmp_v18_engine.py` (fitting engine + leave-one-month-out), `_tmp_v18_ceiling.py`,
`_tmp_v18_reality.py`.

## V18.1 — Per-setup baseline, ungated, Mar–Aug (raw chain points, no filter, no caps)

| setup | n | WR | total pts | pts/trade | months + |
|---|---|---|---|---|---|
| Skew Charm | 1,378 | 60% | **+2,295** | +1.67 | 6/6 |
| — short | 712 | 61% | +1,457 | +2.05 | 6/6 |
| — long | 666 | 60% | +839 | +1.26 | 4/6 |
| ES Absorption | 996 | 53% | +768 | +0.77 | 5/6 |
| DD Exhaustion | 1,192 | 47% | +618 | +0.52 | 4/6 |
| AG Short | 163 | 63% | +272 | +1.67 | 3/6 (March is +311 of it) |
| SB Absorption *(not traded)* | 49 | 65% | +189 | **+3.87** | 4/6 |
| VIX Divergence | 87 | 46% | +160 | +1.84 | 4/6 |
| Vanna Pivot Bounce | 231 | 49% | +155 | +0.67 | 4/6 |
| GEX Long *(disabled)* | 316 | 48% | +85 | +0.27 | 3/6 |
| BofA Scalp, Paradigm Reversal, SB2, Dip-Buy v2, Delta Abs | — | — | all ≤ 0 | — | correctly excluded |

Two structural oddities worth noting: **VIX Divergence shorts (+2.64/t) beat longs (+1.30/t)**
and **VPB shorts (+0.75) beat longs (+0.54)** — yet both setups are long-only in the filter.

## V18.2 — Fitting a filter per setup FAILS out of sample

Greedy and consensus selectors over ~16 numeric + 5 categorical features, thresholds derived
from train data only, leave-one-month-out across 6 folds:

| | trades | WR | total pts | pts/trade |
|---|---|---|---|---|
| no filter at all | 4,047 | 54% | +4,268 | +1.05 |
| **V16 (today's filter)** | 1,326 | 62% | **+3,566** | **+2.69** |
| V18 fitted, ≤2 rules — **out of sample** | 2,857 | 53% | +2,687 | +0.94 |
| V18 fitted, ≤4 rules — **out of sample** | 2,449 | 52% | +1,741 | +0.71 |
| consensus selector (must help 5/5 train months) — OOS | 3,207 | 55% | +4,106 | +1.28 |
| *the same fitting scored IN SAMPLE* | *2,760* | *60%* | *+6,281* | *+2.28* |

**In-sample +6,281 → out-of-sample +1,741.** No fitted variant beat V16, and none beat plain
no-filter. Rule stability across folds is near zero (best recurrence: `paradigm!=AG-LIS` 4 of 6).
This is the third independent demonstration in two days that **selection rules fitted to this
dataset do not generalise.**

**V16 is a genuinely good selector: it captures 84% of every available point using 33% of the
signals.** Its problem was never selection quality — it is that a thin book has a bad drawdown.

## V18.3 — The ceiling: how much money exists

| | trades | points | $ @ 1 MES |
|---|---|---|---|
| every signal, no filter, no cap, 6 months | 4,047 | +4,268 | **$21,340** (≈ $3,556/mo) |
| V16 today | 1,326 | +3,566 | $17,827 |
| perfect foresight — only the winners | 2,177 | +22,528 | $112,641 *(unreachable)* |

The concurrency cap binds harder than the filter: at cap 2 the unfiltered portfolio makes
$10,386 (1 MES flat) against $14,218 with no cap at all.

**Conclusion: no filter can produce $5,000/month at 1–2 MES. The entire signal set is worth
about $3,556/month ungated and uncapped.** Selection is not the binding constraint — size is.

## V18.4 — The real path to $5,000/month

After the 0.81 haircut, 100 sessions, S233-relaxed filter:

| configuration | $/month | MaxDD | DD vs $5,161 | DD vs $12,000 |
|---|---|---|---|---|
| V16, 1 MES + basket 2× — **live today** | $1,590 | −$1,253 | 24% | — |
| S233 relaxed, 1 MES + basket 2× | $2,520 | −$727 | 14% | — |
| S233 relaxed, cap 3/3 | $2,991 | −$773 | 15% | — |
| S233 relaxed, **2 MES base** | $3,700 | −$1,618 | **31%** | 13% |
| S233 relaxed, cap 3/3, **2 MES base** | **$4,312** | −$1,712 | 33% | **14%** |

$5,000/month needs roughly **2.5 MES base size**, which needs roughly **$14–15k of equity** to
carry its drawdown at the same 14% risk level the current account runs. From $5,161, that is a
compounding problem measured in quarters, not a filter problem.

## V18.5 — Reality check: the projections have never been met

| | sessions | broker NET |
|---|---|---|
| May 2026 | 11 | +$896 |
| June 2026 | 20 | −$933 |
| July 2026 | 1 | +$550 |
| **live era total** | **32** | **+$512** = ~$336/month |

Against a chain simulation of ~$1,373 for the same days. **But this comparison is confounded** —
those days ran a *different* config (basket blocking at 0/0/1, GEX Long on, cap 3, the pre-S217
trail bug, the June auto-roll incident). Split at the S217 fix, the pre-fix era captured −9% and
the 11 post-fix sessions actually beat the simulation. Both samples are too small to conclude
from. The clean per-trade measurement (81%, 43 matched trades) remains the best estimate.

**The honest position: no projection in this study has ever been validated forward on the
current config. That is the single most valuable thing the next month of live trading buys.**

---

# V19 — EXIT study: re-optimising stop + trail on 5 months (2026-08-08)

Rationale: three tests showed entry-selection rules don't generalise. Exits apply to *every*
trade, so each parameter is estimated on the whole book instead of a selected slice.

Basis: **clean 1-minute SPX OHLC** (`spx_ohlc_1m`, Feb 19 → Aug 7, 118 sessions) — the 2-minute
`chain_snapshots` resolution is the sampling artifact that faked the original Dip-Buy result.
Each trade is walked from its signal to 15:57 ET the same day. **No lookahead**: the earlier
DD study used `actual elapsed + 30 min` as the horizon, which leaks the real exit time.

Scripts: `_tmp_v19_exit.py` (universe + validation), `_tmp_v19_fast.py` (vectorised walk,
proven identical to `mes_walk` on 1,180 trades), `_tmp_v19_grid2.py` (grid + LOMO).

## ⚠️ Harness bug found and fixed mid-study

The first grid (`_tmp_v19_grid.py`, `_tmp_v19_riskadj.py`) memoised on `id(trades)`. CPython
recycles the id of a garbage-collected list, so different training sets silently shared cached
results. **It reported a +30% out-of-sample gain that does not exist.** Corrected harness keys
on `(trade_id, params)` with the trade lists built once. All V19 numbers below are post-fix.

## Validation

| | vs DB chain outcome | vs real broker fills (37 matched) |
|---|---|---|
| 1-min re-sim, live params | 89% direction agreement, mean diff **+0.19 pt/trade**, median 0.00 | MAE 3.15, bias +0.65 |
| DB chain sim | — | MAE 1.44, bias +0.78 |

The 1-min walk is unbiased but **noisier than the existing chain sim** against real fills, so it
is used for *relative* parameter comparison only.

## Result: the live exit parameters are already at the risk-adjusted optimum

1,440 parameter sets per setup (stop 6–20, activation 4–20 or none, gap 2–10, breakeven
none/6/8/10), chosen on five months and scored on the sixth:

| config | trades | WR | points | MaxDD | ret/DD |
|---|---|---|---|---|---|
| **live (today)** | 4,196 | 56.7% | **+5,145** | **−223** | **23.0** |
| fitted OOS on total points | 4,196 | 45.9% | +4,496 | −567 | 7.9 |
| fitted OOS on return/drawdown | 4,196 | 49.6% | +4,068 | −161 | 25.3 |
| fitted OOS on "most points at no worse drawdown" | 4,196 | 42.0% | +4,861 | −422 | 11.5 |

**Nothing beats the live parameters on raw points.** Sized so each config carries today's
drawdown, only the return/drawdown objective edges ahead — $5,063/mo against $4,617/mo, +10% —
and it gets there by making stops *tighter* (6–8 instead of 12–14), with a much worse April,
May and August. Not worth shipping.

Per setup, on the "no worse drawdown" objective: Skew Charm −420, AG Short −116, VIX Div −22,
VPB −105, DD Exhaustion +133 (but drawdown −397 → −565), ES Absorption +245 (drawdown −86 → −101,
ret/DD unchanged at 15.5). No individual setup is a clear win either.

**Why this is the expected answer:** the live parameters are not a neutral baseline — they are
the output of previous walk-forward studies (S224's DD 10/10, the ES Absorption 6/2 change).
The grid confirming them is those studies passing an independent re-test on a longer, cleaner,
lookahead-free basis. **The exits are done. Stop tuning them.**

## Where that leaves the $5,000/month target

| lever | tested | verdict |
|---|---|---|
| entry filter — drop rules | S233 | fails out of sample (+$228/100 sessions) |
| entry filter — refit per setup | V18 | fails out of sample (worse than V16 and than no filter) |
| entry filter — structural relaxation | S233 | **works** (+$930/mo, drawdown 24% → 14% of equity) |
| exits — stop and trail | V19 | already optimal; no change justified |
| **size / capital** | V18.4 | **the only remaining lever**: $5k/mo needs ~2.5 MES ≈ $14–15k equity |

Three of the four levers are now measured and closed. The projection ceiling on the current
account is roughly **$2,500–3,000/month**, and getting past it is a capital problem.
