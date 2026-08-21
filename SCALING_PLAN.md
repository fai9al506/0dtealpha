# Skew Charm scaling plan — Sep / Oct / Nov 2026

**Agreed 2026-08-17 (S286).** Scale the proven setup, not the account. This plan replaces the
"~2026-09-01, SC-short 1× → 2×" line in Tasks S250 with dated rungs, funding gates, and a checklist
that must be re-run on each due date.

**The principle, now measured three ways:** Skew Charm is better stacked (+$24.9 on the 2nd
concurrent short and +$45.3 on the 3rd, against +$11.5 on the first), better sized, and better with
more slots. So the question is not *whether* to scale it but *where each extra contract buys the
most edge per dollar of margin*.

Basis for every number below: 117 calendar sessions 2026-03-02 → 08-17, filter V20, chain
`outcome_pnl`, −0.6 pt/contract and $1.92/contract round-turn charged inside, basket sizing, cap
2 long / 3 short. Margin $265 per MES (measured: 12 MES = $3,178). Accounts are margined
**separately — there is no cross-margin.**

---

## ⚠️ CORRECTED 2026-08-17 evening (S287–S293) — read this before the table above

Three corrections, all mine, all found by checking rather than assuming:

1. **Sizing is `max(qty,2)`, NOT a multiplier.** `real_trader._effective_qty` says so
   explicitly. The first rung table multiplied, inventing 4-contract trades the live system
   would never place. **R1b as the code actually works adds +$341/mo and makes drawdown
   slightly WORSE**, not better.
2. **The replay was missing the S203 underwater-stack guard** the live system runs. With it
   modelled, today's honest baseline is **$2,139/mo**, and July's scaling damage falls from
   −$260 to −$116.
3. **R1d is the chosen rung** (2 contracts on the 2nd concurrent SC short, 3 on the 3rd) —
   user decision, safer and fundable today.

### The chosen configuration

| | $/month | worst month | worst day | MaxDD |
|---|---|---|---|---|
| today | $2,139 | −$484 | −$697 | −$1,783 |
| R1d alone | $2,604 | −$476 | −$672 | −$1,880 |
| **R1d + day breaker (S293)** | **$2,642** | **−$52** | **−$374** | **−$1,700** |

**The S293 day breaker is a PREREQUISITE for R1d, not an option.** R1d alone makes the tail
worse; with the breaker every risk measure beats today's. Shipped and audited 2026-08-17.

### Why July was the one bad month — answered

29 July was **FOMC** (SPX −1.32%); 30 July was the **relief rally** (SPX +1.7%, Nasdaq +2.8%,
MSFT +16%) plus Core PCE, Advance GDP, BOE and BOJ. Both were trend days, and Skew Charm fired
short into them repeatedly — six full stops in a row on 07-30. The Discord export for that
window says it plainly: *"violent V-bottom off the 7/29 FOMC flush… **Every fade lost.**"*

Data was clean (chain 195/day, Volland 314–329). **Tested and REJECTED:** skipping strongly
contradicting-basket trades (fixes July, breaks June, +$69/mo, and the most-extreme bucket is
positive) and skipping FOMC days (we make **+$147/day on FOMC and +$271 the day after**, both
better than the +$118 average — skipping costs $105/mo).


---

## The rungs — corrected basis (max sizing + S203 guard modelled)

| config | $/month | worst month | worst day | MaxDD | peak short | equity needed |
|---|---|---|---|---|---|---|
| **R0 — today** | $2,139 | −$484 | −$697 | −$1,783 | 6 MES | $2,271 ✅ |
| R1d alone | $2,604 | −$476 | −$672 | −$1,880 | 8 MES | $3,029 ✅ |
| **R1d + S293 day breaker** | **$2,642** | **−$52** | **−$374** | **−$1,700** | 8 MES | **$3,029 ✅** |

**R1d = 2 contracts on the 2nd concurrent Skew Charm short, 3 on the 3rd.** The first short of
a cluster is unchanged. Size follows the edge: the 2nd stacked short earns +$24.9 and the 3rd
+$45.3, against +$11.5 for the first, and that premium survives a within-day control (+$19.1 vs
+$10.1 on the SAME days), so it is not a good-day illusion.

**Fundable today — no transfer needed.** Peak 8 contracts = $2,120 margin against the short
account's $3,271.61.

⚠️ **The other rungs (R1a/R1b/R1c/R2a) were measured on the multiplicative basis and are NOT
valid. Re-measure them with `max` sizing and the S203 guard before quoting them again.**

**Gap risk** — 40-pt gap through the stops, against $6,016 main capital. The $300 breaker blocks
new entries but does **not** flatten, so a gap is unbounded by it:

| config | peak | 40-pt gap | % of main capital |
|---|---|---|---|
| R0 today | 6 MES | $1,200 | 20% |
| **R1d** | 8 MES | $1,600 | **27%** |

---

## 🚨 THE DAILY BREAKER MUST SCALE WITH SIZE (added 2026-08-17)

`REAL_TRADE_DAILY_LOSS_LIMIT` is a **fixed dollar amount** (default $300). It does **not**
scale with contract size. Left alone while size grows it trips on the first bad trade of the
day and silently guts the book:

| size | breaker left at $300 | trades taken over 117 sessions |
|---|---|---|
| 1 MES | −$300 | 868 |
| 2 MES | −$300 | **754** |
| 1 ES (10×) | −$300 | **567** |

**Rule: at every rung, set `REAL_TRADE_DAILY_LOSS_LIMIT` = $300 × the size multiple.** This is
now gate 5 on every scaling step.

**And budget for overshoot.** The breaker refuses new entries; it does **not** flatten, and it
counts **realized** loss only, so open positions are invisible to it. Measured on 37 live days
the worst real day was **−$332.82 against a $300 limit — about 10% past.** Expect the same
proportion at any size: a $3,000 limit realistically ends near −$3,300.

**Why this matters at scale** — the June 5–12 streak, with the breaker scaled correctly:

| size | breaker | that week |
|---|---|---|
| 1 MES | −$300 | −$1,219 |
| 2 MES | −$600 | −$2,438 |
| 5 MES | −$1,500 | −$6,096 |
| **1 ES (10×)** | −$3,000 | **−$12,192** |
| 2 ES (20×) | −$6,000 | −$24,383 |

A bad week scales linearly. Size the book to survive **−$12,000 in five sessions** before
going to 1 ES.

---

## The calendar

| due | step | funding | expected |
|---|---|---|---|
| now → ~2026-09-01 | **stay R0**, S293 breaker live and watched | — | ~10 sessions of V20 + breaker |
| **~2026-09-01** | **R1d + breaker** | **already funded** ($3,029 needed, $3,271 held) | +$503/mo and every risk measure better than today |
| ~2026-10-15 | review a further rung | re-measure first | only on the corrected basis |

---

## Gate checklist — ALL FOUR must pass on the due date, or the rung waits

1. **Funding.** Short account (210VYX91) equity ≥ the figure above. No cross-margin. Read it
   live from the broker, never from memory.
2. **Evidence.** ≥ 10 live sessions on V20 with the S293 breaker running.
3. **Gap.** 40-pt gap ≤ 40% of main capital.
4. **Clean.** No open defect: no mismatch, no orphan, no stuck fill, no feed alert.
5. **Breaker scaled.** `REAL_TRADE_DAILY_LOSS_LIMIT` raised to $300 × the size multiple,
   verified on Railway, before the size change goes live — not after.

**If any gate fails, the rung waits and the date moves. Dates are targets, funding is the rule.**

## How the size is applied

R1b sizes **only Skew Charm SHORT trades that fire while at least one same-side position is already
open**. That is `slot >= 1` in the replay. It is a change to the sizing function, not to the filter
— V20 admits exactly the same trades either way, so the trade count is unchanged at 914 and no new
signal type is introduced.

## What this plan does NOT do

- **It does not touch the longs.** SC long earns +$13.6 on the first position and +$11.4 on the
  second — it does not improve with stacking, so there is nothing to concentrate.
- **It does not raise the cap.** Cap 2/3 is confirmed: money rises with the cap but drawdown rises
  faster (8/8 gives +29% money for +90% drawdown).
- **It does not double the whole book.** That earns about the same as SC-short 3× but with 39%
  more drawdown, and it needs long-account margin too.

Related: `PROJECTION.md`, `FILTER_VERSIONS.md`, Tasks S250/S286, memory
`research_cap_2_2_confirmed_s247`, `research_es_abs_vol_dependent_v20`.
