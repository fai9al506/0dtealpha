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

## The rungs

| rung | what changes | $/month | worst month | best month | MaxDD | peak short MES | short equity needed |
|---|---|---|---|---|---|---|---|
| **R0** | today, everything 1× | $2,171 | −$484 | +$5,025 | −$1,703 | 6 | $2,271 ✅ |
| **R1b** | **SC-short 2× on STACKED trades only** | **$2,712** | **−$184** | +$6,339 | **−$1,428** | 10 | **$3,786** |
| **R1a** | SC-short 2× on ALL | $3,171 | **+$642** | +$7,511 | −$1,648 | 12 | $4,543 |
| R1c | 2× at slot 1, 3× at slot 2 | $2,932 | −$37 | +$6,733 | −$1,213 | 12 | $4,543 |
| R2a | SC-short 3× on all | $4,172 | +$1,659 | +$9,997 | −$2,545 | 18 | $6,814 |

**R1b is the only step on the board that improves money AND risk together** — +$541/month while the
drawdown falls from −$1,703 to −$1,428 — because the extra contracts go only where the edge is
strongest. Everything else buys money with drawdown.

**Gap risk — a 40-point gap through the stops**, against $6,016 of main capital. The $300 daily
breaker blocks new entries; **it does not flatten**, so a gap is unbounded by it:

| rung | peak | 40-pt gap | % of main capital |
|---|---|---|---|
| R0 | 6 MES | $1,200 | 20% |
| **R1b** | 10 MES | $2,000 | **33%** |
| **R1a** | 12 MES | $2,400 | **40%** ← ceiling for this capital |
| R2a | 18 MES | $3,600 | 60% ⛔ |

---

## The calendar

| due | step | funding needed | gap after | expected |
|---|---|---|---|---|
| now → ~2026-09-15 | **stay R0** | — | 20% | collect 20 clean V20 sessions |
| **~2026-09-15** | **R1b** — SC-short 2× on stacked only | short acct **$3,786** (held $3,271.61 on 08-17 → **$515 short**) | 33% | +$541/mo, drawdown improves |
| **~2026-10-15** | **R1a** — SC-short 2× on everything | short acct **$4,543** | 40% | +$1,000/mo vs R0, every month positive |
| **~2026-11-15** | **review only — do NOT arm R2a** | short acct $6,814 | 60% ⛔ | revisit only on much larger capital |

---

## Gate checklist — ALL FOUR must pass on the due date, or the rung waits

1. **Funding.** Short account (210VYX91) equity ≥ the figure above. No cross-margin: the long
   account cannot fund a short rung. Check with the live TS balances API, never from memory.
2. **Evidence.** ≥ 20 live trading sessions completed at the current rung.
3. **Gap.** 40-pt gap ≤ **40% of main capital**. This, not MaxDD, is the risk gate — MaxDD is
   non-monotonic across rungs and noisy to ±$400.
4. **Clean.** No open defect: no position/broker mismatch, no orphan order, no stuck fill
   (S279), no feed alert.

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
