# Filter version ledger

**One row per version. What changed, why, and what it measured.**

Rule agreed with the user on 2026-08-17: **when the rules change, the version number changes.**
The portal dropdown shows only the short **name** (`V21 (live)`) — the description of what the
version contains lives here, never in the label.

A version is a set of **rules**. Turning a setup on or off with an env var is *configuration*, not
a new version — unless it changes which trades we take, in which case it is a rule and it gets a
number.

**Every version must exist in lockstep in 5 places**, or the portal will tell a different story
from the trader (this has broken twice — see `feedback_portal_view_can_hide_real_trades`):

1. `app/real_trader.py` — the whitelist at the real-money choke point
2. `app/main.py :: _passes_live_filter` — the runtime that decides live trades
3. `app/live_filter.py :: passes_vXX` — the canonical filter, stamps `setup_log.live_pass`
4. `app/main.py` portal JS — `passesStrategy(l, 'vXX')`
5. `app/main.py` portal JS — `_tlPassesStrategy(l, 'vXX')`

**After ANY change run `python filter_mirror_sweep.py vXX` and require 0 mismatches.**

---

## V20 — superseded by V21 on 2026-08-18 (S277 / S278)

**V20 = V16 rules + ES Absorption only when VIX ≥ 20 + no Friday.**

| part | detail |
|---|---|
| Base | every V16 rule, unchanged |
| **New rule** | ES Absorption requires **VIX ≥ 20**. Fails **closed** on a missing VIX |
| Also included | the Friday block (`REAL_TRADE_NO_FRIDAY`, armed 2026-08-15, S263) — v7 exempt |
| Env | `ES_ABS_VIX_FLOOR = 20.0` in `live_filter.py`; `ES_ABS_REAL_TRADE_ENABLED` is an emergency kill only, default **true** |

**Why.** ES Absorption's edge is volatility-dependent. V16 book, costs charged, longs only:

| VIX | trades | per trade | win rate |
|---|---|---|---|
| under 18 | 34 | −$2.6 | 40% |
| 18–20 | 46 | −$6.4 | 57% |
| **20–22** | 20 | **+$21.6** (t=+2.4) | 75% |
| **22–26** | 54 | **+$15.2** (t=+2.7) | 74% |
| 26+ | 43 | −$1.0 | 53% |

March–April averaged VIX 24.8 → **+$1,239**. May–August averaged VIX 18.1 → **−$412**.

**Two alternative explanations were tested and rejected.** It is **not** the Rithmic → Sierra ES
feed switch of 2026-04-30, which lands on the same boundary: high-volatility trades on the *new*
feed still win (+$59, 67% WR) and low-volatility trades on the *old* feed were fine. It is **not**
market-wide: Skew Charm still earns **+$18/trade below VIX 18** on the same sessions, so the
weakness belongs to this setup.

**Whole-book effect, cap 2/3 replayed** (this matters — removing a setup frees slots that other,
worse trades then take):

| option | book | vs V16 |
|---|---|---|
| V16 as-is | +$9,574 | — |
| switch ES Abs off entirely | +$8,538 | **−$1,036** |
| **V20 — floor at 20** | **+$9,669** | **+$95** |
| band 20–26 | +$9,789 | +$215 |
| floor at 22 | +$9,237 | −$337 |

**Floor, not band, on purpose.** The band scores $120 better over 5.5 months, but that gain comes
entirely from cutting VIX 26+, which is merely flat (−$1/trade), not harmful. An upper bound is
the part most likely to be curve-fitting. One rule, one number, one stated mechanism: absorption
needs volatility to mean anything.

**It also solves an operational problem.** VIX ≥ 20 happened on 0 of 53 sessions in May, July and
August, so the floor keeps ES Absorption dormant exactly when it should be — but re-arms itself
automatically when volatility returns, with nobody having to remember.

Sweep: **0 mismatches, 16 setups, 3,052 signals.** ES Absorption passes 6 instead of 84.

⚠️ Superseded: `live_pass` now means **V21**. V20's ES Absorption floor is still live inside V21.

---

## V22 — LIVE from 2026-08-20 (S313)

**V22 = V21 with the threshold widened −0.8% → −0.5%, PLUS the LONG half of the same effect.**

| part | detail |
|---|---|
| SHORTS | skipped when the **previous session's open→close < −0.5%** and this signal's VIX < 24 |
| **LONGS** | **`qty = min(qty × 2, 3)`** on those same days — this is new, and it is the bigger half |
| where the long half lives | `real_trader._effective_qty` → `_v22_long_qty`. **A filter cannot express quantity**, so it is not in `passes_v22` |
| env | `V22_PREV_DROP` (−0.5) · `V22_LONG_CAP` (3) · `V22_LONG_SIZEUP_ENABLED` (default **false**) |
| revert | `V22_PREV_DROP=-0.8` + `V22_LONG_SIZEUP_ENABLED=false` restores V21 exactly |

**Why the long half exists.** V21 only ever blocked shorts. On the very same days the longs are
the better trade and nothing was done about it. At **day** level (a day's trades share one tape,
so days are the sample size — counting 31 correlated trades as 31 observations was the error that
started this study):

| previous session | SHORTS | LONGS |
|---|---|---|
| open→close < −0.8% | −5.44 pt, **0/4 days green** | **+5.32 pt, 5/6 days** |
| open→close −0.8..−0.5% | +1.15 pt, 4/7 | **+5.74 pt, 5/6** |
| 2-session cumulative −1.5..−0.8% | −0.20 pt, 3/8 | **+8.77 pt, 7/8** |
| no gap, ground down all day | −3.71 pt, 1/4 | **+8.06 pt, 5/5** |

Six independent slicings agree. **Sizing longs WITHOUT blocking shorts measured $2,178/mo and
MaxDD −$1,601 — worse than doing nothing. The block and the size-up are ONE rule.**

**Measured** — 119 sessions, V20 + cap 2/3 + dedup + S203 + $300 breaker + basket sizing, costs
inside:

| | $/mo | worst month | MaxDD | peak LONG |
|---|---|---|---|---|
| V20 baseline | 2,071 | −225 | −1,585 | 4 MES |
| V21 (block only) | 2,253 | +530 | −906 | 4 MES |
| **V22** | **2,549** | **+1,266** | **−906** | **6 MES** |

Out of sample (fit Mar–May, scored on the unseen Jun–Aug) the edge is **larger** than in sample:
**+$1,118/mo vs +$86**. Random control, same action on the same number of randomly chosen days,
300 trials: **p = 0.003**. Trade-by-trade audit: 17 trigger days, deltas sum exactly to the replay
difference (+$2,706), 0 lookahead violations, 13 days positive-or-zero against 4 small negatives.

### 🔒 THE CAP IS A CAPITAL CONSTRAINT, NOT A RESULT
Uncapped doubling earns **more** — $2,643/mo, worst month +$1,525 — but peaks at **8 MES long =
$2,120 margin = 81%** of the long account's $2,609.80. **At 81% the biggest trades get rejected
after a losing week, which is exactly when this rule pays.** Cap 3 peaks at 6 MES = $1,590 = 61%
and survives a $1,000 drawdown, keeping 76% of the gain.

> **RAISE `V22_LONG_CAP` TO 4 WHEN ACCOUNT `210VYX65` HOLDS $3,029.**
> (8 MES × $265 ÷ 0.70 comfort rule.) It held **$2,609.80** on 2026-08-19 — **short by $419.**
> Tracked as **Tasks S314**. Change the env var only; no code change, no new version.

**Honest limits:** 17 trigger days in 119 sessions; **85% of the gain is June + July**; nothing in
March, May or August. It is a bad-regime rule that sits idle in a good one, and it is the only
tested variant that loses in any month (August, −$74). Full evidence: `S313_PREVDAY_LONG_EDGE.md`.

---

## V21 — superseded by V22 on 2026-08-20 (S300–S307)

**V21 = V20 + no SHORTS when the previous session fell more than 0.8% AND VIX < 24.**

| part | detail |
|---|---|
| Base | every V20 rule, unchanged |
| **New rule** | shorts only: skip when `prev_day_move < −0.8%` **and** `vix < 24` |
| Data source | **`spx_ohlc_1m` (1-minute bars) — NEVER `chain_snapshots`** |
| Fail behaviour | **OPEN** — unknown move or unknown VIX → the trade is taken |

**Why.** After a session that fell more than 0.5%, the next day averaged **+0.22% and rose 68%
of the time**. Our fade shorts sold into that bounce. Per trade, shorts after a down day earn
**+0.81 pt against +4.57 pt otherwise**; after two down days they are **negative**.

**The VIX ceiling is what makes it safe — the effect inverts in high volatility.** At VIX 26+
shorts after a down day earn **+15.74 pt at 100% WR**, because the selling continues instead of
bouncing. Without the ceiling the rule deletes March's **+150 pts** of high-volatility shorts and
is worth **exactly zero**. With it, all 24 of those March trades are kept.

| | V20 | **V21** |
|---|---|---|
| per month | $2,187 | **$2,372** |
| worst month | −$225 | **+$530** |
| max drawdown | −$1,585 | **−$906** (−43%) |
| worst 5-day window | −$1,196 | **−$752** |
| the June 5–12 streak | −$1,219 | **−$465** |
| leave-one-month-out | — | **6/6** (helps 3, unchanged 3, hurts 0) |

Blocks **27 shorts on 4 days in 6 months** — 04-22, 06-08, 06-11, 07-30 — worth −147 pts at
**37% WR (t = −2.01)**, and it beats **500 random 27-short removals on every metric (96–100%)**.

**⚠️ MEASURE THE PREVIOUS MOVE ON `spx_ohlc_1m`, NEVER ON `chain_snapshots`.** The 2-minute
snapshots begin at 09:32 against the bars' 09:31, and that one missing minute shifts the daily
figure by up to 0.08% — enough to flip whole days across the −0.8% line. On snapshots the same
rule reads 37 trades at 51% WR (t = −0.81) and looks like noise. Same rule, different sampling,
opposite conclusion. This cost an hour and nearly killed a good rule.

**Honest limits:** 4 days in 6 months, and 2026-06-11 alone is 59% of the value. It does nothing
in 3 of the 6 months. **It is insurance, not income.**

Sweep: **0 mismatches, 16 setups, 3,058 signals.** `LIVE_VER = "v21-sb"`.

---

## V19 — monitoring only (S263)
V18 + no Friday. Research view. **The live Friday behaviour is in V20/V21, not here.**

## V18 — monitoring only, shipped dormant 2026-08-15 (S260)
V16 minus SHORTS with a +net-GEX wall within 15 pt overhead while VIX < 22. Needs **net** gex and
the **largest** strike (`gex_call_wall` does not reproduce it). At the live 2/3 cap: +$492,
max drawdown −$1,598 → −$1,226, green days 75 → 80. Nothing in the trade path reads it.

## V17 — monitoring only (S233, 2026-08-08)
V16 with the per-setup **quality** rules skipped when the signal's own VIX < 22, for Skew Charm /
AG Short / ES Absorption / DD Exhaustion / VIX Divergence. Full V16 still applies at VIX ≥ 22, DD
shorts still face the V13 stack, Vanna Pivot Bounce is never relaxed. **First live verdict was
that its lead is mostly a cap artifact** (+$80, not +$881) — see
`research_v17_first_two_live_days`.

## V16-fri — the previous live view (S263, 2026-08-15 → 2026-08-17)
V16 + no Friday. **Superseded by V20**, which contains it. Kept in the dropdown for old studies.

## V16 — frozen base (2026-05-17)
The rule set every later version builds on. **Do not edit V16 again** — a change to what we trade
gets a new number instead, so the sequence stays readable.
