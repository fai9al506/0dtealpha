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

## V22 — built 2026-08-19, dormant (S313)

**V22 = V21's filter, UNCHANGED, plus a LONG SIZE-UP: `qty = min(qty × 2, 3)` when the previous
session's open→close was < −0.5% and VIX < 24.**

**The pass/fail filter is byte-identical to V21.** Only quantity changes, and quantity is not
something a filter can express — so the whole of V22 lives in `real_trader._v22_long_qty`.

| part | detail |
|---|---|
| short block | **unchanged from V21**: −0.8%, VIX < 24, fails OPEN |
| **long size-up** | **`min(qty × 2, 3)`** at its own **−0.5%** threshold, fails SAFE |
| env | `V22_LONG_SIZEUP_ENABLED` (default **false**) · `V22_LONG_DROP` (−0.5) · `V22_LONG_CAP` (3) |
| revert | set `V22_LONG_SIZEUP_ENABLED=false` → V21 exactly |

### ⚠️ THE TWO HALVES ARE INDEPENDENT — I got this wrong first
I originally tied both halves to one −0.5% threshold and claimed they "must go together".
**The user rejected that on logic — skipping bad trades and doubling good trades should each work
alone — and the data agreed.** Each half works on its own, and they degrade in **opposite**
directions as the trigger widens:

| | at −0.8% | at −0.5% |
|---|---|---|
| block shorts alone | **+$182/mo, LOMO 6/6** | +$218/mo, LOMO **4/6** |
| size up longs alone | +$80/mo, LOMO 6/6 | **+$189/mo, LOMO 6/6** |

So each half is kept at **its own best threshold**. Tying them to one number threw away the better
setting for one of them and cost a month of leave-one-month-out.

**The interaction is POSITIVE, not overlapping (+$50/mo):** a blocked short frees a slot that a
now-bigger long can take. They help each other.

**Division of labour:** the **long size-up is the profit engine**; the **short block is the
drawdown control** — longs alone run MaxDD −$1,605, and the block pulls it back to −$906.

| | $/mo | worst month | MaxDD | LOMO |
|---|---|---|---|---|
| V20 baseline | 2,071 | −225 | −1,585 | — |
| V21 (block only) | 2,253 | +530 | −906 | 6/6 |
| **V22 = V21 + long size-up** | **2,491** | **+1,181** | **−906** | **6/6** |
| *(rejected: both at −0.5)* | *2,549* | *+1,266* | *−906* | *5/6* |

The rejected variant earns $58/mo more and **fails a month**. V22 as built is the only
configuration where **every component is 6/6 on its own and 6/6 together**.

Out of sample (fit Mar–May, scored on the unseen Jun–Aug): **+$598 to +$1,118/mo against +$86 in
sample**. Random control p = 0.003. Trade-by-trade audit: deltas reconcile to the dollar,
0 lookahead violations.

### 🔒 THE CAP IS CAPITAL, NOT EVIDENCE
Uncapped doubling earns more but peaks at **8 MES long = $2,120 = 81%** of account `210VYX65`'s
$2,609.80 — and at 81% the broker **rejects the biggest trades after a losing week, exactly when
this rule pays.** Cap 3 peaks at 6 MES = 61% and survives a $1,000 drawdown.

> **RAISE `V22_LONG_CAP` TO 4 WHEN `210VYX65` HOLDS $3,029** (8 × $265 ÷ 0.70).
> Held **$2,609.80** on 2026-08-19 — short by **$419**. **Tasks S314.** Env var only.

**Honest limits:** the long size-up fires on **17 days in 119 sessions**, and **85% of the gain is
June + July**. A bad-regime rule that sits idle in a good one. Evidence:
`S313_PREVDAY_LONG_EDGE.md`.

---

## V21 — THE LIVE FILTER (S300–S307). V22 does not change it; it only adds sizing.

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
