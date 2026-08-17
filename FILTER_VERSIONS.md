# Filter version ledger

**One row per version. What changed, why, and what it measured.**

Rule agreed with the user on 2026-08-17: **when the rules change, the version number changes.**
The portal dropdown shows only the short **name** (`V20 (live)`) — the description of what the
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

## V20 — LIVE from 2026-08-18 (S277 / S278)

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

⚠️ **`live_pass` now means V20**, so the recalled book restates. `LIVE_VER = "v20-sb"`.

---

## V19 — monitoring only (S263)
V18 + no Friday. Research view. **The live Friday behaviour is in V20, not here.**

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
