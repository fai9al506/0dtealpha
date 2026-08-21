# S313 — After a down session, the money is in the LONGS

**Date:** 2026-08-19 · **Status: CANDIDATE FOR V22. Not shipped. User decision.**

Follow-up to S312, at the user's direction: *"test it in different ways, change the numbers, check
the LONGS instead of skipping shorts, try doubling the size."*

## The finding in one line

**V21 blocks shorts after a down session. That is the smaller half of the trade — on those same
days our LONGS run +5.3 pt/trade and are green on 5 of 6 days. Sizing them up is worth more than
the short block, costs no extra margin, and does not worsen drawdown.**

## The map — longs and shorts, same days, DAY level

Day level throughout: a day's trades all face one tape, so **days are the sample size, not trades**.

| Previous session | SHORTS pt/trade | SHORTS days green | LONGS pt/trade | LONGS days green |
|---|---|---|---|---|
| **open→close < −0.8%** | **−5.44** | **0 / 4** | **+5.32** | **5 / 6** |
| open→close −0.8 .. −0.5% | +1.15 | 4/7 | **+5.74** | **5/6** |
| close→close −0.8 .. −0.6% (today) | −1.81 | 2/5 | **+7.48** | **4/4** |
| 2-session cumulative −1.5 .. −0.8% | −0.20 | 3/8 | **+8.77** | **7/8** |
| no gap, ground down all day | −3.71 | 1/4 | **+8.06** | **5/5** |
| gap −0.5 .. −0.2% | −0.73 | 4/9 | **+5.88** | **7/7** |

**Six independent slicings, same answer.** The long signal is stronger and more consistent than the
short signal it mirrors.

## The actions — full replay

V20 + cap 2 long / 3 short + 90s dedup + S203 + $300 breaker + basket sizing, haircut and fees
inside, 119 sessions. Env flags read from Railway (GEX Long correctly excluded).

| Variant | $/mo | vs V21 | min month | MaxDD | peak MES | **LOMO** |
|---|---|---|---|---|---|---|
| V20 baseline (no rule) | 2,071 | −182 | −225 | −1,585 | 8 | — |
| **V21 today — block shorts only** | **2,253** | — | +530 | −906 | 8 | 6/6 |
| oc<−0.8 · block + **long floor 2** | 2,364 | **+111** | **+1,011** | −906 | 8 | **6/6** |
| oc<−0.8 · block + **long ×2** | **2,401** | **+148** | +751 | −906 | 8 | **6/6** |
| oc<−0.5 · block + long floor 2 | 2,455 | +202 | +1,007 | −906 | 8 | 5/6 |
| oc<−0.5 · block + **long ×2** | **2,643** | **+390** | **+1,525** | −906 | 8 | 5/6 |

**Sizing longs WITHOUT blocking shorts does not work** (oc<−0.8 long-×2 alone: $2,178, MaxDD
−1,601, worse than baseline). The two halves are one rule.

**Drawdown does not get worse.** −906 on every combined variant, against −1,585 for the baseline.
**Peak contracts stay at 8 — the same as today, so no extra capital is required.**

## Robustness

**1. Out of sample — trained Mar–May, scored Jun–Aug (3 unseen months):**

| Variant | TRAIN $/mo | vs base | TEST $/mo | **vs base** |
|---|---|---|---|---|
| V20 baseline | 2,919 | — | 1,117 | — |
| V21 block only | 2,962 | +42 | 1,456 | +338 |
| oc<−0.8 block + long ×2 | 3,011 | +92 | 1,716 | **+598** |
| oc<−0.5 block + long ×2 | 3,005 | +86 | 2,235 | **+1,118** |

**The effect is 6–13× larger out of sample than in sample.** That is the opposite of an overfit.

**2. Random control** — same action, same number of days, days picked at random:

| | real days | random days (300 trials) | p |
|---|---|---|---|
| oc<−0.8 block+×2 | +2,401 | +2,060 (sd 182) | **0.030** |
| oc<−0.5 block+×2 | +2,643 | +2,048 (sd 207) | **0.003** |

**3. Leave-one-month-out** — oc<−0.8 variants are 6/6 (never lose a month). oc<−0.5 is 5/6, losing
$74 in a partial August.

**4. Threshold is a plateau, not a spike.** Every trigger tested (oc<−0.8, oc<−0.5, oc<−0.3,
cc<−0.6, cc<−0.3, cum2<−0.8, gap<−0.2) beats V21 in the block+size form, in a band of
$2,322–$2,455/mo. **The action matters more than the exact number** — that is what a real effect
looks like.

## What actually gets traded

Trigger `open→close < −0.8%` — 9 trading days of 119, 84 signals:

| LONGS (would be doubled) | n | pt/trade |
|---|---|---|
| DD Exhaustion | 15 | **+9.12** |
| Skew Charm | 30 | +2.95 |
| ES Absorption | 5 | **+8.74** |
| Vanna Pivot Bounce | 7 | +4.86 |

| SHORTS (would be blocked) | n | pt/trade |
|---|---|---|
| **AG Short** | 10 | **−9.80** |
| Skew Charm | 17 | −2.88 |

**The short damage is mostly AG Short, not Skew Charm.** At the wider oc<−0.5 trigger, Skew Charm
short is −0.28 pt — essentially break-even — while AG Short is −6.77. A surgical *"block AG Short
after a down session"* is worth testing separately.

## Honest weaknesses

1. **9 trigger days** (16 at oc<−0.5). Small.
2. **85% of the gain is June + July** (+$976 and +$619 of +$1,870). March, May and August get
   nothing. The rule helps in the weak regime and is inert in the strong one — defensible for a
   risk rule, but it is not a broad-based edge.
3. **35 variants were swept.** The random control and the train/test split are there because of
   that, and both pass — but the honest read is "promising", not "proven".
4. **Long ×2 means up to 4 MES on one long.** Two concurrent longs = 8 MES ≈ $2,120 margin against
   the long account's $2,609.80 — 81% utilisation, above the 70% comfort rule. **The "floor 2"
   variant avoids this entirely** and keeps the best worst-month (+$1,011).

## Recommendation

**`oc < −0.8` · block shorts + long floor 2** as the conservative ship: +$111/mo over V21, best
worst-month of every variant tested, LOMO 6/6, no margin change, no drawdown change.

The ×2 and the oc<−0.5 versions earn more and test better out of sample, but one needs margin the
long account does not comfortably have and the other fails a month. **Take the slot, not the size**
— the same principle already applied to v7.

Would ship as **V22** (`FILTER_VERSIONS.md`, `LIVE_VER`). Not shipped. Scripts:
`_tmp_s313_prevday_map.py`, `_tmp_s313b_actions.py`, `_tmp_s313c_robust.py`,
`_tmp_s313d_compose.py`.
