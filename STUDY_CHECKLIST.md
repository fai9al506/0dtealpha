# Pre-study checklist

**Run this list BEFORE any backtest, projection or filter comparison. Every item on it is
here because getting it wrong once produced a wrong number I reported as fact.**

Ordered by how badly each one has burned us.

---

## 1. Model every RISK CONTROL the live system runs, not just the filter

A replay that applies the filter and the cap only is **not** the live system.

| control | where | modelled? |
|---|---|---|
| Position cap 2 long / 3 short | `real_trader._count_active_for_direction` | usually — do not forget |
| 90-second dedup per setup+direction | `place_trade` | usually |
| Basket sizing, **`max(qty,2)` NOT a multiplier** | `_effective_qty` | **got this wrong 2026-08-17** |
| **S203 underwater-stack guard** | `_underwater_stack_check` | **missed entirely 2026-08-17** |
| **S293 per-setup day breaker** | `_day_breaker_check` | new 2026-08-17 |
| $300 daily loss breaker (blocks entries, does NOT flatten) | `place_trade` | rarely — note it |
| Friday block (v7 exempt) | `REAL_TRADE_NO_FRIDAY` | inside `passes_v20` |
| EOD flatten 15:55 | `EOD_FLATTEN_ET` | via `outcome_elapsed_min` |

**Missing the S203 guard made July look $144 worse than it was** and nearly killed a good
scaling rung on a number that was not real.

## 2. Removing a setup is NOT subtracting its P&L

Under a cap, deleted trades free slots that **other, weaker trades then fill**. Switching ES
Absorption off measured **−$1,036**, not the +$827 its own P&L implied. **Always re-run the
whole replay both ways.** Never subtract a column.

## 3. Score TRADES, not SIGNALS

Judging a setup on every signal it emits — including the ones the cap would reject — read Skew
Charm at **$4.5/trade when the trades we actually take earn $13.8**, and raised two false alarms
on our best setup. Replay first, then judge.

## 4. Report min month, max month AND average — plus the distribution

Never an average alone. Always: the **per-month table**, the **worst** and **best** month, the
total **without the best 3 days**, and the total **without the best month**. If one month carries
the result, the projection is not real.

## 5. Report the TRADE COUNT — it is the risk

Trades, trades/day, $/trade, and the delta versus the baseline. A filter that removes coin-flips
is a win even at flat P&L. Leaving this out made V17 (2,111 trades) look competitive with V20
(914 trades) when it earns less for 2.3× the exposure.

## 6. A month is 21 CALENDAR sessions

Never "sessions that had a trade". Books that trade a subset of days are inflated otherwise —
that error read v7 **6× too high**.

## 7. Leave-one-month-out, every time

A rule must help, or at least not hurt, in **every** month. Rules that win overall by winning
hugely in one month and losing in the rest do not survive forward. Also run a **random control**
where one applies (the Friday gate beat 400/400 random blocks — that is the bar).

## 8. Use the right P&L basis

Chain `outcome_pnl` for anything after 2026-06-13. Charge **−0.6 pt/contract + $1.92/contract
round-turn INSIDE the sim, then stop** — do not also multiply by a capture ratio, that
double-counts. `spx_ohlc_1m` (1-minute) is the basis for path and exit studies; `chain_snapshots`
is 2-minute and its coarseness has faked results before.

## 9. Check the data before believing the result

Snapshot counts per day (chain ~195, Volland ~315), feed lag, known outages. The 2026-07-02 →
08-07 ES feed lag contaminates **every ES Absorption study** in that window.

## 10. Env flags change the answer

`VPB_REAL_TRADE_ENABLED`, `VIX_DIV_REAL_TRADE_ENABLED`, `GEX_LONG_V3_REAL_TRADE_ENABLED`,
`BASKET_SIZING_MODE`, `ES_ABS_REAL_TRADE_ENABLED`. **Set them to Railway's values before running
anything locally**, or setups silently vanish from the book — a local run once showed Vanna Pivot
Bounce with zero trades purely because the flag was unset in the shell.

## 11. State the sample size and confidence

Under 50 trades = directional only. 50–100 = moderate. 100+ = high. Say which one it is, and say
what was excluded and why.

---

Related: `PROJECTION.md`, `FILTER_VERSIONS.md`, `SCALING_PLAN.md`, and Gate 0–3 in `CLAUDE.md`.
