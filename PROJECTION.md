# Monthly Projection Ledger

**Purpose.** One place that answers "what do we expect to earn per month, and what changed it?"
Every setup added, filter refined, or sizing/cap change gets a row here **with its measured
before/after** — so a projection is never a vibe, and we can see whether shipped changes actually
moved the curve.

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

## Current projection (as of 2026-08-07)

**Config:** V16 base filter · GEX Long **OFF** · basket = **sizing only** (2× on confirm, no block)
· concurrency cap **2/2** · 1 MES base · `SPX_EXIT_ENABLED=true`

| | value | basis |
|---|---|---|
| Theoretical (chain) | **$1,400 / mo** | Jun 1 – Aug 6, 47 sessions, $3,183 total |
| × broker capture 0.81 | **~$1,150 / mo** | 43 executed post-S217 trades: chain +41.5 pt → broker +33.8 pt |
| Expected MaxDD | **−$1,439 (28% of equity)** | same window |
| Sanity floor (ex-top-3 days) | **~$1,500 / mo** | 100 sessions at 1 MES flat cap 3/3, no basket |
| Worst observed month | **−$933** | broker, June 2026 |

**Equity at last check:** ~$5,161 (2026-06-23). **Capital, not the edge, is the scaling limit** —
contracts must track *realised* equity, never the projection. 1 ES ≈ $50k equity; 3 ES ≈ $150k.

---

## Change Log

| date | change | Δ / month (theoretical) | measured on | status |
|---|---|---|---|---|
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

| month | theoretical | **actual (broker)** | ratio | notes |
|---|---|---|---|---|
| 2026-05 | — | **+$895.50** | — | 11 sessions, post-V16.1 |
| 2026-06 | — | **−$933.00** | — | drawdown month: execution bugs, auto-roll incident, macro regime |
| 2026-07 | — | **+$549.50** | — | 1 session only — TSRT disabled 07-01 ~11:57 ET |
| 2026-08 | $1,150 | *pending* | | first month on the new config |

Era total 2026-05-14 → 2026-07-01: **+$512 net / 32 sessions / 283 trades.**

---

## Method notes (why these numbers and not bigger ones)

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
