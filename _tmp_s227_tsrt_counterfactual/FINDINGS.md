# S227 — TSRT counterfactual since disable (2026-07-19 session)

**Question:** TSRT disabled 2026-07-01 ~11:57 ET. If it had stayed armed, what would P&L be?

**ANSWER: −$652** (cap 1L/2S). NOT the +$659 first reported — that first number was
wrong because 33/58 trades fell back to an SPX price-walk. See "The correction" below.

| Cap | Corrected (full MES) | First answer (engine fallback) |
|-----|---------------------|-------------------------------|
| 1L/2S | **−$652** | +$659 |
| 2L/2S | −$713 | +$401 |
| 3L/3S | −$912 | +$651 |

47% WR, MaxDD −$752, 6/12 green days.

---

## Method

- **Candidate set = the 99 `setup_log` rows with `real_trade_skip_reason='master_kill'`**
  since 2026-07-01. These passed every upstream gate (live filter, dispatch whitelist,
  es_px) and were blocked ONLY by the kill switch. No filter guesswork needed.
- **Gates re-simulated in `real_trader.place_trade()` order:** 90s dedup → concurrency cap
  → $300 daily-loss breaker → underwater-stack (S203) → qty via basket 012 (confirm→2).
- **Outcome layer:** `mes_sim_outcome_pnl` where populated; otherwise the production
  simulator (`app/mes_sim_backfill.compute_mes_sim_outcome`) run in-memory. SPX 1-min
  walk (`engine.py`) is the last-resort fallback only.

## The correction (why the answer flipped sign)

GEX Long has **zero** `mes_sim` coverage in the DB (not in `V14_WHITELIST`), and 33/58
July trades had none either. The SPX walk called many of them winners. Real MES bars say
they were stop-outs:

```
#4715 DD Exhaustion  eng +21.6  ->  MES -12.0
#4627 Skew Charm     eng +19.4  ->  MES -14.0
#4687 Skew Charm     eng  +9.9  ->  MES -14.0
#4887 GEX Long       eng  +8.9  ->  MES -14.0
```

Same pattern every time: **SPX held, ES wicked through the stop.** This is the documented
S55 / "chain-sim hides high-vol execution losers" effect. SPX-space simulation is
structurally blind to it.

## Validation (Gate 2 — passed)

Replayed all **31 TSRT-enabled broker sessions** (394 placed trades) vs `tsrt_daily_stmt`:

| | engine fallback | **+ MES gap-fill** |
|---|---|---|
| SIM total | +$895 | **+$631** |
| BROKER | +$344 | +$344 |
| mean daily bias | +$17.8 | **+$9.3** (t=+0.24, ns) |
| median abs daily err | $119 | **$107** |
| sign agreement | 26/31 | 25/31 |

Gap-fill **halves the bias** → the corrected numbers are the trustworthy ones.

Contamination check on the 150 gap-filled rows (known `vps_es_range_bars` replay-burst
risk): mean revision −0.75 pts, median 0.00, 1 implausible row. Not an artifact.

## Live-SB window (the one that matters)

**Live 1-min `semi_basket` capture starts 2026-06-11** (390 rows/day). Before that it is
26 rows/day = 15-min backfill, and only 80% of signals match a basket value vs 100% live.
Any SB conclusion drawn from pre-Jun-11 data is unreliable.

Jun 11 → Jul 17, 27 sessions, 344 candidates, 100% MES coverage:

| Policy | $ total | $/day | MaxDD |
|---|---|---|---|
| Baseline V16 1x (no basket) | −$458 | −18.3 | −$798 |
| SB gate only 1x | −$1,262 | −50.5 | −$1,262 |
| **SB 0/1/2 (CURRENT LIVE)** | **−$1,636** | −65.4 | −$1,710 |
| SB confirm-only 1x | −$469 | −18.8 | −$706 |
| SB confirm-only 2x | −$719 | −28.8 | −$1,192 |

**All negative. Current SB policy is the worst of the five.**

The SB advantage seen on the Mar–Jul backfill window (SB gate +$3,983 vs baseline +$2,538)
**does not reproduce on live data.** Treat it as a backfill artifact.

## Broker truth anchor

`tsrt_daily_stmt`, 31 sessions 2026-05-15 → 2026-07-01: **+$344 net, +$11/day**,
15/31 green, MaxDD −$1,686. May +$727 / June −$933 / Jul 1 +$550.

## Open items

1. **Persist the 524 gap-filled MES values to `setup_log`** — computed in memory only
   (`mesfill_cache.json` here). Production DB write, NOT done — needs user OK.
2. **Add "GEX Long" to `V14_WHITELIST`** in `app/mes_sim_backfill.py` (+ `_DEFAULT_PARAMS`
   entry `sl14/act10/gap5`) so it stops being invisible to every filter study.
3. Understand *why* June–July broke on honest execution data before re-arming anything.
   Rec B (neutral-long/falling-SPX guard) is no longer the main story — the whole set is
   underwater, not just one leak.
4. `railway login` expired all session — live caps/env never confirmed. Cap sweep shows
   the conclusion holds for 1/2, 2/2, 3/3 regardless.

## Files

- `engine.py` — SPX 1-min OHLC walk replicating real_trader trail (BE10/act10/gap5, DD 10/10)
- `mesfill.py` / `run_mesfill.py` — production MES sim over gap rows → `mesfill_cache.json`
- `cf.py <capL> <capS>` — July counterfactual (the headline number)
- `live_sb.py` — live-SB-window policy comparison
- `val2b.py` — broker-truth validation with gap-fill
- `era.py` / `policy.py` / `longwin.py` — long-window (backfill era) comparison
- `overlap.py`, `boot.py`, `cf2.py` — enabled-days overlap, bootstrap CI, leak-fix variants
- `cands.pkl` — 1,109 basket-free V16 candidates since Mar 16 (rebuilt via `live_filter.passes_v16`)

**DB URL is not stored in these scripts' repo copies — set `DATABASE_URL` or pull from
`.claude/settings.local.json` before running.**
