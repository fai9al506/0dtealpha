# S244 — Exelza GEX Dealer-Positioning framework → **GEX Long v7 ("Gamma Support")**

**Candidate name: `GEX Long v7`.** It is a GATE on the existing v6 detector, not a standalone
trigger — what was validated is *GEX Long ∩ SUPPORT* (76 trades), NOT the SUPPORT state firing on
its own. Keeping it in the `GEX Long` family preserves the v3/v4/v6 audit trail and the existing
filter plumbing. **Not implemented in the trade path — see §8/§9.**

**Date:** 2026-08-11 (post-market)
**Source doc:** `C:\Users\Faisa\Downloads\GEX guidance` — 19-page Arabic guide, Exelza "GEX Dealer
Positioning" page. Educational guide to reading dealer gamma positioning and turning it into a
Call/Put decision.
**Scripts:** `_tmp_s244_gexstate_build.py` → `_tmp_s244_join_prior.py` → `_tmp_s244_robust_prior.py`,
`_tmp_s244_final.py`, `_tmp_s244_support.py`, `_tmp_s244_validate.py`
**Data:** `chain_snapshots` 2026-01-20 → 2026-08-11, 25,441 snapshots (2-min, ±100 pt strike window).
Joined to 5,629 fired `setup_log` rows with outcomes. **Strictly-prior snapshot only — no lookahead.**

---

## 1. What the guide actually says

Six numbers per symbol, then an eleven-state classifier, then a reliability ranking.

| Card | Meaning |
|---|---|
| **NET GEX** | positive = dealers dampen moves (range); negative = dealers amplify (trend/violence) |
| **ZERO GAMMA** | the flip line. Above = calm regime, below = volatile. *"The most important level on the page."* |
| **MAX GAMMA** | biggest-gamma strike — magnet / pinning, especially near expiry |
| **PUT WALL** | biggest put concentration = support |
| **CALL WALL** | biggest call concentration = resistance |
| **NET DEX** | net delta exposure. Positive = buying pressure, negative = selling pressure |

**The eleven states** (GEX sign × position vs walls × DEX sign):

| Signal | Conditions | Bias |
|---|---|---|
| HIGH VOLATILITY | within 1% of Zero Gamma | per DEX |
| MEAN REVERSION | GEX+ , price between walls | per DEX |
| RESISTANCE | GEX+ , above Call Wall, DEX ≤ 0 | sell |
| BREAKOUT TEST | GEX+ , above Call Wall, DEX + | buy |
| SUPPORT | GEX+ , below Put Wall, DEX ≥ 0 | buy |
| BREAKDOWN TEST | GEX+ , below Put Wall, DEX − | sell |
| SQUEEZE | GEX− , above Call Wall, DEX + | buy |
| FAILED SQUEEZE | GEX− , above Call Wall, DEX fading | sell |
| ACCELERATION | GEX− , below Put Wall, DEX − | sell |
| SHORT COVER BOUNCE | GEX− , below Put Wall, DEX + | buy |
| CHOPPY | GEX− , price between walls | per DEX |

**The key teaching (ch. 4):** *movement power ≠ reliability.* SQUEEZE/ACCELERATION move hardest but
are only medium-reliable. SUPPORT / RESISTANCE / MEAN REVERSION move least but are the **most
reliable**, because dealer hedging genuinely defends the level. And the final page: *"the three
rules — position vs the walls + GEX regime + DEX direction. The name alone is never enough."*

**Named Call traps** (ch. 2, directly relevant to our complaint): buying a Call glued just under
the Call Wall (no room, dealers sell into it), and buying a 0DTE Call pinned at Max Gamma.

---

## 2. What we did not previously have

We already use TS GEX per strike. We had **never** computed: Zero Gamma, Call/Put Wall as
chain-wide maxima, or **Net DEX** (delta exposure) — the guide's direction-confirmer and the thing
that separates SQUEEZE from FAILED SQUEEZE and SUPPORT from BREAKDOWN TEST.

Built all six cards + the eleven-state label from `chain_snapshots` (call Gamma idx 3, Delta 4,
OI 1; put Delta 16, Gamma 17, OI 19; Strike 10).

**Calibration change that was necessary:** the guide's "within 1% of Zero Gamma" is written for
stocks/ETFs. On SPX 1% ≈ 77 pt, which swallowed 43% of all snapshots. Rescaled to an 8-pt band.
Result is insensitive to this (see §5).

**Known limitation:** our chain window is ±100 pt, so "Call Wall / Put Wall" mean *the dominant
wall within ±100 pt*, not the guide's chain-wide wall. For 0DTE with 10-pt targets the near-spot
walls are the ones that matter, but this is not literally the same quantity the guide shows.

---

## 3. The guide's own reliability ranking — validated on our trades

Only trades we took **in the direction the framework itself recommends**:

| Guide's tier | n | WR | total pt | pt/trade |
|---|---:|---:|---:|---:|
| **quiet-reliable** (SUPPORT/RESISTANCE/MEAN REV) | 282 | **61.0%** | +866.0 | **+3.07** |
| explosive (SQUEEZE/ACCELERATION) | 490 | 54.9% | +1034.7 | +2.11 |
| cautionary (FAILED SQUEEZE/SHORT COVER/CHOPPY) | 281 | 54.8% | +469.4 | +1.67 |
| conditional (BREAKOUT/BREAKDOWN TEST) | 381 | 50.4% | +62.7 | +0.16 |

The ranking the guide asserts is the ranking we measure. And its warning that BREAKOUT TEST is
only good *"with confirmation"* shows up exactly: taken raw it is a coin flip.

---

## 4. The GEX Long diagnosis — the user's premise was wrong, but the setup is broken

**"It fires from the top" is not the problem.** Buying strength is GEX Long's *best* bucket:

| Where in the day's range GEX Long fired | n | WR | total |
|---|---:|---:|---:|
| 0–20% (lows) | 51 | 41.2% | −20.0 |
| 20–40% | 41 | 36.6% | −40.5 |
| 40–60% | 71 | 39.4% | −131.7 |
| 60–80% | 76 | 43.4% | −132.7 |
| **80–100% (highs)** | **147** | **52.4%** | **+167.3** |

(Consistent with the earlier finding in `research_gex_void_long_gate` that top-of-range longs win.)

**The real problem is the dealer-positioning state it fires in.**

| GEX Long, all fired | n | WR | total | pt/trade |
|---|---:|---:|---:|---:|
| price **above** Zero Gamma | 198 | 55.6% | +323.3 | +1.63 |
| price **below** Zero Gamma | 188 | **34.0%** | **−480.9** | −2.56 |

The below-flip bucket loses in 5 of 7 months and is −398 pt post-S217 alone. GEX Long is a
mean-reversion-flavoured long being fired into the amplifying regime, where dealers chase price
away from it.

Adding the full SUPPORT condition is stronger still:

| GEX Long variant | n | WR | total pt | pt/trade |
|---|---:|---:|---:|---:|
| as-is (all fired) | 367 | 45.5% | −122.1 | −0.33 |
| V16 only (what the filter admits) | 89 | 42.7% | −80.1 | −0.90 |
| **+ SUPPORT gate** | **76** | **77.6%** | **+537.6** | **+7.07** |
| + SUPPORT, 10:00–14:00 only | 64 | 81.2% | +493.8 | +7.72 |
| + `spot < put_wall` alone | 136 | 61.8% | +525.7 | +3.87 |

v6-detector era only (2026-06-08+, matches today's live definition): as-is 254 t / −187.8 pt;
with SUPPORT 74 t / **79.7% / +555.1 pt** ≈ **+$1,260/mo ≈ SAR 4,730/mo at 1 MES**.

---

## 5. Robustness of the SUPPORT rule (whole long book, not just GEX Long)

`SUPPORT = net_gex > 0 AND net_dex ≥ 0 AND spot < put_wall AND spot ≤ call_wall AND |spot − zero_gamma| ≥ 8`

> **Definition correction (2026-08-11).** The exploratory scripts omitted the `spot ≤ call_wall`
> branch, so 6 trades were labelled SUPPORT that the guide's decision tree calls BREAKOUT_TEST
> (the tree tests "above Call Wall" *before* "below Put Wall"). `app/gex_state.py` implements the
> tree correctly and is now canonical: **GEX Long + SUPPORT = 76 t / 77.6% / +537.6 pt**, not
> 82 / 76.8% / +563.8. Conclusions unchanged. Section 5 tables below are from the exploratory
> sweep and carry the same 6-trade overcount; the direction and magnitude are unaffected.

Baseline all longs: 2,868 t / 51.1% / +0.60 pt per trade.

**Parameter plateau — all 30 combinations tested** (ZG band 0/5/8/12/20 pt × wall by gamma·OI or
raw OI × proximity threshold 0/2.5/5 pt): every single cell lands **58–68% WR, +2.7 to +5.0 pt per
trade**. No knife-edge.

**Load-bearing test** — it is the *conjunction*, exactly as the guide says:

| variant | n | WR | pt/trade |
|---|---:|---:|---:|
| full SUPPORT | 283 | 61.5% | +3.26 |
| drop the ZG band | 342 | 60.8% | +2.91 |
| drop the DEX condition | 330 | 61.8% | +3.24 |
| drop the GEX condition | 297 | 59.3% | +2.89 |
| **`spot < put_wall` alone** | 1,088 | 51.9% | **+0.77** |

**Blind walk-forward** (rule comes from an outside document, not fitted here):

| window | SUPPORT longs | other longs |
|---|---|---|
| TRAIN Feb–May | 129 t / 56.6% / +2.42 pt | 1,348 t / 52.7% / +1.30 pt |
| **TEST Jun 13–Aug 11** | **139 t / 70.5% / +5.13 pt** | 917 t / 43.9% / **−1.50 pt** |

**Within-day control** (50 days containing both kinds — rules out "it just picks good days"):
SUPPORT longs +3.26 pt/trade vs other longs +1.24 pt/trade on the *same* sessions.

**Not a sub-strike artifact:** requiring a real gap (`put_wall − spot > 5 pt`) keeps it at
222 t / 64.4% / +4.27 pt.

**Not a pullback proxy:** it stacks with buying strength — SUPPORT *and* in the top 20% of the
day's range = 127 t / 71.7% / +5.70 pt.

---

## 6. What did NOT survive

- **Blocking "against the gamma regime" book-wide fails leave-one-month-out** (removes a losing
  bucket in only 3 of 7 months; the blocked bucket was profitable in Feb/Apr/May/Jun). This is a
  **sizing** input, not a block — same lesson as the basket gate.
- **Headroom to the Call Wall** — the guide's "wall within 1% = tiny profit space" trap does **not**
  reproduce for SPX 0DTE. Longs with <5 pt of headroom did 50.0% WR, no worse than mid buckets.
  Our 10-pt target and trailing exit are a different game from the guide's weekly option buyer.
- **Admitting the 192 SUPPORT longs that V16 currently rejects** (+563.6 pt) — top-3 days are 91%
  of that total, and they were never broker-validated. Lottery-ticket profile; not recommendable.
- **Net DEX on its own** is regime-dependent: at VIX < 19, DEX-agreeing trades made +2,990 pt vs
  −795 for disagreeing; at VIX ≥ 21 the relationship **inverts**. Only usable inside the
  conjunction.

---

## 7. The exact window, and what is / is not a real caveat

**Window: 2026-03-04 → 2026-08-10 = 5.2 calendar months.** But the sample is not evenly spread:

| month | trades | signal days | WR | total pt | $ @1 MES |
|---|---:|---:|---:|---:|---:|
| 2026-03 | 2 | 1 | 50% | −3.0 | −15 |
| 2026-04 | 1 | 1 | 100% | +12.2 | +61 |
| 2026-05 | 5 | 2 | 40% | −0.5 | −2 |
| 2026-06 | 25 | 4 | 88% | +228.3 | +1,142 |
| 2026-07 | 19 | 5 | 74% | +93.8 | +469 |
| 2026-08 | 30 | 4 | 77% | +233.0 | +1,165 |

Nominally 5.2 months; **effectively 2.2 months** — Jun–Aug hold 69 of 76 trades and +529 of +538 pt.
Rate over the full window: **+108 pt/mo = $540/mo = SAR 2,024/mo at 1 MES.**

### WITHDRAWN caveat — the MES-sim cross-check

My first write-up flagged "no MES-sim cross-check" as a reason for doubt. **That was wrong and it
is withdrawn.** Entries, trailing and exits are decided on the SPX/portal path
(`SPX_EXIT_ENABLED` → `real_trader.check_spx_trail_exit`); MES is only the instrument. The
chain/SPX simulation *is* the model that matches the code — this is the standing rule in Gate 0 and
in `feedback_chainsim_valid_post_s217`. The only legitimate adjustment is the measured chain-vs-broker
bias of −0.18 pt/trade: over 76 trades that is −13.7 pt, taking +537.6 → **+523.9 pt**. Immaterial.

### ⚠️ CORRECTED — the concurrency cap DOES bite, hard

A first pass assumed a 30-min hold and concluded the cap was harmless. **That was wrong.** Actual
median hold is **108 min** (p90 188 min) and peak concurrency reaches 11 positions.

**The cap counts POSITIONS, not contracts** (`real_trader.py:817` — `active_count >= cap` over
active orders per direction). Under live `BASKET_SIZING_MODE=sizeonly`, a basket-CONFIRMED trade
is sized to 2 MES (`real_trader._effective_qty` → `max(qty, 2)`), so N positions can be up to
2N contracts. `basket_pct` is stamped on 74 of the 76 trades (from 2026-06-12); 49 would size to 2.

| cap (positions) | basket sizing | trades | total pt | $ | maxDD $ | peak MES | margin needed |
|---|---|---:|---:|---:|---:|---:|---:|
| **2 (live)** | off (flat 1) | 42 | +184.3 | +921 | −160 | 2 | $1,400 |
| **2 (live)** | **ON (live)** | 42 | +308.2 | **+1,541** | **−385** | 4 | $2,800 |
| 3 | off (flat 1) | 51 | +258.4 | +1,292 | −210 | 3 | $2,100 |
| 3 | ON | 51 | +445.8 | +2,229 | −435 | 6 | $4,200 |
| 4 | off (flat 1) | 57 | +320.2 | +1,601 | −210 | 4 | $2,800 |
| 6 | off (flat 1) | 66 | +424.7 | +2,124 | −210 | 6 | $4,200 |
| 6 | ON | 66 | +735.4 | +3,677 | −435 | 12 | $8,400 |
| unlimited | ON | 76 | +939.7 | +4,699 | −435 | 22 | $15,400 |

Margin = peak MES × $700 intraday (`real_trader._margin_per_mes`). **Long account balance was
$2,667.93 on 2026-08-10, so ~3 MES is the hard ceiling** — every row needing more than $2,668 is
arithmetic, not a plan. Note the live config (cap 2 + basket sizing) already peaks at 4 MES /
$2,800, marginally over the long account; the margin pre-check may be silently skipping trades.

The big days are gutted by the cap: Jun 30 19 signals → 7 taken, +197 → +63 pt; Aug 3 13 → 3,
+168 → +43 pt. This *is* the V17 cap artifact and it applies.

### Sizing: for THIS setup, more positions beats bigger positions

At identical peak exposure (= identical margin), flat 1 MES with a higher cap dominates basket
doubling with a lower cap — same money, **half the drawdown**:

| same peak exposure | scheme | trades | $ | maxDD $ |
|---|---|---:|---:|---:|
| 4 MES | cap 2 + basket doubling | 42 | +1,541 | −385 |
| **4 MES** | **cap 4, flat 1 MES** | 57 | **+1,601** | **−210** |
| 6 MES | cap 3 + basket doubling | 51 | +2,229 | −435 |
| **6 MES** | **cap 6, flat 1 MES** | 66 | +2,124 | **−210** |

Mechanism: the edge is in the *number of independent confirmations*, not the size of each one
(see the cluster tables below). Doubling one entry doubles its variance; adding a second entry
adds a fresh 77%-win-rate draw. **If this setup is armed it should be EXEMPTED from basket
doubling** and given extra slots instead.

### The one caveat that stands — day concentration

This is genuinely worse than the rest of our book, and it is the whole argument:

| bucket (same window Mar 4 – Aug 10) | trades | signal days | total pt | top-3 days | ex-top-3 |
|---|---:|---:|---:|---:|---:|
| V16 book (what TSRT places) | 1,370 | 110 | +3,190.9 | 21% | +2,515.4 |
| Skew Charm V16 (our best setup) | 752 | 104 | +1,999.7 | 22% | +1,565.7 |
| V16 longs only | 911 | 105 | +1,799.5 | 36% | +1,144.8 |
| **GEX Long + SUPPORT** | **76** | **17** | **+537.6** | **79%** | **+110.8** |

It fires on 17 days in 5 months, and three of them (Jun 30 +197, Aug 3 +168, Jul 31 +85) are 80%
of the money. The other 14 days net +113.2 pt ≈ **$110/mo**. So the honest range is:

- if those three days are the *point* (a gamma-supported long running on a trend day) → **~$540/mo**
- if they were luck → **~$110/mo**

Both are positive. Neither is a loss. The downside is well contained: 6 of 17 days lost money,
worst day **−28.0 pt = −$140**, comfortably inside the $300 breaker.

### Clustering: it IS confirmation — and that is exactly why the cap hurts

76 trades on 17 days ≈ 4.5/day. The clusters are not noise, they get *better* as they extend:

| cluster size (signals that day) | days | trades | WR | pt/trade |
|---|---:|---:|---:|---:|
| 1 trade | 4 | 4 | 75% | +8.93 |
| 2–3 trades | 5 | 11 | 36% | −2.30 |
| 4–8 trades | 5 | 26 | 65% | +4.47 |
| **9+ trades** | 3 | 41 | **95%** | **+10.66** |

| position within the day | n | WR | pt/trade |
|---|---:|---:|---:|
| trade #1 | 17 | 59% | +3.64 |
| trade #2 | 13 | 69% | +5.53 |
| trade #3 | 9 | 67% | +2.01 |
| trade #4 | 8 | 75% | +7.85 |
| trade #5 | 6 | 67% | +6.82 |
| **#6 and later** | 29 | **97%** | **+10.63** |

This is knowable in real time (at trade #6 you know 5 have already fired). But the edge lives in
trades 3–19 of a cluster and a 2-slot book can only hold 2 — so the confirmed part is precisely
what we cannot harvest. Two ways to capture it, both costing risk:

| scheme | cap | trades | total pt | $ | maxDD pt | peak MES | worst day |
|---|---:|---:|---:|---:|---:|---:|---:|
| flat 1 MES | 2 | 42 | +181.1 | 905 | −32.1 | 2 | −28.0 |
| +1 MES from trade #3 | 2 | 42 | +228.4 | 1,142 | −59.5 | 4 | −31.5 |
| ramp 1/1/2/2/3 | 2 | 42 | +266.8 | 1,334 | −59.5 | 6 | −31.5 |
| **flat 1 MES, cap 3** | 3 | 51 | +259.1 | 1,296 | **−42.1** | 3 | −28.0 |
| +1 MES from trade #3 | 3 | 51 | +384.4 | 1,922 | −79.5 | 6 | −51.5 |
| flat 1 MES, cap 6 | 6 | 69 | +427.1 | 2,136 | −42.1 | 6 | −28.0 |

**Raising the cap is cleaner than sizing up** — cap 3 buys +$390 with drawdown unchanged, whereas
sizing to 2 MES from trade #3 buys +$236 and nearly doubles drawdown.

### Tail risk — the honest unknown

All **3** big-cluster days (Jun 30, Aug 3, Aug 7) won. We have **never observed a large cluster
fail**, so the flat −42 pt drawdown across caps is an artifact of n=3, not proof of safety. A
failed cluster costs roughly `contracts × SL 14 pt × $5`:

| exposure | cost of a failed cluster |
|---|---|
| cap 2, 1 MES | 28 pt = $140 |
| cap 3, 1 MES | 42 pt = $210 |
| cap 2 sized to 2 MES | 56 pt = $280 |
| cap 6, 1 MES | 84 pt = $420 (breaker trips at $300 realized) |

Mitigation already in place: the $300 realized breaker blocks new entries, so a cluster that starts
losing stops growing — it cannot reach 12 contracts. The S203 underwater-stack guard also blocks a
3rd same-direction entry while 2 open ones are net losing.

### Did the setup "protect itself" in the bearish months? — half true

| month | SPX change | % of all snapshots in SUPPORT structure | GEX Long fires | of which SUPPORT | pass rate |
|---|---:|---:|---:|---:|---:|
| 2026-02 | −56 | 11.4% | 21 | 0 | 0% |
| 2026-03 | **−286** | **5.1%** | 29 | 2 | 7% |
| 2026-04 | **+650** | **18.0%** | 15 | 1 | 7% |
| 2026-05 | +340 | 10.8% | 38 | 5 | 13% |
| 2026-06 | −73 | 13.3% | 69 | 25 | 36% |
| 2026-07 | +33 | 11.5% | 111 | 19 | 17% |
| 2026-08 | +203 | 21.1% | 84 | 30 | 36% |

**True for March:** the market genuinely offered almost no SUPPORT structure (5.1%, the lowest
month) in the worst down month (−286). The state itself stands aside when dealers are not
positioned to defend a level — that is real self-protection.

**Not true for April:** SPX +650, the second-richest month for SUPPORT structure (18.0%), and GEX
Long converted only 1 of 15 fires. The opportunity was there and the detector missed it.

The pass rate steps up 7–13% → 17–36% exactly when the **v6 detector shipped (2026-06-08)**. So most
of the "it didn't fire early on" is the detector changing, not the market. Practical consequence:
**the 5.2-month framing overstates the observation window — the setup as it exists today has really
only been visible for ~2 months.**

### Other notes

- **Join reconciliation (Gate 2):** SQL 387 t / −165.6 pt; joined 367 t / −122.1 pt; 20 dropped ×
  −43.5 pt = exact match. All 20 dropped are pre-09:32 fires with no prior snapshot — they are
  *losers*, so the baseline flatters the current setup, not the proposed gate. A live SUPPORT gate
  would also refuse to fire before the day's first snapshot.
- **Sample size** 76 trades = *moderate confidence* per the protocol, and mostly one detector era
  (v6, from 2026-06-08).

---

## 8. Built (2026-08-11)

`app/gex_state.py` — **monitoring-only, fail-soft, zero touch to the trade loop.**

- `compute(spot, rows)` — pure function, the single implementation of the six cards + 11-state
  tree. Both the live path and any backfill call it, so they cannot drift (the failure mode that
  produced the v4 over-optimism, where `gex_long_v3._features` graded on Volland while the live
  detector used TS).
- `capture()` — scheduler job every 2 min, 09:30–16:05 ET, reads the newest `chain_snapshots` row
  and upserts `gex_state` (et PK, six cards + jsonb payload).
- `stamp_setups(days)` — cron 16:30 ET, writes `setup_log.gex_state / gex_net_dex / gex_net_gex /
  gex_zero_gamma / gex_call_wall / gex_put_wall` using the last snapshot **at or before** each
  entry (strictly prior — never a later one).
- `latest()` / `history(date)` — read accessors behind `GET /api/gex-state/latest` and
  `GET /api/gex-state/history?date=`.

Verified live 2026-08-11 18:xx ET: capture wrote a row, `stamp_setups(5)` stamped 156 `setup_log`
rows, and the module independently reproduces the study headline from raw chain data.

No portal page yet — the API is there for one.

---

## 9. Recommendation

**Do not ship a filter change to real money off this.** Ship instrumentation first — that is free
and it converts the whole question into forward-validated data:

1. **`app/gex_state.py` (monitoring-only, fail-soft).** Compute the six cards + eleven-state label
   each cycle from the chain we already pull. Stamp `setup_log` with `gex_state`, `net_dex`,
   `zero_gamma`, `call_wall`, `put_wall`. Zero trade-path coupling — same pattern as `darkmate.py`.
2. **Portal page `/gex-state`** — the Exelza card layout for SPX 0DTE, plus per-strike gamma and
   the state history for the day. Immediately useful as a manual-trade map, like `/darkmate-fw`.
3. **Then, after ~30 forward sessions**, decide on GEX Long: re-specify its entry as
   `v6 magnet logic AND SUPPORT state AND 10:00–14:00`, expected ~9 fires/month. GEX Long is
   already OFF for real money (`GEX_LONG_V3_REAL_TRADE_ENABLED=false`), so there is no bleed while
   we wait, and nothing to revert.
4. Keep the gamma-regime flag as a **sizing** candidate for the wider book, not a block. On the
   post-S217 V16 book: flat 1× = +181 pt / DD −263; 1× with-regime / 0.5× against = +389 pt /
   DD −161 (return-per-DD 0.69 → 2.42). That is the shape worth forward-testing — it does not
   raise peak exposure.

### Final position on the four open questions

| question | answer | why |
|---|---|---|
| **Arm now?** | **Yes, small** | Worst observed day −$140; even the pessimistic ex-top-3 read is positive; it earns $0 today because it is off. Live broker data beats another month of simulation. |
| **Sizing?** | **Flat 1 MES** | At equal margin, more positions beat bigger positions — same $, half the drawdown (§7). |
| **Basket doubling?** | **No — exempt this setup** | Basket 2× adds +$620 but takes drawdown −$160 → −$385 and needs 4 MES the long account cannot fund. |
| **Cap exception?** | **Yes, to 3 — not more** | Cap 3 flat = +$1,292 / DD −$210 / 3 MES ≈ $2,100 margin, at the ceiling of the $2,668 long account. Higher caps are unfundable, not merely risky. |

Recommended config — **GEX Long v7**: **v6 magnet logic AND SUPPORT state AND 10:00–14:00,
cap 3 positions, flat 1 MES, basket doubling disabled for this setup.** Expected ≈ 9 fires/month, +$249/mo ≈ SAR 934/mo, drawdown
−$210, peak 3 MES.

**Stop rule for the trial:** halt if a large cluster (6+ same-day signals) closes net negative —
that is the one scenario never observed in 5 months and the only thing that would falsify the
clustering-is-confirmation premise. A red *day* is normal; a red *cluster* is new information.

**What is still unknown:** the real observation window is ~2 months, all 3 big clusters won, and
on Jun 30 the setup alone would have wanted 11 concurrent longs (13 across the whole book). The
margin ceiling is what protects against the worst version of that, not a designed control.
