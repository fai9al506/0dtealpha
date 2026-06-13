# GEX Long — TS GEX vs Volland Gamma: Full Analysis (2026-06-08)

## The question
"Is our GEX Long study/backtest based on TS GEX (correct, = live) or Volland gamma (wrong)?"
You were confident it was TS. The answer turned out to be **partly both** — and the part that
was Volland is the part that mattered.

---

## TL;DR (the bottom line)
1. **Live trading fires on TS GEX** — correct, unaffected. ✅
2. **The buried-magnet veto backtest** (`_tmp_buried_magnet_verify.py`) used TS GEX. ✅
3. **The v3.2 review report** (`gex_long_v32_review.html`): its **charts, magnets, targets, and
   WR/PnL are TS** (you were right) — BUT the step that **picks which 18 trades** runs on
   **Volland gamma** (`gex_long_v3._build_cache` → `_features`). ❌
4. Volland and TS classify GEX structure **very differently** (verdicts agree only **41%**).
5. So the 18 trades you reviewed are **not** the set live fires. Only **12 overlap**. Your
   study **dropped 6** trades live won't fire and **never showed 9** trades live does fire.
6. **Those 9 unseen trades are ALL losses.** That's why your report showed 78% but the
   real (TS) set grades ~**59–60%**.
7. **No real money was lost** — GEX Long has never fired live (0 rows in `real_trade_orders`);
   first fire would have been Monday. We caught it in time.
8. **Action taken:** paused live GEX Long (`GEX_LONG_V3_REAL_TRADE_ENABLED=false`).

---

## Where TS vs Volland actually live in the code

| Component | File | GEX source |
|---|---|---|
| LIVE detector (what fires real trades) | `app/main.py::_gex_long_v3_features` | **TS chain** ✅ |
| Buried-veto backtest | `_tmp_buried_magnet_verify.py` | **TS chain** ✅ |
| v3.2 review report — **charts/targets/outcomes** | `_tmp_build_gexlong_report.py` (lines 53–78) | **TS chain** ✅ |
| v3.2 review report — **trade SELECTION** | `_build_cache`→`gex_long_v3._features` | **Volland gamma** ❌ |
| Portal v3.x dropdowns + V16 (the overlay) | `app/gex_long_v3.py::_features` | **Volland gamma** ❌ |

TS GEX = `chain_snapshots`, per strike `C_Gamma*C_OI − P_Gamma*P_OI` (positional cols
Strike=10, C_OI=1, C_Gamma=3, P_Gamma=17, P_OI=19). Charm is Volland in BOTH (correct).

---

## The hard numbers

**Magnet strike agreement (Volland vs TS):** 38/112 = **34%** agree.
(lid 3156 — the one you checked, 7500 magnet — was one of the 34% that agree, so it looked
TS-confirming but didn't actually distinguish the two.)

**Verdict agreement (what actually drives selection):** 46/112 = **41%** agree.

**pass_v3.2 selection overlap:**
- BOTH = **12**  · Volland-only = **6**  · TS-only = **9**
- Volland v3.2 total = 18 (= your report) · TS v3.2 total = 21–22

**Performance (re-simulated SL14 / magnet target / trail 15-5):**
- Volland-selected 18 (your report): ~78% WR
- TS-selected 22 (what live fires): **13W / 9L = 59% WR / +115.1p**
- v4 (TS + buried veto), trail-only (the shipped exit): **20t / 60% WR / +98.9p ≈ ~$140/mo @1MES**
- 15-min cooldown removes 0 trades (already spaced) — does not rescue the gap.

So v4 still has **positive edge on correct data**, just ~half the PnL and lower WR than the
Volland study implied (it was **16t / 75% / +176.9p** — over-optimistic).

---

## Why the gap exists (the 9 unseen losers)

The 9 **TS-only** trades (live fires, your report never showed) — **every one is a loss**:
`#200 #346 #429 #762 #1457 #3094 #3183 #3186 #3192`

Several cluster on **GEX-TARGET** paradigm in the **afternoon** (#3094 10:07, #3183 13:54,
#3186 14:14, #3192 14:29 on May 21–22). The Volland verdict happened to reject these; the TS
verdict (live) accepts them → live takes a block of losers your study never saw.

### The 3 buckets (lids to review)
- **SHARED 12** (live fires + you reviewed): `#123 #340 #439 #445 #630 #640 #2432 #2881 #2882 #2884 #3148 #3156`
- **VOLLAND-ONLY 6** (your 18, TS drops, live won't fire): `#227 #263 #456 #798 #1504 #1642`
- **TS-ONLY 9** (live fires, never reviewed — all losses): `#200 #346 #429 #762 #1457 #3094 #3183 #3186 #3192`

---

## Proposed fix (mechanism-backed, not curve-fit)
We **already block GEX-TARGET afternoon longs** for Skew Charm / DD / ES Absorption (rule S180,
`_passes_live_filter` ~line 4177) — the logic: GEX-TARGET = price already AT the +GEX magnet
(destination reached) → PM mean-reversion. **GEX Long was never added to that block.** Adding it
should remove most of the TS-only loser cluster. To validate next session: re-run the TS set
with GEX Long in the GEX-TARGET-PM block and see if WR climbs back toward the 70s.

---

## Decisions made this session
- **Paused** live GEX Long: `GEX_LONG_V3_REAL_TRADE_ENABLED=false` on Railway (instant, no deploy).
- **Held (uncommitted)** the portal TS-overlay fix + v4 dropdown wiring in `app/gex_long_v3.py`
  and `app/main.py` — nothing deployed.
- **Memory** updated (`project_gex_long_v4_live.md` → PAUSED + root cause).

## Next steps (when you're ready)
1. Review `gex_long_TS_review.html` (22 TS trades) and the 3-chart-per-lid HTML.
2. Decide the filter (GEX-TARGET-PM block first candidate); I'll quantify its WR lift.
3. If the filtered TS edge is good → commit the portal TS-fix, re-enable env, ship.
4. If not → keep paused; GEX Long needs more work before real money.
