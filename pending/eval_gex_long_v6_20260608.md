# VPS task — GEX Long v6 on Eval (2026-06-08)

**Status on the Railway/TSRT side (done by PC session):** GEX Long **v6** is LIVE on real TSRT
at **1 MES**. Env on `0dtealpha`: `GEX_LONG_V3_ENABLED=true` + `GEX_LONG_V3_REAL_TRADE_ENABLED=true`
+ **`GEX_LONG_V6_MODE=true`**. Code commits `9f09b6c` (live detector) + `a82b7fd` (portal).

## What v6 is
v4 was PAUSED — the study that validated it (75% WR) had selected trades on **Volland gamma**
(a bug); TS-correct v4 was only ~60% WR. **v6** is the user-driven re-derivation, all on TS GEX:
- Entry: TS GEX has a **real positive magnet** above spot + **magnet-dominance ≥ 1.0** (positive
  magnet GEX ÷ strongest negative wall; a magnet dwarfed by the negative GEX is fake) + **drop
  GEX-TARGET afternoon (hr ≥ 13)** + hour < 15 + align ≥ 0 OR bull-paradigm.
- Exit: **TRAIL-ONLY** — SL 14, continuous trail activation 15 / gap 5, NO fixed target.
- Backtest 21t / **86% WR** / +270p trail-only, OOS-stable (monotonic dominance sweep; H1 88% /
  H2 80%; every month ≥ 80%; max DD = one stop). **Sample = 21 trades / 3 losses → directional.**

## ⚠️ Eval will AUTO-CONSUME v6 — this is the thing to handle
`/api/eval/signals` applies the SAME `_passes_live_filter` as TSRT. Now that the env is on, the
API **emits v6 GEX Long signals**. If the LONG eval config still has GEX Long enabled (it was, for
v4), eval will **auto-trade v6 at its configured qty (2 MES)** the moment a signal fires — no VPS
change needed to START it. So this task is **verify & decide**, not "add from scratch."

### Do this:
1. **Confirm eval's GEX Long config matches v6's exit:** trail-only, SL 14, continuous trail
   act 15 / gap 5 (no fixed target). If it still has a fixed target or SL 8 from older config, fix it.
2. **Decide qty.** TSRT runs v6 at 1 MES. Eval LONG default is 2 MES. Either keep 2 MES (2× TSRT
   during the trial) or drop to 1 MES to match — user's call.
3. **Confirm align ≥ 0 / bull gate** is applied (the LONG eval's local `greek_filter` align≥2
   mirror would over-block v6 align 0/1 — same hidden blocker that bit v4; make sure it's the
   align≥0/bull carve-out, not align≥2).
4. **Watch the first v6 fill** — confirm eval receives it, the trail-only exit engages, fill is clean.
5. If you'd rather eval NOT trade v6 until you've reviewed: disable GEX Long in the eval config
   first, then re-enable deliberately.

## Revert (either side)
- TSRT/API: `railway variables --service 0dtealpha --set "GEX_LONG_V6_MODE=false"` (or
  `GEX_LONG_V3_REAL_TRADE_ENABLED=false`) — instant, stops emission for both TSRT and eval.
- Eval-only: disable GEX Long in eval config.
- **Trigger:** 3 losses in first 8 fills, OR net < −$150. (21-trade backtest — expect live WR below 86%.)

See memory `project_gex_long_v4_live.md` + `daily_trade_logs/gex_long_TS_vs_volland_analysis.md`.
