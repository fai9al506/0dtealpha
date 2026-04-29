---
name: IV Momentum Setup (Apollo)
description: Vol-confirmed momentum SHORT setup — tracks per-strike put IV changes to confirm downtrend
type: project
---

## IV Momentum Setup (LOG-ONLY, deployed 2026-03-21)

**Origin:** Apollobix's Discord insight — "Look at fixed strike vols. When they come down, support plays out."
Tested as a momentum-following strategy (opposite of our contrarian system).

**Signal logic (SHORT only):**
- Spot dropped >= 5 pts in last 10 min (confirmed downtrend)
- Avg put IV at ATM/ATM-5/ATM-10 rose >= 0.05 (vol buyers confirming fear)
- 30-min cooldown between signals
- Time gate: 10:00-15:50 ET (skip the open)

**Backtest (88 days, Nov-Mar):**
- 47 trades, 64% WR, +309 pts, PF 4.02, MaxDD 25.4
- Best hours: 11:00-13:00 ET (80% WR, PF 8.92)
- Shorts dominate (60% WR), longs terrible (34% WR) — shorts only deployed
- IV threshold >= 0.05 IS the entire edge. At 0.02: random. At 0.10+: too few trades.

**Risk management:** SL=8, TP=20 (1:2.5 R:R). Momentum carries, let winners run.

**vs Contrarian system:**
- Low overlap (29%) — detect different things. Momentum = reactive, contrarian = predictive.
- Combined adds ~42 pts to contrarian's 734 — marginal as add-on.
- Viable standalone for manual trading on Sierra Chart.

**Why:** User recognized personal tendency to fight trends. This setup is the antidote — ride confirmed downtrends instead of catching falling knives.

**How to apply:** Monitor LOG signals. When 15+ signals collected, compare live WR vs backtest 64%. If validated, enable on SIM. Consider for manual trading on Sierra Chart.

**Files:** `app/setup_detector.py` (evaluate_iv_momentum, update_iv_momentum_tracker), `app/main.py` (tracker call, format, filter). Sierra: `sierra_studies/` folder.
