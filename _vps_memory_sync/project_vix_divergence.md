---
name: VIX Divergence Setup (replaces VIX Compression)
description: Two-phase VIX-SPX divergence detector. Phase 1=VIX suppression during SPX move, Phase 2=VIX compression while SPX flat. Both LONG+SHORT. Deployed 2026-03-29.
type: project
---

## VIX Divergence Setup (deployed 2026-03-29)

**Replaces VIX Compression (v1).** User discovered the two-phase pattern on Mar 27. Discord-validated by Apollo's vol-seller framework.

**The pattern:**
1. Phase 1 — "VIX suppression": SPX moves >6 pts but VIX doesn't react (<0.20). Vol sellers/buyers absorbing.
2. Phase 2 — "VIX compression": VIX moves >0.25 against Phase 1 direction while SPX flat (<10 pts). Spring loading.
3. Signal fires when Phase 2 completes → explosion follows.

**Detection thresholds:**
- Phase 1: SPX move >= 6 pts, VIX react <= 0.20, window 10-30 min
- Phase 2: VIX compress >= 0.25, SPX flat <= 10 pts, window 15-60 min
- Time: 10:00-14:30 ET
- VIX gate: shorts only when VIX < 26

**Grading (Phase 1 SPX move strength):**
- A+ (>=12pt), A (>=10pt), B (>=8pt), C (<8pt)
- SHORT B-grade = 100% WR (5/5) in backtest

**Risk Management:**
- SHORT: SL=8, BE@8, trail activation=10, gap=5
- LONG: SL=8, IMM trail gap=8 (continuous from entry)
- No fixed TP — trail-only captures explosive moves

**Backtest (24 days, Feb 24 - Mar 27):**
- SHORT: 20 signals, 56% WR, +82 pts, PF 2.28, MaxDD 32
- LONG: 23 signals, 39% WR, +50 pts, PF 1.65, MaxDD 42
- Combined March: 36 signals, +131 pts, PF 2.11, 58% green days

**Why old VIX Compression failed (2 signals, 2 losses):**
1. SVB/vanna gates killed most signals (11 raw → 2 after gates)
2. No Phase 1 detection — fired on noise without vol-suppression context
3. VIX drop threshold too high (>1.0) — real compression was 0.3-0.7

**Stop-entry confirmation (deployed 2026-03-29):**
- Instead of MARKET entry, waits for first 1.5pt move in signal direction
- LONG: confirm when spot >= signal_spot + 1.5. SHORT: confirm when spot <= signal_spot - 1.5
- 30-min timeout → outcome = TIMEOUT (no fill)
- Backtest: WR 50→68%, PnL +149→+262 (doubled), MaxDD 29→11, avg MAE 5.3→2.3
- 97% fill rate — only 1 signal missed across 38 signals
- Stop/target levels computed from confirmed entry price, not signal spot
- `_pending_stop_entry` flag in trade dict, checked each cycle in `_check_setup_outcomes()`

**Status:** LOG-ONLY. Blocked in `_passes_live_filter()`. Collecting live signals for validation.

**Files:** `app/setup_detector.py` (evaluate_vix_divergence), `app/main.py` (trail params, filter, format, stop-entry logic)
