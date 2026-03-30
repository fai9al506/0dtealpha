---
name: Vanna Butterfly Setup
description: 40pt call butterfly centered on 0DTE vanna pin strike. GREEN vanna gate. Grading v2 deployed 2026-03-23.
type: project
---

## Vanna Butterfly Setup (grading v2 — 2026-03-23)

**Core concept:** 0DTE vanna strike with largest absolute value near spot acts as magnet/pin.
GREEN (positive) vanna = magnet pulling price toward it. RED (negative) = pushes away.

**Trade structure:** 40pt call butterfly centered on max abs vanna strike (was 30pt, widened for better P&L)
- Buy 1 call @ (pin - 20)
- Sell 2 calls @ pin
- Buy 1 call @ (pin + 20)
- Entry at ~15:00 ET, hold to expiry (no management needed, defined risk)

**Grading v2 backtest (27 trades, Feb 11 - Mar 20):**
- GREEN vanna: **72.7% WR**, avg +$4.67/contract (40pt), +$51.38 total
- RED vanna: 18.8% WR, barely breakeven → grade "LOG"
- Overall: 40.7% WR, but GREEN filter makes it 72.7%
- Best combo: GREEN + gap<=30 = **80% WR**, +$53.68 total (10 trades)

**Why GREEN works:** GREEN vanna = dealers LONG vanna = hedge INTO the strike = price magnet.
RED = dealers SHORT vanna = push price AWAY from strike.

**Key findings from backtest:**
- Pin sign (GREEN/RED) is #1 predictor — overwhelms everything else
- Gap barely matters for P&L (r=-0.056) because cheaper cost offsets lower pin probability
- Cost vs gap: r=-0.795 (strong) — wider gap = much cheaper butterfly
- VIX higher = worse pinning (r=+0.387 with dist_at_close)
- Net vanna near pin: r=+0.438 with WIN (positive net = stronger magnet)
- Vanna strength (raw |vanna|) is NOT predictive — removed from scoring
- Wider butterfly = better: 40pt > 30pt > 20pt (more room for close near pin)

**New grading v2 formula (GREEN only, RED = LOG):**
1. Gap proximity (0-30): <=10=30, <=15=25, <=20=20, <=30=10
2. VIX environment (0-25): <=18=25, <=22=20, <=25=15, >25=5
3. Net vanna magnitude (0-25): 50M+=25, 20M+=15, else=10
4. Cost efficiency (0-20): <=3=20, <=5=15, <=8=10, >8=5
- Thresholds: A+ >= 80, A >= 60, B >= 40, else C

**Changes from grading v1:**
- Width: 30pt → 40pt (higher total P&L, same WR)
- Gap filter: 20 → 30 (GREEN pulls price even from 25+ pts)
- GREEN gate: RED → grade "LOG" (only 18.8% WR, not tradeable)
- Removed: vanna strength score (NOT predictive, inverted)
- Removed: old cost_score replaced with data-driven brackets

**Expected returns (GREEN vanna, 40pt):**
- ~7.4 trades/month, 80% WR
- Avg cost: $668/contract, Avg profit: $537/contract
- Monthly: ~$3,970/contract/month
- Max win: $1,559 (Feb 13, dist=1.3pts)
- Max loss: $337 (Feb 26, dist=18pts)

**Status:** Portal/logging only. Logged to setup_log with outcome tracking (WIN/LOSS at expiry). Blocked from Telegram and auto-trade in `_passes_live_filter()`.

**Files:** `app/setup_detector.py` (evaluate_vanna_butterfly), `app/main.py` (setup_log INSERT, EOD butterfly P&L)
