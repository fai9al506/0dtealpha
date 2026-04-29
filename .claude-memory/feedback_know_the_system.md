---
name: Diagnostic discipline — STOP before diagnosing
description: MANDATORY pre-diagnosis checklist. Read system_cheat_sheet.md + verify from CODE before declaring any bug. Session 64 wasted 30 min chasing a non-bug.
type: feedback
---

## The Problem (Session 64)

User asked about trade #1352 discrepancy (portal WIN +36.3 vs real trader LOSS -$70). Claude:
1. Jumped straight to DB queries and code investigation
2. Never checked SL config per component (confused eval SL=12 with real SL=14)
3. Never considered SPX-MES basis slippage (~2 pts at VIX 30)
4. Declared it a "trail resilience bug" — spent 30 min building a wrong diagnosis
5. User corrected 3 times before Claude reached the right answer: **legitimate stop, not a bug**

User: "the bug is with YOU that u r not aware of our system"

## MANDATORY Pre-Diagnosis Checklist

Before investigating ANY trade issue, discrepancy, or suspected bug:

### Step 1: Read context FIRST (before any query)
- Read `system_cheat_sheet.md` — configs, SLs, trail params, price spaces per component
- Identify WHICH component is involved (portal? real? eval? SIM?)
- Identify WHICH price space (SPX or MES?)

### Step 2: Check the basics
- **SL per component:** Portal=`_compute_setup_levels()`, Eval=`eval_trader_config_real.json`, Real=same as portal, SIM=same as portal
- **Price space:** SPX setups track in SPX, execute in MES. ~15-45pt basis (wider at high VIX). A 1-3pt SPX-MES divergence is NORMAL slippage, not a bug.
- **Trail mechanism:** Portal tracks SPX cycle extremes. Real/SIM trail depends on main.py calling `update_stop()`. Eval has its own trail logic.

### Step 3: Simple explanations first
Before declaring a bug, rule out:
- **Basis slippage** (SPX SL survived but MES SL hit due to 1-3pt basis shift)
- **Config difference** (eval SL != real SL != portal SL for some setups)
- **Timing difference** (portal checks every 30s cycle, broker fills are real-time)
- **Known behavior** (trail doesn't advance until activation threshold)

### Step 4: Only THEN query/investigate
If the basics don't explain it, THEN query DB, read code, check logs.

## Key System Facts (quick reference)
- **3 traders, 3 brokers, 2 price spaces** — never mix them
- **SC SL:** Portal/Real/SIM = 14pts, Eval = 12pts
- **SPX-MES basis:** 15-45pts depending on VIX/rates. Wider at high VIX.
- **Basis slippage of 1-3pts is NORMAL** — not a bug
- Full reference: `system_cheat_sheet.md`
