---
name: Manual trading plan — FundingPips + Sierra Chart
description: User plans to trade manually on FundingPips ($36 eval) using Sierra Chart for analysis, alongside automated E2T
type: project
---

**Plan (2026-03-15):**
- Trade manually on **FundingPips** ($36 for $5K eval) using MT5 or cTrader
- Use **Sierra Chart** for charting/order flow analysis (not connected to FundingPips — separate platforms)
- Automated **eval_trader** continues running on E2T via Rithmic on work desktop

**Platform research:**
- FundingPips: MT5, cTrader, Match-Trader only. NO Sierra Chart (different ecosystem — Forex/CFD vs futures/Rithmic)
- FTMO: MT4, MT5, cTrader, DXtrade. No Sierra. Accepts Saudi.
- FundedNext: MT4, MT5, cTrader, Match-Trader. No Sierra. Accepts Saudi.
- Apex Trader Funding: Rithmic + Sierra — but site blocked, couldn't confirm Saudi acceptance
- Topstep: Pushes own "TopstepX" platform now. CME futures only.

**Sierra Chart data feed solution:**
- Cannot use E2T Rithmic credentials (concurrent session conflict with eval_trader)
- **Option A:** Sierra Chart's **Denali Exchange Data Feed** — independent of Rithmic, covers CME (ES/MES). ~$4-12/mo non-pro CME fee. Needs monthly broker verification (brief Rithmic connect or qualifying funded account).
- **Option B (user's preferred):** Brother registers with E2T → gets separate Rithmic credentials → Sierra Chart connects to brother's Rithmic. No conflicts. Also opens second funded account opportunity.

**Why:** User wants to stay engaged, develop discretionary skills, use system signals as framework for manual entries. $36 risk is negligible.

**How to apply:** Don't suggest connecting Sierra to the same Rithmic account as eval_trader. Always remind about concurrent session limit.
