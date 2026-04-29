---
name: Analysis Validation Protocol — MANDATORY before presenting results
description: Critical feedback — Claude must validate data quality and cross-check results BEFORE presenting any analysis. User caught 4 errors in one session that Claude missed.
type: feedback
---

## Rule: NEVER present analysis results without completing validation first

**Why:** Session 46 (Mar 27) — user had to push back 4 times on wrong analysis:
1. Baseline PnL off by 463 pts (sim bug vs DB actual)
2. DST timezone bug shifted all OHLC bars by 1 hour
3. 4 contaminated Mar 26 trades contributing 72% of PnL
4. Mixed SL=20 and SL=14 eras combined without flagging

Each time the USER caught the problem, not Claude. If user hadn't noticed, system changes would have been based on wrong data. This is a **real money risk**.

## MANDATORY Validation Checklist (before ANY study/backtest)

### Step 1: Data Quality Gate
Before running any analysis, answer these questions:
- [ ] **Date range**: What dates does this data cover? Are any dates known-contaminated (API outages, deploys)?
- [ ] **Staleness check**: Cross-reference with known outages (Mar 26 TS outage, any future ones). Query `chain_snapshots` for frozen-spot periods.
- [ ] **Parameter changes**: Did any parameters change during the data period? (SL changes, filter changes, grading changes). Split the data at change boundaries.
- [ ] **Sample size per era**: If parameters changed, only use the era matching current live config. State the sample size prominently.
- [ ] **Timezone**: Are timestamps in ET? Check DST boundaries (2nd Sunday of March, 1st Sunday of November).

### Step 2: Cross-Check Against Known Truth
After computing results, verify against a known source:
- [ ] **DB vs simulation**: Run the same params on the same trades in both. If they diverge by >10%, the simulation is broken — DO NOT proceed.
- [ ] **Known totals**: Compare computed PnL against DB `SUM(outcome_pnl)` for the same filter. Flag any discrepancy.
- [ ] **Outlier check**: Any trade with MFE > 50 or PnL > 40 gets individually verified (could be data glitch).

### Step 3: Present with Mandatory Caveats
When presenting results:
- [ ] **State the clean sample size** (not the total — only the era matching current config)
- [ ] **State what was excluded** and why
- [ ] **State confidence level**: "42 trades = directional signal, not precise magnitude"
- [ ] **If simulation used**: state the match rate vs DB actuals and what it means

### Step 4: Sanity Check the Recommendation
Before suggesting a code change:
- [ ] **Does the magnitude make sense?** If a 1-parameter change claims 2x PnL, that's a red flag.
- [ ] **What's the worst case?** Not just the backtest — what if the backtest is wrong?
- [ ] **Is this parameter change for real money?** If yes, extra scrutiny. State explicitly what could go wrong.

## How to Apply
- This applies to ANY analysis: trail optimization, filter testing, setup evaluation, P&L reporting
- If time-pressured, at minimum do Steps 1 and 2 before presenting
- When in doubt, present the raw data and validation issues FIRST, then the analysis
- NEVER hide uncertainty behind confident-sounding numbers
