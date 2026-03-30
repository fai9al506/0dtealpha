---
name: Real auto trader cash account arrangement
description: Plan for live SPY options trading on IBKR cash account — sizing, filters, scaling, PDT workaround
type: project
---

# Real Auto Trader — Cash Account Plan

## Broker & Account
- **Broker:** IBKR (user's existing account U10235312)
- **Account type:** Cash (convert from margin, or open separate cash account) — NO PDT in cash accounts
- **Paper account for testing:** DU5930842
- **Residence:** Saudi Arabia, but account is under IB LLC (US entity) — PDT applies on margin
- **Why cash:** No PDT rule. Downside is T+1 settlement (each trade's cash locked until next day)

## Why SPY (not SPX/ES options)
- SPY option at 0.30 delta costs ~$300-500 per contract
- SPX costs ~$3,000-8,000 (too big for $5K account)
- /ES options: PDT exempt but same size as SPX
- 1 SPXW contract ~ 10 SPY contracts in P&L

## Starting Capital: $5,000

## Scaling Plan (filter progression)
1. **Start: SC only (Skew Charm)** — ~13 trades/day, ~$5,200 cash needed
   - Fits $5K (barely), worst day -$170, WR 50%, +29.3% in 9 days
   - Safest filter, MVP setup
2. **At $7K+: Add AG Short** → SC + AG — ~15 trades/day, ~$6,000 cash needed
   - Worst day only -$89, WR 50.7%, +34.6% in 9 days
3. **At $10K+: Full V7+AG** — ~31 trades/day, ~$12,400 cash needed
   - Best P&L (+56.5%) but needs more capital, worst day -$433

## March Backtest Results (real SPXW prices / 10 for SPY)

### SC only — $5K start
| Day | Date | Capital | P&L |
|-----|------|---------|-----|
| 0 | Start | $5,000 | $0 |
| 9 | Mar 12 | $6,464 | +$1,464 total |
- Return: +29.3%, worst day: -$170

### SC + AG — $5K start
| Day | Date | Capital | P&L |
|-----|------|---------|-----|
| 0 | Start | $5,000 | $0 |
| 9 | Mar 12 | $6,731 | +$1,731 total |
- Return: +34.6%, worst day: -$89

### Full V7+AG — $5K start (would run out of cash ~day 3)
- Total potential: +$2,824 (+56.5%) but needs $12K+ cash

## Risk Profile
- **Max loss per trade = option premium ($3-8).** No margin call, no negative balance.
- **Worst case bug:** Script buys options and never closes → all expire worthless EOD → max loss = total premiums that day (~$5K worst case)
- **0DTE = no overnight risk** — everything expires same day

## T+1 Settlement Impact
- Each trade locks ~$300-500 until next business day
- 15 trades × $400 = $6,000 locked per day
- $5K covers ~12 trades, $7K covers all 15, $10K covers 25+

## IBKR Connection (TODO)
- IB Gateway on local machine (port 4002 for paper)
- Python `ibapi` library for order execution
- Same signal flow: Railway API → local script → IB Gateway → IBKR
- Need to build IBKR connector (similar to TS options_trader.py but with ibapi)

## Alternative Explored & Rejected
- **Derayah (Saudi broker using IBKR):** Won't work
- **/ES options:** PDT exempt but $3K-8K per contract, only 1-2 trades/day with $5-10K
- **TS margin $10K:** PDT applies (under $25K)
- **Fund to $25K:** Not enough capital available now
- **PDT rule change:** FINRA approved elimination Sep 2025, SEC still reviewing as of Mar 2026
