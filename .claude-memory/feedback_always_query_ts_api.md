---
name: Always query TS API for real numbers
description: Never guess or estimate account balances, P&L, trades, or orders — always use the API
type: feedback
---

Never guess or estimate real-money balances, P&L, trade fills, or open orders. Always query the TS API.

**Why:** On Day 1, I reported -$213.60 loss when the actual verified total was -$255.90. Multiple balance estimates throughout the day were wrong. Real money demands real numbers.

**How to apply:** Use `tmp_check_accounts.py` via `railway run` or the `/api/real-trade/status` endpoint to get verified data. For trade history, query the `real_trade_orders` DB table. Never present P&L or account state from log parsing or mental math.
