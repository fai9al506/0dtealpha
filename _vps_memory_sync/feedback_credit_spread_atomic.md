---
name: Credit spreads must be atomic multi-leg orders
description: Never place spread legs as separate orders — use TS API Legs array for atomic fill
type: feedback
---

Credit spread orders must be placed as a SINGLE atomic order with both legs, NOT as two separate orders.

**Why:** User corrected this — placing two separate SELLTOOPEN and BUYTOOPEN orders risks one leg filling without the other (leg risk). In manual trading, spreads are always placed as one order. The TS API supports this via the `Legs[]` array in `OrderRequestDefinition`.

**How to apply:** When placing any multi-leg options order (credit spread, debit spread, iron condor, etc.), always use the `Legs` array in the order payload. The top-level `LimitPrice` is the net credit or debit for the entire spread. Both legs fill together or not at all.

```json
{
  "Symbol": "SPY 260319P670",
  "LimitPrice": "0.90",
  "TradeAction": "SELLTOOPEN",
  "Legs": [
    {"Symbol": "SPY 260319P670", "Quantity": "1", "TradeAction": "SELLTOOPEN"},
    {"Symbol": "SPY 260319P668", "Quantity": "1", "TradeAction": "BUYTOOPEN"}
  ]
}
```

Also applies to closing: BUYTOCLOSE + SELLTOCLOSE in one atomic order.
