---
name: Live Trading Systems Status
description: Current enabled/disabled status of all 4 auto-trading systems — verified from Railway env vars Apr 2 2026
type: project
---

## Live Trading Systems (as of 2026-04-02)

| System | Status | Switch | Account | Qty | Setups |
|--------|--------|--------|---------|-----|--------|
| **Real Trader** | **ACTIVE** | Longs=true, Shorts=true | 210VYX65 (L), 210VYX91 (S) | 1 MES | SC only (hardcoded) |
| **Eval Trader** | **ACTIVE** | enabled=true (local json) | falde5482tcp50d170088 (E2T) | 8 MES | SC, AG, DD, ES Abs, PR, GEX Vel |
| **SIM Auto-Trader** | **OFF** | AUTO_TRADE_ENABLED=false | SIM2609239F | 10 MES (dormant) | All (when on) |
| **Options Trader** | **OFF** | OPTIONS_TRADE_ENABLED=false | SIM2609238M | 1 SPY spread | All (when on) |

**Real money:** Real trader + Eval trader only.
**SIM API (sim-api.tradestation.com):** NOT being called for orders. Only live API for data.

**Why:** SIM was turned off after real trader went live. Options was experimental (credit spreads), disabled.

**How to apply:** When diagnosing trade issues, only Real and Eval are active. Don't investigate SIM/Options unless user specifically re-enables them.
