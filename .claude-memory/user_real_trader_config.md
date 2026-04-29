---
name: Real money trader config
description: User's real money auto-trader uses 1 MES, SC-only, via NinjaTrader/Rithmic
type: user
---

Real money auto-trader:
- **1 MES** contract (NOT 8 — config shows 8 but user confirmed 1 for real money)
- **SC (Skew Charm) only** for real money (other setups enabled in config but not for live)
- V11 filter applied
- SL = 12 pts (real config, not 14 from setup_log)
- Via NinjaTrader 8 → Rithmic → real broker
- Commission ~$1.12 round trip per MES contract
