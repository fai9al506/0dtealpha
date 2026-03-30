---
name: VPS Cloud Migration & VIX Data
description: Kamatera VPS ($62/mo) replaces local PC — Sierra for ES+VIX data, NT8 for execution, saves $60/mo over Rithmic
type: project
---

## VPS Cloud Migration (Mar 28-29 2026)

**Goal:** Move trading infrastructure from local PC to cloud VPS. Get tick-level VIX futures data. Cancel Rithmic ($122/mo).

**Provider:** Kamatera (NY datacenter), 30-day free trial started Mar 29.
- Specs: 2 vCPU, 4GB RAM, 50GB SSD, Windows Server 2022
- Name: `0dte-vps`
- Monthly: $62/mo (after trial)

**Architecture:**
- Sierra Chart + Denali → ES + VIX tick data → DTC → vps_data_bridge.py → Railway API
- NT8 + Rithmic → E2T execution (eval_trader on VPS)
- IBKR TWS → Sierra monthly brokerage auth (once/month)

**Rithmic:** Confirmed by Cameron Growney (Mar 27) — NO CBOE/CFE connectivity. $122/mo. To be canceled after VPS proven reliable.

**Databento:** Research showed CFE normalized data NOT yet available (PCAPs only at $750/mo). Not viable. Sierra on VPS is the right path.

**Sierra actual costs:**
- Subscription + DOM: $46/mo
- CME Market Depth (non-pro): $13.50/mo
- CFE Market Depth: $12/mo
- Total Sierra: $71.50/mo

**Cost comparison:**
- Current: Rithmic $122 + Sierra $71.50 = $193.50/mo
- After: Sierra $71.50 + Kamatera $62 = $133.50/mo
- **Savings: $60/mo** + VIX data + 24/7 reliability

**Phased plan:**
1. VPS setup + eval_trader migration (no code changes)
2. ES data bridge (Sierra DTC → Railway, parallel with Rithmic)
3. VIX tick pipeline (new capability)
4. Monitoring layer (heartbeat + auto-restart)
5. Cutover (disable Rithmic on Railway, cancel subscription)

**How to apply:** Phase 1 is manual VPS setup. Code work (vps_data_bridge.py, Railway endpoints, vps_monitor.py) starts after Sierra+NT8 confirmed working on VPS.
