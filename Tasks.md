# Tasks

Pending tasks, research, and implementation ideas for 0DTE Alpha.
Two types: **Scheduled** (time-based, checked every session) and **Backlog** (do when free).

Last updated: 2026-03-25

---

## SCHEDULED TASKS (Check Every Session)

These tasks are time-sensitive. Claude checks them at session start and alerts if due.

| # | Task | Trigger | Action | Status |
|---|------|---------|--------|--------|
| S1 | **Mar 25 deployment verification** | First market day after Mar 25 | All 5 checks PASS. SPY DD flowing, gap=+64.2 (longs blocked), combined DD working. Two bugs found+fixed: (1) gap SQL used wrong column, (2) SPY DD not reaching setup detector. | DONE 2026-03-25 |
| S2 | **SB2 Absorption data check** | Every 5 trading days | 7 signals (6W/1L, 86% WR, +77 pts). Backtest done: OR gate + SVB + cd=20 = 47% WR, PF 1.50. Deploy scheduled S11. | PENDING (7/15 signals) |
| S3 | **IV Momentum data check** | Every 5 trading days | Query setup_log for IV Momentum signals. When 50+ signals collected with live data, compare WR vs backtest 64%. If validated, enable on SIM. | PENDING (LOG-ONLY) |
| S4 | **Vanna Butterfly data check** | Every 5 trading days | Query setup_log for Vanna Butterfly signals. Track GREEN vanna WR. When 20+ GREEN signals, decide: enable on SIM or keep logging. Expected: 80% WR, $3,970/mo/contract. | PENDING (LOG-ONLY) |
| S5 | **VIX Compression data check** | Every 5 trading days | Query setup_log for VIX Compression signals. Currently 4 trades, 100% WR. When 10+ signals, consider enabling. Volland gate (SVB>1, vanna ratio<5) keeps it clean. | PENDING (LOG-ONLY) |
| S6 | **GEX Long live signal check** | Every 5 trading days | Query setup_log for GEX Long signals on SIM. Need 15+ signals to validate force alignment rewrite. Currently disabled on Eval Real. | PENDING |
| S7 | **GEX Velocity live signal check** | Every 5 trading days | Query setup_log for GEX Velocity signals. Separate from GEX Long. Monitoring on SIM. | PENDING |
| S8 | **Options circuit breaker analysis** | When 30+ days of V11 option data | Re-run circuit breaker study: stop trading after 4 consecutive option losses. Backtest showed +48% improvement. Needs 30+ days V8+ data. | WAITING (need data) |
| S9 | **Stock GEX Support Bounce — live alerts** | Each trading day 10:00-14:00 ET | Monitor `/stock-gex-live` for stocks dipping 1% below -GEX with CLEAN structure. Telegram channel connected. | ACTIVE |
| S10 | **Real money daily P&L check** | Each trading day after 16:05 ET | Check Telegram for real_trader EOD summary. Verify no bugs, no missed trades, no ghost positions. Accounts: 210VYX65 (longs), 210VYX91 (shorts). | ACTIVE |
| S11 | **SB2 Absorption v2 tuning deploy** | 2026-03-25 after 16:10 ET | Deployed: OR gate (vol>=1.2x OR dlt>=1.3x), cd=20, time 9:45-15:00, SVB key fixed. +260 pts, 47.7% WR, PF 1.52. | DONE 2026-03-25 |
| S12 | **Push 0DTE GEX improvements** | 2026-03-25 after 16:10 ET | Deployed: last-scan timestamp, 2-min spot refresh, history viewer (date/time picker). Commit `cc213a3`. | DONE 2026-03-25 |
| S13 | **Push AG Short 15-min cooldown** | 2026-03-25 after 16:10 ET | Commit+push AG Short cooldown fix: 15-min time floor prevents flicker re-fires. Data: <15min signals = 63% WR (weak), 15-30min = 85% WR (best). Changes in `setup_detector.py`. | DONE 2026-03-25 |

---

## BACKLOG TASKS

### Implementation (Build)

| # | Task | Priority | Details | Source |
|---|------|----------|---------|--------|
| ~~B1~~ | ~~SPY DD into setup detector~~ | ~~HIGH~~ | DONE — Combined DD (SPX+SPY) feeds DD Exhaustion. Deployed Mar 25. See completion log. | DONE |
| B2 | **Charm S/R as FILTER (not entry)** | HIGH | Limit entry disabled (44% fill rate), but charm S/R range could work as a quality filter — block entries when range is unfavorable. Needs research first. | PROJECT_BRAIN |
| B3 | **0DTE GEX tab on Stock GEX Live page** | HIGH | Add SPX/SPY/QQQ/IWM 0DTE chains to `/stock-gex-live`. Different from stocks: same-day exp, SPXW symbol, wider strikes. Verify with live market. | `project_0dte_gex_tab.md` |
| B4 | **DD alignment boost (V4) for filter** | MEDIUM | SC + DD_aligned + no toxic paradigm = 85.7% WR, PF 6.44. Three options: (A) SC DD-Aligned Only, (B) Hybrid V10 + DD gate on SC, (C) keep V10. User interested but undecided. Needs 50+ days data. | PROJECT_BRAIN |
| B5 | **Dashboard restyle (main 0DTE page)** | MEDIUM | Apply approved dark style from `project_dashboard_style.md` to main dashboard (`/`). Outfit body + JetBrains Mono for numbers. Revert tag: `pre-gex-redesign`. | `project_dashboard_style.md` |
| B6 | **AI Copilot — Claude Code skills** | MEDIUM | `/morning-brief` (10 AM market read), `/review-trades` (EOD analysis), `/check-discord` (filter actionable info). Zero extra cost, uses existing Claude Code subscription. | `project_ai_copilot.md` |
| B7 | **IBKR Cash Account connector** | LOW | Build IB Gateway + ibapi connector for IBKR cash account (U10235312). Starting capital $5,000. No PDT rule. Infrastructure not yet built. | `project_cash_account_plan.md` |
| B11 | **Autonomous Copilot Worker** | MEDIUM | `copilot_worker.py` Railway service: collects market data (DD, paradigm, charm, gap, signals) + Discord messages, calls Claude API for analysis, sends bias/actionable calls to Telegram every 30 min during market hours. Cost ~$0.40-4/day. Needs: Claude API key, Telegram channel, periodic Discord export or bot. Design in `copilot_market_rules.md`. | Session 36, Mar 25 |
| B12 | **Discord Live Monitor (self-bot)** | HIGH | Read-only Discord gateway listener using user token. Monitors `#volland-daytrading-central` + `#0dte-alerts`. Parses Apollo/LordHelmet/Wizard messages for vol flow, DD calls, levels, bias. Feeds into Copilot Worker (B11). Raw WebSocket approach (no library), proper Identify with auto-fetched build number, residential IP. Detection risk: very low (read-only = identical to browser tab). Token from browser DevTools. Run locally or Railway. | Session 44, Mar 26 |
| B8 | **SB10 Absorption recalibration** | LOW | Only 10 signals in 56 days — needs multiplier recalibration (1.3x-1.5x) for 10-pt range bars. | `project_sb_absorption.md` |
| B9 | **FOMC Event Day Filter** | LOW | Known FOMC dates = no trading or reduced sizing. Low effort (date list check). | `research_discord_ideas_mar23.md` |
| B10 | **SPX GEX Bounce — full study** | LOW | Complete SPY/QQQ/IWM downloads, run dip study on all 4, test 10:00-13:00 time filter, calculate actual options P&L (not just pts). | `project_spx_gex_bounce.md` |

### Research (Study / Analyze)

| # | Task | Priority | Details | Source |
|---|------|----------|---------|--------|
| R1 | ~~Gap-day filter~~ | ~~HIGH~~ | DONE — Implemented as gap-up longs block (gap > +30 pts). See completion log. | DONE |
| R2 | **Per-strike charm near spot as filter** | HIGH | Strongest SC differentiator: winners -8.3M, losers +10.2M. Needs work to convert to a filter. | MEMORY (SC grading v2) |
| R3 | **ES Absorption redesign** | MEDIUM | Current design flaw: fires up to 40 bars after swing. User's correct model: high-volume bar IS the comparison point, fire immediately. Neither approach clearly superior in backtest. Deferred. | PROJECT_BRAIN |
| R4 | **Fixed strike vol for vanna interpretation** | MEDIUM | Discord idea: vanna support only holds when fixed-strike vol is declining. Needs investigation. | `research_discord_ideas_mar23.md` |
| R5 | **Panic vs structural put buying** | MEDIUM | Distinguish geopolitical panic from institutional structural put buying. Different trading responses. | `research_discord_ideas_mar23.md` |
| R6 | **Volatility spike pause** | MEDIUM | If ES range bar volatility exceeds 3x normal, pause entries 15-30 min. | `research_discord_ideas_mar23.md` |
| R7 | **DD per-strike for ES Absorption stacking** | LOW | Revisit when ES Absorption trade count grows. Not enough data yet. | `research_gamma_dd_perstrike.md` |
| R8 | **Gamma per-strike on dashboard** | LOW | Visual awareness only. No filter impact expected. | `research_gamma_dd_perstrike.md` |
| R9 | **EOD DD trajectory for manual butterflies** | LOW | Display DD direction into close for discretionary butterfly entries. 50% direction accuracy — needs timing skill. | `research_gamma_dd_perstrike.md` |
| R10 | **ThetaData — OpEx pinning study** | LOW | Data already downloaded. GEX pins monthly expiry strikes. | `project_thetadata_ideas.md` |
| R11 | **ThetaData — IV crush around events** | LOW | Needs more data collection. Pre-event IV spike → post-event collapse. | `project_thetadata_ideas.md` |
| R12 | **Options strategy expansion** | LOW | Non-directional setups: butterfly, IC, iron fly. Pin criteria: charm concentration, DD neutrality, low paradigm conviction, VIX term structure. | PROJECT_BRAIN |

### Parked (Revisit Later)

| # | Task | Details | Why Parked |
|---|------|---------|------------|
| P1 | **Sierra Chart VolDetector** | Apollo-style vol seller/buyer dots on ES chart. `VolDetector.cpp` built, threshold ~150 for VIX. | Needs live market calibration with Apollo's real-time posts for comparison. |
| P2 | **FundingPips manual trading** | Trade manually on FundingPips ($36 eval) using Sierra Chart. | Needs separate data solution (Denali feed or brother's Rithmic). |
| P3 | **SPY real options account** | TradeStation #11697180, $4,000 funded. V8 filter, 1 SPY contract, 0DTE at ~0.30 delta. | 2-week validation period (~Mar 14-28). Need to check status. |

---

## Completion Log

| Date | Task | Result |
|------|------|--------|
| 2026-03-25 | Gap-up longs filter (gap > +30 pts) | DONE — blocks longs all day on gap-up. Backtest: +290.9 pts saved, 112 trades. FOMC filter rejected (FOMC day = best day). |
| 2026-03-25 | Combined DD into setup detector | DONE — SPX+SPY DD feeds DD Exhaustion. Boundary: Mar 25 (SPX-only before). |
| 2026-03-25 | SPY DD capture (v2 worker + dashboard) | DONE — deployed, verify at market open (S1) |
| 2026-03-24 | Charm S/R limit entry disabled | DONE — market orders beat all limit thresholds |
| 2026-03-24 | V11 SC grade gate deployed | DONE — A+/A/B pass, C/LOG blocked |
| 2026-03-24 | General Telegram channel cleaned | DONE — ~160 msgs/day reduced to ~7-9/day |
| 2026-03-23 | SB2 Absorption setup created | DONE — LOG-ONLY, collecting data |
| 2026-03-23 | Vanna Butterfly grading v2 | DONE — GREEN vanna gate, 80% WR |
| 2026-03-23 | VIX Compression tuning v2 | DONE — 100% WR with Volland gate |
| 2026-03-22 | SC grading v2 recomputed | DONE — old grades were anti-predictive, fixed |
| 2026-03-21 | Stock GEX Scanner deployed | DONE — 23 stocks, every 30 min |
| 2026-03-21 | Stock GEX Live page built | DONE — 56 stocks, streaming, `/stock-gex-live` |
