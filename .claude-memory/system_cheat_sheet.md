# 0DTE Alpha System Cheat Sheet

Canonical reference for all components, configs, parameters, and interconnections.
Last updated: 2026-04-02.

---

## 1. Component Map

| Component | Runs On | Broker | API Base | Account(s) | Purpose |
|-----------|---------|--------|----------|------------|---------|
| `app/main.py` (FastAPI) | Railway (`0dtealpha` service) | TradeStation LIVE API | `api.tradestation.com/v3` | N/A (data only) | Chain data, setup detection, outcome tracking, dashboard |
| `volland_worker_v2.py` | Railway (`Volland` service) | N/A | N/A | N/A | Playwright scraper for charm/vanna/gamma from vol.land |
| `app/auto_trader.py` | Railway (inside main.py) | TradeStation **SIM** | `sim-api.tradestation.com/v3` | `SIM2609239F` | MES SIM trades on all setups |
| `app/real_trader.py` | Railway (inside main.py) | TradeStation **LIVE** | `api.tradestation.com/v3` | `210VYX65` (longs), `210VYX91` (shorts) | MES REAL trades, SC only |
| `app/options_trader.py` | Railway (inside main.py) | TradeStation **SIM** | `sim-api.tradestation.com/v3` | `SIM2609238M` | SPXW 0DTE credit spreads (SIM/log-only) |
| `eval_trader.py` | Local PC (laptop) | NinjaTrader 8 via OIF | N/A (file-based) | `falde5482tcp50d170088` (E2T Rithmic) | Polls Railway API, places MES via NT8 for E2T evaluation |
| `app/stock_gex_scanner.py` | Railway (inside main.py) | TradeStation LIVE API | `api.tradestation.com/v3` | N/A (data only) | Scans 23 stocks for GEX levels every 30 min |

---

## 2. Setup Config Matrix

### 2.1 Portal Outcome Tracking (main.py `_compute_setup_levels` + `_trail_params`)

| Setup | Initial SL | Fixed Target | Trail Mode | Trail Activation | Trail Gap | BE Trigger | Price Space | Time Window |
|-------|-----------|-------------|-----------|-----------------|-----------|------------|-------------|-------------|
| **Skew Charm** | 14 pts | None (trail-only) | hybrid | 10 | 5 | 10 | SPX spot | 09:45-15:45 ET |
| **AG Short** | LIS+5 / -GEX (max 20) | None (trail-only) | hybrid | 12 | 5 | 10 | SPX spot | anytime (GEX paradigm) |
| **DD Exhaustion** | 12 pts | None (trail-only) | continuous | 20 | 5 | N/A | SPX spot | 10:00-15:30 ET |
| **GEX Long** | 8 pts | None (trail-only) | hybrid | 10 | 5 | 8 | SPX spot | anytime (GEX paradigm) |
| **GEX Velocity** | 8 pts | None (trail-only) | hybrid | 10 | 5 | 8 | SPX spot | anytime (GEX paradigm) |
| **ES Absorption** | 8 pts | 10 pts | fixed (no trail) | N/A | N/A | N/A | **ES price** | anytime (bar-based) |
| **SB Absorption** | 8 pts | 10 pts | hybrid | 20 | 10 | 10 | **ES price** | anytime (bar-based) |
| **SB10 Absorption** | 8 pts | 10 pts | hybrid | 20 | 10 | 10 | **ES price** | anytime (bar-based) |
| **SB2 Absorption** | 8 pts | 12 pts | hybrid | 20 | 10 | 10 | **ES price** | 09:45-15:00 ET |
| **Delta Absorption** | 8 pts | None (trail-only) | continuous | 0 (immediate) | 8 | N/A | **ES price** | 12:30-15:00 ET |
| **Paradigm Reversal** | 15 pts | 10 pts | fixed (no trail) | N/A | N/A | N/A | SPX spot | 10:00+ (flip age < 180s) |
| **BofA Scalp** | 12 pts (beyond LIS) | 10 pts (from spot) | fixed (no trail) | N/A | N/A | N/A | SPX spot | 10:00-15:30 ET, max hold 30 min |
| **VIX Divergence** (portal-only) | 8 pts | None | long: continuous act=0/gap=8; short: hybrid be=8/act=10/gap=5 | varies | varies | varies | SPX spot | 10:00-14:30 ET |
| **IV Momentum** (portal-only) | 8 pts | 20 pts | fixed (no trail) | N/A | N/A | N/A | SPX spot | 10:00-15:50 ET |
| **Vanna Pivot Bounce** (disabled) | 8 pts | 10 pts | fixed (no trail) | N/A | N/A | N/A | SPX spot | 10:00-15:30 ET |
| **Vanna Butterfly** (portal-only) | None (defined risk) | Pin strike | N/A | N/A | N/A | N/A | SPX spot | ~15:00 ET, once/day |

### 2.2 SIM Auto-Trader (`app/auto_trader.py`)

- **Account:** `SIM2609239F`
- **Symbol:** Auto-rollover MES (e.g. `MESH26`)
- **Qty:** 10 MES (env `MES_TOTAL_QTY=10`). Currently OFF (`AUTO_TRADE_ENABLED=false`)
- **Commission:** $0.50/side
- **All setups ON** by default (toggles in `_toggles` dict)
- **T1 target:** 10 pts (all setups)

| Setup | Flow | Order Structure |
|-------|------|----------------|
| BofA Scalp | A (single target) | Entry + Stop + Limit @+10 |
| Paradigm Reversal | A (single target) | Entry + Stop + Limit @+10 |
| ES Absorption | A (single target) | Entry + Stop + Limit @+10 |
| GEX Long | B (split target) | Entry + Stop + T1 Limit @+10 + T2 Limit @full target |
| GEX Velocity | B (split target) | Entry + Stop + T1 Limit @+10 + T2 Limit @full target |
| AG Short | B (split/trail) | Entry + Stop + T1 Limit @+10 (T2=trail-only, no limit) |
| DD Exhaustion | B (split/trail) | Entry + Stop + T1 Limit @+10 (T2=trail-only, no limit) |
| Skew Charm | B (split/trail) | Entry + Stop + T1 Limit @+10 (T2=trail-only, no limit) |

### 2.3 Real Trader (`app/real_trader.py`)

- **Accounts:** `210VYX65` (longs only), `210VYX91` (shorts only)
- **Account whitelist:** `{210VYX65, 210VYX91}` (hardcoded safety)
- **Symbol:** Auto-rollover MES
- **Qty:** 1 MES
- **Max concurrent per direction:** 1
- **Margin per MES:** $700 (TS intraday $686.75)
- **Daily loss limit:** $300
- **Master switches:** `REAL_TRADE_LONGS_ENABLED`, `REAL_TRADE_SHORTS_ENABLED` (both default OFF)
- **ONLY Skew Charm allowed** (hardcoded in `place_trade()`: `if setup_name != "Skew Charm": return`)
- **Exit strategy:** Opt2 (trail-only, no partial TP)
  - When `target_pts=None` passed from main.py: entry + stop only, no target limit order
- **Trail params:** BE trigger=10, activation=10, gap=5 (constants in file)
- **BE buffer:** 0.25 pts (1 tick above entry)
- **Charm S/R limit entry:** timeout 30 min (currently disabled, `charm_limit_price=None`)
- **State persistence:** `real_trade_orders` DB table (JSONB)
- **EOD flatten:** 15:50 ET cron

### 2.4 Eval Trader (`eval_trader.py` — local)

Config from `eval_trader_config_real.json`:

- **Account:** `falde5482tcp50d170088` (E2T Rithmic via NT8)
- **Qty:** 8 MES
- **BE trigger:** 5.0 pts
- **Max stop loss:** 12 pts
- **Max losses/day:** 999 (effectively unlimited)
- **E2T daily loss limit:** $1,100 (buffer $100)
- **E2T trailing drawdown:** $2,000 from peak EOD balance
- **E2T peak balance:** $55,158.95
- **E2T max contracts (ES equiv):** 6
- **Market hours:** 08:30-15:20 CT (no new trades after 15:20 CT)
- **Flatten time:** 15:44 CT
- **Daily PnL cap:** $1,800
- **Commission:** $2.16/contract

**Setup rules (eval real):**

| Setup | Enabled | Stop | Target |
|-------|---------|------|--------|
| Skew Charm | YES | 12 | None (trail) |
| AG Short | YES | 12 | None (trail) |
| GEX Velocity | YES | 8 | None (trail) |
| ES Absorption | YES | 8 | 10 |
| Paradigm Reversal | YES | 12 | 10 |
| DD Exhaustion | YES | 12 | None (trail) |
| GEX Long | NO | 8 | None (trail) |
| BofA Scalp | NO | 12 | "msg" (30 min max hold) |
| Vanna Pivot Bounce | NO | 8 | 10 |

**Trail params (eval_trader `_TRAIL_PARAMS`):**

| Setup | Mode | Activation | Gap | BE Trigger |
|-------|------|-----------|-----|------------|
| DD Exhaustion | continuous | 20 | 5 | (from config: 5.0) |
| GEX Long | hybrid | 10 | 5 | 8 |
| GEX Velocity | hybrid | 10 | 5 | 8 |
| AG Short | hybrid | 12 | 5 | 10 |
| Skew Charm | hybrid | 10 | 5 | 10 |
| All others | (BE only) | N/A | N/A | 5.0 (config) |

### 2.5 Options Trader (`app/options_trader.py`)

- **Account:** `SIM2609238M` (equities SIM, separate from futures SIM)
- **Mode:** `OPTIONS_LOG_ONLY=true` (default) — records theoretical trades, no SIM orders
- **Strategy:** `credit_spread` (default)
- **Underlying:** `SPY`
- **Spread width:** $2
- **Qty:** 1 contract
- **Target delta:** 0.50 (ATM)
- **Max hold (single_leg only):** 90 min
- **EOD flatten:** 15:55 ET cron
- **State persistence:** `options_trade_orders` DB table (JSONB)
- **Credit spread entry:** sell ATM put/call spread; bullish=bull put spread, bearish=bear call spread

---

## 3. Live Filter — V12-fix

**Single source of truth:** `_passes_live_filter()` in `app/main.py` (line ~3719)

**Used for:** Telegram sends, auto-trade gating (SIM + real + eval), outcome notifications.
**NOT used for:** Portal logging (all setups still log to `setup_log`).

### Hard blocks (always return False):
- VIX Divergence, IV Momentum, Vanna Butterfly

### SC grade gate:
- Skew Charm: block C and LOG grades (only A+/A/B pass)

### Time-of-day gates (V11):
- SC/DD: blocked 14:30-15:00 ET (charm dead zone: 35% WR)
- SC/DD: blocked 15:30-16:00 ET (too little time)
- BofA Scalp: blocked after 14:30 ET (0% WR in 10 trades)

### Gap filter (V12-fix):
- Block LONGS only before 10:00 ET when `|_daily_gap_pts| > 30`
- Shorts before 10:00 NOT blocked (71% WR)
- Rule A (all-day gap-up block) REMOVED

### Longs (V10):
- `alignment >= +2` required
- Skew Charm: exempt from VIX gate (return True immediately)
- VIX > 22: require `overvix >= +2` (allow longs when VIX overvixed)

### Shorts whitelist (V10):
- Skew Charm: allowed (except GEX-LIS paradigm)
- AG Short: allowed (except AG-TARGET paradigm)
- DD Exhaustion: allowed if `alignment != 0` (except GEX-LIS paradigm)
- Everything else: blocked

---

## 4. Price Spaces

| Category | Price Space | Setups |
|----------|------------|--------|
| **SPX spot** | SPX index price (~5700s) | Skew Charm, AG Short, DD Exhaustion, GEX Long, GEX Velocity, Paradigm Reversal, BofA Scalp, VIX Divergence, IV Momentum |
| **ES/MES price** | E-mini S&P 500 futures (~SPX + 15-20 pts) | ES Absorption, SB Absorption, SB10 Absorption, SB2 Absorption, Delta Absorption |

**Conversion (main.py):** When passing to auto_trader/real_trader, stop/target distances are computed in the setup's native price space (SPX or ES), then applied to the current ES/MES quote stream price.

**ES-based flag:** `_es_based` setups in `_check_setup_outcomes`: uses ES range bars for outcome tracking (per-bar scan with trail advancement). SPX setups use `spx_cycle_low`/`spx_cycle_high`.

---

## 5. Grading Thresholds

| Setup | A+ | A | A-Entry/B | C | LOG | Notes |
|-------|----|---|-----------|---|-----|-------|
| GEX Long | >= 85 | >= 70 | >= 50 | N/A | N/A | Force alignment framework (6 forces, max 100) |
| GEX Velocity | >= 80 | >= 65 | >= 50 | N/A | N/A | Speed + gap + GEX + time + LIS direction |
| AG Short | >= 85 | >= 70 | >= 50 | N/A | N/A | Same framework as GEX Long but bearish |
| BofA Scalp | >= 85 | >= 70 | >= 50 | N/A | N/A | Stability + width + charm + time + midpoint |
| ES Absorption | >= 75 | >= 55 | >= 35 (B) | N/A | N/A | Divergence + volume + DD + paradigm + LIS |
| Paradigm Reversal | >= 80 | >= 60 | >= 45 | N/A | N/A | Proximity + ES volume + charm + DD + time |
| Skew Charm | >= 80 | >= 65 | >= 50 (B) | >= 35 | < 35 | Paradigm subtype + time(inv) + VIX + charm(inv) + skew mag |
| DD Exhaustion | (v2 scoring) | (v2 scoring) | (v2 scoring) | (v2 scoring) | (v2 scoring) | Paradigm subtype + alignment(contrarian) + VIX + time + charm |
| VIX Divergence | >= 12 (P1 move) | >= 10 | >= 8 (B) | < 8 | N/A | Phase 1 SPX move strength |
| IV Momentum | (scoring-based) | | | | | Short-only, vol-confirmed downtrend |
| Vanna Pivot Bounce | >= 85 | >= 70 | >= 50 (B) | < 50 | N/A | DISABLED |
| Vanna Butterfly | >= 85 | >= 70 | >= 50 (B) | < 50 | N/A | GREEN vanna = 73% WR, RED = LOG |

---

## 6. Cooldown Settings

| Setup | Cooldown Type | Duration/Distance | Notes |
|-------|--------------|-------------------|-------|
| GEX Long | Grade-based + expiry debounce | Re-fires on grade upgrade or gap improvement; 3 cycles for expiry | Resets daily |
| GEX Velocity | Grade-based + expiry debounce | Same as GEX Long | Resets daily |
| AG Short | Grade-based + time floor | 15 min minimum between fires | `AG_MIN_COOLDOWN_MINUTES = 15` |
| BofA Scalp | Grade-based + per-side time | 40 min per side (long/short separate) | `BOFA_SIDE_COOLDOWN_MINUTES = 40` |
| ES Absorption | Bar-index based | 10 bars between same-direction signals | Daily reset |
| SB Absorption | Bar-index based | 10 bars | Daily reset |
| SB10 Absorption | Bar-index based | 5 bars (10-pt bars = 50 pts between) | Daily reset |
| SB2 Absorption | Bar-index based | 20 bars | Daily reset |
| Delta Absorption | Bar-index based | 5 bars | Daily reset |
| Paradigm Reversal | Time-based per direction | 30 min | Daily reset |
| DD Exhaustion | Time-based per direction | 30 min | Daily reset |
| Skew Charm | Time-based per direction | 30 min | Daily reset |
| VIX Divergence | Once per day per direction | 1 signal/day/direction | Daily reset |
| IV Momentum | Time-based | 30 min | Short-only |
| Vanna Butterfly | Once per day | 1 signal/day | Daily reset |
| Vanna Pivot Bounce | Time-based per direction | 15 min | DISABLED |

---

## 7. Scheduler Jobs (main.py)

| Job | Type | Interval | ID | Notes |
|-----|------|----------|-----|-------|
| `run_market_job` | interval | 30s | `pull` | SPX chain pull, setup detection, outcome tracking |
| `run_spy_market_job` | interval | 30s | `spy_pull` | SPY chain pull (isolated) |
| `save_history_job` | cron | every 2 min | `save` | Save chain snapshots to DB |
| `save_playback_snapshot` | cron | every 2 min | `playback` | Save playback data |
| `_save_rithmic_bars` | cron | every 2 min | `rithmic_range_save` | Save Rithmic ES range bars |
| `pull_spx_ohlc` | interval | 2 min | `spx_ohlc_pull` | Fetch SPX 1-min OHLC from TS barcharts |
| `_auto_trade_premarket_reconcile` | cron | 09:25 ET | - | Pre-market SIM cleanup |
| `_auto_trade_eod_flatten` | cron | 15:55 ET | - | SIM EOD flatten |
| `_options_trade_eod_flatten` | cron | 15:55 ET | - | Options SIM EOD flatten |
| `_real_trade_eod_flatten` | cron | 15:50 ET | - | REAL trader EOD flatten |
| `_real_trade_fast_poll` | interval | 3s | - | Fast poll real trader order status |
| `_pipeline_watchdog` | interval | 30s | - | Pipeline health monitoring |
| `_auto_trade_orphan_check` | interval | 5 min | - | Detect orphaned SIM positions |
| `_broker_poll` | interval | 30s | - | Poll SIM auto-trader + options order status |
| `_send_setup_eod_summary` | cron | 16:05 ET | - | Daily setup outcome summary |
| `fetch_economic_calendar` | cron | Monday 08:00 ET | - | Weekly economic calendar fetch |
| Stock GEX weekly scan | cron | Mon 10:00, Wed 10:00, Fri 10:00 | - | Weekly stock GEX scan |
| Stock GEX opex scan | cron | 3rd Friday 09:35 | - | Monthly opex GEX scan |
| Stock GEX spot monitor | interval | (varies) | - | Spot price monitoring for alerts |
| `_stock_gex_live_monitor` | interval | 2 min | - | Stock GEX live level monitoring |
| `_0dte_gex_monitor` | interval | 2 min | - | 0DTE GEX monitoring |
| Stock GEX EOD summary | cron | 16:05 ET | - | Stock GEX daily summary |

---

## 8. Database Tables (db_init)

### Core Data Tables
| Table | Purpose |
|-------|---------|
| `chain_snapshots` | SPX/SPXW options chain data (every 2 min) |
| `spy_chain_snapshots` | SPY options chain data (isolated, same schema) |
| `volland_exposures` | Raw Volland scraped data + statistics |
| `volland_exposure_points` | Parsed per-strike exposure points (charm, vanna, gamma, deltaDecay) |
| `playback_snapshots` | Historical visualization snapshots |
| `spx_ohlc_1m` | Real 1-min SPX OHLC from TS barcharts (for backtesting) |

### ES Data Tables
| Table | Purpose |
|-------|---------|
| `es_delta_snapshots` | ES cumulative delta state (every 30s) |
| `es_delta_bars` | ES 1-min delta bars (UpVolume-DownVolume) |
| `es_range_bars` | ES 5-pt range bars (bid/ask delta, CVD). **WARNING:** has overlapping bar_idx from live/rithmic sources — filter by `source = 'rithmic'` |

### Setup & Trade Tables
| Table | Purpose |
|-------|---------|
| `setup_log` | All fired setups with scores, outcomes, Greek context |
| `setup_cooldowns` | Persisted cooldown state (trade_date PK, JSONB) |
| `auto_trade_orders` | SIM auto-trader state (setup_log_id PK, JSONB) |
| `real_trade_orders` | REAL trader state (setup_log_id PK, JSONB) |
| `options_trade_orders` | Options SIM trader state (setup_log_id PK, JSONB) |

### Config & Admin Tables
| Table | Purpose |
|-------|---------|
| `alert_settings` | Telegram alert toggles (id=1, singleton) |
| `setup_settings` | Setup detector config (id=1, singleton) |
| `users` | Dashboard auth users |
| `contact_messages` | Access request messages |
| `economic_events` | Weekly economic calendar data |

### Stock GEX Tables
| Table | Purpose |
|-------|---------|
| `stock_gex_scans` | Stock GEX data every 30 min (23 stocks, weekly/opex) |
| `stock_gex_alerts` | Triggered stock GEX alerts |

### VPS Data Bridge Tables
| Table | Purpose |
|-------|---------|
| `vps_es_range_bars` | ES range bars from VPS Rithmic (independent) |
| `vps_vix_ticks` | VIX tick data from VPS |
| `vps_heartbeats` | VPS component health heartbeats |

---

## 9. Telegram Alert Functions

| Function/Mechanism | Chat ID | Triggers |
|-------------------|---------|----------|
| `send_telegram()` | `TELEGRAM_CHAT_ID` (general) | Pipeline health, auth alerts, LIS/paradigm (disabled), volume spikes (disabled) |
| `send_telegram_setups()` | `TELEGRAM_CHAT_ID_SETUPS` | Setup fires (filtered), outcome WIN/LOSS, EOD summary |
| `_alert_401()` | `TELEGRAM_CHAT_ID` | TS API persistent 401 errors (5-min cooldown) |
| Pipeline health | `TELEGRAM_CHAT_ID` | TS data stale >5min, Volland stale >10min, recovery |
| Volland worker alerts | Own `TELEGRAM_BOT_TOKEN`/`CHAT_ID` (Railway env) | 0-points (3 cycles), auto-restart (5 cycles), session expiry |
| Stock GEX alerts | `TELEGRAM_CHAT_ID_STOCK_GEX` | Stock GEX level touches |
| Real trader `_alert()` | `TELEGRAM_CHAT_ID_SETUPS` | All real trade events (placement, fills, stops, errors, circuit breaker) |

---

## 10. State Persistence

| Component | Persistence Mechanism | Recovery |
|-----------|---------------------|----------|
| SIM auto-trader | `auto_trade_orders` DB table (JSONB per setup_log_id) | Restored on startup via `_load_active_orders()` |
| Real trader | `real_trade_orders` DB table (JSONB) | Restored on startup, pre-market cleanup if outside market hours |
| Options trader | `options_trade_orders` DB table (JSONB) | Restored on startup via `_load_active_orders()` |
| Eval trader position | `eval_trader_position.json` (local file) | Loaded on startup; stale overnight = auto-flatten |
| Eval trader state | `eval_trader_state.json` (daily PnL, trade count) | Loaded on startup; daily reset |
| Eval trader API state | `eval_trader_api_state.json` (seen signals/outcomes) | Loaded on startup; daily reset |
| Setup cooldowns | `setup_cooldowns` DB table (trade_date PK, JSONB) | Loaded in `db_init()` via `_load_cooldowns()` |
| Open trades (portal) | `_setup_open_trades` list in memory + `_restore_open_trades()` from `setup_log` on startup | Reconstructed from DB on restart |
| Alert settings | `alert_settings` DB table | Loaded on startup via `load_alert_settings()` |
| Setup settings | `setup_settings` DB table | Loaded on startup via `load_setup_settings()` |

---

## 11. Key Functions (One-Line Descriptions)

### main.py
| Function | Description |
|----------|-------------|
| `_compute_setup_levels(r)` | Extract (target_level, stop_level) from a setup result dict for outcome tracking |
| `_check_setup_outcomes(spot)` | Main outcome loop: check each open trade for WIN/LOSS/EXPIRED every ~30s cycle |
| `_passes_live_filter(...)` | Single source of truth for V12-fix filter (gates Telegram + auto-trade + real trade) |
| `run_market_job()` | Main 30s scheduler: pull chain, compute GEX, detect setups, check outcomes, pipeline health |
| `_send_setup_eod_summary()` | 16:05 ET cron: expire remaining opens, send daily summary with trades/PnL/WR |
| `_run_absorption_detection()` | Run ES Absorption detector on completed range bars (separate from main setup loop) |
| `send_telegram_setups(msg)` | Send to setups Telegram channel |
| `_alert_401(source)` | Send persistent 401 auth failure alert (5-min cooldown) |
| `db_init()` | Create all DB tables, load settings/cooldowns, backfill outcomes |
| `_broker_submit(fn, *args)` | Fire-and-forget thread pool submission for broker API calls |

### setup_detector.py
| Function | Description |
|----------|-------------|
| `evaluate_gex_long(...)` | Force alignment framework: LIS/GEX as support+magnets, blocked GEX-TARGET/MESSY |
| `evaluate_gex_velocity(...)` | GEX paradigm speed-based detection (LIS velocity > 15 pts) |
| `evaluate_ag_short(...)` | Bearish counterpart of GEX Long |
| `evaluate_bofa_scalp(...)` | LIS-based scalp with stability/width/charm scoring, 30-min max hold |
| `evaluate_absorption(...)` | Volume-gated CVD divergence on ES 5-pt bars with Volland confluence |
| `evaluate_single_bar_absorption(...)` | Single ES bar volume+delta spike with CVD trend alignment |
| `evaluate_sb2_absorption(...)` | Two-bar flush+recovery pattern on ES bars |
| `evaluate_delta_absorption(...)` | Delta-focused absorption (doji, trend precondition) |
| `evaluate_skew_charm(...)` | Skew % change + charm direction alignment, v2 data-driven scoring |
| `evaluate_dd_exhaustion(...)` | DD-charm divergence contrarian signal, v2 scoring |
| `evaluate_paradigm_reversal(...)` | Paradigm flip near LIS with ES volume confirmation |
| `evaluate_vix_divergence(...)` | Two-phase VIX-SPX divergence (suppression + compression) |
| `evaluate_iv_momentum(...)` | Vol-confirmed momentum shorts (short-only) |
| `evaluate_vanna_butterfly(...)` | Vanna pin butterfly at max absolute vanna strike, ~15:00 ET |
| `evaluate_vanna_pivot_bounce(...)` | DISABLED: vanna level + swing divergence on range bars |
| `export_cooldowns()` / `import_cooldowns()` | Serialize/deserialize all cooldown state to/from DB |

### auto_trader.py
| Function | Description |
|----------|-------------|
| `place_trade(...)` | Place MES SIM order (Flow A single target or Flow B split target) |
| `update_stop(lid, new_price)` | Replace stop order with new price (trail advancement) |
| `close_trade(lid, result_type)` | Cancel remaining orders, close position |
| `flatten_all_eod()` | EOD: cancel all orders, close all positions |
| `poll_order_status()` | Check fill status of pending entry/stop/target orders |

### real_trader.py
| Function | Description |
|----------|-------------|
| `place_trade(...)` | Place 1 MES REAL trade (SC only, direction-routed to correct account) |
| `update_stop(lid, new_price)` | Replace stop order on real account |
| `update_trail(lid, es_price)` | Compute BE/trail from current ES price, call update_stop if needed |
| `close_trade(lid, result_type)` | Close real position |
| `flatten_all_eod()` | EOD: flatten both accounts |

### eval_trader.py
| Class/Method | Description |
|-------------|-------------|
| `APIPoller.poll()` | Poll Railway `/api/eval/signals` every 2s with Bearer auth |
| `NT8Bridge.place_order(...)` | Write OIF file to NT8 incoming folder |
| `ComplianceGate.can_trade(...)` | Check E2T 50K TCP rules (daily loss, max contracts, hours) |
| `PositionTracker.check_trail(es_price)` | Trail logic mirroring Railway's `_trail_params` |

---

## 12. Environment Variables

### Railway — `0dtealpha` service
```
TS_CLIENT_ID, TS_CLIENT_SECRET, TS_REFRESH_TOKEN
DATABASE_URL
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_CHAT_ID_SETUPS, TELEGRAM_CHAT_ID_STOCK_GEX
EVAL_API_KEY
ADMIN_PASSWORD
AUTO_TRADE_ENABLED (default: false)
MES_TOTAL_QTY (default: 1)
REAL_TRADE_LONGS_ENABLED (default: false)
REAL_TRADE_SHORTS_ENABLED (default: false)
REAL_TRADE_LONGS_ACCOUNT (default: 210VYX65)
REAL_TRADE_SHORTS_ACCOUNT (default: 210VYX91)
REAL_TRADE_MES_SYMBOL (default: auto)
REAL_TRADE_MARGIN_PER_MES (default: 700)
REAL_TRADE_DAILY_LOSS_LIMIT (default: 300)
OPTIONS_TRADE_ENABLED (default: false)
OPTIONS_LOG_ONLY (default: true)
OPTIONS_STRATEGY (default: credit_spread)
OPTIONS_SPREAD_WIDTH (default: 2)
```

### Railway — `Volland` service
```
VOLLAND_EMAIL, VOLLAND_PASSWORD
VOLLAND_WORKSPACE_URL (or VOLLAND_URL fallback)
TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (separate from main service)
```

### Local — eval_trader
```
Config in eval_trader_config_real.json (not env vars)
```

---

## 13. How main.py Calls Each Trader

When a setup fires and passes `_passes_live_filter()`:

1. **SIM auto-trader** (`auto_trader.place_trade`): Called for ALL setups that pass filter. Passes `es_price` from quote stream, `stop_dist` from `_compute_setup_levels`, `target_dist` (None for trail-only setups), `full_target_pts` for split-target T2. `limit_entry_price=None` always.

2. **Options trader** (`options_trader.place_trade`): Called for ALL setups that pass filter. Only passes `setup_log_id`, `setup_name`, `direction`, `spot`. Options trader handles its own chain lookup and strike selection.

3. **Real trader** (`real_trader.place_trade`): Called ONLY for Skew Charm (`if setup_name == "Skew Charm"`). Passes `target_pts=None` (Opt2 trail-only), `stop_dist` from levels, `charm_limit_price=None`. Real trader's own hardcoded check also blocks non-SC setups.

4. **ES Absorption path** (separate from main loop): Calls `auto_trader.place_trade` and `options_trader.place_trade` directly (NOT through `_broker_submit` for auto_trader). Uses ES price from range bar entry.

5. **Eval trader** (external): Polls `/api/eval/signals` endpoint every 2s from local PC. Receives signals + outcomes + es_price. Applies its own compliance gate + filter.

---

## 14. Quick Reference: What's Enabled Where

| Setup | Portal (log) | SIM Auto | Real Trader | Eval (real) | Options SIM |
|-------|-------------|----------|-------------|-------------|-------------|
| Skew Charm | YES | YES | YES (SC only) | YES | YES |
| AG Short | YES | YES | NO | YES | YES |
| DD Exhaustion | YES | YES | NO | YES | YES |
| GEX Long | YES | YES | NO | NO | YES |
| GEX Velocity | YES | YES | NO | YES | YES |
| ES Absorption | YES | YES | NO | YES | YES |
| Paradigm Reversal | YES | YES | NO | YES | YES |
| BofA Scalp | YES | YES | NO | NO | YES |
| SB Absorption | YES | NO | NO | NO | NO |
| SB10 Absorption | YES | NO | NO | NO | NO |
| SB2 Absorption | YES | NO | NO | NO | NO |
| Delta Absorption | YES | NO | NO | NO | NO |
| VIX Divergence | YES (portal) | NO (filter blocks) | NO | NO | NO |
| IV Momentum | YES (portal) | NO (filter blocks) | NO | NO | NO |
| Vanna Butterfly | YES (portal) | NO (filter blocks) | NO | NO | NO |
| Vanna Pivot Bounce | DISABLED | NO | NO | NO | NO |
