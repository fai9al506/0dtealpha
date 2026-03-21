"""
eval_trader_sierra.py — E2T Evaluation Auto-Trader for Sierra Chart (DTC)

Same trading logic as eval_trader.py but uses Sierra Chart's DTC Protocol
via WebSocket instead of NinjaTrader 8 OIF files.

Advantages over NT8:
  - Real-time position queries (no ghost orders from stale files)
  - Streaming fill notifications (no file polling)
  - Structured order reject messages
  - No OIF format quirks

Requirements: Python 3.10+, requests, websocket-client
Usage: python eval_trader_sierra.py --config eval_trader_config_sierra.json

Architecture:
  Railway (setup fires) → /api/eval/signals → this script → DTC WebSocket → Sierra Chart → Rithmic → E2T
"""

import sys
import time
import logging
import argparse
from datetime import datetime, time as dtime

# Import everything reusable from eval_trader (classes, helpers, constants)
from eval_trader import (
    # Constants
    CT, ET, MES_POINT_VALUE, MES_TICK_SIZE, MAX_SIGNAL_AGE_S,
    TRADE_DEDUP_WINDOW, _ENV_OVERRIDE_THRESHOLD,
    TICK_TRADE_TIME_ET, TICK_TRADE_TICKS,
    CONFIG_FILE, STATE_FILE, POSITION_FILE, API_STATE_FILE,
    SCRIPT_DIR, _trade_dedup,
    # Functions
    _init_file_paths, _acquire_singleton_lock, _init_log_file,
    load_config, save_config, current_mes_symbol, _round_tick, _calc_qty,
    parse_signal, parse_outcome,
    # Classes
    APIPoller, TelegramPoller, TSQuotePoller, ComplianceGate,
    PositionTracker,
)
from sierra_bridge import SierraBridge

log = logging.getLogger("eval_trader")

# ─── MES Symbol for Sierra Chart ─────────────────────────────────────────────
# Sierra/Rithmic uses "MESM26.CME" format (root + month code + year + .CME)

_MES_MONTHS = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]

def _third_friday(year, month):
    import calendar
    from datetime import date
    cal = calendar.monthcalendar(year, month)
    fridays = [w[calendar.FRIDAY] for w in cal if w[calendar.FRIDAY] != 0]
    return date(year, month, fridays[2])

def current_sierra_symbol() -> str:
    """Return front-month MES symbol in Sierra format (e.g. MESM6.CME)."""
    from datetime import date, timedelta
    today = date.today()
    for month_num, code in _MES_MONTHS:
        year = today.year
        expiry = _third_friday(year, month_num)
        rollover = expiry - timedelta(days=8)
        if today <= rollover:
            return f"MES{code}{year % 10}.CME"
    year = today.year + 1
    return f"MESH{year % 10}.CME"


# ─── Sierra Banner ────────────────────────────────────────────────────────────

def _banner_sierra(cfg: dict, sierra_sym: str):
    """Print startup banner for Sierra Chart version."""
    dynamic = cfg.get("dynamic_sizing", False)
    max_risk = cfg.get("max_trade_risk", 300)
    dd_floor = cfg["e2t_peak_balance"] - cfg["e2t_eod_trailing_drawdown"]

    log.info("=" * 60)
    log.info("  E2T EVALUATION AUTO-TRADER (Sierra Chart DTC)")
    log.info(f"  Config:    {CONFIG_FILE.name}")
    log.info("=" * 60)
    log.info(f"  Symbol:    {sierra_sym}")
    log.info(f"  Account:   {cfg.get('sierra_account_id', '?')}")
    log.info(f"  DTC:       {cfg.get('sierra_host', '127.0.0.1')}:{cfg.get('sierra_port', 11099)}")
    log.info(f"  Sizing:    {'DYNAMIC' if dynamic else 'FIXED'} "
             f"(max risk: ${max_risk}/trade)")
    log.info(f"  Balance:   ${cfg['e2t_starting_balance']:,.0f} (peak: ${cfg['e2t_peak_balance']:,.0f})")
    log.info(f"  DD floor:  ${dd_floor:,.0f}")
    log.info(f"  Daily lim: ${cfg['e2t_daily_loss_limit']:,.0f} (buffer: ${cfg['e2t_daily_loss_buffer']:.0f})")
    loss_floor = cfg.get("daily_loss_floor", -800)
    log.info(f"  Loss floor: ${loss_floor}/day (stop trading below this)")
    log.info(f"  BE trigger: +{cfg.get('be_trigger_pts', 5.0)} pts")
    log.info(f"  Cutoff:    {cfg['no_new_trades_after_ct']} CT | Flatten: {cfg['flatten_time_ct']} CT")
    log.info("-" * 60)

    daily_cap = cfg.get("e2t_daily_pnl_cap", 0)
    if daily_cap > 0:
        log.info(f"  P&L cap:   ${daily_cap:.0f}/day (E2T consistency rule)")

    log.info("  %-20s %5s %8s %4s %6s  %s" % ("Setup", "Stop", "Target", "Qty", "Risk", "Status"))
    log.info("  %-20s %5s %8s %4s %6s  %s" % ("-" * 20, "----", "------", "---", "-----", "------"))
    for name, rules in cfg["setup_rules"].items():
        enabled = rules.get("enabled", True)
        stop = rules.get("stop", 15)
        target = rules.get("target")
        qty = _calc_qty(cfg, stop) if dynamic else cfg["qty"]
        risk = stop * qty * MES_POINT_VALUE
        status = "ON" if enabled else "OFF"
        tgt_str = "Volland" if target == "msg" else ("trail" if target is None else f"{target}pt")
        log.info("  %-20s %5d %8s %4d $%5.0f  %s" % (name, stop, tgt_str, qty, risk, status))

    log.info("=" * 60)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    cfg = load_config()
    use_api = cfg.get("signal_source", "api") == "api"

    # Validate required fields
    required = {
        "railway_api_url": cfg.get("railway_api_url", ""),
        "eval_api_key": cfg.get("eval_api_key", ""),
        "sierra_account_id": cfg.get("sierra_account_id", ""),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        log.error(f"Missing required config in {CONFIG_FILE}:")
        for m in missing:
            log.error(f"  - {m}")
        log.error("Fill in these fields and restart.")
        save_config(cfg)
        sys.exit(1)

    # Resolve Sierra symbol (auto-rollover or manual)
    sierra_sym = cfg.get("sierra_symbol", "auto")
    if sierra_sym.lower() == "auto":
        sierra_sym = current_sierra_symbol()
        log.info(f"Auto-rollover: Sierra symbol resolved to {sierra_sym}")

    _banner_sierra(cfg, sierra_sym)

    # Initialize components
    api_poller = APIPoller(cfg["railway_api_url"], cfg["eval_api_key"])
    log.info(f"Signal source: Railway API ({cfg['railway_api_url']})")

    quote_poller = TSQuotePoller()
    compliance = ComplianceGate(cfg)

    # Create Sierra DTC bridge (drop-in for NT8Bridge)
    bridge = SierraBridge(
        cfg.get("sierra_host", "127.0.0.1"),
        cfg.get("sierra_port", 11099),
        cfg["sierra_account_id"],
        sierra_sym,
    )
    bridge.connect()

    # PositionTracker accepts bridge as 'nt8' parameter — same interface
    tracker = PositionTracker(bridge, compliance, cfg, quote_poller)

    # Auto-flatten stale positions from previous day
    if getattr(tracker, '_stale_flatten', False):
        # Sierra can verify position actually exists before flatten
        pos = bridge.get_position()
        if pos is None:
            log.info("Sierra confirms: no open position — skipping stale flatten")
            tracker.position = None
            tracker.compliance.has_open_position = False
            tracker._save()
        else:
            log.info(f"Sierra confirms position: qty={pos['qty']} avg={pos['avg_price']:.2f}")
            tracker.flatten("STALE_OVERNIGHT")
        tracker._stale_flatten = False

    # Cancel orphaned pending limit order from previous session
    stale_oid = getattr(tracker, '_stale_pending_cancel_oid', None)
    if stale_oid:
        log.warning(f"Cancelling orphaned limit order in Sierra: {stale_oid}")
        bridge.cancel(stale_oid)
        tracker._stale_pending_cancel_oid = None

    # Startup reconciliation
    if tracker.is_open:
        # Recover from mid-reverse crash
        if tracker.position.get("stop_oid") is None:
            pos = tracker.position
            is_long = pos["direction"] in ("long", "bullish")
            exit_side = "SELL" if is_long else "BUY"
            stop_oid = bridge._oid("s")
            log.warning("CRASH RECOVERY: position has no stop order — placing now")
            bridge._submit_order(exit_side, pos['qty'], 3,  # ORDER_TYPE_STOP
                                 price=pos['stop_price'], client_oid=stop_oid)
            tracker.position["stop_oid"] = stop_oid
            tracker._save()
            log.info(f"  Stop placed: {stop_oid} @ {pos['stop_price']:.2f}")

            if not pos.get("trail_only") and pos.get("target_price") and pos.get("target_oid") is None:
                time.sleep(0.3)
                target_oid = bridge._oid("t")
                bridge._submit_order(exit_side, pos['qty'], 2,  # ORDER_TYPE_LIMIT
                                     price=pos['target_price'], client_oid=target_oid)
                tracker.position["target_oid"] = target_oid
                tracker._save()
                log.info(f"  Target placed: {target_oid} @ {pos['target_price']:.2f}")

        log.info("Reconciling restored position against Sierra...")
        tracker.check_nt8_fills()  # check DTC cache for fills

        # Sierra-specific: verify position/orders actually exist in broker
        if tracker.is_open and tracker.position.get("pending_limit"):
            # Pending limit: no position yet, but limit order may be working.
            # check_nt8_fills() handles fill/timeout — but also verify the order
            # is still alive in Sierra (may have been cancelled externally)
            entry_oid = tracker.position.get("entry_oid")
            if entry_oid:
                state = bridge.check_order_state(entry_oid)
                if state and state["status"] in ("CANCELED", "REJECTED"):
                    log.warning(f"[Sierra] Pending limit {entry_oid} is {state['status']} "
                                f"— clearing state")
                    tracker.position = None
                    tracker.compliance.has_open_position = False
                    tracker._save()
                elif state and state["status"] == "FILLED":
                    log.info(f"[Sierra] Pending limit {entry_oid} FILLED — "
                             f"check_nt8_fills will process")
                else:
                    log.info(f"[Sierra] Pending limit {entry_oid} still working "
                             f"(state={state['status'] if state else 'unknown'})")
        elif tracker.is_open:
            sc_pos = bridge.get_position()
            if sc_pos is None:
                log.warning("[Sierra] Position NOT found in broker — clearing state")
                # Cancel any lingering stop/target orders
                if tracker.position.get("stop_oid"):
                    bridge.cancel(tracker.position["stop_oid"])
                if tracker.position.get("target_oid"):
                    bridge.cancel(tracker.position["target_oid"])
                tracker.position = None
                tracker.compliance.has_open_position = False
                tracker._save()
            else:
                log.info(f"[Sierra] Position confirmed: qty={sc_pos['qty']} "
                         f"avg={sc_pos['avg_price']:.2f}")

        if tracker.is_open:
            log.info(f"Position confirmed open: {tracker.position['setup_name']} "
                     f"{tracker.position['direction']}")

    # Check for untracked positions/orders (we're flat but Sierra isn't)
    if not tracker.is_open:
        sc_pos = bridge.get_position()
        if sc_pos:
            log.warning(f"[Sierra] Sierra has position qty={sc_pos['qty']} "
                        f"avg={sc_pos['avg_price']:.2f} but eval_trader is flat")
        # Cancel ALL working orders when we're flat — prevents orphan fills
        bridge.cancel_all()
        log.info("[Sierra] Cancelled any working orders (startup flat cleanup)")

    poll_interval = cfg.get("telegram_poll_interval_s", 2)
    log.info(f"Polling every {poll_interval}s...")
    last_trail_check = 0.0
    last_reconcile = time.time()
    TRAIL_CHECK_INTERVAL = 5.0
    RECONCILE_INTERVAL = 60.0
    latest_es_price = None

    try:
        while True:
            now_ct = datetime.now(CT)
            compliance.daily_reset()

            # E2T Tick Trade
            now_et = datetime.now(ET)
            if (cfg.get("tick_trade_enabled", True)
                    and now_et.time() >= TICK_TRADE_TIME_ET
                    and not compliance.tick_trade_done
                    and compliance.trades_today == 0
                    and not tracker.is_open
                    and latest_es_price):
                tick_pts = TICK_TRADE_TICKS * MES_TICK_SIZE
                stop_px = latest_es_price - tick_pts
                target_px = latest_es_price + tick_pts
                log.info(f"TICK TRADE: No trades today, placing 1 MES BUY "
                         f"@ ~{latest_es_price:.2f} TP={target_px:.2f} SL={stop_px:.2f}")
                oids = bridge.place_bracket("long", 1, stop_px, target_px)
                tracker.position = {
                    "setup_name": "TickTrade",
                    "direction": "long",
                    "grade": "TICK",
                    "entry_price": latest_es_price,
                    "spx_spot": latest_es_price,
                    "stop_price": stop_px,
                    "target_price": target_px,
                    "stop_pts": tick_pts,
                    "target_pts": tick_pts,
                    "trail_only": False,
                    "qty": 1,
                    "ts": datetime.now(CT).isoformat(),
                    "max_hold_min": 5,
                    "es_entry_price": latest_es_price,
                    "be_triggered": False,
                    **oids,
                }
                tracker.compliance.has_open_position = True
                tracker._save()
                compliance.tick_trade_done = True
                compliance.save()

            # EOD flatten
            flatten_time = datetime.strptime(cfg["flatten_time_ct"], "%H:%M").time()
            if now_ct.time() >= flatten_time and tracker.is_open:
                tracker.flatten("EOD_FLATTEN", es_price=latest_es_price)

            # Check fills EVERY cycle (Sierra DTC streams fills in real-time,
            # so the cache is always current — no need to wait 5s)
            if tracker.is_open:
                tracker.check_nt8_fills()

            # Trailing stop (every 5s)
            if tracker.is_open and time.time() - last_trail_check >= TRAIL_CHECK_INTERVAL:
                tracker.check_trail(latest_es_price)
                last_trail_check = time.time()

            # Periodic reconciliation — Sierra position query
            # Skip for pending limit orders (no position yet, just a working order)
            if (tracker.is_open
                    and not tracker.position.get("pending_limit")
                    and time.time() - last_reconcile >= RECONCILE_INTERVAL):
                sc_pos = bridge.get_position()
                if sc_pos is None and tracker.is_open:
                    log.warning("[Sierra] Position closed externally — clearing state")
                    # Cancel any lingering stop/target orders
                    if tracker.position.get("stop_oid"):
                        bridge.cancel(tracker.position["stop_oid"])
                    if tracker.position.get("target_oid"):
                        bridge.cancel(tracker.position["target_oid"])
                    tracker.position = None
                    tracker.compliance.has_open_position = False
                    tracker._save()
                last_reconcile = time.time()

            # Poll for signals
            new_signals, new_outcomes, poll_es_price = api_poller.poll()
            if poll_es_price:
                latest_es_price = poll_es_price

            for signal in new_signals:
                log.info(f"Signal received: {signal['setup_name']} "
                         f"{signal['direction'].upper()} [{signal.get('grade', '?')}] "
                         f"@ {signal['spot']:.2f}")

                if signal["setup_name"] not in cfg["setup_rules"]:
                    log.info(f"  SKIPPED: unknown setup '{signal['setup_name']}'")
                    continue

                # Staleness check
                sig_ts = signal.get("signal_ts")
                if sig_ts:
                    try:
                        sig_dt = datetime.fromisoformat(sig_ts)
                        if sig_dt.tzinfo is None:
                            sig_dt = sig_dt.replace(tzinfo=ET)
                        age_s = (datetime.now(ET) - sig_dt).total_seconds()
                        if age_s > MAX_SIGNAL_AGE_S:
                            log.info(f"  SKIPPED: signal too old ({age_s:.0f}s > {MAX_SIGNAL_AGE_S}s)")
                            continue
                    except Exception:
                        pass

                # Reversal check
                if tracker.is_open and tracker.is_opposite(signal):
                    # Sierra safety: re-check fills before acting on reversal
                    # (stop may have filled between last check and now)
                    tracker.check_nt8_fills()
                    if not tracker.is_open:
                        log.info("  Position already closed (stop filled) — skipping reversal")
                        continue

                    conviction = 1
                    reasons = [f"{signal['setup_name']} {signal['direction'].upper()}"]
                    pos_dir = tracker.position["direction"].lower()

                    alignment = signal.get("greek_alignment")
                    if alignment is not None and alignment > 0:
                        conviction += 1
                        reasons.append(f"greeks={alignment:+d} vs {pos_dir.upper()}")

                    paradigm = (signal.get("paradigm") or "").upper()
                    if pos_dir in ("short", "bearish") and "GEX" in paradigm:
                        conviction += 1
                        reasons.append("regime=GEX vs SHORT")
                    elif pos_dir in ("long", "bullish") and "AG" in paradigm:
                        conviction += 1
                        reasons.append("regime=AG vs LONG")

                    log.info(f"  ENV CHECK: conviction={conviction}/{_ENV_OVERRIDE_THRESHOLD} "
                             f"[{', '.join(reasons)}]")

                    if conviction >= _ENV_OVERRIDE_THRESHOLD:
                        # Final safety: verify position still exists in Sierra
                        sc_pos = bridge.get_position()
                        if sc_pos is None:
                            log.warning("  Position gone from Sierra — stop already filled, skipping")
                            tracker.check_nt8_fills()
                            continue

                        compliance.has_open_position = False
                        allowed, reason = compliance.check(signal)
                        compliance.has_open_position = True
                        if allowed:
                            log.info(f"  REVERSING: conviction {conviction}/3 — "
                                     f"environment opposes {pos_dir.upper()} position")
                            tracker.reverse(signal, latest_es_price)
                        else:
                            log.info(f"  CLOSING FLAT: conviction {conviction}/3 "
                                     f"but reverse blocked ({reason}) — flattening")
                            tracker.flatten(reason=f"env_override conviction={conviction}",
                                           es_price=latest_es_price)
                    else:
                        if latest_es_price:
                            tracker.tighten_stop(latest_es_price)
                            log.info(f"  LOW CONVICTION ({conviction}/3): tightened SL, holding")
                        else:
                            log.warning(f"  LOW CONVICTION ({conviction}/3): no ES price")
                    continue

                # Dedup
                dedup_key = (signal["setup_name"], signal["direction"].lower())
                now_ts = time.time()
                if dedup_key in _trade_dedup and (now_ts - _trade_dedup[dedup_key]) < TRADE_DEDUP_WINDOW:
                    log.info(f"  DEDUP: already traded {now_ts - _trade_dedup[dedup_key]:.0f}s ago")
                    continue

                allowed, reason = compliance.check(signal)
                if not allowed:
                    log.info(f"  BLOCKED: {reason}")
                    continue
                _trade_dedup[dedup_key] = now_ts
                tracker.open_trade(signal)

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("")
        log.info("Shutting down...")
        if tracker.is_open:
            log.warning("POSITION STILL OPEN — manage manually in Sierra Chart!")
            log.warning(f"  {tracker.position['setup_name']} {tracker.position['direction']} "
                        f"@ {tracker.position['entry_price']:.2f}")
        bridge.disconnect()
        compliance.save()
        save_config(cfg)
        log.info("State saved. Goodbye.")


def test_mode(test_dir: str = "buy"):
    """Quick test: place 1 MES market order, wait for fill, then flatten."""
    cfg = load_config()
    sierra_sym = cfg.get("sierra_symbol", current_sierra_symbol())

    bridge = SierraBridge(
        cfg.get("sierra_host", "127.0.0.1"),
        cfg.get("sierra_port", 11099),
        cfg.get("sierra_account_id", ""),
        sierra_sym,
    )
    bridge.connect()

    direction = "short" if test_dir.lower() in ("sell", "short") else "long"
    side = "BUY" if direction == "long" else "SELL"
    log.info(f"TEST: placing 1 MES {side} at market on {sierra_sym}")

    oid = bridge._submit_order(side, 1, 1)  # ORDER_TYPE_MARKET=1
    log.info(f"  Order submitted: {oid}")

    # Wait for fill
    for i in range(20):
        time.sleep(0.5)
        state = bridge.check_order_state(oid)
        if state:
            log.info(f"  Order state: {state}")
            if state["status"] in ("FILLED", "REJECTED", "CANCELED"):
                break

    # Check position
    pos = bridge.get_position()
    log.info(f"  Position: {pos}")

    if pos:
        log.info("  Flattening in 3 seconds...")
        time.sleep(3)
        close_side = "SELL" if direction == "long" else "BUY"
        close_oid = bridge._submit_order(close_side, 1, 1)
        time.sleep(2)
        close_state = bridge.check_order_state(close_oid)
        log.info(f"  Close state: {close_state}")

    bridge.disconnect()
    log.info("Test complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E2T Auto-Trader (Sierra Chart DTC)")
    parser.add_argument("--config", default="eval_trader_config_sierra.json",
                        help="Config file path (default: eval_trader_config_sierra.json)")
    parser.add_argument("--test", nargs="?", const="buy",
                        help="Test mode: place 1 MES market order. Optional: buy/sell")
    args = parser.parse_args()

    _init_file_paths(args.config)
    _acquire_singleton_lock()
    _init_log_file()

    if args.test:
        test_mode(args.test)
    else:
        main()
