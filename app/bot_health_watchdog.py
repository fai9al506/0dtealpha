"""Bot-down watchdog (S28).

Catches the silent-bot-down failure mode: setup_log has whitelisted signals
firing but real_trade_orders has zero entries — bot is running but not
dispatching to real trader.

History: 9-day P&L audit (session 66) found Mar 25 (5 SC signals, 0 placed)
and Mar 31 (12 SC signals, 0 placed) — bot was down both days, no Telegram
alert ever fired. Cost ~$200 in missed P&L.

Logic: every 30 min during market hours, look at the rolling 2-hour window.
If 5+ whitelisted setup_log entries fired AND 0 real_trade_orders rows
were created → send Telegram alert. Cooldown 60 min so it doesn't spam.

Self-contained module. Receives engine + send_telegram_fn via init().
"""
from __future__ import annotations

from datetime import datetime, time as dtime, timedelta
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")

WHITELIST_SETUPS = (
    "Skew Charm",
    "AG Short",
    "Vanna Pivot Bounce",
    "VIX Divergence",
    "ES Absorption",
)
WINDOW_HOURS = 2
SIGNAL_THRESHOLD = 5  # need >= this many signals before alerting
ALERT_COOLDOWN_MIN = 60  # don't re-alert for 60 min after one fires

_engine = None
_send_telegram = None
_last_alert_at: datetime | None = None


def init(engine, send_telegram_fn) -> None:
    global _engine, _send_telegram
    _engine = engine
    _send_telegram = send_telegram_fn


def _market_hours_now() -> bool:
    now = datetime.now(NY)
    if now.weekday() >= 5:
        return False
    return dtime(9, 30) <= now.time() <= dtime(16, 0)


def check() -> None:
    """One health check pass. Schedule from APScheduler every 30 min."""
    global _last_alert_at
    if _engine is None or _send_telegram is None:
        return
    if not _market_hours_now():
        return

    # Cooldown
    if _last_alert_at is not None:
        if (datetime.now(NY) - _last_alert_at).total_seconds() < ALERT_COOLDOWN_MIN * 60:
            return

    setup_filter = ",".join(f"'{s}'" for s in WHITELIST_SETUPS)
    sql_signals = f"""
        SELECT COUNT(*)
        FROM setup_log
        WHERE setup_name IN ({setup_filter})
          AND grade != 'LOG' AND grade IS NOT NULL
          AND ts >= NOW() - INTERVAL '{WINDOW_HOURS} hours'
          AND (ts AT TIME ZONE 'America/New_York')::time
              BETWEEN '09:30' AND '16:00'
    """
    sql_placed = f"""
        SELECT COUNT(*)
        FROM real_trade_orders
        WHERE created_at >= NOW() - INTERVAL '{WINDOW_HOURS} hours'
    """

    try:
        from sqlalchemy import text
        with _engine.connect() as conn:
            n_signals = conn.execute(text(sql_signals)).scalar() or 0
            n_placed = conn.execute(text(sql_placed)).scalar() or 0
    except Exception as e:
        print(f"[watchdog] query error: {e}", flush=True)
        return

    if n_signals >= SIGNAL_THRESHOLD and n_placed == 0:
        msg = (
            f"🚨 <b>BOT-DOWN ALERT</b>\n"
            f"Last {WINDOW_HOURS}h window:\n"
            f"  • {n_signals} whitelist signals fired in setup_log\n"
            f"  • 0 trades placed in real_trade_orders\n"
            f"\n"
            f"Bot may be down or dispatch is broken. Check:\n"
            f"  (1) Railway 0dtealpha service logs\n"
            f"  (2) AUTO_TRADE_ENABLED env var\n"
            f"  (3) real_trader._active_orders concurrent caps"
        )
        try:
            _send_telegram(msg)
            _last_alert_at = datetime.now(NY)
            print(f"[watchdog] ALERT SENT: signals={n_signals} placed={n_placed}",
                  flush=True)
        except Exception as e:
            print(f"[watchdog] telegram error: {e}", flush=True)
    else:
        print(f"[watchdog] OK: {n_signals} signals, {n_placed} placed in last "
              f"{WINDOW_HOURS}h", flush=True)
