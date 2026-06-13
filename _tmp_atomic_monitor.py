"""
Background monitor for ATOMIC_BRACKET_ENABLED rollout (2026-05-20).

Polls DB every 60s during market hours. Reports:
  - New setup_log entries (any whitelist signal fires)
  - New real_trade_orders entries (any TSRT placements)
  - State changes on existing orders (pending_entry → filled → closed/stopped)
  - skip_reason populated (filter blocks, ts_reject:..., cap_full, atomic_rejection)
  - Atomic vs sequential path used per fire

Auto-exits at 16:15 ET. Print-only (no Telegram — user has separate alerts).
"""
import os
import sys
import time
import json
import psycopg2
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
END_TIME = datetime.now(ET).replace(hour=16, minute=15, second=0, microsecond=0)
POLL_SEC = 60

WHITELIST = ("Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption",
             "DD Exhaustion", "VIX Divergence", "GEX Long")


def now_et():
    return datetime.now(ET).strftime("%H:%M:%S")


def conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


def log(msg, urgent=False):
    prefix = "**" if urgent else "  "
    try:
        print(f"[{now_et()}] {prefix} {msg}", flush=True)
    except UnicodeEncodeError:
        safe = (f"[{now_et()}] {prefix} {msg}").encode("ascii", "replace").decode("ascii")
        print(safe, flush=True)


def main():
    log(f"MONITOR START — polling every {POLL_SEC}s, auto-exit at {END_TIME.strftime('%H:%M ET')}")

    c = conn()
    cur = c.cursor()

    cur.execute("SELECT MAX(id) FROM setup_log")
    last_setup_id = cur.fetchone()[0] or 0
    cur.execute("SELECT MAX(setup_log_id) FROM real_trade_orders")
    last_rto_id = cur.fetchone()[0] or 0

    log(f"baseline: last setup_log id={last_setup_id}, last real_trade_orders id={last_rto_id}")

    seen_rto_state = {}

    while datetime.now(ET) < END_TIME:
        try:
            cur.execute("""
                SELECT id, ts, setup_name, direction, grade, paradigm,
                       greek_alignment, notified, real_trade_skip_reason
                FROM setup_log
                WHERE id > %s AND setup_name = ANY(%s)
                ORDER BY id
            """, (last_setup_id, list(WHITELIST)))
            for row in cur.fetchall():
                sid, ts, setup, direction, grade, paradigm, align, notified, skip = row
                tag = []
                if notified: tag.append("NOTIFIED")
                if skip: tag.append(f"SKIP:{skip[:40]}")
                tag_str = " | " + " ".join(tag) if tag else ""
                log(f"SIGNAL lid={sid} {setup} {direction} grade={grade} align={align} para={paradigm}{tag_str}")
                last_setup_id = sid

            cur.execute("""
                SELECT setup_log_id, state, created_at, updated_at
                FROM real_trade_orders
                WHERE setup_log_id > %s
                ORDER BY setup_log_id
            """, (last_rto_id,))
            for setup_log_id, state, created, updated in cur.fetchall():
                if isinstance(state, str):
                    state = json.loads(state)
                atomic = state.get("atomic_bracket", False)
                acct = state.get("account_id")
                setup_name = state.get("setup_name", "?")
                direction = state.get("direction", "?")
                status = state.get("status", "?")
                fill = state.get("fill_price")
                qty = state.get("quantity", 1)
                path = "[ATOMIC]" if atomic else "[SEQUENTIAL]"
                log(f"PLACED lid={setup_log_id} {path} {setup_name} {direction} {qty}x @ {fill} acct={acct} status={status}",
                    urgent=True)
                seen_rto_state[setup_log_id] = status
                last_rto_id = setup_log_id

            cur.execute("""
                SELECT setup_log_id, state, updated_at
                FROM real_trade_orders
                WHERE setup_log_id = ANY(%s)
            """, (list(seen_rto_state.keys()),))
            for setup_log_id, state, updated in cur.fetchall():
                if isinstance(state, str):
                    state = json.loads(state)
                status = state.get("status", "?")
                prev = seen_rto_state.get(setup_log_id)
                if prev != status:
                    setup_name = state.get("setup_name", "?")
                    close_price = state.get("close_fill_price") or state.get("fill_price")
                    close_reason = state.get("close_reason", "")
                    log(f"STATUS lid={setup_log_id} {setup_name} {prev} → {status} "
                        f"close={close_price} reason={close_reason}", urgent=True)
                    seen_rto_state[setup_log_id] = status

            c.commit()
        except Exception as e:
            log(f"poll error: {e!r}")
            try:
                c.rollback()
            except Exception:
                pass
            time.sleep(5)
            try:
                c = conn()
                cur = c.cursor()
            except Exception:
                pass

        time.sleep(POLL_SEC)

    log("MONITOR EXIT — market session done")


if __name__ == "__main__":
    main()
