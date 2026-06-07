"""Vol-event detector (S209, 2026-06-07) — Wizard of Ops spot-vol framework.

A "vol event" = on a down day, the Volland spot-vol deviation closes >= 2
sigma beyond the normal SPX/VIX relationship (panic overpricing vol). Wiz's
stat: ~93% revisit of the prior day's close within 3 weeks. Our own DB has
n=1 verified (Mar 6 2026 -> target hit in 4 days, then -480pts more — the
bounce is NOT a bottom).

Pure ALERTING — reads `volland_snapshots` (the scraper already captures
spot_vol_beta every ~2 min), sends Telegram to the MAIN alerts channel
(TELEGRAM_CHAT_ID), touches NO trading logic. Two triggers:

1. INTRADAY LIKELY — deviation >= 2.0 on a down day (spot below prior
   session close). Once per day. Fri Jun 5 this would have fired 12:55 ET.
2. CONFIRMED — Volland's `vixEvents` array turns non-empty (populated after
   the 16:15 VIX settle; typically visible in the NEXT session's snapshots).
   Once per trigger date, with target price + 3-week deadline.

Dedup persisted in `vol_event_alerts` table so restarts/redeploys don't
re-spam. Fail-soft: every path wrapped, never raises into the scheduler.
"""
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

NY = ZoneInfo("America/New_York")
DEVIATION_THRESHOLD = 2.0
SNAPSHOT_MAX_AGE_MIN = 10   # intraday check needs a fresh snapshot
CONFIRMED_MAX_AGE_DAYS = 7  # don't alert stale events found after long gaps

_engine = None


def init(engine) -> None:
    global _engine
    _engine = engine
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS vol_event_alerts (
                    key TEXT PRIMARY KEY,
                    alerted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))
        print("[vol-event] initialized", flush=True)
    except Exception as e:
        print(f"[vol-event] init table error (non-fatal): {e}", flush=True)


def _send(msg: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")  # main 0DTE Alpha alerts channel
    if not token or not chat:
        print("[vol-event] telegram not configured", flush=True)
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          data={"chat_id": chat, "text": msg, "parse_mode": "HTML"},
                          timeout=15)
        ok = r.status_code == 200
        print(f"[vol-event] telegram send: {r.status_code}", flush=True)
        return ok
    except Exception as e:
        print(f"[vol-event] telegram error: {e}", flush=True)
        return False


def _already_alerted(conn, key: str) -> bool:
    row = conn.execute(text("SELECT 1 FROM vol_event_alerts WHERE key = :k"),
                       {"k": key}).fetchone()
    return row is not None


def _mark_alerted(conn, key: str) -> None:
    conn.execute(text("""
        INSERT INTO vol_event_alerts (key) VALUES (:k)
        ON CONFLICT (key) DO NOTHING
    """), {"k": key})


def _prior_session_close(conn, today_str: str):
    row = conn.execute(text("""
        SELECT spot FROM chain_snapshots
        WHERE spot IS NOT NULL
          AND (ts AT TIME ZONE 'America/New_York')::date < :d
        ORDER BY ts DESC LIMIT 1
    """), {"d": today_str}).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def check(_test_now=None, _test_snap_before=None) -> None:
    """Scheduler entry point (every 5 min, 9-16 ET weekdays via cron).
    Fail-soft — never raises. `_test_now`/`_test_snap_before` are test hooks
    (replay the check as-of a past time / against a past snapshot)."""
    if _engine is None:
        return
    try:
        now = _test_now or datetime.now(NY)
        if now.weekday() >= 5:
            return
        with _engine.begin() as conn:
            snap_filter = "AND ts <= :before" if _test_snap_before else ""
            row = conn.execute(text(f"""
                SELECT ts, payload->'statistics'->'spot_vol_beta' AS svb,
                       payload->>'current_price' AS px
                FROM volland_snapshots
                WHERE payload->'statistics' ? 'spot_vol_beta' {snap_filter}
                ORDER BY ts DESC LIMIT 1
            """), {"before": _test_snap_before} if _test_snap_before else {}).fetchone()
            if not row:
                return
            snap_ts, svb, px = row
            if isinstance(svb, str):
                svb = json.loads(svb or "{}")
            svb = svb or {}
            corr = svb.get("correlation")
            vix_events = svb.get("vixEvents") or []
            today = now.date().isoformat()

            # ── 1. CONFIRMED events (any fresh snapshot, incl. next morning) ──
            for ev in vix_events:
                trig = (ev.get("triggerDate") or "")[:10]
                if not trig:
                    continue
                try:
                    trig_age = (now.date() - datetime.strptime(trig, "%Y-%m-%d").date()).days
                except ValueError:
                    continue
                if trig_age > CONFIRMED_MAX_AGE_DAYS:
                    continue  # stale event seen after downtime — skip
                key = f"confirmed-{trig}"
                if _already_alerted(conn, key):
                    continue
                tgt = ev.get("targetPrice")
                dl = (ev.get("deadline") or "")[:10]
                tgt_s = f"{float(tgt):,.2f}" if tgt is not None else "?"
                _send(
                    "⚡ <b>VOL EVENT CONFIRMED</b>\n"
                    f"Trigger: {trig} · Target: <b>{tgt_s}</b> (prior close) · Deadline: {dl}\n"
                    "Wiz stat: ~93% revisit target within 3 weeks.\n"
                    "<i>Our DB n=1: Mar 6 hit target in 4 days, then fell 480pts more — "
                    "expect a bounce, NOT a bottom.</i>"
                )
                _mark_alerted(conn, key)

            # ── 2. INTRADAY LIKELY (needs fresh snapshot + market hours) ──
            if corr is None or float(corr) < DEVIATION_THRESHOLD:
                return
            snap_age_min = (now - snap_ts.astimezone(NY)).total_seconds() / 60.0
            if snap_age_min > SNAPSHOT_MAX_AGE_MIN:
                return  # stale (overnight/weekend) — intraday alert needs live data
            key = f"intraday-{today}"
            if _already_alerted(conn, key):
                return
            # spot: volland current_price is sometimes 0 — fall back to chain spot
            spot = None
            try:
                spot = float(px) if px and float(px) > 0 else None
            except (TypeError, ValueError):
                spot = None
            if spot is None:
                r2 = conn.execute(text("""
                    SELECT spot FROM chain_snapshots
                    WHERE spot IS NOT NULL
                      AND (ts AT TIME ZONE 'America/New_York')::date = :d
                    ORDER BY ts DESC LIMIT 1
                """), {"d": today}).fetchone()
                spot = float(r2[0]) if r2 and r2[0] is not None else None
            prior = _prior_session_close(conn, today)
            if spot is None or prior is None or spot >= prior:
                return  # only a down day can trigger a vol event
            _send(
                "⚡ <b>VOL EVENT LIKELY</b>\n"
                f"Spot-vol deviation <b>{float(corr):.2f}σ</b> (≥{DEVIATION_THRESHOLD:.0f}σ) "
                f"on a down day (spot {spot:,.0f} vs prior close {prior:,.0f}).\n"
                "Confirms if it holds into the 16:15 ET VIX settle.\n"
                "<i>Context: extreme vol overpricing — historically a dead-cat bounce "
                "to prior close follows within days, but Mar 6 kept falling after the touch.</i>"
            )
            _mark_alerted(conn, key)
    except Exception as e:
        print(f"[vol-event] check error (non-fatal): {e}", flush=True)
