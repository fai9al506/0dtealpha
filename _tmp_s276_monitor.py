# -*- coding: utf-8 -*-
"""Live session monitor. One stdout line per NEW event only."""
import os, sys, time, json
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

ET = ZoneInfo("America/New_York")
E = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)

seen_sig, seen_trade, seen_alert, seen_close = set(), set(), set(), set()
warned = set()
first = True

def et_now():
    return datetime.now(ET)

while True:
    try:
        with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
            now = et_now()
            today = now.date()

            # --- new signals ---
            for r in c.execute(text("""
                SELECT id, setup_name, direction, grade, spot, vix, live_pass,
                       ts AT TIME ZONE 'America/New_York' et
                FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date = current_date
                ORDER BY id""")).all():
                if r[0] in seen_sig: continue
                seen_sig.add(r[0])
                if not first:
                    print(f"SIGNAL lid={r[0]} {r[1]} {r[2]} grade={r[3]} spot={r[4]} vix={r[5]} "
                          f"live_pass={r[6]} at {r[7]:%H:%M:%S}", flush=True)

            # --- new real trades ---
            for r in c.execute(text("""
                SELECT o.setup_log_id, o.state, l.setup_name, l.direction
                FROM real_trade_orders o JOIN setup_log l ON l.id=o.setup_log_id
                WHERE (o.created_at AT TIME ZONE 'America/New_York')::date = current_date
                   OR (l.ts AT TIME ZONE 'America/New_York')::date = current_date""")).all():
                st = r[1] if isinstance(r[1], dict) else json.loads(r[1])
                key = r[0]
                if key not in seen_trade:
                    seen_trade.add(key)
                    print(f"*** REAL TRADE lid={key} {r[2]} {r[3]} qty={st.get('quantity')} "
                          f"acct={st.get('account_id')} entry={st.get('entry_fill_price') or st.get('entry_price')} "
                          f"status={st.get('status')}", flush=True)
                elif st.get("status") in ("closed", "flat") and key not in seen_close:
                    seen_close.add(key)
                    px = st.get("close_fill_price") or st.get("stop_fill_price") or st.get("target_fill_price")
                    print(f"*** CLOSED lid={key} {r[2]} reason={st.get('close_reason')} exit={px} "
                          f"pnl_pts={st.get('pnl_points')}", flush=True)
                    if px is None:
                        print(f"!!! WARN lid={key} closed with NO exit price (S243 watchdog condition)", flush=True)

            # --- telegram alerts (read what it SAID) ---
            for r in c.execute(text("""
                SELECT id, ts AT TIME ZONE 'America/New_York', left(message, 220)
                FROM telegram_alerts WHERE (ts AT TIME ZONE 'America/New_York')::date = current_date
                ORDER BY id""")).all():
                if r[0] in seen_alert: continue
                seen_alert.add(r[0])
                msg = (r[2] or "").replace("\n", " | ")
                if not first and any(k in msg.lower() for k in
                        ("alert", "error", "fail", "mismatch", "orphan", "stale", "halt",
                         "breaker", "critical", "reject", "naked", "warn", "skip")):
                    print(f"ALERT {r[1]:%H:%M} {msg}", flush=True)

            # --- STUCK FILL GUARD (lid 6090, 2026-08-17): a lid sitting in
            #     pending_entry for minutes means the fill poll missed it. The
            #     stop then never realigns to the fill and the trade never trails.
            for r in c.execute(text("""
                SELECT o.setup_log_id, o.state, l.setup_name,
                       EXTRACT(epoch FROM (now() - l.ts)) age_s
                FROM real_trade_orders o JOIN setup_log l ON l.id=o.setup_log_id
                WHERE (l.ts AT TIME ZONE 'America/New_York')::date = current_date""")).all():
                st = r[1] if isinstance(r[1], dict) else json.loads(r[1])
                if st.get("status") != "pending_entry":
                    continue
                if float(r[3] or 0) < 180:
                    continue
                key = f"stuck{r[0]}"
                if key in warned:
                    continue
                warned.add(key)
                print(f"!!! STUCK FILL lid={r[0]} {r[2]} still pending_entry after "
                      f"{float(r[3])/60:.0f} min — CHECK BROKER, stop may be un-realigned "
                      f"(current_stop={st.get('current_stop')} acct={st.get('account_id')})", flush=True)

            # --- health gates, once each ---
            if now.hour >= 9 and (now.hour, now.minute) >= (9, 40) and "volland" not in warned:
                n = c.execute(text("SELECT count(*) FROM volland_snapshots "
                                   "WHERE (ts AT TIME ZONE 'America/New_York')::date=current_date")).scalar()
                warned.add("volland")
                print(f"HEALTH volland rows today = {n} (S269 proof: must climb toward ~315)", flush=True)

            lag = c.execute(text("""
                SELECT extract(epoch FROM percentile_cont(0.5) WITHIN GROUP (
                         ORDER BY received_at - ts_end))
                FROM vps_es_range_bars
                WHERE received_at > now() - interval '30 minutes'""")).scalar()
            if lag is not None and lag > 60 and "eslag" not in warned:
                warned.add("eslag")
                print(f"!!! ES FEED LAG median {lag:.0f}s over last 30 min (>60s = S243 page condition)", flush=True)

            first = False
            if now.hour >= 16 and now.minute >= 15:
                print("SESSION END 16:15 ET - monitor stopping", flush=True)
                break
    except Exception as e:
        print(f"monitor-error: {str(e)[:150]}", flush=True)
    time.sleep(45)
