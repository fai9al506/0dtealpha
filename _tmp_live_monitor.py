"""Live monitor — polls DB every 90s, emits events to stdout for harness Monitor tool.

Watches:
  - New volland_snapshots saves (paradigm + LIS + DD + freshness)
  - New real_trade_orders (entry / close / skip)
  - Stale Volland (>5 min since last save)
"""
import os, sys, time
from sqlalchemy import create_engine, text
from datetime import datetime, timezone, timedelta

# Force UTF-8 stdout so unicode chars from DB strings don't crash on cp1252
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

eng = create_engine(os.environ["DATABASE_URL"])

# Initial high-water marks — seed from current DB max (id-based to avoid TZ bugs)
last_volland_ts = None
with eng.connect() as _c:
    _r = _c.execute(text("SELECT COALESCE(MAX(setup_log_id), 0) FROM real_trade_orders")).scalar()
    last_rt_lid = int(_r or 0)
    _r2 = _c.execute(text("SELECT COALESCE(MAX(id), 0) FROM setup_log WHERE real_trade_skip_reason IS NOT NULL")).scalar()
    last_skip_id = int(_r2 or 0)
print(f"[monitor-init] high-water last_rt_lid={last_rt_lid} last_skip_id={last_skip_id}", flush=True)

while True:
    try:
        with eng.connect() as c:
            # 1) Latest Volland save
            r = c.execute(text("""
                SELECT ts AT TIME ZONE 'America/New_York' AS et,
                       payload->'statistics'->>'paradigm' AS paradigm,
                       payload->'statistics'->>'lines_in_sand' AS lis,
                       payload->'statistics'->>'delta_decay_hedging' AS dd_spx,
                       payload->'spy_statistics'->>'delta_decay_hedging' AS dd_spy,
                       (payload->>'exposure_points_saved')::int AS pts,
                       EXTRACT(EPOCH FROM (now() - ts))::int AS age_s
                FROM volland_snapshots ORDER BY ts DESC LIMIT 1
            """)).fetchone()
            if r:
                d = dict(r._mapping)
                if last_volland_ts != d["et"]:
                    last_volland_ts = d["et"]
                    age = d["age_s"]
                    pts = d["pts"] or 0
                    para = d["paradigm"] or "N/A"
                    lis = d["lis"] or "N/A"
                    flag = "" if pts > 0 and age < 240 else " ⚠️"
                    print(f"[volland] {d['et'].strftime('%H:%M:%S')} paradigm={para} lis={lis} dd_spx={d['dd_spx']} pts={pts} age={age}s{flag}", flush=True)
                elif d["age_s"] > 300:
                    # Stale alert (no new save)
                    print(f"[volland-stale] last save {d['age_s']}s ago — investigate VPS", flush=True)

            # 2) New real-trade entries (look for new lids in real_trade_orders)
            r = c.execute(text("""
                SELECT rto.setup_log_id, rto.state->>'status' AS status,
                       rto.state->>'fill_price' AS fill, rto.state->>'close_reason' AS close_reason,
                       sl.setup_name, sl.direction, sl.grade,
                       rto.created_at AT TIME ZONE 'America/New_York' AS et,
                       rto.updated_at AT TIME ZONE 'America/New_York' AS up_et
                FROM real_trade_orders rto
                JOIN setup_log sl ON sl.id = rto.setup_log_id
                WHERE rto.setup_log_id > :lid
                ORDER BY rto.setup_log_id ASC
            """), {"lid": last_rt_lid}).fetchall()
            for row in r:
                d = dict(row._mapping)
                last_rt_lid = max(last_rt_lid, d["setup_log_id"])
                print(f"[real-trade] NEW lid={d['setup_log_id']} {d['setup_name']} {d['direction']} grade={d['grade']} status={d['status']} fill={d['fill']} at {d['et'].strftime('%H:%M:%S')}", flush=True)

            # 3) New skip events — id-based high-water (no TZ gymnastics)
            r = c.execute(text("""
                SELECT id, setup_name, direction, grade, real_trade_skip_reason
                FROM setup_log
                WHERE real_trade_skip_reason IS NOT NULL
                  AND id > :last_id
                ORDER BY id ASC
            """), {"last_id": last_skip_id}).fetchall()
            for row in r:
                d = dict(row._mapping)
                last_skip_id = max(last_skip_id, d["id"])
                if d["real_trade_skip_reason"] == "master_kill":
                    print(f"[SKIP-MK] lid={d['id']} {d['setup_name']} {d['direction']} BLOCKED (master_kill active!)", flush=True)
                elif d["real_trade_skip_reason"] not in ("whitelist_reject",):
                    print(f"[skip] lid={d['id']} {d['setup_name']} {d['direction']} -> {d['real_trade_skip_reason']}", flush=True)
    except Exception as e:
        print(f"[monitor-err] {type(e).__name__}: {e}", flush=True)

    time.sleep(90)
