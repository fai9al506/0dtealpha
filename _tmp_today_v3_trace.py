"""What did the v3 GEX-structure classifier actually say through this morning?
The user cited 9:52 as a 'perfect' GEX Long. Trace verdict + structure every snapshot."""
import psycopg2
from app.gex_long_v3 import _features, _classify

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
cur = psycopg2.connect(DB).cursor()
cur.execute("""SELECT ts, (ts AT TIME ZONE 'America/New_York')::time, current_price
               FROM (SELECT ts, (payload->>'current_price') as current_price
                     FROM volland_snapshots WHERE ts::date='2026-06-02') s
               WHERE current_price IS NOT NULL
                 AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:40' AND '12:30'
               ORDER BY ts""")
print(f"{'ET':9s} {'spot':>8s} {'verdict':8s} {'magnet':>8s} CORE_R3(+gex>spot) CORE_R2(-gex<spot) R5_align R_VETO")
for ts, et, price in cur.fetchall():
    spot = float(price)
    try:
        f = _features(cur, ts, spot)
    except Exception:
        f = None
    v = _classify(f)
    if f is None:
        print(f"{str(et)[:8]} {spot:8.1f} {v:8s}  (no features)")
        continue
    print(f"{str(et)[:8]} {spot:8.1f} {v:8s} {str(f['gex_magnet_strike']):>8s}  "
          f"R3={f['CORE_R3']!s:5s} R2={f['CORE_R2']!s:5s} R5={f['R5_align']!s:5s} VETO={f['R_VETO']!s:5s}")
