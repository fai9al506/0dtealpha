import os, psycopg, json
from datetime import date, timedelta
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York"); UTC=ZoneInfo("UTC")
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()

# state + timestamps for the 3 lids
cur.execute("SELECT setup_log_id,state,created_at,updated_at FROM real_trade_orders WHERE setup_log_id = ANY(%s)",([3900,3905,3926],))
states={}
for lid,st,ca,ua in cur.fetchall():
    s=st if isinstance(st,dict) else json.loads(st)
    states[lid]=(s,ca,ua)
    print(f"\n=== lid {lid} {s.get('setup_name')} {s.get('direction')} ===")
    print(f"   entry fill={s.get('fill_price')} stop_level(init)={s.get('current_stop')} target={s.get('target_price')} trail_only={s.get('trail_only')}")
    print(f"   close_fill={s.get('close_fill_price')} stop_fill={s.get('stop_fill_price')} reason={s.get('close_reason')} be_trig={s.get('be_triggered')} maxfav={s.get('max_favorable')}")
    print(f"   ts_placed={s.get('ts_placed')}  created={ca.astimezone(ET).strftime('%H:%M:%S')} updated={ua.astimezone(ET).strftime('%H:%M:%S')}")

# ES range bars around each window
def bars(lid, start_et, end_et):
    print(f"\n--- ES 5pt range bars for lid {lid}  {start_et.strftime('%H:%M')}–{end_et.strftime('%H:%M')} ET ---")
    cur.execute("""SELECT ts_start, open, high, low, close FROM vps_es_range_bars
       WHERE range_pts=5 AND ts_start >= %s AND ts_start <= %s ORDER BY ts_start""",
       (start_et.astimezone(UTC), end_et.astimezone(UTC)))
    for r in cur.fetchall():
        t=r[0].astimezone(ET).strftime("%H:%M:%S")
        print(f"   {t}  O {r[1]:.2f} H {r[2]:.2f} L {r[3]:.2f} C {r[4]:.2f}")

today=date(2026,6,11)
def et_at(h,m): 
    from datetime import datetime; return datetime(2026,6,11,h,m,tzinfo=ET)
bars(3900, et_at(12,7), et_at(12,40))
bars(3905, et_at(12,38), et_at(13,15))
