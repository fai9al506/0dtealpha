# -*- coding: utf-8 -*-
import os, json, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
cur.execute("""SELECT rto.setup_log_id, rto.state,
   to_char(sl.ts AT TIME ZONE 'America/New_York','MM-DD HH24:MI') t,
   date(sl.ts AT TIME ZONE 'America/New_York') d, sl.setup_name, sl.direction
   FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
   WHERE date(sl.ts AT TIME ZONE 'America/New_York') >= '2026-06-20' ORDER BY sl.ts""")
rows=cur.fetchall(); c.close()
print(f"real_trade_orders rows Jun-22+: {len(rows)}")
# inspect available keys on first row
if rows:
    s0=rows[0][1]; s0=s0 if isinstance(s0,dict) else json.loads(s0)
    print("state keys:", sorted(s0.keys()))
from collections import defaultdict
day=defaultdict(lambda:[0.0,0,0])
print(f"\n{'lid':>5} {'t':>11} {'setup':<14}{'dir':<7}{'entry':>9}{'exit':>9}{'qty':>4}{'pnl$':>9} status")
for lid,st,t,d,nm,dr in rows:
    s=st if isinstance(st,dict) else json.loads(st)
    entry=s.get('fill_price')
    ex=s.get('close_fill_price') or s.get('stop_fill_price') or s.get('exit_price')
    qty=s.get('quantity') or 0
    status=s.get('status')
    if entry and ex and qty:
        islong=dr in ('long','bullish')
        pts=(ex-entry) if islong else (entry-ex)
        pnl=pts*qty*5
        day[str(d)][0]+=pnl; day[str(d)][1]+=1; day[str(d)][2]+=(1 if pts>0 else 0)
        print(f"{lid:>5} {t:>11} {str(nm):<14}{str(dr):<7}{entry:>9.2f}{ex:>9.2f}{qty:>4}{pnl:>+9.1f} {status}")
    else:
        print(f"{lid:>5} {t:>11} {str(nm):<14}{str(dr):<7}{'?':>9}{'?':>9}{qty:>4}{'--':>9} {status} (incomplete)")
print(f"\n{'day':<12}{'net$':>10}{'trades':>8}{'wins':>6}")
for d in sorted(day): v=day[d]; print(f"{d:<12}{v[0]:>+10.1f}{v[1]:>8}{v[2]:>6}")
tot=sum(v[0] for v in day.values()); print(f"{'TOTAL 22+':<12}{tot:>+10.1f}")
