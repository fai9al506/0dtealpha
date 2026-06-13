import os, psycopg, json
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
# real-fill daily net (post V16) split direction
cur.execute("""SELECT s.ts, s.direction, r.state FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id=s.id
  WHERE s.ts::date>='2026-05-18' AND (r.state->>'fill_price') IS NOT NULL""")
rday=defaultdict(lambda:{'net':0.0,'L':0.0,'S':0.0})
for ts,dirn,st in cur.fetchall():
    s=st if isinstance(st,dict) else json.loads(st)
    e=s.get("fill_price"); x=s.get("close_fill_price") or s.get("stop_fill_price")
    if not e or not x: continue
    lng=str(dirn).lower() in ("long","bullish"); g=((x-e) if lng else (e-x))*5
    d=ts.astimezone(ET).date(); rday[d]['net']+=g; rday[d]['L' if lng else 'S']+=g
# cage width per day ~10:00
def cage(rows,spot):
    lv=[]
    for r in rows:
        try: lv.append((float(r[10]),((r[1] or 0)-(r[19] or 0))*(r[3] or 0)*100))
        except: pass
    above=[(s,g) for s,g in lv if s>spot]; below=[(s,g) for s,g in lv if s<spot]
    res=max(above,key=lambda x:x[1],default=None); sup=min(below,key=lambda x:x[1],default=None)
    return (res[0]-sup[0]) if (res and sup and res[1]>0 and sup[1]<0) else None
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, spot, rows FROM (
  SELECT ts,spot,rows,row_number() OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York')
    ORDER BY abs(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'America/New_York')::time - TIME '10:00:00'))) rn
  FROM chain_snapshots WHERE ts::date>='2026-05-18' AND spot IS NOT NULL AND rows IS NOT NULL) q WHERE rn=1""")
cg={}
for d,spot,rows in cur.fetchall():
    rows=rows if isinstance(rows,list) else json.loads(rows); cg[d]=cage(rows,float(spot))
print(f"{'date':>10} {'cage':>5} {'regime':>8} | {'realNet$':>8} {'L$':>7} {'S$':>7}")
narrow_net=narrow_n=wide_net=wide_n=0
for d in sorted(rday):
    c=cg.get(d); reg="?" if c is None else ("NARROW" if c<=90 else "wide")
    n=rday[d]
    print(f"{str(d):>10} {(c or 0):>5.0f} {reg:>8} | {n['net']:>8.0f} {n['L']:>7.0f} {n['S']:>7.0f}")
    if c is not None:
        if c<=90: narrow_net+=n['net']; narrow_n+=1
        else: wide_net+=n['net']; wide_n+=1
print(f"\nREAL $ by cage regime (post-V16):")
print(f"  NARROW (<=90): n={narrow_n} net ${narrow_net:+.0f} avg ${narrow_net/max(narrow_n,1):+.0f}/day")
print(f"  WIDE  (>90):   n={wide_n} net ${wide_net:+.0f} avg ${wide_net/max(wide_n,1):+.0f}/day")
