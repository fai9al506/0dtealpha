import os, psycopg, json
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()

def walls(rows, spot):
    lv=[]
    for r in rows:
        try: lv.append((float(r[10]), ((r[1] or 0)-(r[19] or 0))*(r[3] or 0)*100))
        except: pass
    above=[(s,g) for s,g in lv if s>spot]; below=[(s,g) for s,g in lv if s<spot]
    res=max(above,key=lambda x:x[1],default=None); sup=min(below,key=lambda x:x[1],default=None)
    cage=(res[0]-sup[0]) if (res and sup and res[1]>0 and sup[1]<0) else None
    return res,sup,cage

# example days: narrow (5/28) and wide (6/11)
print("=== HOW THE WALLS WORK (real examples, ~10am chain) ===")
for ex in ['2026-05-28','2026-06-11']:
    cur.execute("""SELECT spot, rows FROM chain_snapshots WHERE ts::date=%s AND spot IS NOT NULL AND rows IS NOT NULL
       ORDER BY abs(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'America/New_York')::time - TIME '10:00:00')) LIMIT 1""",(ex,))
    spot,rows=cur.fetchone(); rows=rows if isinstance(rows,list) else json.loads(rows)
    res,sup,cage=walls(rows,float(spot))
    print(f"\n  {ex}  spot={spot:.0f}")
    print(f"    CALL WALL above (resistance/ceiling): strike {res[0]:.0f}  (+{res[1]/1e3:.1f}K gamma)  -> {res[0]-spot:.0f}pt above")
    print(f"    PUT WALL below  (support/floor):      strike {sup[0]:.0f}  ({sup[1]/1e3:.1f}K gamma)  -> {spot-sup[0]:.0f}pt below")
    print(f"    CAGE WIDTH = {res[0]:.0f} - {sup[0]:.0f} = {cage:.0f}pt  -> {'NARROW (MR)' if cage<=90 else 'WIDE (trend room)'}")

# day-by-day backtest post-V16
cur.execute("""SELECT s.ts, s.direction, r.state FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id=s.id
  WHERE s.ts::date>='2026-05-18' AND (r.state->>'fill_price') IS NOT NULL""")
rday=defaultdict(float)
for ts,dirn,st in cur.fetchall():
    s=st if isinstance(st,dict) else json.loads(st)
    e=s.get("fill_price"); x=s.get("close_fill_price") or s.get("stop_fill_price")
    if not e or not x: continue
    lng=str(dirn).lower() in ("long","bullish"); rday[ts.astimezone(ET).date()]+=((x-e) if lng else (e-x))*5
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, spot, rows FROM (
  SELECT ts,spot,rows,row_number() OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York')
    ORDER BY abs(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'America/New_York')::time - TIME '10:00:00'))) rn
  FROM chain_snapshots WHERE ts::date>='2026-05-18' AND spot IS NOT NULL AND rows IS NOT NULL) q WHERE rn=1""")
cg={}
for d,spot,rows in cur.fetchall():
    rows=rows if isinstance(rows,list) else json.loads(rows); _,_,c=walls(rows,float(spot)); cg[d]=c
print("\n\n=== DAY-BY-DAY: actual vs cage-sized (wide day = HALF size) — post-V16 ===")
print(f"{'date':>10} {'cage':>5} {'regime':>7} {'actual$':>8} {'sized$':>7} {'delta':>6}")
tA=tS=0
for d in sorted(rday):
    c=cg.get(d); reg='NARROW' if (c is not None and c<=90) else ('WIDE' if c is not None else '?')
    a=rday[d]; mult=0.5 if reg=='WIDE' else 1.0; s=a*mult
    tA+=a; tS+=s
    mark=' <-- halved' if reg=='WIDE' else ''
    print(f"{str(d):>10} {(c or 0):>5.0f} {reg:>7} {a:>8.0f} {s:>7.0f} {s-a:>+6.0f}{mark}")
print(f"\n  ACTUAL total: ${tA:+.0f}   |   CAGE-SIZED (half on wide): ${tS:+.0f}   |   DELTA: ${tS-tA:+.0f}")
# skip variant
tSkip=sum(rday[d] for d in rday if not (cg.get(d) and cg[d]>90))
print(f"  If WIDE days SKIPPED entirely: ${tSkip:+.0f}   |   DELTA: ${tSkip-tA:+.0f}")
