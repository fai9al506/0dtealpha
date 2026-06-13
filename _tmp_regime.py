import os, psycopg, json
from datetime import date, time
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()

# 1) daily TSRT net split long/short (from real fills, all post-V16 days)
cur.execute("""SELECT s.ts, s.direction, r.state FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id=s.id
  WHERE s.ts::date>='2026-05-18' AND (r.state->>'fill_price') IS NOT NULL ORDER BY s.ts""")
day=defaultdict(lambda:{'net':0.0,'L':0.0,'S':0.0,'nL':0,'nS':0})
for ts,dirn,st in cur.fetchall():
    s=st if isinstance(st,dict) else json.loads(st)
    e=s.get("fill_price"); x=s.get("close_fill_price") or s.get("stop_fill_price")
    if not e or not x: continue
    lng=str(dirn).lower() in ("long","bullish"); g=((x-e) if lng else (e-x))*5
    d=ts.astimezone(ET).date()
    day[d]['net']+=g
    if lng: day[d]['L']+=g; day[d]['nL']+=1
    else: day[d]['S']+=g; day[d]['nS']+=1

# 2) morning gamma regime (dte0 SPX, first scan each day)
cur.execute("""SELECT scan_date, total_net_gex FROM (
   SELECT scan_date, total_net_gex, row_number() OVER (PARTITION BY scan_date ORDER BY scan_ts) rn
   FROM dte0_gex_scans WHERE symbol IN ('$SPXW.X','SPX')) q WHERE rn=1""")
gamma={r[0]:(float(r[1]) if r[1] is not None else None) for r in cur.fetchall()}

# 3) daily features from chain_snapshots: open spot, prev close, vix, vix3m, day range
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d,
   min(spot) lo, max(spot) hi,
   (array_agg(spot ORDER BY ts))[1] open_spot,
   (array_agg(spot ORDER BY ts DESC))[1] close_spot,
   (array_agg(vix ORDER BY ts))[1] vix, (array_agg(vix3m ORDER BY ts))[1] vix3m
   FROM chain_snapshots WHERE ts::date>='2026-05-15' AND spot IS NOT NULL GROUP BY 1 ORDER BY 1""")
feat={}; prev_close=None
for r in cur.fetchall():
    d,lo,hi,op,cl,vix,vix3m=r
    feat[d]={'lo':float(lo),'hi':float(hi),'open':float(op),'close':float(cl),
             'vix':float(vix) if vix else None,'vix3m':float(vix3m) if vix3m else None,
             'gap':(float(op)-prev_close) if prev_close else None,
             'range':float(hi)-float(lo),'trend':float(cl)-float(op)}
    prev_close=float(cl)

print(f"{'date':>10} {'net$':>6} {'L$':>6} {'S$':>6} | {'gamma(net_gex)':>14} {'gap':>6} {'range':>6} {'trend':>6} {'vix':>5}")
rows=[]
for d in sorted(day):
    n=day[d]; f=feat.get(d,{}); g=gamma.get(d)
    gs = f"{g/1e9:+.2f}B" if g is not None else "  n/a "
    rows.append((d,n,f,g))
    print(f"{str(d):>10} {n['net']:>6.0f} {n['L']:>6.0f} {n['S']:>6.0f} | {gs:>14} "
          f"{(f.get('gap') or 0):>6.0f} {(f.get('range') or 0):>6.0f} {(f.get('trend') or 0):>+6.0f} {(f.get('vix') or 0):>5.1f}")

# separation: gamma sign vs net
print("\n--- separation by morning gamma sign (net_gex) ---")
pos=[r for r in rows if r[3] is not None and r[3]>0]; neg=[r for r in rows if r[3] is not None and r[3]<0]
def agg(lst):
    return (len(lst), sum(r[1]['net'] for r in lst), sum(r[1]['L'] for r in lst), sum(r[1]['S'] for r in lst))
for lbl,lst in [("+gamma days",pos),("-gamma days",neg)]:
    n,net,L,S=agg(lst); print(f"  {lbl}: n={n} net=${net:+.0f} (L ${L:+.0f} / S ${S:+.0f})  avg/day ${net/n if n else 0:+.0f}")
# separation by |trend| (trending day = bad for MR?)
print("\n--- separation by realized |trend| (open->close) ---")
big=[r for r in rows if r[2].get('trend') is not None and abs(r[2]['trend'])>30]
sml=[r for r in rows if r[2].get('trend') is not None and abs(r[2]['trend'])<=30]
for lbl,lst in [("trend day |move|>30",big),("chop day |move|<=30",sml)]:
    n,net,L,S=agg(lst); print(f"  {lbl}: n={n} net=${net:+.0f} (L ${L:+.0f}/S ${S:+.0f}) avg/day ${net/n if n else 0:+.0f}")
# range
print("\n--- separation by realized range ---")
br=[r for r in rows if r[2].get('range',0)>70]; sr=[r for r in rows if 0<r[2].get('range',999)<=70]
for lbl,lst in [("wide range >70",br),("tight range <=70",sr)]:
    n,net,L,S=agg(lst); print(f"  {lbl}: n={n} net=${net:+.0f} (L ${L:+.0f}/S ${S:+.0f}) avg/day ${net/n if n else 0:+.0f}")
