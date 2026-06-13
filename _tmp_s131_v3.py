import os, psycopg, json, statistics
from datetime import timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo
from app.mes_sim_backfill import mes_walk
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
PARAMS={"Skew Charm":(14,10,5),"AG Short":(12,12,5),"DD Exhaustion":(12,20,5)}
def spx_b(eb,sp,e,espx,lng,sl,act,gap):
    slm=e-sl if lng else e+sl; tdis=None
    for ts_s,_,o,h,l,c in eb:
        if (lng and l<=slm) or ((not lng) and h>=slm): tdis=ts_s; break
    mf=0.0; ttr=None
    for ts,s in sp:
        fav=(s-espx) if lng else (espx-s); mf=max(mf,fav)
        if mf>=act:
            tr=espx+(mf-gap) if lng else espx-(mf-gap)
            if (lng and s<=tr) or ((not lng) and s>=tr): ttr=ts; break
    cand=[]
    if tdis: cand.append((tdis,"dis"))
    if ttr: cand.append((ttr,"tr"))
    if not cand:
        c=eb[-1][5]; return (c-e) if lng else (e-c)
    cand.sort(); t,rs=cand[0]
    if rs=="dis": return (slm-e) if lng else (e-slm)
    fill=e
    for ts_s,_,o,h,l,c in eb:
        if ts_s<=t: fill=c
        else: break
    return (fill-e) if lng else (e-fill)
cur.execute("""SELECT s.id,s.ts,s.setup_name,s.direction,r.state FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id=s.id
  WHERE s.setup_name IN ('Skew Charm','AG Short','DD Exhaustion') AND (r.state->>'fill_price') IS NOT NULL AND s.ts::date>='2026-04-15' ORDER BY s.ts""")
res=[]
for lid,ts,name,dirn,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    e=st.get("fill_price"); x=st.get("close_fill_price") or st.get("stop_fill_price")
    if not e or not x: continue
    lng=str(dirn).lower() in ("long","bullish"); bk=(x-e) if lng else (e-x)
    sl,act,gap=PARAMS[name]; end=ts+timedelta(minutes=150)
    cur.execute("SELECT ts_start,ts_end,bar_open,bar_high,bar_low,bar_close FROM vps_es_range_bars WHERE range_pts=5 AND ts_start>=%s AND ts_start<=%s ORDER BY ts_start",(ts,end))
    eb=[(r[0],r[1],float(r[2]),float(r[3]),float(r[4]),float(r[5])) for r in cur.fetchall()]
    if not eb: continue
    cur.execute("SELECT ts,spot FROM chain_snapshots WHERE ts>=%s AND ts<=%s AND spot IS NOT NULL ORDER BY ts",(ts,end))
    sp=[(r[0],float(r[1])) for r in cur.fetchall()]
    espx=sp[0][1] if sp else e
    a=mes_walk(eb,e,lng,sl,None,0,act,gap,150)["pnl"]; b=spx_b(eb,sp,e,espx,lng,sl,act,gap)
    d=ts.astimezone(ET).date()
    res.append((d,name,lng,bk,a,b))
# Gate-2 by era (proves April mechanism differs)
print("Gate-2 |A_sim - broker| median by era (high = mechanism mismatch):")
for lbl,lo,hi in [("pre-S131 (Apr15-May17)","2026-04-15","2026-05-17"),("post-S131 (May18+)","2026-05-18","2026-12-31")]:
    import datetime as dt
    sub=[r for r in res if dt.date.fromisoformat(lo)<=r[0]<=dt.date.fromisoformat(hi)]
    if sub: print(f"  {lbl}: median={statistics.median([abs(r[3]-r[4]) for r in sub]):.1f}pt  n={len(sub)}")
# POST-S131 only, by direction
import datetime as dt
post=[r for r in res if r[0]>=dt.date(2026,5,18)]
print(f"\n=== POST-S131 (May 18+) only, n={len(post)} ===")
for lbl,filt in [("ALL",lambda r:True),("LONGS",lambda r:r[2]),("SHORTS",lambda r:not r[2])]:
    g=[r for r in post if filt(r)]
    if not g: continue
    ta=sum(r[4] for r in g); tb=sum(r[5] for r in g)
    better=sum(1 for r in g if r[5]-r[4]>1); worse=sum(1 for r in g if r[5]-r[4]<-1)
    print(f"  {lbl:<7} n={len(g):>3}  A {ta:+5.0f}pt  B {tb:+5.0f}pt  delta ${ (tb-ta)*5:+5.0f}  (B better {better} / worse {worse})")
# post-S131 by setup x direction
print("\n  by setup x direction (post-S131, delta B-A $):")
agg=defaultdict(lambda:[0,0])
for r in post:
    k=f"{r[1]} {'long' if r[2] else 'short'}"; agg[k][0]+=r[5]-r[4]; agg[k][1]+=1
for k in sorted(agg): print(f"    {k:<22} ${agg[k][0]*5:+6.0f} (n={agg[k][1]})")
# month split post-S131
print("\n  post-S131 month delta (B-A $):")
m=defaultdict(lambda:[0,0])
for r in post: m[r[0].strftime('%Y-%m')][0]+=r[5]-r[4]; m[r[0].strftime('%Y-%m')][1]+=1
for k in sorted(m): print(f"    {k}: ${m[k][0]*5:+6.0f} (n={m[k][1]})")

print("\n=== SHORTS post-S131: direction edge by month (robustness) ===")
sh=[r for r in post if not r[2]]
mm=defaultdict(lambda:[0,0,0,0])  # dA, dB, n, b_better
for r in sh:
    k=r[0].strftime('%Y-%m'); mm[k][0]+=r[4]; mm[k][1]+=r[5]; mm[k][2]+=1
    if r[5]-r[4]>1: mm[k][3]+=1
for k in sorted(mm):
    a,b,n,bb=mm[k]
    print(f"  {k}: A {a:+5.0f}pt  B {b:+5.0f}pt  delta ${ (b-a)*5:+5.0f}  n={n}  (B better {bb})")
print("\n  SHORTS by setup x month:")
sm=defaultdict(lambda:[0,0])
for r in sh:
    k=f"{r[1][:9]} {r[0].strftime('%m')}"; sm[k][0]+=r[5]-r[4]; sm[k][1]+=1
for k in sorted(sm): print(f"    {k}: ${sm[k][0]*5:+5.0f} (n={sm[k][1]})")
print("\n=== LONGS post-S131 by month (is the -$ all June regime?) ===")
lo=[r for r in post if r[2]]
lm=defaultdict(lambda:[0,0,0])
for r in lo:
    k=r[0].strftime('%Y-%m'); lm[k][0]+=r[4]; lm[k][1]+=r[5]; lm[k][2]+=1
for k in sorted(lm):
    a,b,n=lm[k]; print(f"  {k}: A {a:+5.0f}pt  B {b:+5.0f}pt  delta ${ (b-a)*5:+5.0f}  n={n}")
