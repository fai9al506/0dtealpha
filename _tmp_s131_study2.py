import os, psycopg, json
from datetime import timedelta
from zoneinfo import ZoneInfo
from app.mes_sim_backfill import mes_walk
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
PARAMS={"Skew Charm":(14,10,5),"AG Short":(12,12,5),"DD Exhaustion":(12,20,5)}
def spx_b(esbars,spots,e,espx,lng,sl,act,gap):
    slm=e-sl if lng else e+sl; tdis=None
    for ts_s,_,o,h,l,c in esbars:
        if (lng and l<=slm) or ((not lng) and h>=slm): tdis=ts_s; break
    mf=0.0; ttr=None
    for ts,sp in spots:
        fav=(sp-espx) if lng else (espx-sp); mf=max(mf,fav)
        if mf>=act:
            tr=espx+(mf-gap) if lng else espx-(mf-gap)
            if (lng and sp<=tr) or ((not lng) and sp>=tr): ttr=ts; break
    cand=[]
    if tdis: cand.append((tdis,"dis"))
    if ttr: cand.append((ttr,"tr"))
    if not cand:
        c=esbars[-1][5]; return (c-e) if lng else (e-c)
    cand.sort(); t,rs=cand[0]
    if rs=="dis": return (slm-e) if lng else (e-slm)
    fill=e
    for ts_s,_,o,h,l,c in esbars:
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
    a=mes_walk(eb,e,lng,sl,None,0,act,gap,150)["pnl"]
    b=spx_b(eb,sp,e,espx,lng,sl,act,gap)
    res.append((lid,ts.astimezone(ET),name,bk,a,b))
import statistics
ta=sum(r[4] for r in res); tb=sum(r[5] for r in res)
print(f"n={len(res)}  A(current) {ta:+.0f}pt(${ta*5:+.0f})  B(SPX-timing) {tb:+.0f}pt(${tb*5:+.0f})  delta {(tb-ta)*5:+.0f}$")
print(f"  Gate2 median|A-broker|={statistics.median([abs(r[4]-r[3]) for r in res]):.1f}pt")
# per-month
print("\nper-month delta (B-A):")
from collections import defaultdict
m=defaultdict(lambda:[0,0])
for r in res:
    k=r[1].strftime("%Y-%m"); m[k][0]+=r[5]-r[4]; m[k][1]+=1
for k in sorted(m): print(f"  {k}: {m[k][0]*5:+6.0f}$  (n={m[k][1]})")
# per-setup
print("\nper-setup delta (B-A):")
s=defaultdict(lambda:[0,0])
for r in res: s[r[2]][0]+=r[5]-r[4]; s[r[2]][1]+=1
for k in s: print(f"  {k}: {s[k][0]*5:+6.0f}$  (n={s[k][1]})")
# concentration
deltas=sorted([(r[5]-r[4],r) for r in res],reverse=True)
top5=sum(d for d,_ in deltas[:5]); allpos=sum(d for d,_ in deltas if d>0)
print(f"\nconcentration: total B-A delta={ (tb-ta)*5:+.0f}$; top-5 winners alone={top5*5:+.0f}$")
better=[r for r in res if r[5]-r[4]>1]; worse=[r for r in res if r[5]-r[4]<-1]
print(f"  B better: {len(better)} (+{sum(r[5]-r[4] for r in better)*5:.0f}$) | B worse: {len(worse)} ({sum(r[5]-r[4] for r in worse)*5:.0f}$) | ~same: {len(res)-len(better)-len(worse)}")
print("\ntop B-improvements:")
for d,r in deltas[:6]: print(f"  {r[1].date()} lid{r[0]:>5} {r[2]:<12} A{r[4]:+6.1f} B{r[5]:+6.1f} d{d:+5.1f}")
print("worst B-regressions:")
for d,r in deltas[-6:]: print(f"  {r[1].date()} lid{r[0]:>5} {r[2]:<12} A{r[4]:+6.1f} B{r[5]:+6.1f} d{d:+5.1f}")
