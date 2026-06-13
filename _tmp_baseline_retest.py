import os, psycopg, json, statistics
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
# daily chain & mes net (only live_pass trades that HAVE mes_sim -> apples to apples)
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, direction,
   outcome_pnl chain, mes_sim_outcome_pnl mes
   FROM setup_log WHERE live_pass=true AND mes_sim_outcome_pnl IS NOT NULL AND ts::date>='2026-04-01'""")
ch=defaultdict(float); me=defaultdict(float); chL=defaultdict(float); meL=defaultdict(float); chS=defaultdict(float); meS=defaultdict(float)
for d,dirn,c,m in cur.fetchall():
    c=float(c); m=float(m); lng=str(dirn).lower() in ("long","bullish")
    ch[d]+=c; me[d]+=m
    if lng: chL[d]+=c; meL[d]+=m
    else: chS[d]+=c; meS[d]+=m
# features
def walls(rows,spot):
    lv=[]
    for r in rows:
        try: lv.append((float(r[10]),((r[1] or 0)-(r[19] or 0))*(r[3] or 0)*100))
        except: pass
    ab=[(s,g) for s,g in lv if s>spot]; be=[(s,g) for s,g in lv if s<spot]
    res=max(ab,key=lambda x:x[1],default=None); sup=min(be,key=lambda x:x[1],default=None)
    return (res[0]-sup[0]) if (res and sup and res[1]>0 and sup[1]<0) else None
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, spot, rows FROM (
  SELECT ts,spot,rows,row_number() OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York')
    ORDER BY abs(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'America/New_York')::time - TIME '10:00:00'))) rn
  FROM chain_snapshots WHERE ts::date>='2026-04-01' AND spot IS NOT NULL AND rows IS NOT NULL) q WHERE rn=1""")
cage={}
for d,spot,rows in cur.fetchall():
    rows=rows if isinstance(rows,list) else json.loads(rows); cage[d]=walls(rows,float(spot))
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, min(spot) lo,max(spot) hi,
   (array_agg(spot ORDER BY ts))[1] op,(array_agg(spot ORDER BY ts DESC))[1] cl,(array_agg(vix ORDER BY ts))[1] vix
   FROM chain_snapshots WHERE ts::date>='2026-04-01' AND spot IS NOT NULL GROUP BY 1""")
feat={}
for d,lo,hi,op,cl,vix in cur.fetchall():
    feat[d]={'range':float(hi)-float(lo),'trend':abs(float(cl)-float(op)),'vix':float(vix) if vix else None}
days=sorted(d for d in ch if d in cage and cage[d] is not None and d in feat)
print(f"Apr-Jun window, n={len(days)} days (both baselines, same trades)\n")
def cmp(name, pred):
    g=[d for d in days if pred(d)]; 
    if not g: print(f"  {name}: n=0"); return
    c=sum(ch[d] for d in g)/len(g); m=sum(me[d] for d in g)/len(g)
    print(f"  {name:<20} n={len(g):>2}  chain {c:+5.1f}p/d   MES {m:+5.1f}p/d   (gap {c-m:+4.1f})")

print("=== SIGNAL: gamma CAGE ===")
cmp("narrow <=90", lambda d: cage[d]<=90); cmp("wide >90", lambda d: cage[d]>90)
print("=== SIGNAL: realized RANGE ===")
cmp("tight <=60", lambda d: feat[d]['range']<=60); cmp("wide >60", lambda d: feat[d]['range']>60)
print("=== SIGNAL: realized TREND |move| ===")
cmp("chop <=30", lambda d: feat[d]['trend']<=30); cmp("trend >30", lambda d: feat[d]['trend']>30)
print("=== SIGNAL: VIX ===")
cmp("VIX<19", lambda d:(feat[d]['vix'] or 0)<19); cmp("VIX>=19", lambda d:(feat[d]['vix'] or 0)>=19)
print("\n=== DIRECTION on each baseline (Apr-Jun) ===")
cL=sum(chL[d] for d in days); mL=sum(meL[d] for d in days); cS=sum(chS[d] for d in days); mS=sum(meS[d] for d in days)
print(f"  LONGS:  chain {cL:+.0f}p   MES {mL:+.0f}p   (chain overstates {cL-mL:+.0f})")
print(f"  SHORTS: chain {cS:+.0f}p   MES {mS:+.0f}p   (chain overstates {cS-mS:+.0f})")

# (b) cage sizing rule on MES-sim — proper, with drawdown + Sharpe
print("\n=== (b) CAGE SIZING on MES-sim baseline ===")
def stats(series):
    tot=sum(series); peak=0;eq=0;dd=0
    for v in series: eq+=v;peak=max(peak,eq);dd=min(dd,eq-peak)
    sh=(statistics.mean(series)/statistics.pstdev(series)*(252**0.5)) if len(series)>1 and statistics.pstdev(series)>0 else 0
    return tot,dd,sh
base=[me[d] for d in days]
for nm,mult in [("baseline (all 1x)",None),("narrow 1x / wide 0.5x",('w',0.5)),("narrow 1.3x / wide 0.5x",('b',1.3,0.5)),("narrow 1x / wide 0x (skip)",('w',0.0))]:
    if mult is None: s=base
    elif mult[0]=='w': s=[me[d]*(mult[1] if cage[d]>90 else 1.0) for d in days]
    else: s=[me[d]*(mult[1] if cage[d]<=90 else mult[2]) for d in days]
    tot,dd,sh=stats(s)
    print(f"  {nm:<26} tot {tot:+6.0f}p (${tot*5:+5.0f})  maxDD {dd:+5.0f}p  Sharpe {sh:.2f}")
