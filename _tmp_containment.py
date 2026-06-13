import os, psycopg, json
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()

# daily P&L portal V16 split direction
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, direction, outcome_pnl
  FROM setup_log WHERE live_pass=true AND outcome_pnl IS NOT NULL AND ts::date>='2026-02-01'""")
day=defaultdict(lambda:{'net':0.0,'L':0.0,'S':0.0})
for d,dirn,p in cur.fetchall():
    p=float(p); lng=str(dirn).lower() in ("long","bullish")
    day[d]['net']+=p; day[d]['L' if lng else 'S']+=p

# containment metric from ~10:00 ET chain snapshot each day
def gex_metrics(rows, spot):
    levels=[]
    for r in rows:
        try:
            cg,coi,strike,pg,poi=r[3],r[1],r[10],r[17],r[19]
            gamma=(cg or 0); net=( (coi or 0)-(poi or 0) )*gamma*100  # net dealer gamma per strike
            levels.append((float(strike),net))
        except: pass
    if not levels: return None
    above=[(s,g) for s,g in levels if s>spot]
    below=[(s,g) for s,g in levels if s<spot]
    # resistance wall = strongest +gamma above; support = strongest -gamma below
    res=max(above,key=lambda x:x[1],default=None)
    sup=min(below,key=lambda x:x[1],default=None)
    cage = (res[0]-sup[0]) if (res and sup and res[1]>0 and sup[1]<0) else None
    # gamma density within +/-25pts of spot (abs net)
    dens=sum(abs(g) for s,g in levels if abs(s-spot)<=25)
    total=sum(abs(g) for s,g in levels) or 1
    return {'cage':cage,'dens_frac':dens/total,'res_d':(res[0]-spot) if res else None,'sup_d':(spot-sup[0]) if sup else None,
            'res_g':res[1] if res else 0,'sup_g':sup[1] if sup else 0}

cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, spot, rows FROM (
  SELECT ts, spot, rows, row_number() OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York')
    ORDER BY abs(EXTRACT(EPOCH FROM (ts AT TIME ZONE 'America/New_York')::time - TIME '10:00:00'))) rn
  FROM chain_snapshots WHERE ts::date>='2026-02-01' AND spot IS NOT NULL AND rows IS NOT NULL) q WHERE rn=1""")
met={}
for d,spot,rows in cur.fetchall():
    rows=rows if isinstance(rows,list) else json.loads(rows)
    m=gex_metrics(rows,float(spot))
    if m: met[d]=m
days=sorted(d for d in day if d in met and met[d]['cage'] is not None)
print(f"n days with cage metric = {len(days)}")
def blk(name,pred,lst=None):
    g=[d for d in (lst or days) if pred(d)]
    if not g: print(f"  {name}: n=0"); return
    net=sum(day[d]['net'] for d in g); L=sum(day[d]['L'] for d in g); S=sum(day[d]['S'] for d in g)
    print(f"  {name:<22} n={len(g):>3}  net {net:+7.0f}p (L {L:+6.0f}/S {S:+6.0f}) avg {net/len(g):+5.1f}p")
import statistics
cages=sorted(met[d]['cage'] for d in days); med=statistics.median(cages)
print(f"cage width: median={med:.0f}pt  range {cages[0]:.0f}-{cages[-1]:.0f}")
print("\n=== CONTAINMENT (cage width: narrow=pinned/MR, wide=trend room) ===")
blk("narrow cage <=median", lambda d: met[d]['cage']<=med)
blk("wide cage >median", lambda d: met[d]['cage']>med)
print("\n=== gamma density near spot (high=pinned/MR) ===")
dm=statistics.median(met[d]['dens_frac'] for d in days)
blk("high density >=med", lambda d: met[d]['dens_frac']>=dm)
blk("low density <med", lambda d: met[d]['dens_frac']<dm)
print("\n=== resistance wall proximity (spot near a +gamma ceiling = capped/MR?) ===")
blk("res wall <=15pt away", lambda d: (met[d]['res_d'] or 99)<=15)
blk("res wall >15pt away", lambda d: (met[d]['res_d'] or 99)>15)
# by-month robustness for narrow-cage edge
print("\n=== narrow-cage net by MONTH (robust or fluke?) ===")
mm=defaultdict(lambda:[0.0,0])
for d in days:
    if met[d]['cage']<=med: mm[d.strftime('%Y-%m')][0]+=day[d]['net']; mm[d.strftime('%Y-%m')][1]+=1
for k in sorted(mm): print(f"  {k}: {mm[k][0]:+7.0f}p (n={mm[k][1]})")
