import os, psycopg, json
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
# coverage of mes_sim among live_pass trades
cur.execute("""SELECT to_char(ts,'YYYY-MM') m, count(*) tot, count(mes_sim_outcome_pnl) withmes
  FROM setup_log WHERE live_pass=true AND outcome_pnl IS NOT NULL AND ts::date>='2026-02-01' GROUP BY 1 ORDER BY 1""")
print("mes_sim coverage among live_pass trades by month (tot / with_mes_sim):")
for m,t,w in cur.fetchall(): print(f"  {m}: {w}/{t}")

# per-day portal(chain) vs mes_sim net, only trades that HAVE mes_sim (apples-to-apples)
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d,
   sum(outcome_pnl) chain, sum(mes_sim_outcome_pnl) mes
   FROM setup_log WHERE live_pass=true AND mes_sim_outcome_pnl IS NOT NULL AND ts::date>='2026-04-01'
   GROUP BY 1""")
pd={}
for d,ch,me in cur.fetchall(): pd[d]=(float(ch),float(me))
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
cg={}
for d,spot,rows in cur.fetchall():
    rows=rows if isinstance(rows,list) else json.loads(rows); cg[d]=walls(rows,float(spot))
days=sorted(d for d in pd if d in cg and cg[d] is not None)
print(f"\n=== THE TEST: chain-sim vs MES-sim BY CAGE REGIME (Apr+, n={len(days)}) ===")
for lbl,pred in [("NARROW (<=90)",lambda d:cg[d]<=90),("WIDE (>90)",lambda d:cg[d]>90)]:
    g=[d for d in days if pred(d)]
    ch=sum(pd[d][0] for d in g); me=sum(pd[d][1] for d in g)
    print(f"  {lbl}: n={len(g)}  chain-sim {ch:+.0f}p (${ch*5:+.0f})  |  MES-sim {me:+.0f}p (${me*5:+.0f})  |  gap {ch-me:+.0f}p")
    print(f"        per-day: chain {ch/len(g):+.1f}p  vs  MES {me/len(g):+.1f}p")
# cage-sizing on MES-sim baseline
base=[pd[d][1] for d in days]; half=[pd[d][1]*(0.5 if cg[d]>90 else 1.0) for d in days]
print(f"\n=== cage-sizing vs MES-SIM baseline (realistic exec) ===")
print(f"  baseline (all 1x): {sum(base):+.0f}p (${sum(base)*5:+.0f})")
print(f"  half-size wide:    {sum(half):+.0f}p (${sum(half)*5:+.0f})  delta ${(sum(half)-sum(base))*5:+.0f}")
# per month
mm=defaultdict(lambda:[0.0,0.0])
for d in days: mm[d.strftime('%Y-%m')][0]+=pd[d][1]; mm[d.strftime('%Y-%m')][1]+=pd[d][1]*(0.5 if cg[d]>90 else 1.0)
print("  per-month delta (MES-sim baseline):")
for k in sorted(mm):
    b,h=mm[k]; nw=sum(1 for d in days if d.strftime('%Y-%m')==k and cg[d]>90)
    print(f"    {k}: base {b:+5.0f}p sized {h:+5.0f}p delta ${(h-b)*5:+5.0f} ({nw} wide)")
