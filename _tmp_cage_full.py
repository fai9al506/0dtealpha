import os, psycopg, json
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
# portal V16 daily net (the baseline, ~85% of TSRT)
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, sum(outcome_pnl)
  FROM setup_log WHERE live_pass=true AND outcome_pnl IS NOT NULL AND ts::date>='2026-02-01' GROUP BY 1""")
pday={d:float(p) for d,p in cur.fetchall()}
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
  FROM chain_snapshots WHERE ts::date>='2026-02-01' AND spot IS NOT NULL AND rows IS NOT NULL) q WHERE rn=1""")
cg={}
for d,spot,rows in cur.fetchall():
    rows=rows if isinstance(rows,list) else json.loads(rows); cg[d]=walls(rows,float(spot))
days=sorted(d for d in pday if d in cg and cg[d] is not None)
def maxdd(series):
    peak=0; eq=0; dd=0
    for v in series: eq+=v; peak=max(peak,eq); dd=min(dd,eq-peak)
    return dd
base=[pday[d] for d in days]
half=[pday[d]*(0.5 if cg[d]>90 else 1.0) for d in days]
skip=[pday[d]*(0.0 if cg[d]>90 else 1.0) for d in days]
print(f"FULL HISTORY portal V16 baseline, n={len(days)} days ({days[0]}->{days[-1]})")
print(f"  wide days (cage>90): {sum(1 for d in days if cg[d]>90)}  | narrow: {sum(1 for d in days if cg[d]<=90)}")
print(f"\n  {'variant':<26}{'total pts':>10}{'$@1MES':>9}{'maxDD pts':>10}")
for nm,s in [('BASELINE (all 1x)',base),('cage: half-size wide',half),('cage: SKIP wide',skip)]:
    print(f"  {nm:<26}{sum(s):>+10.0f}{sum(s)*5:>+9.0f}{maxdd(s):>+10.0f}")
print(f"\n  per-month delta (half-size vs baseline):")
mm=defaultdict(lambda:[0.0,0.0])
for d in days:
    mm[d.strftime('%Y-%m')][0]+=pday[d]; mm[d.strftime('%Y-%m')][1]+=pday[d]*(0.5 if cg[d]>90 else 1.0)
for k in sorted(mm):
    b,h=mm[k]; print(f"    {k}: baseline {b:+6.0f}p  sized {h:+6.0f}p  delta {(h-b)*5:+5.0f}$  ({sum(1 for d in days if d.strftime('%Y-%m')==k and cg[d]>90)} wide days)")
# how do wide days do on full history?
wide=[pday[d] for d in days if cg[d]>90]; narrow=[pday[d] for d in days if cg[d]<=90]
print(f"\n  wide-cage days full history: n={len(wide)} net {sum(wide):+.0f}p avg {sum(wide)/len(wide):+.1f}p/day  (losers: {sum(1 for v in wide if v<0)}/{len(wide)})")
print(f"  narrow-cage days:            n={len(narrow)} net {sum(narrow):+.0f}p avg {sum(narrow)/len(narrow):+.1f}p/day  (losers: {sum(1 for v in narrow if v<0)}/{len(narrow)})")
