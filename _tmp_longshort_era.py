import os, psycopg2
from collections import defaultdict
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
cur.execute("""SELECT direction, basket_pct, outcome_pnl,
     to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM') mo
   FROM setup_log
   WHERE live_pass=true AND outcome_pnl IS NOT NULL AND basket_pct IS NOT NULL
     AND ts AT TIME ZONE 'America/New_York' >= '2026-06-11' ORDER BY ts""")
rows=[(('long' if d in('long','bullish') else 'short'),float(b),float(p),mo) for d,b,p,mo in cur.fetchall()]
def s(g,lab):
    if not g: print(f"  {lab:<32} n=0"); return
    w=sum(1 for x in g if x[2]>0); pts=sum(x[2] for x in g)
    # 012 sizing $: confirm=2x
    def conf(x): return (x[1]>=0.15) if x[0]=='long' else (x[1]<=-0.15)
    u1=pts*5-len(g)
    u012=sum(x[2]*5*(2 if conf(x) else 1)-(2 if conf(x) else 1) for x in g)
    print(f"  {lab:<32} n={len(g):>3} WR={w/len(g)*100:>4.0f}% pts={pts:>8.1f}  $@1={u1:>7.0f}  $@012={u012:>7.0f}")
print("=== Jun11-Jul9 live_pass by direction ===")
s([x for x in rows if x[0]=='long'],"ALL LONGS")
s([x for x in rows if x[0]=='short'],"ALL SHORTS")
print("=== LONGS by month ===")
for mo in ('2026-06','2026-07'):
    s([x for x in rows if x[0]=='long' and x[3]==mo],f"longs {mo}")
print("=== SHORTS by month ===")
for mo in ('2026-06','2026-07'):
    s([x for x in rows if x[0]=='short' and x[3]==mo],f"shorts {mo}")
print("=== TOTAL book ===")
s(rows,"ALL live_pass")
print("\n=== SIZING COUNTERFACTUALS on full era (what if we changed sizing) ===")
def total(mult_fn,lab):
    tot=0
    for x in rows:
        tot+=x[2]*5*mult_fn(x)-mult_fn(x)
    print(f"  {lab:<40} $ = {tot:>7.0f}")
def conf(x): return (x[1]>=0.15) if x[0]=='long' else (x[1]<=-0.15)
total(lambda x:1,"flat 1x all")
total(lambda x:2 if conf(x) else 1,"current 0/1/2 (2x confirmed)")
total(lambda x:1 if x[0]=='long' else (2 if conf(x) else 1),"longs always 1x, shorts 0/1/2")
total(lambda x:(2 if conf(x) else 1) if x[0]=='short' else (2 if conf(x) else 1),"check==current")
c.close()
