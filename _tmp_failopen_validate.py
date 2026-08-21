import os, psycopg2
from collections import defaultdict
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
# Full basket era: live_pass longs, bucket by open-basket confirm vs neutral(fail-open)
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, setup_name,
     to_char(ts AT TIME ZONE 'America/New_York','HH24:MI') hm, basket_pct, outcome_pnl
   FROM setup_log
   WHERE live_pass=true AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL
     AND basket_pct IS NOT NULL
     AND ts AT TIME ZONE 'America/New_York' >= '2026-06-11'
   ORDER BY ts""")
rows=cur.fetchall()
def summ(g,lab):
    if not g: print(f"  {lab:<40} n=0"); return
    w=sum(1 for r in g if float(r[4])>0); pts=sum(float(r[4]) for r in g)
    print(f"  {lab:<40} n={len(g):>3} WR={w/len(g)*100:>4.0f}% pts={pts:>8.1f}  $@1MES={pts*5-len(g):>7.0f}")
print(f"=== Basket-era (Jun11-Jul9) live_pass LONGS: confirm vs neutral/fail-open (n={len(rows)}) ===")
summ([r for r in rows if float(r[3])>=0.15],"open-basket CONFIRM (>=+0.15)")
summ([r for r in rows if abs(float(r[3]))<0.15],"open-basket NEUTRAL/fail-open (<0.15)")
print("  --- neutral split ---")
summ([r for r in rows if float(r[3])==0.0],"  basket EXACTLY 0.00 (uncomputed@open)")
summ([r for r in rows if 0<abs(float(r[3]))<0.15],"  basket flat but nonzero")
print("  --- neutral by time ---")
summ([r for r in rows if abs(float(r[3]))<0.15 and r[2]<'10:00'],"  neutral & before 10:00 ET")
summ([r for r in rows if abs(float(r[3]))<0.15 and r[2]>='10:00'],"  neutral & 10:00+ ET")
print("  --- confirm by time (control) ---")
summ([r for r in rows if float(r[3])>=0.15 and r[2]<'10:00'],"  confirm & before 10:00 ET")
summ([r for r in rows if float(r[3])>=0.15 and r[2]>='10:00'],"  confirm & 10:00+ ET")
# per setup, neutral longs
print("  --- neutral/fail-open longs by setup ---")
bys=defaultdict(list)
for r in rows:
    if abs(float(r[3]))<0.15: bys[r[1]].append(r)
for s,g in sorted(bys.items(),key=lambda x:-len(x[1])):
    summ(g,f"  {s}")
c.close()
