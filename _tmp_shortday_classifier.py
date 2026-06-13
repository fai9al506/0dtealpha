import os, psycopg2
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo
ET=ZoneInfo('America/New_York')
c=psycopg2.connect(os.environ['DATABASE_URL']); cur=c.cursor()

# 1) per-day open spot, 10:30 spot, eod spot
cur.execute("SELECT ts, spot FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY ts")
byday=defaultdict(list)
for ts,sp in cur.fetchall():
    e=ts.astimezone(ET)
    if dtime(9,30)<=e.time()<=dtime(16,0):
        byday[e.date()].append((e,float(sp)))
day_meta={}
for d,rows in byday.items():
    if len(rows)<5: continue
    op=rows[0][1]
    # spot nearest 10:30
    s1030=min(rows,key=lambda r:abs((r[0].hour*60+r[0].minute)-630))[1]
    eod=rows[-1][1]
    day_meta[d]={'open':op,'fh':s1030-op,'daymove':eod-op}

# 2) MR LONG trades (SC+DD) firing AFTER 10:30, with outcome
cur.execute("""SELECT ts, setup_name, outcome_pnl, outcome_result FROM setup_log
  WHERE setup_name IN ('Skew Charm','DD Exhaustion') AND direction IN ('long','bullish')
  AND outcome_result IS NOT NULL AND outcome_result<>'OPEN' ORDER BY id""")
trades=[]
for ts,sn,pnl,res in cur.fetchall():
    e=ts.astimezone(ET); d=e.date()
    if e.time()<dtime(10,30): continue   # classifier known by 10:30
    if d not in day_meta: continue
    trades.append((d,sn,float(pnl) if pnl is not None else 0,res,day_meta[d]['fh']))

def stat(s):
    n=len(s)
    if not n: return "n=0"
    w=sum(1 for t in s if t[3]=='WIN'); net=sum(t[2] for t in s)
    return f"n={n:<4} WR={w/n*100:3.0f}%  net={net:+7.1f}p (${net*5:+6.0f})"

for TH in (-8,-12,-20):
    flagged=[t for t in trades if t[4]<=TH]   # down first-hour
    other=[t for t in trades if t[4]>TH]
    print(f"\n### first-hour open->10:30 <= {TH}pt  (SC/DD longs after 10:30) ###")
    print("  FLAGGED down-morning:", stat(flagged))
    print("  other days          :", stat(other))
    # era stability of the flagged (what we'd block)
    bym=defaultdict(list)
    for t in flagged: bym[str(t[0])[:7]].append(t)
    print("  flagged by month (what blocking would remove):")
    for m in sorted(bym): print(f"     {m}: {stat(bym[m])}")
cur.close(); c.close()
