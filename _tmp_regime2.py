import os, psycopg
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
# daily portal-V16 P&L (live_pass) split direction, Feb-Jun
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, direction, outcome_pnl
  FROM setup_log WHERE live_pass=true AND outcome_pnl IS NOT NULL AND ts::date>='2026-02-01' ORDER BY 1""")
day=defaultdict(lambda:{'net':0.0,'L':0.0,'S':0.0})
for d,dirn,p in cur.fetchall():
    p=float(p); lng=str(dirn).lower() in ("long","bullish")
    day[d]['net']+=p
    if lng: day[d]['L']+=p
    else: day[d]['S']+=p
# daily features
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, min(spot) lo,max(spot) hi,
   (array_agg(spot ORDER BY ts))[1] op,(array_agg(spot ORDER BY ts DESC))[1] cl,
   (array_agg(vix ORDER BY ts))[1] vix
   FROM chain_snapshots WHERE ts::date>='2026-01-28' AND spot IS NOT NULL GROUP BY 1 ORDER BY 1""")
feat={}; pc=None
for d,lo,hi,op,cl,vix in cur.fetchall():
    feat[d]={'range':float(hi)-float(lo),'trend':float(cl)-float(op),'vix':float(vix) if vix else None,
             'gap':(float(op)-pc) if pc else None}; pc=float(cl)
days=sorted(d for d in day if d in feat)
print(f"n trading days = {len(days)}  ({days[0]} -> {days[-1]})")

def block(name, pred):
    g=[d for d in days if pred(d)]
    if not g: return
    net=sum(day[d]['net'] for d in g); L=sum(day[d]['L'] for d in g); S=sum(day[d]['S'] for d in g)
    print(f"  {name:<26} n={len(g):>3}  net {net:+7.0f}p  (L {L:+7.0f} / S {S:+7.0f})  avg/day {net/len(g):+5.1f}p")

print("\n=== RANGE regime (full history, portal V16 pts) ===")
block("wide range >70", lambda d: feat[d]['range']>70)
block("mid range 45-70", lambda d: 45<feat[d]['range']<=70)
block("tight range <=45", lambda d: feat[d]['range']<=45)
print("\n=== is VIX@open a usable MORNING proxy for the bad regime? ===")
block("VIX>=20", lambda d: (feat[d]['vix'] or 0)>=20)
block("VIX 17-20", lambda d: 17<=(feat[d]['vix'] or 0)<20)
block("VIX<17", lambda d: 0<(feat[d]['vix'] or 99)<17)
print("\n=== PERSISTENCE: does a wide-range day predict the NEXT day? ===")
wide=set(d for d in days if feat[d]['range']>70)
nxt_after_wide=[]; nxt_after_tight=[]
for i in range(1,len(days)):
    prev,cur_d=days[i-1],days[i]
    (nxt_after_wide if prev in wide else nxt_after_tight).append(cur_d)
for lbl,lst in [("day AFTER wide-range",nxt_after_wide),("day AFTER tight/mid",nxt_after_tight)]:
    if lst:
        net=sum(day[d]['net'] for d in lst); L=sum(day[d]['L'] for d in lst)
        print(f"  {lbl:<24} n={len(lst):>3}  net {net:+7.0f}p (L {L:+7.0f})  avg/day {net/len(lst):+5.1f}p")
print("\n=== PERSISTENCE: day after a LOSING day (<-50p portal)? ===")
lossday=set(d for d in days if day[d]['net']<-50)
af=[days[i] for i in range(1,len(days)) if days[i-1] in lossday]
aw=[days[i] for i in range(1,len(days)) if days[i-1] not in lossday]
for lbl,lst in [("day after LOSS day",af),("day after non-loss",aw)]:
    net=sum(day[d]['net'] for d in lst); L=sum(day[d]['L'] for d in lst)
    print(f"  {lbl:<24} n={len(lst):>3}  net {net:+7.0f}p (L {L:+7.0f})  avg/day {net/len(lst):+5.1f}p")
# by-month range check (robustness)
print("\n=== RANGE>70 net by month (robust or era-fluke?) ===")
mm=defaultdict(lambda:[0.0,0])
for d in days:
    if feat[d]['range']>70: mm[d.strftime('%Y-%m')][0]+=day[d]['net']; mm[d.strftime('%Y-%m')][1]+=1
for k in sorted(mm): print(f"  {k}: {mm[k][0]:+7.0f}p (n={mm[k][1]} wide days)")
