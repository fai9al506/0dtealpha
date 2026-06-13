import os, psycopg2
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')
conn = psycopg2.connect(os.environ['DATABASE_URL']); cur = conn.cursor()
cur.execute("select ts,spot from chain_snapshots where ts::date>='2026-02-25' and spot is not null order by ts")
days = defaultdict(list)
for ts, sp in cur.fetchall():
    et = ts.astimezone(ET); days[et.date()].append((et, float(sp)))
conn.close()
daylist = sorted(days)
WS, WE, CUT = dtime(9,30), dtime(11,30), dtime(16,0)
DIP, CONF, T, S = 8, 4, 10, 8

def find(d):
    hi=-1e9; ind=False; lo=1e9
    for et,sp in days[d]:
        if et.time()<WS: continue
        if et.time()>WE: break
        hi=max(hi,sp)
        if not ind:
            if sp<=hi-DIP: ind=True; lo=sp
        else:
            lo=min(lo,sp)
            if sp>=lo+CONF: return (sp,et)
    return None

def walk(d,entry,et0):
    after=[(e,sp) for e,sp in days[d] if e>et0]; last=None
    for e,sp in after:
        if e.time()>CUT: break
        last=sp
        if sp<=entry-S: return -S,'LOSS'
        if sp>=entry+T: return T,'WIN'
    return (round(last-entry,2),'EXPIRED') if last is not None else (0,'EXPIRED')

trades=[]
for d in daylist:
    f=find(d)
    if not f: continue
    e,et0=f; pnl,r=walk(d,e,et0); trades.append((d,pnl,r))

bym=defaultdict(list)
for d,p,r in trades: bym[d.strftime('%Y-%m')].append((p,r))
print('=== MONTHLY (dip-buy long, ONLY filter = the dip trigger, no regime gate) ===')
print('month    | trades | WR%  | totP   | avgP | maxDD')
for m in sorted(bym):
    if m < '2026-03': continue
    t=bym[m]; n=len(t); w=sum(1 for p,r in t if r=='WIN'); tot=sum(p for p,r in t)
    cum=0; pk=0; dd=0
    for p,r in t:
        cum+=p; pk=max(pk,cum); dd=min(dd,cum-pk)
    print(f' {m}  |  {n:>3}   | {100*w/n:>4.1f} | {tot:>6.1f} | {tot/n:>4.2f} | {dd:>5.1f}')

mm=[(d,p,r) for d,p,r in trades if d.isoformat()>='2026-03-01']
nn=len(mm); tt=sum(p for d,p,r in mm); ww=sum(1 for d,p,r in mm if r=='WIN')
ndays=len([d for d in daylist if d.isoformat()>='2026-03-01'])
print(f' TOTAL    |  {nn:>3}   | {100*ww/nn:>4.1f} | {tt:>6.1f} | {tt/nn:>4.2f} |')
print(f'\nMar-May: {ndays} trading days, {nn} triggered a dip-buy ({100*nn/ndays:.0f}% of days)')
print(f'Points/MONTH: {tt/3:.1f}p  (~${tt/3*5:.0f}/mo at 1 MES, ~${tt/3*50:.0f}/mo at 1 ES)')
print(f'Points/WEEK:  {tt/13:.1f}p  (~${tt/13*5:.0f}/wk at 1 MES)')
nw=sum(1 for d,p,r in mm if r=='WIN'); nl=sum(1 for d,p,r in mm if r=='LOSS'); ne=sum(1 for d,p,r in mm if r=='EXPIRED')
print(f'\nOutcome mix: WIN {nw}, LOSS {nl}, EXPIRED {ne}')
