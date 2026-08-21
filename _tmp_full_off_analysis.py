import os, json, psycopg2
from collections import defaultdict
from datetime import timedelta
NZ=lambda x:(x.replace(tzinfo=None) if getattr(x,'tzinfo',None) else x)
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
COMM=1.0; BREAKER=-300.0
off=('2026-07-02','2026-07-06','2026-07-07','2026-07-08','2026-07-09')

def cls(bk,dr):
    if bk is None or abs(bk)<0.15: return 'neutral'
    il=dr in('long','bullish')
    return 'confirm' if ((il and bk>0) or ((not il) and bk<0)) else 'contradict'

# preload chain spot series per day
spotser=defaultdict(list)
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, ts AT TIME ZONE 'America/New_York' et, spot
  FROM chain_snapshots WHERE date(ts AT TIME ZONE 'America/New_York') IN %s AND spot IS NOT NULL ORDER BY ts""",(off,))
for d,et,sp in cur.fetchall(): spotser[str(d)].append((NZ(et),float(sp)))
# preload semi_basket per day
bask=defaultdict(list)
cur.execute("""SELECT date(et AT TIME ZONE 'America/New_York') d, et AT TIME ZONE 'America/New_York' e, basket_pct
  FROM semi_basket WHERE date(et AT TIME ZONE 'America/New_York') IN %s ORDER BY et""",(off,))
for d,e,bp in cur.fetchall():
    if bp is not None: bask[str(d)].append((NZ(e),float(bp)))

def rangepos(day,et,spot):
    ser=[s for (t,s) in spotser[day] if t<=et]
    if len(ser)<3: return None,None
    lo,hi=min(ser),max(ser); op=spotser[day][0][1]
    rp=(spot-lo)/(hi-lo) if hi>lo else 0.5
    return rp, spot-op   # range position 0..1, from_open

def mom(day,et,win=15):
    ser=bask[day]
    if not ser: return None
    cur_v=None; past=None
    for (t,bp) in ser:
        if t<=et: cur_v=bp
        if t<=et-timedelta(minutes=win): past=bp
    if cur_v is None or past is None: return None
    return cur_v-past  # basket change over last `win` min

cur.execute("""SELECT id, ts AT TIME ZONE 'America/New_York' et, date(ts AT TIME ZONE 'America/New_York') d,
     setup_name, direction, grade, basket_pct, outcome_pnl, spot
   FROM setup_log WHERE date(ts AT TIME ZONE 'America/New_York') IN %s AND live_pass=true AND outcome_pnl IS NOT NULL
   ORDER BY ts""",(off,))
trades=[]
for idv,et,d,nm,dr,gr,bk,pnl,sp in cur.fetchall():
    et=NZ(et); d=str(d); rp,fo=rangepos(d,et,float(sp)) if sp else (None,None)
    trades.append(dict(id=idv,et=et,d=d,nm=nm,dr=dr,gr=gr,bk=(float(bk) if bk is not None else None),
        pnl=float(pnl),spot=float(sp) if sp else None,rp=rp,fo=fo,mom=mom(d,et),cls=cls(bk,dr)))

# ---- DAILY PnL raw + breaker ----
print("=== OFF-PERIOD daily PnL (V16-SB, chain-sim) ===")
print(f"{'day':<12}{'n':>3}{'pts':>8}{'$@1':>7}{'$@012':>8}   {'$@1+brk':>9}{'$@012+brk':>10}")
T=defaultdict(float)
for d in off:
    dts=[t for t in trades if t['d']==d]
    pts=sum(t['pnl'] for t in dts)
    u1=sum(t['pnl']*5-COMM for t in dts)
    u012=sum(t['pnl']*5*(2 if t['cls']=='confirm' else 1)-COMM*(2 if t['cls']=='confirm' else 1) for t in dts)
    # breaker: chronological, halt once realized<=BREAKER
    b1=0; halt1=False; b12=0; halt12=False
    for t in dts:
        m=2 if t['cls']=='confirm' else 1
        if not halt1:
            b1+=t['pnl']*5-COMM
            if b1<=BREAKER: halt1=True
        if not halt12:
            b12+=t['pnl']*5*m-COMM*m
            if b12<=BREAKER: halt12=True
    print(f"{d:<12}{len(dts):>3}{pts:>8.1f}{u1:>7.0f}{u012:>8.0f}   {b1:>9.0f}{b12:>10.0f}")
    T['n']+=len(dts);T['pts']+=pts;T['u1']+=u1;T['u012']+=u012;T['b1']+=b1;T['b12']+=b12
print(f"{'TOTAL':<12}{int(T['n']):>3}{T['pts']:>8.1f}{T['u1']:>7.0f}{T['u012']:>8.0f}   {T['b1']:>9.0f}{T['b12']:>10.0f}")

# ---- LOSERS breakdown: direction, time, range-position ----
print("\n=== who's losing? (chain-sim pts) ===")
for grp,fn in [('LONG',lambda t:t['dr'] in('long','bullish')),('SHORT',lambda t:t['dr'] in('short','bearish'))]:
    g=[t for t in trades if fn(t)]
    w=sum(1 for t in g if t['pnl']>0); pts=sum(t['pnl'] for t in g)
    print(f"  {grp:<6} n={len(g):>2} WR={w/len(g)*100:>4.0f}% pts={pts:>7.1f}")

print("\n=== LONGS by intraday range-position at entry ===")
longs=[t for t in trades if t['dr'] in('long','bullish') and t['rp'] is not None]
for lab,lo,hi in [('bottom 0-40%',0,.4),('mid 40-70%',.4,.7),('TOP 70-100%',.7,1.01)]:
    g=[t for t in longs if lo<=t['rp']<hi]
    if g:
        w=sum(1 for t in g if t['pnl']>0)
        print(f"  {lab:<14} n={len(g):>2} WR={w/len(g)*100:>4.0f}% pts={sum(t['pnl'] for t in g):>7.1f}  (mean from_open {sum(t['fo'] for t in g)/len(g):+.0f})")

print("\n=== LONGS by hour ===")
byh=defaultdict(list)
for t in trades:
    if t['dr'] in('long','bullish'): byh[t['et'].hour].append(t)
for h in sorted(byh):
    g=byh[h]; w=sum(1 for t in g if t['pnl']>0)
    print(f"  {h:02d}:00  n={len(g):>2} WR={w/len(g)*100:>4.0f}% pts={sum(t['pnl'] for t in g):>7.1f}")

# ---- OPEN-SB vs MOMENTUM-SB on the longs ----
print("\n=== OPEN-basket vs MOMENTUM-basket on LONGS ===")
lm=[t for t in longs if t['mom'] is not None]
print(f"  longs with momentum data: {len(lm)}/{len(longs)}")
for lab,fn in [('mom>0 (tech rising last15m)',lambda t:t['mom']>0),
               ('mom<=0 (tech flat/falling)',lambda t:t['mom']<=0)]:
    g=[t for t in lm if fn(t)]
    if g:
        w=sum(1 for t in g if t['pnl']>0)
        print(f"  {lab:<30} n={len(g):>2} WR={w/len(g)*100:>4.0f}% pts={sum(t['pnl'] for t in g):>7.1f}")
# would momentum filter (skip long if mom<=0) change PnL?
kept=[t for t in lm if t['mom']>0]; skip=[t for t in lm if t['mom']<=0]
print(f"  --> MOMENTUM FILTER (take longs only if tech rising): keep {len(kept)} pts={sum(t['pnl'] for t in kept):+.1f} | skip {len(skip)} pts={sum(t['pnl'] for t in skip):+.1f}")
c.close()
