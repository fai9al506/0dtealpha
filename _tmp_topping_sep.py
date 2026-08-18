import os, psycopg2
from collections import defaultdict
from datetime import timedelta
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
NZ=lambda x:(x.replace(tzinfo=None) if getattr(x,'tzinfo',None) else x)

# --- longs with stamped features ---
cur.execute("""SELECT id, ts AT TIME ZONE 'America/New_York' et, date(ts AT TIME ZONE 'America/New_York') d,
   setup_name, spot, lis, target, max_plus_gex, upside, gap_to_lis, vix, overvix,
   vanna_all, spot_vol_beta, greek_alignment, v13_gex_above, v13_dd_near, paradigm, outcome_pnl
   FROM setup_log WHERE live_pass=true AND direction IN ('long','bullish') AND outcome_pnl IS NOT NULL
   ORDER BY ts""")
L=[]
for r in cur.fetchall():
    k=['id','et','d','nm','spot','lis','target','mpg','upside','g2l','vix','overvix','vanna','svb','galign','v13gex','v13dd','para','pnl']
    dd=dict(zip(k,r)); dd['et']=NZ(dd['et']); dd['d']=str(dd['d']); dd['pnl']=float(dd['pnl'])
    dd['spot']=float(dd['spot']) if dd['spot'] else None
    L.append(dd)
days=tuple(sorted(set(x['d'] for x in L)))

# --- chain spot series for price extension ---
spot=defaultdict(list)
cur.execute("""SELECT date(ts AT TIME ZONE 'America/New_York') d, ts AT TIME ZONE 'America/New_York' et, spot
   FROM chain_snapshots WHERE date(ts AT TIME ZONE 'America/New_York') IN %s AND spot IS NOT NULL ORDER BY ts""",(days,))
for d,et,sp in cur.fetchall(): spot[str(d)].append((NZ(et),float(sp)))
def px(day,et):
    ser=[(t,s) for (t,s) in spot[day] if t<=et]
    if len(ser)<3: return {}
    op=spot[day][0][1]; sp=ser[-1][1]; hi=max(s for _,s in ser); lo=min(s for _,s in ser)
    p30=None
    for t,s in ser:
        if t<=et-timedelta(minutes=30): p30=s
    return dict(from_open=sp-op, rng=(sp-lo)/(hi-lo) if hi>lo else .5,
        below_hi=hi-sp, run30=(sp-p30) if p30 is not None else None)
for x in L:
    x.update(px(x['d'],x['et']))

def rgm(x):  # regime by vix
    return 'hivol' if (x['vix'] and float(x['vix'])>=19) else 'lovol'
def show(pred,lab):
    g=[x for x in L if pred(x)]
    if not g: return
    w=sum(1 for x in g if x['pnl']>0); pts=sum(x['pnl'] for x in g)
    hi=[x for x in g if rgm(x)=='hivol']; lo=[x for x in g if rgm(x)=='lovol']
    def sub(s): 
        if not s: return "   n=0"
        return f"n={len(s):>3} {sum(1 for x in s if x['pnl']>0)/len(s)*100:>3.0f}% {sum(x['pnl'] for x in s):>+7.0f}p"
    print(f"  {lab:<34} n={len(g):>3} WR={w/len(g)*100:>3.0f}% {pts:>+7.0f}p | HIVOL {sub(hi)} | LOVOL {sub(lo)}")

print(f"=== {len(L)} live_pass longs (Feb-Jul). Format: bucket | overall | VIX>=19 | VIX<19 ===\n")
print("[BASELINE]"); show(lambda x:True,"all longs")

print("\n[A. Spot vs TS +GEX wall (max_plus_gex)]  (topping = spot at/above wall)")
show(lambda x:x['mpg'] and x['spot'] and x['spot']-float(x['mpg'])<=-10,"spot >10 BELOW wall (room up)")
show(lambda x:x['mpg'] and x['spot'] and -10<x['spot']-float(x['mpg'])<0,"spot 0-10 below wall")
show(lambda x:x['mpg'] and x['spot'] and 0<=x['spot']-float(x['mpg'])<8,"spot AT wall (0-8 above)")
show(lambda x:x['mpg'] and x['spot'] and x['spot']-float(x['mpg'])>=8,"spot >8 ABOVE wall (extended)")

print("\n[B. upside (room to target)]")
for lo,hi,lab in [(-999,0,'upside<=0 (past target)'),(0,10,'upside 0-10'),(10,30,'upside 10-30'),(30,999,'upside>30')]:
    show(lambda x,lo=lo,hi=hi:x['upside'] is not None and lo<=float(x['upside'])<hi,lab)

print("\n[C. Price extension from_open]")
for lo,hi,lab in [(-999,-10,'from_open<-10 (down day)'),(-10,10,'from_open -10..10 (flat)'),(10,30,'from_open 10-30 (up)'),(30,999,'from_open>30 (gap/rallied)')]:
    show(lambda x,lo=lo,hi=hi:'from_open' in x and lo<=x['from_open']<hi,lab)

print("\n[D. Intraday range position]")
for lo,hi,lab in [(0,.4,'bottom 0-40%'),(.4,.75,'mid 40-75%'),(.75,1.01,'TOP 75-100%')]:
    show(lambda x,lo=lo,hi=hi:'rng' in x and lo<=x['rng']<hi,lab)

print("\n[E. below day-high (how far under running high)]")
for lo,hi,lab in [(0,3,'within 3pt of high (AT TOP)'),(3,10,'3-10 below'),(10,999,'>10 below high')]:
    show(lambda x,lo=lo,hi=hi:'below_hi' in x and lo<=x['below_hi']<hi,lab)

print("\n[F. run30 (30-min momentum into entry)]")
for lo,hi,lab in [(-999,-5,'run30<-5 (falling in)'),(-5,5,'flat'),(5,999,'run30>5 (rallied in)')]:
    show(lambda x,lo=lo,hi=hi:x.get('run30') is not None and lo<=x['run30']<hi,lab)

print("\n[G. Paradigm]")
paras=defaultdict(int)
for x in L: 
    if x['para']: paras[x['para']]+=1
for p in sorted(paras,key=lambda k:-paras[k]):
    if paras[p]>=15: show(lambda x,p=p:x['para']==p,p)

print("\n[H. Volland vanna_all sign]")
show(lambda x:x['vanna'] is not None and float(x['vanna'])>0,"vanna_all > 0")
show(lambda x:x['vanna'] is not None and float(x['vanna'])<0,"vanna_all < 0")

print("\n[I. spot_vol_beta (topping/vol-event stretch)]")
for lo,hi,lab in [(-999,0,'svb<0'),(0,0.5,'svb 0-0.5'),(0.5,1,'svb 0.5-1'),(1,999,'svb>1')]:
    show(lambda x,lo=lo,hi=hi:x['svb'] is not None and lo<=float(x['svb'])<hi,lab)

print("\n[J. overvix (VIX-VIX3M)]")
show(lambda x:x['overvix'] is not None and float(x['overvix'])>=0,"overvix>=0 (backwardation/stress)")
show(lambda x:x['overvix'] is not None and float(x['overvix'])<0,"overvix<0 (contango/calm)")
c.close()
