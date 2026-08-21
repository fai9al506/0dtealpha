import os, psycopg2
from collections import defaultdict
from datetime import timedelta
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
NZ=lambda x:(x.replace(tzinfo=None) if getattr(x,'tzinfo',None) else x)
off=('2026-07-02','2026-07-06','2026-07-07','2026-07-08','2026-07-09')

bask=defaultdict(list)
cur.execute("SELECT date(et) d, et, basket_pct FROM semi_basket WHERE date(et) IN %s ORDER BY et",(off,))
for d,e,bp in cur.fetchall():
    if bp is not None: bask[str(d)].append((NZ(e),float(bp)))

def val_at(day,et):
    v=None
    for t,bp in bask[day]:
        if t<=et: v=bp
        else: break
    return v
def mom(day,et,win):
    now=val_at(day,et); past=val_at(day,et-timedelta(minutes=win))
    return None if (now is None or past is None) else now-past

cur.execute("""SELECT id, ts AT TIME ZONE 'America/New_York' et, date(ts AT TIME ZONE 'America/New_York') d,
     setup_name, direction, basket_pct, outcome_pnl
   FROM setup_log WHERE date(ts AT TIME ZONE 'America/New_York') IN %s AND live_pass=true AND outcome_pnl IS NOT NULL
     AND direction IN ('long','bullish') ORDER BY ts""",(off,))
longs=[]
for idv,et,d,nm,dr,bk,pnl in cur.fetchall():
    et=NZ(et); d=str(d)
    longs.append(dict(id=idv,et=et,d=d,nm=nm,bk=float(bk) if bk is not None else None,pnl=float(pnl),
        open_bk=val_at(d,et), m10=mom(d,et,10), m20=mom(d,et,20), m30=mom(d,et,30)))

print(f"LONGS n={len(longs)}  (per-trade: open-basket vs momentum)")
print(f"{'id':>5} {'et':<6} {'setup':<10} {'openBk':>7} {'m20':>6} {'m30':>6} {'pnl':>6}")
for t in longs:
    print(f"{t['id']:>5} {str(t['et'])[11:16]:<6} {t['nm']:<10} {('%.2f'%t['open_bk']) if t['open_bk'] is not None else '  -':>7} "
          f"{('%+.2f'%t['m20']) if t['m20'] is not None else '   -':>6} {('%+.2f'%t['m30']) if t['m30'] is not None else '   -':>6} {t['pnl']:>+6.1f}")

def summ(g,lab):
    if not g: return
    w=sum(1 for t in g if t['pnl']>0); pts=sum(t['pnl'] for t in g)
    print(f"  {lab:<34} n={len(g):>2} WR={w/len(g)*100:>4.0f}% pts={pts:>7.1f}  $@1MES={pts*5-len(g):>6.0f}")

print("\n=== OPEN-basket confirm (current gate) ===")
summ([t for t in longs if t['open_bk'] is not None and t['open_bk']>=0.15],"open-basket CONFIRM (>=+0.15)")
summ([t for t in longs if t['open_bk'] is not None and abs(t['open_bk'])<0.15],"open-basket NEUTRAL (fail-open longs)")

for win in ('m20','m30'):
    print(f"\n=== MOMENTUM {win} (tech change over last {win[1:]}min) ===")
    summ([t for t in longs if t[win] is not None and t[win]>0.05],f"{win}>+0.05 (rising) -> KEEP")
    summ([t for t in longs if t[win] is not None and -0.05<=t[win]<=0.05],f"{win} flat")
    summ([t for t in longs if t[win] is not None and t[win]<-0.05],f"{win}<-0.05 (rolling over) -> SKIP")

# The key cross: open-confirm BUT momentum negative = "long at top on fading tape"
print("\n=== THE PATTERN: open-basket says CONFIRM but momentum says FADING ===")
trap=[t for t in longs if t['open_bk'] is not None and t['open_bk']>=0.15 and t['m20'] is not None and t['m20']<-0.05]
summ(trap,"open>=0.15 AND m20<-0.05 (the 'top' longs)")
ok=[t for t in longs if t['open_bk'] is not None and t['open_bk']>=0.15 and t['m20'] is not None and t['m20']>=-0.05]
summ(ok,"open>=0.15 AND m20>=-0.05 (tech still firm)")
c.close()
