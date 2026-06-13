"""Robustness of the prior-close filter for the dip-buy long, full chain history."""
import os, psycopg2
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')
conn = psycopg2.connect(os.environ['DATABASE_URL']); cur = conn.cursor()
cur.execute("select ts,spot from chain_snapshots where spot is not null order by ts")
days = defaultdict(list)
for ts, sp in cur.fetchall():
    et = ts.astimezone(ET); days[et.date()].append((et, float(sp)))
conn.close()
daylist = sorted(days)
close = {d: days[d][-1][1] for d in daylist}
openp = {}
for d in daylist:
    for et,sp in days[d]:
        if et.time()>=dtime(9,30): openp[d]=sp; break
def prev_close(d):
    i=daylist.index(d); return close[daylist[i-1]] if i>=1 else None

WS, WE, CUT = dtime(9,30), dtime(11,30), dtime(16,0)
DIP, CONF, T, S = 8, 4, 10, 8
def gen(d):
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
    for e,sp in days[d]:
        if e<=et0: continue
        if e.time()>CUT: break
        if sp<=entry-S: return 'LOSS'
        if sp>=entry+T: return 'WIN'
    return 'EXPIRED'

trades=[]
for d in daylist:
    g=gen(d)
    if not g: continue
    entry,et0=g; res=walk(d,entry,et0); pc=prev_close(d)
    if pc is None: continue
    o=openp.get(d); c=close[d]
    dtype = 'UP' if (c-o)>15 else ('DOWN' if (c-o)<-15 else 'RANGE')
    trades.append(dict(d=d,m=d.strftime('%Y-%m'),res=res,vs_pc=entry-pc,dtype=dtype))

def pnl(t): return 10.0 if t['res']=='WIN' else (-8.0 if t['res']=='LOSS' else 0.0)
def stat(rows):
    if not rows: return (0,0,0,0)
    n=len(rows); w=sum(1 for t in rows if t['res']=='WIN'); tot=sum(pnl(t) for t in rows)
    cum=0;pk=0;dd=0
    for t in sorted(rows,key=lambda x:x['d']):
        cum+=pnl(t);pk=max(pk,cum);dd=min(dd,cum-pk)
    return (n,100*w/n,tot,dd)
FILT=lambda t: t['vs_pc']>-2   # prior-close filter (>= prevclose - 2)

print("=== PER MONTH: baseline vs prior-close filter ===")
print(f"{'month':<9}| {'BASE n/WR/totP/DD':<28}| {'FILTERED n/WR/totP/DD':<28}| helps?")
months=sorted(set(t['m'] for t in trades))
for m in months:
    b=[t for t in trades if t['m']==m]; f=[t for t in b if FILT(t)]
    bn,bw,bt,bd=stat(b); fn,fw,ft,fd=stat(f)
    help='YES' if (fw>=bw and fd>=bd) else ('mixed' if ft>0 else 'NO')
    print(f"{m:<9}| n={bn:<3} WR={bw:>3.0f} P={bt:>6.0f} DD={bd:>5.0f}   | n={fn:<3} WR={fw:>3.0f} P={ft:>6.0f} DD={fd:>5.0f}   | {help}")

print("\n=== PER WEEK (ISO) ===")
def wk(d): y,w,_=d.isocalendar(); return f'{y}-W{w:02d}'
weeks=sorted(set(wk(t['d']) for t in trades))
bad=0
for w in weeks:
    b=[t for t in trades if wk(t['d'])==w]; f=[t for t in b if FILT(t)]
    bn,bw_,bt,bd=stat(b); fn,fw,ft,fd=stat(f)
    flag=''
    if fn>0 and ft<bt-1: flag=' <-- filter LOWERED pnl'
    if fn>0 and ft<0: flag=' <-- filter NEGATIVE'
    if ft<0: bad+=1
    print(f"{w}: BASE n={bn:<2} P={bt:>5.0f} DD={bd:>4.0f} | FILT n={fn:<2} WR={fw:>3.0f} P={ft:>5.0f} DD={fd:>4.0f}{flag}")
print(f"\nWeeks where filtered set went negative: {bad} of {len(weeks)}")

print("\n=== BY DAY-TYPE (regime) ===")
for dt in ('UP','RANGE','DOWN'):
    b=[t for t in trades if t['dtype']==dt]; f=[t for t in b if FILT(t)]
    bn,bw,bt,bd=stat(b); fn,fw,ft,fd=stat(f)
    print(f"{dt:<6}: BASE n={bn:<3} WR={bw:>3.0f} P={bt:>6.0f} DD={bd:>5.0f} | FILT n={fn:<3} WR={fw:>3.0f} P={ft:>6.0f} DD={fd:>5.0f}")

print("\n=== OVERALL ===")
print('BASELINE:', stat(trades))
print('FILTERED:', stat([t for t in trades if FILT(t)]))
# what the filter removes
removed=[t for t in trades if not FILT(t)]
print('REMOVED  :', stat(removed), '<- these are the trades the filter drops (should be net-bad)')
