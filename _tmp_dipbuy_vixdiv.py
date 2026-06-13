"""VIX divergence at each dip-buy wick.
Hypothesis: WINs = price made lower low but VIX DIDN'T spike (divergence = exhausted selling).
LOSSes = VIX confirmed the drop (real fear)."""
import os, psycopg2
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')
conn = psycopg2.connect(os.environ['DATABASE_URL']); cur = conn.cursor()
cur.execute("""select ts,spot,vix from chain_snapshots
  where ts::date>='2026-02-25' and spot is not null order by ts""")
days = defaultdict(list)
for ts, sp, vix in cur.fetchall():
    et = ts.astimezone(ET)
    days[et.date()].append((et, float(sp), (float(vix) if vix is not None else None)))
conn.close()
daylist = sorted(days)
close = {d: days[d][-1][1] for d in daylist}
def prev_close(d):
    i = daylist.index(d); return close[daylist[i-1]] if i >= 1 else None

WS, WE, CUT = dtime(9,30), dtime(11,30), dtime(16,0)
DIP, CONF, T, S = 8, 4, 10, 8

def gen(d):
    hi=-1e9; vix_hi=None; ind=False; lo=1e9; vix_lo=None
    for et,sp,vix in days[d]:
        if et.time()<WS: continue
        if et.time()>WE: break
        if sp>hi: hi=sp; vix_hi=vix      # VIX at the peak the dip falls from
        if not ind:
            if sp<=hi-DIP: ind=True; lo=sp; vix_lo=vix
        else:
            if sp<lo: lo=sp; vix_lo=vix   # track VIX at the wick low
            if sp>=lo+CONF:
                return dict(et=et, spot=sp, vix_entry=vix, hi=hi, lo=lo,
                            vix_hi=vix_hi, vix_lo=vix_lo, dip=hi-lo)
    return None
def walk(d, entry, et0):
    for e,sp,_ in days[d]:
        if e<=et0: continue
        if e.time()>CUT: break
        if sp<=entry-S: return 'LOSS'
        if sp>=entry+T: return 'WIN'
    return 'EXPIRED'

trades=[]
for d in daylist:
    if not ('2026-03-01'<=d.isoformat()<='2026-05-31'): continue
    g=gen(d)
    if not g: continue
    g['res']=walk(d,g['spot'],g['et']); g['d']=d; g['m']=d.strftime('%Y-%m')
    # VIX divergence metrics
    if g['vix_hi'] is not None and g['vix_lo'] is not None:
        g['vix_rise']=g['vix_lo']-g['vix_hi']          # how much VIX rose during the drop
        g['vix_per_pt']=g['vix_rise']/max(g['dip'],1)  # VIX rise per point of price drop
    else:
        g['vix_rise']=None; g['vix_per_pt']=None
    if g['vix_entry'] is not None and g['vix_lo'] is not None:
        g['vix_bounce']=g['vix_entry']-g['vix_lo']     # VIX change as price bounced (neg=confirms)
    else:
        g['vix_bounce']=None
    trades.append(g)

W=[t for t in trades if t['res']=='WIN']; L=[t for t in trades if t['res']=='LOSS']
def avg(rows,k):
    v=[r[k] for r in rows if r.get(k) is not None]; return sum(v)/len(v) if v else None
print(f"Trades {len(trades)} | WIN {len(W)} LOSS {len(L)}\n")
print(f"{'metric':<16}{'WIN':>10}{'LOSS':>10}   (interpretation)")
print(f"{'vix_rise':<16}{avg(W,'vix_rise'):>10.3f}{avg(L,'vix_rise'):>10.3f}   VIX rise hi->lo (lower=divergence)")
print(f"{'vix_per_pt':<16}{avg(W,'vix_per_pt'):>10.4f}{avg(L,'vix_per_pt'):>10.4f}   VIX rise / price-drop pt")
print(f"{'vix_bounce':<16}{avg(W,'vix_bounce'):>10.3f}{avg(L,'vix_bounce'):>10.3f}   VIX chg on bounce (neg=confirms)")

def pnl(t): return 10.0 if t['res']=='WIN' else (-8.0 if t['res']=='LOSS' else 0)
def report(rows,lbl):
    if not rows: print(f"{lbl:<42} n=0"); return
    n=len(rows); w=sum(1 for t in rows if t['res']=='WIN'); tot=sum(pnl(t) for t in rows)
    cum=0;pk=0;dd=0
    for t in sorted(rows,key=lambda x:x['d']):
        cum+=pnl(t);pk=max(pk,cum);dd=min(dd,cum-pk)
    print(f"{lbl:<42} n={n:<3} WR={100*w/n:>4.0f}% totP={tot:>6.1f} maxDD={dd:>5.1f}")

print("\n================ VIX-DIVERGENCE FILTERS ================")
report(trades,"BASELINE")
report([t for t in trades if (t['vix_rise'] or 99)<=0.30], "VD1: VIX rose <= 0.30 during drop (diverge)")
report([t for t in trades if (t['vix_rise'] or 99)<=0.10], "VD2: VIX rose <= 0.10 (strong diverge)")
report([t for t in trades if (t['vix_rise'] or 99)<=0.0],  "VD3: VIX flat/fell during drop")
report([t for t in trades if (t['vix_per_pt'] or 99)<=0.02],"VD4: VIX/pt <= 0.02")
report([t for t in trades if (t['vix_bounce'] or 99)<=0],  "VD5: VIX fell on bounce")
report([t for t in trades if (t['vix_rise'] or 99)<=0.30 and (t['vix_bounce'] or 99)<=0],"VD6: diverge AND vix fell on bounce")

print("\n-- CONFIRMATION (opposite: VIX spiked = real fear) --")
report([t for t in trades if (t['vix_rise'] or -99)>0.30], "VIX rose > 0.30 (confirmed drop)")

print("\n-- per-trade --")
print(f"{'date':<11}{'res':<5}{'vix_hi':>7}{'vix_lo':>7}{'rise':>6}{'bounce':>7}{'dip':>5}")
for t in sorted(trades,key=lambda x:x['d']):
    fn=lambda v: (f'{v:.2f}' if v is not None else 'n/a')
    print(f"{t['d'].isoformat():<11}{t['res']:<5}{fn(t['vix_hi']):>7}{fn(t['vix_lo']):>7}{fn(t['vix_rise']):>6}{fn(t['vix_bounce']):>7}{t['dip']:>5.0f}")
