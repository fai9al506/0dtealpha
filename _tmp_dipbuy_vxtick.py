"""VIX divergence at each wick using TICK-level VX (vps_vix_ticks, Mar23-May29).
For each dip-buy: during the price drop (high->wick low), did VX confirm (new high / positive CVD = fear)
or diverge (flat/fall / negative CVD = selloff not believed -> bounce)?"""
import os, psycopg2
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo
from bisect import bisect_left

ET = ZoneInfo('America/New_York')
conn = psycopg2.connect(os.environ['DATABASE_URL']); cur = conn.cursor()
cur.execute("""select ts,spot from chain_snapshots where ts::date>='2026-03-20' and spot is not null order by ts""")
days = defaultdict(list)
for ts, sp in cur.fetchall():
    et = ts.astimezone(ET); days[et.date()].append((et, float(sp)))
# VX ticks per ET date: list of (et, price, delta)
cur.execute("""select ts,price,delta from vps_vix_ticks where ts>='2026-03-20' order by ts""")
vx = defaultdict(list)
for ts, pr, dl in cur.fetchall():
    et = ts.astimezone(ET); vx[et.date()].append((et, float(pr), int(dl or 0)))
conn.close()
for d in vx: vx[d].sort()
daylist = sorted(days)

WS, WE, CUT = dtime(9,30), dtime(11,30), dtime(16,0)
DIP, CONF, T, S = 8, 4, 10, 8

def gen(d):
    hi=-1e9; ind=False; lo=1e9; et_hi=None; et_lo=None
    for et,sp in days[d]:
        if et.time()<WS: continue
        if et.time()>WE: break
        if sp>hi: hi=sp; et_hi=et
        if not ind:
            if sp<=hi-DIP: ind=True; lo=sp; et_lo=et
        else:
            if sp<lo: lo=sp; et_lo=et
            if sp>=lo+CONF:
                return dict(et=et, spot=sp, hi=hi, lo=lo, et_hi=et_hi, et_lo=et_lo, dip=hi-lo)
    return None
def walk(d, entry, et0):
    for e,sp in days[d]:
        if e<=et0: continue
        if e.time()>CUT: break
        if sp<=entry-S: return 'LOSS'
        if sp>=entry+T: return 'WIN'
    return 'EXPIRED'

def vx_window(d, t0, t1):
    """VX behavior between t0 (price high) and t1 (wick low)."""
    arr = vx.get(d)
    if not arr: return None
    ets = [a[0] for a in arr]
    i0 = bisect_left(ets, t0); i1 = bisect_left(ets, t1)
    seg = arr[max(0,i0-1):i1+1]
    if not seg: return None
    p_start = seg[0][1]; p_end = seg[-1][1]
    pmax = max(a[1] for a in seg); pmin = min(a[1] for a in seg)
    cvd = sum(a[2] for a in seg)
    return dict(vx_start=p_start, vx_end=p_end, vx_max=pmax, vx_min=pmin,
                vx_rise=p_end-p_start, vx_newhigh=pmax-p_start, vx_cvd=cvd, ticks=len(seg))

trades=[]
for d in daylist:
    if not ('2026-03-23'<=d.isoformat()<='2026-05-31'): continue
    g=gen(d)
    if not g: continue
    g['res']=walk(d,g['spot'],g['et']); g['d']=d
    w=vx_window(d, g['et_hi'], g['et_lo'])
    g['vx']=w
    trades.append(g)

have=[t for t in trades if t['vx']]
W=[t for t in have if t['res']=='WIN']; L=[t for t in have if t['res']=='LOSS']
def avg(rows,k):
    v=[r['vx'][k] for r in rows if r['vx'] and r['vx'].get(k) is not None]; return sum(v)/len(v) if v else 0
print(f"Trades w/ VX ticks: {len(have)} | WIN {len(W)} LOSS {len(L)}\n")
print(f"{'metric':<14}{'WIN':>9}{'LOSS':>9}   interpretation")
print(f"{'vx_rise':<14}{avg(W,'vx_rise'):>9.3f}{avg(L,'vx_rise'):>9.3f}   VX chg start->end of drop (lower=diverge)")
print(f"{'vx_newhigh':<14}{avg(W,'vx_newhigh'):>9.3f}{avg(L,'vx_newhigh'):>9.3f}   VX max rise during drop")
print(f"{'vx_cvd':<14}{avg(W,'vx_cvd'):>9.1f}{avg(L,'vx_cvd'):>9.1f}   VX order-flow delta (pos=vol buying=fear)")

def pnl(t): return 10.0 if t['res']=='WIN' else (-8.0 if t['res']=='LOSS' else 0)
def report(rows,lbl):
    if not rows: print(f"{lbl:<44} n=0"); return
    n=len(rows); w=sum(1 for t in rows if t['res']=='WIN'); tot=sum(pnl(t) for t in rows)
    cum=0;pk=0;dd=0
    for t in sorted(rows,key=lambda x:x['d']):
        cum+=pnl(t);pk=max(pk,cum);dd=min(dd,cum-pk)
    print(f"{lbl:<44} n={n:<3} WR={100*w/n:>4.0f}% totP={tot:>6.1f} maxDD={dd:>5.1f}")

print("\n============ VX-TICK DIVERGENCE FILTERS ============")
report(have,"BASELINE (trades w/ VX data)")
report([t for t in have if t['vx']['vx_cvd']<=0], "VC1: VX CVD <= 0 (vol selling/complacent)")
report([t for t in have if t['vx']['vx_cvd']<0],  "VC2: VX CVD < 0 (net vol selling)")
report([t for t in have if t['vx']['vx_cvd']>0],  "opp: VX CVD > 0 (vol BUYING=fear)")
report([t for t in have if t['vx']['vx_newhigh']<=0.10], "VN1: VX made NO new high (diverge)")
report([t for t in have if t['vx']['vx_rise']<=0],  "VR1: VX flat/fell during drop")
report([t for t in have if t['vx']['vx_rise']>0.15], "opp: VX rose >0.15 (confirmed)")
report([t for t in have if t['vx']['vx_cvd']<=0 and t['vx']['vx_newhigh']<=0.15], "VD-combo: CVD<=0 AND no new high")

print("\n-- per-trade (VX ticks) --")
print(f"{'date':<11}{'res':<5}{'vx_rise':>8}{'newhi':>7}{'cvd':>8}{'dip':>5}")
for t in sorted(have,key=lambda x:x['d']):
    v=t['vx']
    print(f"{t['d'].isoformat():<11}{t['res']:<5}{v['vx_rise']:>8.3f}{v['vx_newhigh']:>7.2f}{v['vx_cvd']:>8d}{t['dip']:>5.0f}")

# ===== STACK TEST: VX divergence x prior-close =====
_close={d:days[d][-1][1] for d in daylist}
def pc(d):
    i=daylist.index(d); return _close[daylist[i-1]] if i>=1 else None
for t in have:
    p=pc(t['d']); t['vs_pc']=(t['spot']-p) if p else None
print("\n============ STACK: VX-divergence x prior-close ============")
report(have,"BASELINE (VX subset)")
report([t for t in have if (t['vs_pc'] or -99)>-2], "A: entry>=prevclose-2 only")
report([t for t in have if t['vx']['vx_newhigh']<=0.10], "B: VX no-new-high only")
report([t for t in have if (t['vs_pc'] or -99)>-2 and t['vx']['vx_newhigh']<=0.10], "A+B: prior-close AND VX-diverge")
report([t for t in have if (t['vs_pc'] or -99)>-2 or t['vx']['vx_newhigh']<=0.10], "A or B: either")
# overlap stats
A=set(t['d'] for t in have if (t['vs_pc'] or -99)>-2)
B=set(t['d'] for t in have if t['vx']['vx_newhigh']<=0.10)
print(f"\nA(prior-close) days={len(A)}, B(VX-diverge) days={len(B)}, overlap={len(A&B)}, A-only={len(A-B)}, B-only={len(B-A)}")
