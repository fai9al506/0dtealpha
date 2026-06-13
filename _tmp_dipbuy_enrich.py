import os, psycopg2, re, json
from collections import defaultdict
from datetime import time as dtime
from zoneinfo import ZoneInfo
from bisect import bisect_left

ET = ZoneInfo('America/New_York')
conn = psycopg2.connect(os.environ['DATABASE_URL']); cur = conn.cursor()

# --- chain spot+vix+overvix ---
cur.execute("""select ts,spot,vix,overvix from chain_snapshots
  where ts::date>='2026-02-25' and spot is not null order by ts""")
days = defaultdict(list)
for ts, sp, vix, ov in cur.fetchall():
    et = ts.astimezone(ET)
    days[et.date()].append((et, float(sp), (float(vix) if vix is not None else None),
                            (float(ov) if ov is not None else None)))
daylist = sorted(days)
close = {d: days[d][-1][1] for d in daylist}
def sma_prev(d, n):
    i = daylist.index(d)
    if i < n: return None
    return sum(close[p] for p in daylist[i-n:i]) / n
def prev_close(d):
    i = daylist.index(d)
    return close[daylist[i-1]] if i >= 1 else None

# --- volland snapshots: ts -> stats ---
def dd_num(s):
    if not s: return None
    s = str(s).replace(',', '').replace('$', '').strip()
    neg = s.startswith('-') or s.startswith('(')
    m = re.search(r'[\d.]+', s)
    if not m: return None
    v = float(m.group())
    return -v if neg else v
cur.execute("""select coalesce(data_ts,ts) t, payload->'statistics' st
  from volland_snapshots where coalesce(data_ts,ts)>='2026-02-28'
  and payload->'statistics'->>'paradigm' is not null order by 1""")
vts = []; vstat = []
for t, st in cur.fetchall():
    if st is None: continue
    if isinstance(st, str): st = json.loads(st)
    et = t.astimezone(ET)
    svb = st.get('spot_vol_beta')
    svbc = svb.get('correlation') if isinstance(svb, dict) else svb
    try: svbc = float(svbc)
    except: svbc = None
    lis = st.get('lines_in_sand') or ''
    mlis = re.search(r'[\d,]+\.?\d*', lis.replace(',', ''))
    lisv = float(mlis.group()) if mlis else None
    try: charm = float(st.get('aggregatedCharm') or 0)
    except: charm = None
    vts.append(et)
    vstat.append({'para': st.get('paradigm'),
                  'dd': dd_num(st.get('delta_decay_hedging')),
                  'charm': charm, 'svb': svbc, 'lis': lisv})
conn.close()

def near_vol(et):
    if not vts: return None
    i = bisect_left(vts, et)
    best = None; bd = 1e9
    for j in (i-1, i):
        if 0 <= j < len(vts):
            d = abs((vts[j]-et).total_seconds())
            if d < bd: bd = d; best = vstat[j]
    return best if bd <= 900 else None  # within 15 min

# --- generate dip-buy trades ---
WS, WE, CUT = dtime(9,30), dtime(11,30), dtime(16,0)
DIP, CONF, T, S = 8, 4, 10, 8
def gen(d):
    hi=-1e9; ind=False; lo=1e9; sess_open=None
    for et,sp,vix,ov in days[d]:
        if et.time()<WS: continue
        if sess_open is None: sess_open=sp
        if et.time()>WE: break
        hi=max(hi,sp)
        if not ind:
            if sp<=hi-DIP: ind=True; lo=sp
        else:
            lo=min(lo,sp)
            if sp>=lo+CONF:
                return dict(et=et, spot=sp, vix=vix, ov=ov, hi=hi, lo=lo,
                            dip=hi-lo, mins=(et.hour*60+et.minute)-(9*60+30), sopen=sess_open)
    return None
def walk(d, entry, et0):
    last=None
    for e,sp,_,_ in days[d]:
        if e<=et0: continue
        if e.time()>CUT: break
        last=sp
        if sp<=entry-S: return 'LOSS'
        if sp>=entry+T: return 'WIN'
    return 'EXPIRED'

trades=[]
for d in daylist:
    if not ('2026-03-01'<=d.isoformat()<='2026-05-31'): continue
    g=gen(d)
    if not g: continue
    res=walk(d,g['spot'],g['et'])
    v=near_vol(g['et']) or {}
    pc=prev_close(d); sma=sma_prev(d,3)
    para=v.get('para') or ''
    fam=('GEX' if para.startswith('GEX') else 'AG' if para.startswith('AG')
         else 'BOFA' if para.upper().startswith('BOFA') else 'SIDIAL' if para.startswith('SIDIAL') else 'other')
    trades.append(dict(d=d, m=d.strftime('%Y-%m'), res=res, **g,
        gap=(g['sopen']-pc) if pc else None,
        vs_pc=(g['spot']-pc) if pc else None,
        uptrend=(g['sopen']>sma) if sma else None,
        para=para, fam=fam, dd=v.get('dd'), charm=v.get('charm'),
        svb=v.get('svb'), lis=v.get('lis'),
        lis_dist=(abs(g['spot']-v['lis']) if v.get('lis') else None)))

# ---- winners vs losers feature comparison (pooled Mar-May) ----
W=[t for t in trades if t['res']=='WIN']; L=[t for t in trades if t['res']=='LOSS']
def avg(rows,k):
    vals=[r[k] for r in rows if r.get(k) is not None]
    return sum(vals)/len(vals) if vals else None
print(f"Trades: {len(trades)} | WIN {len(W)} | LOSS {len(L)}\n")
print(f"{'feature':<14}{'WIN avg':>12}{'LOSS avg':>12}")
for k in ['vix','ov','dip','mins','gap','vs_pc','dd','charm','svb','lis_dist']:
    aw=avg(W,k); al=avg(L,k)
    sw=f'{aw:.2f}' if aw is not None else 'n/a'; sl=f'{al:.2f}' if al is not None else 'n/a'
    print(f"{k:<14}{sw:>12}{sl:>12}")

print("\n--- paradigm family: WR ---")
fam=defaultdict(lambda:[0,0])
for t in trades:
    fam[t['fam']][0]+=1; fam[t['fam']][1]+= (1 if t['res']=='WIN' else 0)
for f,(n,w) in sorted(fam.items(),key=lambda x:-x[1][0]):
    print(f"  {f:<8} n={n:<3} WR={100*w/n:.0f}%")

print("\n--- DD sign: WR ---")
for lbl,cond in [('DD>0 (bullish)',lambda t:t['dd'] is not None and t['dd']>0),
                 ('DD<0 (bearish)',lambda t:t['dd'] is not None and t['dd']<0)]:
    sub=[t for t in trades if cond(t)]; w=sum(1 for t in sub if t['res']=='WIN')
    print(f"  {lbl:<16} n={len(sub):<3} WR={100*w/len(sub):.0f}%" if sub else f"  {lbl} n=0")

print("\n--- per-trade detail (sorted by month) ---")
print(f"{'date':<11}{'res':<5}{'vix':>5}{'ov':>6}{'dip':>5}{'min':>4}{'gap':>6}{'fam':>7}{'dd':>7}{'svb':>6}")
for t in sorted(trades,key=lambda x:x['d']):
    f=lambda v,p='.1f': (format(v,p) if v is not None else 'n/a')
    ddm = (t['dd']/1e9 if t['dd'] is not None else None)
    print(f"{t['d'].isoformat():<11}{t['res']:<5}{f(t['vix']):>5}{f(t['ov']):>6}{f(t['dip'],'.0f'):>5}{t['mins']:>4}{f(t['gap'],'.0f'):>6}{t['fam']:>7}{f(ddm,'.2f'):>7}{f(t['svb'],'.2f'):>6}")

# ================== FILTER TESTING ==================
def pnl(t): return 10.0 if t['res']=='WIN' else (-8.0 if t['res']=='LOSS' else 0.0)
def report(rows, label):
    if not rows: print(f"{label:<40} n=0"); return
    n=len(rows); w=sum(1 for t in rows if t['res']=='WIN'); tot=sum(pnl(t) for t in rows)
    cum=0;pk=0;dd=0
    for t in sorted(rows,key=lambda x:x['d']):
        cum+=pnl(t); pk=max(pk,cum); dd=min(dd,cum-pk)
    print(f"{label:<40} n={n:<3} WR={100*w/n:>4.0f}% totP={tot:>6.1f} avgP={tot/n:>4.2f} maxDD={dd:>5.1f}")

print("\n================ FILTER CANDIDATES (Mar-May) ================")
report(trades, "BASELINE (no filter)")
report([t for t in trades if t['vs_pc'] is not None and t['vs_pc']>0], "F1: entry > prior close")
report([t for t in trades if t['gap'] is not None and t['gap']>=0], "F2: gap up (open>=prior close)")
report([t for t in trades if t['vix'] is not None and t['vix']<=21], "F3: vix <= 21")
report([t for t in trades if t['vix'] is not None and t['vix']<=20], "F4: vix <= 20")
report([t for t in trades if (t['vs_pc'] or -99)>0 and (t['vix'] or 99)<=21], "F5: entry>prevclose AND vix<=21")
report([t for t in trades if (t['vs_pc'] or -99)>-2], "F6: entry >= prevclose-2 (tolerance)")
report([t for t in trades if (t['gap'] or -99)>=0 and (t['vix'] or 99)<=22], "F7: gap-up AND vix<=22")

print("\n-- F1 (entry>prevclose) per MONTH --")
for m in ('2026-03','2026-04','2026-05'):
    report([t for t in trades if t['m']==m and (t['vs_pc'] or -99)>0], f"  {m}")
print("\n-- BASELINE per MONTH (for reference) --")
for m in ('2026-03','2026-04','2026-05'):
    report([t for t in trades if t['m']==m], f"  {m}")
