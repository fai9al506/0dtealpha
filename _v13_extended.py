"""V13 extended test: Mar 1 - Apr 17 (includes Apr 16-17 new data)."""
import os, json, pickle, bisect, random
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict

os.environ['DATABASE_URL'] = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
eng = create_engine(os.environ['DATABASE_URL'])
ET = ZoneInfo('America/New_York')

def v12fix(setup, align, para, grade, ts_et):
    if setup in ("VIX Divergence","IV Momentum","Vanna Butterfly"): return False
    if setup == "Skew Charm" and grade in ("C","LOG"): return False
    t = ts_et.time()
    if setup in ("Skew Charm","DD Exhaustion"):
        if dtime(14,30)<=t<dtime(15,0): return False
        if t>=dtime(15,30): return False
    if setup == "BofA Scalp" and t>=dtime(14,30): return False
    if setup in ("Skew Charm","DD Exhaustion") and para == "GEX-LIS": return False
    if setup == "AG Short" and para == "AG-TARGET": return False
    if setup in ("Skew Charm","AG Short"): return True
    if setup == "DD Exhaustion" and (align or 0) != 0: return True
    return False

def compute_gex_above(chain, spot):
    gex = []
    for row in chain:
        try:
            strike = row[10]
            cg = (row[3] or 0) * (row[1] or 0)
            pg = (row[17] or 0) * (row[19] or 0)
            gex.append((strike, cg - pg))
        except: continue
    above = [(s,v) for s,v in gex if s > spot]
    if not above: return None, 0
    top = max(above, key=lambda x: x[1])
    return top[0], top[1]

print('Loading data...', flush=True)
with eng.connect() as c:
    chain_rows = c.execute(text("SELECT ts, spot, rows FROM chain_snapshots WHERE ts >= '2026-03-01' AND ts <= '2026-04-18' ORDER BY ts")).fetchall()
    dd_raw = c.execute(text("SELECT ts_utc, CAST(strike AS FLOAT), CAST(value AS FLOAT) FROM volland_exposure_points WHERE ts_utc >= '2026-03-01' AND ts_utc <= '2026-04-18' AND ticker='SPX' AND greek='deltaDecay' AND expiration_option='TODAY'")).fetchall()
    sigs = c.execute(text("SELECT id, ts, setup_name, grade, paradigm, spot, greek_alignment, outcome_result, outcome_pnl, vix, overvix FROM setup_log WHERE ts >= '2026-03-01' AND ts <= '2026-04-17 23:59:59' AND direction = 'short' AND setup_name IN ('Skew Charm','AG Short','DD Exhaustion') AND outcome_pnl IS NOT NULL ORDER BY ts")).fetchall()

chain_by_ts = {}
for ts, sp, rows in chain_rows:
    chain_by_ts[ts] = (float(sp) if sp else None, rows if isinstance(rows, list) else json.loads(rows) if rows else None)
chain_ts_sorted = sorted(chain_by_ts.keys())

dd_by_ts = defaultdict(list)
for ts,k,v in dd_raw: dd_by_ts[ts].append((k,v))
dd_ts_sorted = sorted(dd_by_ts.keys())

def find_nearest(t, arr, max_sec=300):
    i = bisect.bisect_left(arr, t)
    c = []
    if i>0: c.append(arr[i-1])
    if i<len(arr): c.append(arr[i])
    if not c: return None
    b = min(c, key=lambda x: abs((x-t).total_seconds()))
    return b if abs((b-t).total_seconds()) <= max_sec else None

print(f'Raw signals: {len(sigs)}', flush=True)
results = []
for sig in sigs:
    sid, ts, setup, grade, para, spot, align, out, pnl, vix, ov = sig
    ts_et = ts.astimezone(ET)
    if not v12fix(setup, align, para, grade, ts_et): continue
    ch_ts = find_nearest(ts, chain_ts_sorted, 180)
    if not ch_ts: continue
    _, chain = chain_by_ts[ch_ts]
    if not chain: continue
    pk, pv = compute_gex_above(chain, float(spot))
    dd_ts = find_nearest(ts, dd_ts_sorted, 300)
    dd_abs = 0
    if dd_ts:
        near = [(k,v) for k,v in dd_by_ts[dd_ts] if abs(k - float(spot)) <= 10]
        dd_abs = max((abs(v) for _,v in near), default=0)
    results.append({
        'id':sid,'ts':ts,'ts_et':ts_et,'date':ts_et.date(),'setup':setup,'grade':grade,
        'paradigm':para,'spot':float(spot),'out':out,'pnl':float(pnl),
        'vix':float(vix) if vix else None,
        'plus_above_v':pv or 0,'plus_above_k':pk,'dd_max_abs':dd_abs
    })

def stats(arr):
    if not arr: return (0,0,0.0)
    w = sum(1 for x in arr if x['out']=='WIN')
    return len(arr), w, sum(x['pnl'] for x in arr)

def v13b(r):
    if r['setup'] not in ('Skew Charm','DD Exhaustion'): return False
    return r['plus_above_v']>=70 or r['dd_max_abs']>=3e9

print(f'\nV12-fix signals Mar 1 - Apr 17: {len(results)}', flush=True)

# V12 vs V13
n,w,p = stats(results)
kept = [r for r in results if not v13b(r)]
n2,w2,p2 = stats(kept)
blk_all = [r for r in results if v13b(r)]
bn,bw,bp = stats(blk_all)
print(f'\n{"="*70}')
print(f'V12-fix: {n}t {w}W={w/n*100:.1f}% WR {p:+.1f} pts')
print(f'V13:     {n2}t {w2}W={w2/n2*100:.1f}% WR {p2:+.1f} pts')
print(f'Blocked: {bn}t {bw}W={bw/bn*100:.0f}% WR avoided {-bp:+.1f}')
print(f'DELTA:   {p2-p:+.1f} pts ({(p2/p-1)*100:+.1f}%)')

# April daily
print(f'\n{"="*70}')
print(f'APRIL DAILY (SC+DD)')
print(f'{"="*70}')
scdd = [r for r in results if r['setup'] in ('Skew Charm','DD Exhaustion')]
by_date = defaultdict(list)
for r in scdd: by_date[r['date']].append(r)
print(f"{'Date':<12} {'Day':<4} {'#sig':<5} {'Base':<9} {'Blk':<4} {'Blk$':<9} {'After':<9} {'Delta':<7}")
print('-'*65)
Ta=Tb=0.0; Tn=Tbn=0
for d in sorted(by_date.keys()):
    if d.month < 4: continue
    arr = by_date[d]; n,w,p = stats(arr)
    bl = [r for r in arr if v13b(r)]; bn,bw,bp = stats(bl)
    print(f"{str(d):<12} {d.strftime('%a'):<4} {n:<5} {p:<+9.1f} {bn:<4} {bp:<+9.1f} {p-bp:<+9.1f} {-bp:<+7.1f}")
    Ta+=p; Tn+=n; Tb+=bp; Tbn+=bn
print('-'*65)
print(f"{'APR TOTAL':<12} {'':<4} {Tn:<5} {Ta:<+9.1f} {Tbn:<4} {Tb:<+9.1f} {Ta-Tb:<+9.1f} {-Tb:<+7.1f}")

# Apr 16-17 detail
print(f'\n{"="*70}')
print(f'APR 16-17 NEW SIGNALS DETAIL')
print(f'{"="*70}')
new = sorted([r for r in results if r['date'] >= datetime(2026,4,16).date()], key=lambda x: x['ts'])
if not new:
    print('  NO new signals on Apr 16-17')
else:
    for r in new:
        tag = '[BLOCK]' if v13b(r) else '       '
        extra = []
        if r['plus_above_v']>=30: extra.append(f"GEX+={r['plus_above_v']:+.0f}M")
        if r['dd_max_abs']>=1e9: extra.append(f"DD={r['dd_max_abs']/1e9:.1f}B")
        print(f"  {r['ts_et'].strftime('%m-%d %H:%M')} {tag} {r['setup']:<15} {r['grade'] or '-':<4} spot={r['spot']:.0f} pnl={r['pnl']:+.1f} {r['out']}  {' '.join(extra)}")

# TSRT SC-only
print(f'\n{"="*70}')
print('TSRT SC-ONLY')
print(f'{"="*70}')
sc = [r for r in results if r['setup']=='Skew Charm']
sn,sw,sp = stats(sc)
sc_blk = [r for r in sc if v13b(r)]; sbn,sbw,sbp = stats(sc_blk)
sc_kept = [r for r in sc if not v13b(r)]; skn,skw,skp = stats(sc_kept)
print(f"V12-fix SC shorts: {sn}t {sw}W={sw/sn*100:.0f}% WR {sp:+.1f}")
print(f"V13 keeps:         {skn}t {skw}W={skw/skn*100:.0f}% WR {skp:+.1f}")
print(f"V13 blocks:        {sbn}t ({sbw}W/{sbn-sbw}L) avoided {-sbp:+.1f}")

# March safety
march_s = [r for r in scdd if r['date'].month==3]
mn,mw,mp = stats(march_s)
mb = [r for r in march_s if v13b(r)]; mbn,mbw,mbp = stats(mb)
print(f'\nMarch safety: {mn}t {mp:+.1f} | V13 blocks {mbn}t PnL={mbp:+.1f} (saves {-mbp:+.1f})')

# Bootstrap
random.seed(42)
imps = []
for _ in range(2000):
    s = [scdd[random.randint(0,len(scdd)-1)] for _ in range(len(scdd))]
    b = sum(r['pnl'] for r in s)
    f = sum(r['pnl'] for r in s if not v13b(r))
    imps.append(f-b)
imps.sort()
pn = sum(1 for x in imps if x<=0)/len(imps)
print(f'\nBootstrap: p5={imps[100]:+.1f} p50={imps[1000]:+.1f} p95={imps[1900]:+.1f}')
print(f'P(improvement <= 0) = {pn*100:.1f}%')
