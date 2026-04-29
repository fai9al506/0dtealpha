"""V13 GEX tuning: raise threshold + add distance requirement. DD stays at 3B."""
import os, json, pickle, bisect
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
    if setup in ("Skew Charm","DD Exhaustion") and para == "GEX-LIS": return False
    if setup == "AG Short" and para == "AG-TARGET": return False
    if setup in ("Skew Charm","AG Short"): return True
    if setup == "DD Exhaustion" and (align or 0) != 0: return True
    return False

def compute_gex_above(chain, spot):
    gex = []
    for row in chain:
        try:
            s=row[10]; cg=(row[3] or 0)*(row[1] or 0); pg=(row[17] or 0)*(row[19] or 0)
            gex.append((s, cg-pg))
        except: continue
    above = [(s,v) for s,v in gex if s > spot]
    if not above: return None, 0
    top = max(above, key=lambda x: x[1])
    return top[0], top[1]

print('Loading...', flush=True)
with eng.connect() as c:
    chains = c.execute(text("SELECT ts,spot,rows FROM chain_snapshots WHERE ts>='2026-03-01' AND ts<='2026-04-18' ORDER BY ts")).fetchall()
    dd_raw = c.execute(text("SELECT ts_utc,CAST(strike AS FLOAT),CAST(value AS FLOAT) FROM volland_exposure_points WHERE ts_utc>='2026-03-01' AND ts_utc<='2026-04-18' AND ticker='SPX' AND greek='deltaDecay' AND expiration_option='TODAY'")).fetchall()
    sigs = c.execute(text("SELECT id,ts,setup_name,grade,paradigm,spot,greek_alignment,outcome_result,outcome_pnl,vix,overvix FROM setup_log WHERE ts>='2026-03-01' AND ts<='2026-04-17 23:59:59' AND direction='short' AND setup_name IN ('Skew Charm','AG Short','DD Exhaustion') AND outcome_pnl IS NOT NULL ORDER BY ts")).fetchall()

chain_by_ts = {}
for ts,sp,rows in chains:
    chain_by_ts[ts] = (float(sp) if sp else None, rows if isinstance(rows,list) else json.loads(rows) if rows else None)
chain_ts = sorted(chain_by_ts.keys())
dd_by_ts = defaultdict(list)
for ts,k,v in dd_raw: dd_by_ts[ts].append((k,v))
dd_ts = sorted(dd_by_ts.keys())

def fn(t, arr, mx=300):
    i = bisect.bisect_left(arr, t)
    c = []
    if i>0: c.append(arr[i-1])
    if i<len(arr): c.append(arr[i])
    if not c: return None
    b = min(c, key=lambda x: abs((x-t).total_seconds()))
    return b if abs((b-t).total_seconds())<=mx else None

results = []
for sig in sigs:
    sid,ts,setup,grade,para,spot,align,out,pnl,vix,ov = sig
    ts_et = ts.astimezone(ET)
    if not v12fix(setup, align, para, grade, ts_et): continue
    ct = fn(ts, chain_ts, 180)
    if not ct: continue
    _,chain = chain_by_ts[ct]
    if not chain: continue
    pk, pv = compute_gex_above(chain, float(spot))
    dt = fn(ts, dd_ts, 300)
    dd_abs = 0
    if dt:
        near = [(k,v) for k,v in dd_by_ts[dt] if abs(k-float(spot))<=10]
        if near: dd_abs = max(abs(v) for _,v in near)
    dist = (pk - float(spot)) if pk else 0
    results.append({
        'id':sid,'ts':ts,'ts_et':ts_et,'date':ts_et.date(),'setup':setup,
        'spot':float(spot),'out':out,'pnl':float(pnl),
        'gex_v':pv or 0,'gex_k':pk,'gex_dist':dist,
        'dd_abs':dd_abs
    })

scdd = [r for r in results if r['setup'] in ('Skew Charm','DD Exhaustion')]
sc = [r for r in results if r['setup']=='Skew Charm']
print(f'V12-fix: {len(results)} all, {len(scdd)} SC+DD, {len(sc)} SC', flush=True)

def stats(arr):
    if not arr: return (0,0,0.0)
    w = sum(1 for x in arr if x['out']=='WIN')
    return len(arr), w, sum(x['pnl'] for x in arr)

def v13(r, gex_thr=70, gex_dist_min=0, dd_thr=3e9):
    """Returns True if V13 blocks this trade."""
    if r['setup'] not in ('Skew Charm','DD Exhaustion'): return False
    if r['gex_v'] >= gex_thr and r['gex_dist'] >= gex_dist_min: return True
    if r['dd_abs'] >= dd_thr: return True
    return False

# === SECTION 1: Fine GEX threshold sweep (DD stays 3B) ===
print('\n' + '='*75)
print('GEX THRESHOLD SWEEP (DD fixed at 3B) on SC+DD')
print('='*75)
sn,sw,sp = stats(scdd)
print(f"Baseline: {sn}t {sw}W={sw/sn*100:.0f}% {sp:+.1f}")
print(f"{'Config':<35} {'Blk':<5} {'WR':<5} {'BlkPnL':<9} {'After':<9} {'Delta':<7}")
for gt in [70, 75, 80, 85, 90, 100]:
    blk = [r for r in scdd if v13(r, gex_thr=gt)]
    bn,bw,bp = stats(blk)
    if bn==0: continue
    print(f"  GEX>={gt}M dist>=0 + DD>=3B     {bn:<5} {bw/bn*100:<5.0f} {bp:<+9.1f} {sp-bp:<+9.1f} {-bp:<+7.1f}")

# === SECTION 2: Distance requirement on GEX ===
print('\n' + '='*75)
print('GEX THRESHOLD + DISTANCE (DD fixed at 3B) on SC+DD')
print('='*75)
for gt in [70, 75, 80]:
    for dm in [0, 5, 10, 15, 20]:
        blk = [r for r in scdd if v13(r, gex_thr=gt, gex_dist_min=dm)]
        bn,bw,bp = stats(blk)
        if bn==0: continue
        print(f"  GEX>={gt}M dist>={dm}pt + DD>=3B   {bn:<5} {bw/bn*100:<5.0f} {bp:<+9.1f} {sp-bp:<+9.1f} {-bp:<+7.1f}")

# === SECTION 3: SC-ONLY with tuned threshold (TSRT impact) ===
print('\n' + '='*75)
print('SC-ONLY THRESHOLD SWEEP (TSRT IMPACT)')
print('='*75)
sn,sw,sp = stats(sc)
print(f"SC baseline: {sn}t {sw}W={sw/sn*100:.0f}% {sp:+.1f}")
for gt in [70, 75, 80, 85, 90, 100]:
    for dm in [0, 10, 15, 20]:
        blk = [r for r in sc if v13(r, gex_thr=gt, gex_dist_min=dm)]
        bn,bw,bp = stats(blk)
        if bn==0: continue
        print(f"  GEX>={gt}M dist>={dm}pt + DD>=3B   blk {bn}({bw}W={bw/bn*100:.0f}%) bp={bp:+.1f} | SC after={sp-bp:+.1f} (delta {-bp:+.1f})")

# === SECTION 4: Apr 15-16 side-by-side with different configs ===
print('\n' + '='*75)
print('APR 15 + APR 16 TSRT COMPARISON (SC-only, actual placed trades)')
print('='*75)
# TSRT placed ids
tsrt_ids_15 = [1793, 1802, 1821, 1845]
tsrt_ids_16 = [1848, 1853, 1863, 1875]
tsrt_all = {r['id']:r for r in results if r['id'] in tsrt_ids_15+tsrt_ids_16}

# TSRT actual MES PnL
tsrt_pnl = {1793:-14.0, 1802:+5.25, 1821:-14.0, 1845:-4.5,
            1848:+18.5, 1853:-14.0, 1863:+8.75, 1875:+6.75}

for gt in [70, 75, 80]:
    for dm in [0, 10, 15]:
        print(f"\n  --- GEX>={gt}M dist>={dm}pt + DD>=3B ---")
        for day_label, ids in [("Apr 15", tsrt_ids_15), ("Apr 16", tsrt_ids_16)]:
            kept_pnl = 0.0; blocked_pnl = 0.0; n_kept=0; n_blocked=0
            detail = []
            for tid in ids:
                r = tsrt_all.get(tid)
                if not r: continue
                blocked = v13(r, gex_thr=gt, gex_dist_min=dm)
                mes_pnl = tsrt_pnl[tid]
                if blocked:
                    blocked_pnl += mes_pnl; n_blocked += 1
                    detail.append(f"#{tid} BLOCK {mes_pnl:+.2f}")
                else:
                    kept_pnl += mes_pnl; n_kept += 1
                    detail.append(f"#{tid} KEEP  {mes_pnl:+.2f}")
            orig = sum(tsrt_pnl[i] for i in ids)
            print(f"    {day_label}: orig=${orig*5:+.0f} -> V13=${kept_pnl*5:+.0f} (delta ${(kept_pnl-orig)*5:+.0f})  [{', '.join(detail)}]")
        # 2-day net
        all_kept = 0.0
        for tid in tsrt_ids_15+tsrt_ids_16:
            r = tsrt_all.get(tid)
            if not r: continue
            if not v13(r, gex_thr=gt, gex_dist_min=dm):
                all_kept += tsrt_pnl[tid]
        orig_total = sum(tsrt_pnl[i] for i in tsrt_ids_15+tsrt_ids_16)
        print(f"    2-day net: orig=${orig_total*5:+.0f} -> V13=${all_kept*5:+.0f}")

# === SECTION 5: Check #1848 specifically ===
print('\n' + '='*75)
print('TRADE #1848 DETAIL (the good trade you want to keep)')
print('='*75)
r1848 = tsrt_all.get(1848)
if r1848:
    print(f"  spot={r1848['spot']:.0f} GEX+above={r1848['gex_v']:+.0f}M at K={r1848['gex_k']:.0f} (dist={r1848['gex_dist']:.0f}pts)")
    print(f"  DD near={r1848['dd_abs']/1e9:.2f}B")
    print(f"  MES PnL={tsrt_pnl[1848]:+.2f} pts = ${tsrt_pnl[1848]*5:+.2f}")
    for gt in [70,73,75,80]:
        for dm in [0,10,15,20]:
            blocked = r1848['gex_v']>=gt and r1848['gex_dist']>=dm
            dd_blk = r1848['dd_abs']>=3e9
            tag = 'BLOCK' if (blocked or dd_blk) else 'PASS'
            if tag == 'PASS':
                print(f"    GEX>={gt}M dist>={dm}pt: {tag}  <-- keeps this trade")
