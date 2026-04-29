"""Re-assess DD Hedging Magnet filter with fresh data through Apr 15."""
import pickle, bisect
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict
import os

os.environ['DATABASE_URL'] = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
eng = create_engine(os.environ['DATABASE_URL'])
ET = ZoneInfo('America/New_York')

with open('enriched_shorts.pkl','rb') as f: R = pickle.load(f)

# Pre-load deltaDecay data (per-strike per-ts) — expensive but once
print('Pre-loading deltaDecay (per-strike)...', flush=True)
with eng.connect() as c:
    # Get all DD points Mar 1 - Apr 15
    all_dd = c.execute(text("""
        SELECT ts_utc, CAST(strike AS FLOAT) AS k, CAST(value AS FLOAT) AS v
        FROM volland_exposure_points
        WHERE ts_utc >= '2026-03-01' AND ts_utc <= '2026-04-16'
          AND ticker='SPX' AND greek='deltaDecay' AND expiration_option='TODAY'
        ORDER BY ts_utc
    """)).fetchall()
    print(f'  loaded {len(all_dd)} DD points', flush=True)

# Index by ts
dd_by_ts = defaultdict(list)
for ts, k, v in all_dd:
    dd_by_ts[ts].append((k, v))
dd_ts_sorted = sorted(dd_by_ts.keys())
print(f'  indexed {len(dd_ts_sorted)} unique ts', flush=True)

def find_nearest(target_ts, sorted_ts, max_delta_sec=300):
    i = bisect.bisect_left(sorted_ts, target_ts)
    candidates = []
    if i > 0: candidates.append(sorted_ts[i-1])
    if i < len(sorted_ts): candidates.append(sorted_ts[i])
    if not candidates: return None
    best = min(candidates, key=lambda x: abs((x-target_ts).total_seconds()))
    if abs((best-target_ts).total_seconds()) > max_delta_sec: return None
    return best

# For each short signal, compute DD-magnet features
print('Enriching signals with DD-magnet features...', flush=True)
enriched = []
for r in R:
    if r['setup'] not in ('Skew Charm','DD Exhaustion'): continue
    cm_ts = find_nearest(r['ts'], dd_ts_sorted)
    if not cm_ts: continue
    points = dd_by_ts[cm_ts]
    # Filter to near-spot strikes (+/- 10 pts per original study)
    spot = r['spot']
    near = [(k, v) for k, v in points if abs(k - spot) <= 10]
    if not near:
        r['dd_max_abs'] = 0
        r['dd_max_k'] = None
    else:
        # Strongest absolute value
        top = max(near, key=lambda x: abs(x[1]))
        r['dd_max_abs'] = abs(top[1])
        r['dd_max_k'] = top[0]
        r['dd_max_v'] = top[1]  # signed
    enriched.append(r)

print(f'Enriched: {len(enriched)} SC+DD signals', flush=True)

def stats(arr):
    if not arr: return (0,0,0.0)
    w = sum(1 for x in arr if x['out']=='WIN')
    return len(arr), w, sum(x['pnl'] for x in arr)

# === THRESHOLD SWEEP (in billions) ===
print('\n=== DD MAGNET THRESHOLD SWEEP (|DD| >= T near spot +/-10pt) ===')
print(f"Baseline: {stats(enriched)}")
for thr_b in [0.5, 1.0, 1.5, 2.0, 2.3, 2.5, 3.0, 3.5, 4.0, 5.0]:
    thr = thr_b * 1e9
    blk = [r for r in enriched if r['dd_max_abs']>=thr]
    n,w,p = stats(enriched); bn,bw,bp = stats(blk)
    if bn == 0:
        print(f"  >={thr_b:.1f}B: 0 blocks"); continue
    print(f"  >={thr_b:.1f}B: {bn}t {bw}W={bw/bn*100:.0f}% bp={bp:+.1f} | after {p-bp:+.1f} (delta {-bp:+.1f})")

# OOS stability — split sample
print('\n=== OOS STABILITY (median date split) ===')
enriched.sort(key=lambda x: x['ts'])
mid = len(enriched)//2
for half, label in [(enriched[:mid],'H1'), (enriched[mid:],'H2')]:
    for thr_b in [2.0, 2.3, 3.0]:
        thr = thr_b * 1e9
        blk = [r for r in half if r['dd_max_abs']>=thr]
        n,w,p = stats(half); bn,bw,bp = stats(blk)
        d1, d2 = half[0]['date'], half[-1]['date']
        print(f"  {label} {d1} -> {d2} @ >={thr_b}B: n={n} base={p:+.1f} | blk {bn}({bw}W) bp={bp:+.1f} | after {p-bp:+.1f}")

# By-date analysis
print('\n=== DAILY PNL @ 2.3B (original best threshold) ===')
by_date = defaultdict(list)
for r in enriched: by_date[r['date']].append(r)
print(f"{'Date':<12} {'#sig':<5} {'Base':<9} {'Blk':<4} {'Blk$':<9} {'After':<9} {'Delta':<8}")
Tb_p = Tblk_p = 0.0; Tb_n = Tblk_n = 0
for d in sorted(by_date.keys()):
    arr = by_date[d]
    n,w,p = stats(arr)
    blk = [r for r in arr if r['dd_max_abs']>=2.3e9]
    bn,bw,bp = stats(blk)
    print(f"{str(d):<12} {n:<5} {p:<+9.1f} {bn:<4} {bp:<+9.1f} {p-bp:<+9.1f} {-bp:<+8.1f}")
    Tb_p += p; Tb_n += n; Tblk_p += bp; Tblk_n += bn
print('-'*70)
print(f"{'TOTAL':<12} {Tb_n:<5} {Tb_p:<+9.1f} {Tblk_n:<4} {Tblk_p:<+9.1f} {Tb_p-Tblk_p:<+9.1f} {-Tblk_p:<+8.1f}")

# Check if Apr 2 still dominates
apr2_contrib = 0.0
other_contrib = 0.0
for r in enriched:
    if r['dd_max_abs'] >= 2.3e9:
        if r['date'] == datetime(2026,4,2).date():
            apr2_contrib -= r['pnl']
        else:
            other_contrib -= r['pnl']
print(f"\nApr 2 contribution to filter delta: {apr2_contrib:+.1f}")
print(f"Other days contribution to filter delta: {other_contrib:+.1f}")
print(f"Total delta: {apr2_contrib+other_contrib:+.1f}")
print(f"Apr 2 share: {apr2_contrib/(apr2_contrib+other_contrib)*100:.0f}%")
