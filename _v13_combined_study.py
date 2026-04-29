"""V13 study: combine GEX Magnet ≥70M + DD Magnet ≥XB. Overlap analysis and incremental contribution."""
import pickle, bisect
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from collections import defaultdict
import os

os.environ['DATABASE_URL'] = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
eng = create_engine(os.environ['DATABASE_URL'])

with open('enriched_shorts.pkl','rb') as f: R = pickle.load(f)

# Load DD data
print('Loading DD data...', flush=True)
with eng.connect() as c:
    all_dd = c.execute(text("""
        SELECT ts_utc, CAST(strike AS FLOAT), CAST(value AS FLOAT)
        FROM volland_exposure_points
        WHERE ts_utc >= '2026-03-01' AND ts_utc <= '2026-04-16'
          AND ticker='SPX' AND greek='deltaDecay' AND expiration_option='TODAY'
    """)).fetchall()

dd_by_ts = defaultdict(list)
for ts, k, v in all_dd:
    dd_by_ts[ts].append((k, v))
dd_ts_sorted = sorted(dd_by_ts.keys())

def find_nearest(target_ts, sorted_ts, max_sec=300):
    i = bisect.bisect_left(sorted_ts, target_ts)
    cands = []
    if i > 0: cands.append(sorted_ts[i-1])
    if i < len(sorted_ts): cands.append(sorted_ts[i])
    if not cands: return None
    best = min(cands, key=lambda x: abs((x-target_ts).total_seconds()))
    if abs((best-target_ts).total_seconds()) > max_sec: return None
    return best

# Enrich SC+DD signals with DD-magnet feature
SCDD = [r for r in R if r['setup'] in ('Skew Charm','DD Exhaustion')]
for r in SCDD:
    cm = find_nearest(r['ts'], dd_ts_sorted)
    if not cm:
        r['dd_max_abs'] = 0
        continue
    near = [(k,v) for k,v in dd_by_ts[cm] if abs(k - r['spot']) <= 10]
    r['dd_max_abs'] = max((abs(v) for _,v in near), default=0)

def stats(arr):
    if not arr: return (0,0,0.0)
    w = sum(1 for x in arr if x['out']=='WIN')
    return len(arr), w, sum(x['pnl'] for x in arr)

def summary(label, arr):
    n,w,p = stats(arr)
    wr = w/n*100 if n else 0
    return f"{label}: {n}t {w}W={wr:.0f}% {p:+.1f}"

print(f"\nTotal SC+DD signals (V12-fix, Mar 1 - Apr 15): {len(SCDD)}")
print(summary("Baseline", SCDD))

# ──────────────────────────────────────────────────────────────
# OVERLAP ANALYSIS
# ──────────────────────────────────────────────────────────────
GEX_THR = 70  # M
# We'll test DD at multiple thresholds
for DD_THR_B in [2.0, 2.3, 2.5, 3.0, 3.5]:
    DD_THR = DD_THR_B * 1e9
    print(f"\n{'='*70}")
    print(f"OVERLAP: GEX>={GEX_THR}M  AND/OR  DD>={DD_THR_B}B")
    print('='*70)

    g_only = []
    d_only = []
    both = []
    neither = []
    for r in SCDD:
        g = r['plus_above_v'] >= GEX_THR
        d = r['dd_max_abs'] >= DD_THR
        if g and d: both.append(r)
        elif g: g_only.append(r)
        elif d: d_only.append(r)
        else: neither.append(r)

    print(summary("  G-only (GEX triggers, DD doesn't)", g_only))
    print(summary("  D-only (DD triggers, GEX doesn't)", d_only))
    print(summary("  Both triggered", both))
    print(summary("  Neither (would pass both filters)", neither))

    # What's the incremental contribution of D-only?
    d_only_pnl = sum(r['pnl'] for r in d_only)
    d_only_w = sum(1 for r in d_only if r['out']=='WIN')
    d_only_n = len(d_only)

    g_blk = g_only + both  # GEX-alone would block these
    g_only_delta = -sum(r['pnl'] for r in g_blk)

    combined_blk = g_only + d_only + both
    combined_delta = -sum(r['pnl'] for r in combined_blk)

    dd_alone_blk = d_only + both
    dd_alone_delta = -sum(r['pnl'] for r in dd_alone_blk)

    print(f"\n  --- FILTER DELTAS ---")
    print(f"  GEX alone (>={GEX_THR}M):        block {len(g_blk)}t  delta {g_only_delta:+.1f}")
    print(f"  DD alone (>={DD_THR_B}B):          block {len(dd_alone_blk)}t  delta {dd_alone_delta:+.1f}")
    print(f"  V13 UNION (GEX OR DD):    block {len(combined_blk)}t  delta {combined_delta:+.1f}")
    print(f"  Incremental gain adding DD: {combined_delta - g_only_delta:+.1f} pts")
    print(f"  D-only trades (the incremental): {d_only_n}t {d_only_w}W={d_only_w/max(d_only_n,1)*100:.0f}% WR  PnL={d_only_pnl:+.1f}")

# Best threshold choice for V13
print('\n' + '='*70)
print('V13 BEST CONFIG SEARCH: GEX>=70M fixed, sweep DD threshold')
print('='*70)
for DD_THR_B in [1.5, 2.0, 2.3, 2.5, 3.0, 3.5, 4.0]:
    DD_THR = DD_THR_B * 1e9
    blk = [r for r in SCDD if (r['plus_above_v']>=70) or (r['dd_max_abs']>=DD_THR)]
    n,w,p = stats(SCDD); bn,bw,bp = stats(blk)
    print(f"  GEX>=70 OR DD>={DD_THR_B}B: block {bn}t {bw}W={bw/max(bn,1)*100:.0f}% bp={bp:+.1f} | after {p-bp:+.1f} (delta {-bp:+.1f})")

# Full daily PnL for V13 @ best config
print('\n' + '='*70)
print('V13 DAILY PNL (GEX>=70 OR DD>=3.0B) — SC+DD only')
print('='*70)
by_date = defaultdict(list)
for r in SCDD: by_date[r['date']].append(r)
print(f"{'Date':<12} {'#sig':<5} {'Base':<9} {'Gblk':<5} {'Dblk':<5} {'Both':<5} {'V13blk':<7} {'V13$':<9} {'After':<9} {'Delta':<7}")
T_base_p = T_v13_blk_p = 0.0; T_base_n = T_v13_blk_n = 0
for d in sorted(by_date.keys()):
    arr = by_date[d]
    n,w,p = stats(arr)
    g_set = [r for r in arr if r['plus_above_v']>=70]
    d_set = [r for r in arr if r['dd_max_abs']>=3e9]
    both_set = [r for r in arr if r['plus_above_v']>=70 and r['dd_max_abs']>=3e9]
    v13 = [r for r in arr if r['plus_above_v']>=70 or r['dd_max_abs']>=3e9]
    bn,bw,bp = stats(v13)
    after = p - bp
    delta = -bp
    print(f"{str(d):<12} {n:<5} {p:<+9.1f} {len(g_set):<5} {len(d_set):<5} {len(both_set):<5} {bn:<7} {bp:<+9.1f} {after:<+9.1f} {delta:<+7.1f}")
    T_base_p += p; T_base_n += n; T_v13_blk_p += bp; T_v13_blk_n += bn
print('-'*95)
print(f"{'TOTAL':<12} {T_base_n:<5} {T_base_p:<+9.1f} {'':5} {'':5} {'':5} {T_v13_blk_n:<7} {T_v13_blk_p:<+9.1f} {T_base_p-T_v13_blk_p:<+9.1f} {-T_v13_blk_p:<+7.1f}")

# March safety at V13
march = [r for r in SCDD if r['date'].month == 3]
mbn,mbw,mbp = stats([r for r in march if r['plus_above_v']>=70 or r['dd_max_abs']>=3e9])
print(f"\nMarch V13 safety: {len(march)}t baseline {stats(march)[2]:+.1f} | V13 blocks {mbn}t PnL={mbp:+.1f}")

# OOS stability V13
print('\n=== V13 OOS STABILITY ===')
scdd_sorted = sorted(SCDD, key=lambda x: x['ts'])
mid = len(scdd_sorted)//2
for half, label in [(scdd_sorted[:mid],'H1'), (scdd_sorted[mid:],'H2')]:
    blk = [r for r in half if r['plus_above_v']>=70 or r['dd_max_abs']>=3e9]
    n,w,p = stats(half); bn,bw,bp = stats(blk)
    d1,d2 = half[0]['date'], half[-1]['date']
    print(f"  {label} {d1} -> {d2}: n={n} base={p:+.1f} | V13 blocks {bn}({bw}W) bp={bp:+.1f} | after {p-bp:+.1f}")

# D-only trades detail (the ones DD adds beyond GEX)
print('\n=== D-ONLY TRADES (DD triggers but GEX doesn\'t) — this is what DD ADDS ===')
d_only_list = sorted([r for r in SCDD if r['dd_max_abs']>=3e9 and r['plus_above_v']<70], key=lambda x: x['ts'])
for r in d_only_list:
    print(f"  {r['ts_et'].strftime('%m-%d %H:%M')} {r['setup']:<15} {r['grade'] or '-':<4} spot={r['spot']:.0f} GEX+above={r['plus_above_v']:+.0f}M DD={r['dd_max_abs']/1e9:.2f}B pnl={r['pnl']:+.1f} {r['out']}")
