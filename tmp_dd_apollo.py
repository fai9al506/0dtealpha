"""
Check what DD data is actually available in volland_snapshots,
and test Apollo's framework: big DD flip + vanna support = good trade.
"""
import os, json
from collections import defaultdict
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL)

def q(sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

# ─── 1. What keys exist in volland statistics? ──────────────────────────────
print("=== STEP 1: What's in volland_snapshots statistics? ===")
sample = q("""
    SELECT payload->'statistics' as stats
    FROM volland_snapshots
    WHERE payload->'statistics' IS NOT NULL
    ORDER BY ts DESC LIMIT 1
""")
if sample:
    stats_obj = sample[0]['stats']
    if isinstance(stats_obj, str):
        stats_obj = json.loads(stats_obj)
    print(f"Keys: {list(stats_obj.keys())}")
    for k, v in stats_obj.items():
        print(f"  {k}: {str(v)[:80]}")

# ─── 2. Check if aggregatedDeltaDecay exists ────────────────────────────────
print("\n=== STEP 2: Check aggregatedDeltaDecay availability ===")
dd_check = q("""
    SELECT COUNT(*) as total,
           COUNT(payload->'statistics'->>'aggregatedDeltaDecay') as has_agg_dd,
           COUNT(payload->'statistics'->>'deltaDecay') as has_dd,
           COUNT(payload->'statistics'->>'delta_decay') as has_dd2
    FROM volland_snapshots
    WHERE payload->'statistics' IS NOT NULL
""")
print(f"  Total snapshots with stats: {dd_check[0]['total']}")
print(f"  Has aggregatedDeltaDecay: {dd_check[0]['has_agg_dd']}")
print(f"  Has deltaDecay: {dd_check[0]['has_dd']}")
print(f"  Has delta_decay: {dd_check[0]['has_dd2']}")

# Check all keys that have "delta" or "dd" or "decay"
print("\n=== STEP 3: Any key with 'delta' or 'decay' in statistics ===")
keys_check = q("""
    SELECT DISTINCT jsonb_object_keys(payload->'statistics') as key_name
    FROM volland_snapshots
    WHERE payload->'statistics' IS NOT NULL
    LIMIT 100
""")
for r in keys_check:
    k = r['key_name'].lower()
    if 'delta' in k or 'decay' in k or 'dd' in k or 'hedg' in k:
        print(f"  MATCH: {r['key_name']}")

# ─── 3. Get DD from exposure_points (deltaDecay greek) ─────────────────────
print("\n=== STEP 4: DD total from exposure_points (deltaDecay) ===")
dd_totals = q("""
    SELECT ts_utc, SUM(value) as dd_total, COUNT(*) as strikes
    FROM volland_exposure_points
    WHERE greek = 'deltaDecay'
    GROUP BY ts_utc
    ORDER BY ts_utc DESC
    LIMIT 5
""")
for r in dd_totals:
    print(f"  {r['ts_utc']}: dd_total={r['dd_total']:.0f}, strikes={r['strikes']}")

# ─── 4. Build DD shift series from exposure_points ──────────────────────────
print("\n=== STEP 5: Build DD shift series ===")
dd_series = q("""
    SELECT ts_utc, SUM(value) as dd_total
    FROM volland_exposure_points
    WHERE greek = 'deltaDecay'
    GROUP BY ts_utc
    ORDER BY ts_utc
""")
print(f"  Total DD snapshots: {len(dd_series)}")

# Build shift series (diff between consecutive snapshots)
dd_shifts = []
for i in range(1, len(dd_series)):
    curr = dd_series[i]
    prev = dd_series[i-1]
    dt = (curr['ts_utc'] - prev['ts_utc']).total_seconds()
    if dt < 600:  # within 10 min (same session)
        dd_shifts.append({
            'ts': curr['ts_utc'],
            'dd_total': float(curr['dd_total']),
            'dd_prev': float(prev['dd_total']),
            'dd_shift': float(curr['dd_total']) - float(prev['dd_total']),
        })
print(f"  DD shifts computed: {len(dd_shifts)}")
if dd_shifts:
    shifts_abs = [abs(s['dd_shift']) for s in dd_shifts]
    print(f"  Shift range: {min(shifts_abs):.0f} to {max(shifts_abs):.0f}")
    print(f"  Shift avg: {sum(shifts_abs)/len(shifts_abs):.0f}")
    print(f"  Sample: {dd_shifts[-1]}")

# ─── 5. Load DD trades ─────────────────────────────────────────────────────
trades = q("""
    SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, ts as created_at,
           direction, grade, score, spot, outcome_result, outcome_pnl,
           outcome_max_profit, outcome_max_loss, paradigm, vix
    FROM setup_log WHERE setup_name = 'DD Exhaustion' AND outcome_result IS NOT NULL ORDER BY ts
""")
print(f"\n=== STEP 6: Matching {len(trades)} DD trades with DD shift + vanna ===")

# ─── 6. Load vanna by type ─────────────────────────────────────────────────
vanna_by_type = q("""
    SELECT ts_utc, expiration_option, SUM(value) as total
    FROM volland_exposure_points
    WHERE greek = 'vanna'
    GROUP BY ts_utc, expiration_option
    ORDER BY ts_utc
""")
# Build: ts -> {type: value}
vanna_map = defaultdict(dict)
for r in vanna_by_type:
    vanna_map[r['ts_utc']][r['expiration_option'] or 'NULL'] = float(r['total'])
print(f"  Vanna snapshots: {len(vanna_map)}")

# List available vanna types
all_vtypes = set()
for v in vanna_map.values():
    all_vtypes.update(v.keys())
print(f"  Vanna types available: {sorted(all_vtypes)}")

# ─── 7. Match each trade to nearest DD shift + vanna ───────────────────────
def find_nearest_idx(sorted_list, ts, ts_field, max_s=300):
    best_i, bd = None, max_s+1
    for i, item in enumerate(sorted_list):
        d = abs((item[ts_field] - ts).total_seconds())
        if d < bd: best_i, bd = i, d
        elif d > bd + 600: break
    return best_i

def find_nearest_map_key(m, ts, max_s=300):
    bk, bd = None, max_s+1
    for k in m:
        d = abs((k - ts).total_seconds())
        if d < bd: bk, bd = k, d
    return bk if bd <= max_s else None

enriched = []
for t in trades:
    row = dict(t)
    ts = t['created_at']

    # Find nearest DD shift
    ds_idx = find_nearest_idx(dd_shifts, ts, 'ts', 300)
    if ds_idx is not None:
        ds = dd_shifts[ds_idx]
        row['dd_total'] = ds['dd_total']
        row['dd_shift'] = ds['dd_shift']
        row['dd_shift_abs'] = abs(ds['dd_shift'])
    else:
        row['dd_total'] = row['dd_shift'] = row['dd_shift_abs'] = None

    # Find nearest vanna (all types)
    vk = find_nearest_map_key(vanna_map, ts, 300)
    if vk:
        row['vanna'] = vanna_map[vk]
    else:
        row['vanna'] = {}

    # Time
    row['hour'] = t['ts_et'].hour if t['ts_et'] else None
    row['date_str'] = t['ts_et'].strftime('%Y-%m-%d') if t['ts_et'] else '?'
    row['time_str'] = t['ts_et'].strftime('%H:%M') if t['ts_et'] else '?'
    row['pnl'] = float(t.get('outcome_pnl') or 0)
    row['max_profit'] = float(t.get('outcome_max_profit') or 0)

    # Category
    res = t.get('outcome_result', '')
    if res in ('WIN', 'WIN_TRAIL'): row['cat'] = 'W'
    elif res == 'LOSS': row['cat'] = 'L'
    else: row['cat'] = 'E'

    enriched.append(row)

dd_matched = sum(1 for t in enriched if t.get('dd_shift') is not None)
vanna_matched = sum(1 for t in enriched if t.get('vanna'))
print(f"  DD shift matched: {dd_matched}/{len(enriched)}")
print(f"  Vanna matched: {vanna_matched}/{len(enriched)}")

# ─── Stats ──────────────────────────────────────────────────────────────────
def stats(tl, label=""):
    if not tl: return None
    n=len(tl)
    w=sum(1 for t in tl if t['cat']=='W')
    l=sum(1 for t in tl if t['cat']=='L')
    e=sum(1 for t in tl if t['cat']=='E')
    p=sum(t['pnl'] for t in tl)
    wr=w/max(w+l,1)*100
    return {"n":n,"w":w,"l":l,"e":e,"wr":round(wr,1),"pnl":round(p,1),"avg":round(p/n,1),"label":label}

def ps(s):
    if not s or s['n']==0: return
    print(f"  {s['label']:55s} | N={s['n']:3d} | {s['w']:2d}W/{s['l']:2d}L/{s['e']:2d}E | WR={s['wr']:5.1f}% | PnL={s['pnl']:+8.1f} | Avg={s['avg']:+6.1f}")

# ═══════════════════════════════════════════════════════════════════════════
# APOLLO'S FRAMEWORK: Big DD flip + Vanna supports trade direction
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*100}")
print("APOLLO'S FRAMEWORK: DD flip + Vanna support")
print("For DD Exhaustion LONG: DD shifted bearish, charm positive")
print("  -> Vanna 'supports' if it creates upward pressure (positive vanna for long)")
print("For DD Exhaustion SHORT: DD shifted bullish, charm negative")
print("  -> Vanna 'supports' if it creates downward pressure (negative vanna for short)")
print(f"{'='*100}")

# Test each vanna type for direction support
for vtype in sorted(all_vtypes):
    print(f"\n--- Vanna type: {vtype} ---")
    has_vtype = [t for t in enriched if vtype in t.get('vanna', {})]
    if not has_vtype:
        print("  No data")
        continue

    # "Vanna supports direction" = positive vanna for long, negative for short
    supports = [t for t in has_vtype if
        (t['direction']=='long' and t['vanna'][vtype] > 0) or
        (t['direction']=='short' and t['vanna'][vtype] < 0)]
    opposes = [t for t in has_vtype if
        (t['direction']=='long' and t['vanna'][vtype] < 0) or
        (t['direction']=='short' and t['vanna'][vtype] > 0)]
    # Also neutral (near zero)
    neutral = [t for t in has_vtype if abs(t['vanna'][vtype]) < 50]

    ps(stats(supports, f"Vanna {vtype} SUPPORTS direction"))
    ps(stats(opposes, f"Vanna {vtype} OPPOSES direction"))

    # Also test: vanna positive vs negative (regardless of direction)
    pos = [t for t in has_vtype if t['vanna'][vtype] > 0]
    neg = [t for t in has_vtype if t['vanna'][vtype] < 0]
    ps(stats(pos, f"Vanna {vtype} POSITIVE (raw)"))
    ps(stats(neg, f"Vanna {vtype} NEGATIVE (raw)"))

# ─── Now test with DD shift magnitude ──────────────────────────────────────
print(f"\n{'='*100}")
print("DD SHIFT MAGNITUDE + VANNA SUPPORT (Apollo's confluence)")
print(f"{'='*100}")

dd_trades = [t for t in enriched if t.get('dd_shift') is not None]
print(f"Trades with DD shift data: {len(dd_trades)}")

if dd_trades:
    # DD shift buckets
    for lo, hi, lab in [(0, 50, "shift<50"), (50, 200, "shift 50-200"),
                         (200, 500, "shift 200-500"), (500, float('inf'), "shift 500+")]:
        bucket = [t for t in dd_trades if lo <= t['dd_shift_abs'] < hi]
        if bucket:
            ps(stats(bucket, f"DD {lab}"))

    # Cross: DD shift x Vanna support (for each vtype)
    for vtype in sorted(all_vtypes):
        has_both = [t for t in dd_trades if vtype in t.get('vanna', {})]
        if len(has_both) < 5: continue

        print(f"\n  --- DD shift x Vanna {vtype} support ---")
        for shift_lab, shift_fn in [("Big DD shift (>200)", lambda t: t['dd_shift_abs']>200),
                                     ("Small DD shift (<200)", lambda t: t['dd_shift_abs']<=200)]:
            for vanna_lab, vanna_fn in [
                ("Vanna SUPPORTS", lambda t, vt=vtype: (t['direction']=='long' and t['vanna'][vt]>0) or (t['direction']=='short' and t['vanna'][vt]<0)),
                ("Vanna OPPOSES", lambda t, vt=vtype: (t['direction']=='long' and t['vanna'][vt]<0) or (t['direction']=='short' and t['vanna'][vt]>0)),
            ]:
                subset = [t for t in has_both if shift_fn(t) and vanna_fn(t)]
                if subset:
                    ps(stats(subset, f"  {shift_lab} + {vanna_lab}"))

# ─── Test with ALL vanna type specifically ──────────────────────────────────
print(f"\n{'='*100}")
print("VANNA ALL: DIRECTION SUPPORT DEEP DIVE")
print(f"{'='*100}")

all_vanna_trades = [t for t in enriched if 'ALL' in t.get('vanna', {})]
print(f"Trades with Vanna ALL: {len(all_vanna_trades)}")

# Magnitude of vanna support
for t in all_vanna_trades:
    va = t['vanna']['ALL']
    if t['direction'] == 'long':
        t['vanna_support_strength'] = va  # positive = supports long
    else:
        t['vanna_support_strength'] = -va  # negative vanna = supports short, so negate

# Bucket by support strength
print(f"\n  Vanna support strength (positive = supports trade direction):")
for lo, hi, lab in [
    (float('-inf'), -500, "Strong OPPOSE (<-500)"),
    (-500, -100, "Moderate OPPOSE (-500 to -100)"),
    (-100, 0, "Weak OPPOSE (-100 to 0)"),
    (0, 100, "Weak SUPPORT (0 to 100)"),
    (100, 500, "Moderate SUPPORT (100 to 500)"),
    (500, float('inf'), "Strong SUPPORT (>500)"),
]:
    bucket = [t for t in all_vanna_trades if lo <= t.get('vanna_support_strength', 0) < hi]
    if bucket:
        ps(stats(bucket, f"  {lab}"))

# ─── Now each vanna sub-type ───────────────────────────────────────────────
print(f"\n{'='*100}")
print("VANNA BY SUB-TYPE: Which vanna type matters most?")
print(f"{'='*100}")

for vtype in sorted(all_vtypes):
    vt_trades = [t for t in enriched if vtype in t.get('vanna', {})]
    if len(vt_trades) < 10: continue

    # Compute support strength for this type
    for t in vt_trades:
        va = t['vanna'][vtype]
        if t['direction'] == 'long':
            t[f'vs_{vtype}'] = va
        else:
            t[f'vs_{vtype}'] = -va

    supports = [t for t in vt_trades if t[f'vs_{vtype}'] > 0]
    opposes = [t for t in vt_trades if t[f'vs_{vtype}'] <= 0]

    s_sup = stats(supports, f"{vtype} SUPPORTS trade")
    s_opp = stats(opposes, f"{vtype} OPPOSES trade")

    if s_sup and s_opp:
        wr_diff = s_sup['wr'] - s_opp['wr']
        pnl_diff = s_sup['pnl'] - s_opp['pnl']
        print(f"\n  --- {vtype} ---")
        ps(s_sup)
        ps(s_opp)
        print(f"    WR gap: {wr_diff:+.1f}% | PnL gap: {pnl_diff:+.1f}")

# ─── Per-trade detail with vanna support ────────────────────────────────────
print(f"\n{'='*100}")
print("PER-TRADE DETAIL: DD shift + Vanna ALL support")
print(f"{'='*100}")

# Check which vanna types are most commonly available
common_vtypes = []
for vtype in sorted(all_vtypes):
    n = sum(1 for t in enriched if vtype in t.get('vanna', {}))
    if n >= 50:
        common_vtypes.append(vtype)
        print(f"  {vtype}: {n} trades have data")

vtypes_header = " | ".join(f"V_{vt[:4]:>5s}" for vt in common_vtypes[:5])
print(f"\n  {'ID':>4} | {'Date':10} | {'Time':5} | {'Dir':5} | {'Res':3} | {'PnL':>7} | {'MP':>4} | {'DDshft':>8} | {vtypes_header} | {'Paradigm':15}")
print(f"  {'-'*160}")

for t in enriched:
    ds = f"{t['dd_shift']:+.0f}" if t.get('dd_shift') is not None else "?"
    vanna_cols = []
    for vt in common_vtypes[:5]:
        if vt in t.get('vanna', {}):
            val = t['vanna'][vt]
            # Mark if supports or opposes direction
            if t['direction'] == 'long':
                marker = '+' if val > 0 else '-'
            else:
                marker = '+' if val < 0 else '-'
            vanna_cols.append(f"{marker}{abs(val):>5.0f}")
        else:
            vanna_cols.append(f"{'?':>6}")
    vc_str = " | ".join(vanna_cols)
    para = str(t.get('paradigm') or '?')[:15]
    print(f"  {t['id']:>4} | {t['date_str']:10} | {t['time_str']:5} | {t['direction']:5} | {t['cat']:3} | {t['pnl']:+7.1f} | {t['max_profit']:+4.0f} | {ds:>8} | {vc_str} | {para:15}")

conn.close()
print(f"\n{'='*100}")
print("DONE")
