"""Deep study: SC longs by paradigm × VIX level. Is AG-PURE + high-VIX a real filter?

Apply full V12-fix baseline filter, then decompose SC longs by:
- Paradigm (all possible values)
- VIX bucket (<18, 18-22, 22-25, 25-30, 30+)
- Grade
- Time of day
- Alignment
- Month stability
- OOS test (train vs test split)
"""
import psycopg2
from collections import defaultdict
import statistics

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# All SC long trades with full context
cur.execute("""
SELECT id, ts, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
       (ts AT TIME ZONE 'America/New_York')::date as d,
       outcome_max_profit, outcome_max_loss
FROM setup_log
WHERE setup_name = 'Skew Charm' AND direction IN ('long', 'bullish')
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""")
all_sc_longs = cur.fetchall()
print(f"Total SC long trades (all history): {len(all_sc_longs)}")
print(f"Date range: {all_sc_longs[0][12]} to {all_sc_longs[-1][12]}")

# Apply V12-fix (now includes SIDIAL-EXTREME block + grade block + align>=2)
def passes_v12fix_sc_long(t):
    tid, ts, grade, paradigm, spot, outcome, pnl, align, vix, ovx, h, m, d, mfe, mae = t
    if grade in ('C', 'LOG'): return False
    if paradigm == 'SIDIAL-EXTREME': return False
    if align is None or align < 2: return False
    # SC exempt from VIX gate
    return True

v12 = [t for t in all_sc_longs if passes_v12fix_sc_long(t)]
print(f"V12-fix SC longs: {len(v12)}, total PnL: {sum(float(t[6] or 0) for t in v12):+.1f}")

# Overall performance
def stats(trades, label=None):
    pnl = sum(float(t[6] or 0) for t in trades)
    w = sum(1 for t in trades if t[5]=='WIN')
    l = sum(1 for t in trades if t[5]=='LOSS')
    wr = 100*w/max(1,w+l)
    return {'n': len(trades), 'pnl': pnl, 'wr': wr, 'w': w, 'l': l,
            'avg': pnl/max(1,len(trades))}

print()
print(f"V12-fix SC long baseline: n={len(v12)}, pnl={sum(float(t[6] or 0) for t in v12):+.1f}")

# ============ By paradigm ============
print()
print("=== SC LONG V12-FIX: By Paradigm ===")
by_par = defaultdict(list)
for t in v12:
    par = t[3] or 'NONE'
    by_par[par].append(t)
print(f"{'Paradigm':<16}{'N':>4}{'W':>4}{'L':>4}{'WR':>7}{'PnL':>8}{'Avg':>7}")
for par, tr in sorted(by_par.items(), key=lambda x: -stats(x[1])['pnl']):
    s = stats(tr)
    print(f"{par:<16}{s['n']:>4}{s['w']:>4}{s['l']:>4}{s['wr']:>6.1f}%{s['pnl']:>+8.1f}{s['avg']:>+7.2f}")

# ============ By VIX bucket ============
print()
print("=== SC LONG V12-FIX: By VIX level ===")
vix_buckets = [(0, 16, '<16'), (16, 18, '16-18'), (18, 20, '18-20'), (20, 22, '20-22'),
               (22, 25, '22-25'), (25, 30, '25-30'), (30, 100, '30+')]
print(f"{'VIX range':<10}{'N':>4}{'W':>4}{'L':>4}{'WR':>7}{'PnL':>8}{'Avg':>7}")
for lo, hi, label in vix_buckets:
    tr = [t for t in v12 if t[8] is not None and lo <= float(t[8]) < hi]
    if tr:
        s = stats(tr)
        print(f"{label:<10}{s['n']:>4}{s['w']:>4}{s['l']:>4}{s['wr']:>6.1f}%{s['pnl']:>+8.1f}{s['avg']:>+7.2f}")

# ============ Paradigm × VIX cross-tab ============
print()
print("=== SC LONG V12-FIX: Paradigm × VIX cross-tab ===")
paradigms_significant = [p for p, tr in by_par.items() if len(tr) >= 8]
print(f"{'VIX×Par':<10}", end='')
for p in paradigms_significant:
    print(f"{p[:9]:>10}", end='')
print()
for lo, hi, label in vix_buckets:
    print(f"{label:<10}", end='')
    for p in paradigms_significant:
        tr = [t for t in v12 if t[8] is not None and lo <= float(t[8]) < hi and (t[3] or 'NONE') == p]
        if tr:
            s = stats(tr)
            cell = f"{s['n']}/{s['pnl']:+.0f}"
        else:
            cell = '—'
        print(f"{cell:>10}", end='')
    print()

# ============ AG-PURE isolated ============
print()
print("=== AG-PURE SC LONG ANALYSIS (the candidate filter) ===")
ag_pure = [t for t in v12 if t[3] == 'AG-PURE']
print(f"AG-PURE SC long total: {stats(ag_pure)}")

for lo, hi, label in vix_buckets:
    tr = [t for t in ag_pure if t[8] is not None and lo <= float(t[8]) < hi]
    if tr:
        s = stats(tr)
        print(f"  AG-PURE VIX {label:<10}: n={s['n']:>3} W={s['w']:>3} L={s['l']:>3} WR={s['wr']:>5.1f}% pnl={s['pnl']:+7.1f}")

# Specific threshold: VIX > 25
print()
agp_hi = [t for t in ag_pure if t[8] is not None and float(t[8]) > 25]
agp_lo = [t for t in ag_pure if t[8] is not None and float(t[8]) <= 25]
print(f"AG-PURE + VIX > 25: {stats(agp_hi)}")
print(f"AG-PURE + VIX <= 25: {stats(agp_lo)}")

# Monthly stability
print()
print("=== AG-PURE + VIX > 25: Monthly stability ===")
by_month = defaultdict(list)
for t in agp_hi:
    k = f"{t[12].year}-{t[12].month:02d}"
    by_month[k].append(t)
for m in sorted(by_month.keys()):
    s = stats(by_month[m])
    print(f"  {m}: n={s['n']}, W={s['w']}, L={s['l']}, pnl={s['pnl']:+.1f}, wr={s['wr']:.1f}%")

# ============ If we add filter, what's the impact? ============
print()
print("=== PROPOSED V14 RULE: Block SC long when AG-PURE AND VIX > 25 ===")
kept = [t for t in v12 if not (t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 25)]
blocked = [t for t in v12 if t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 25]
s_before = stats(v12)
s_after = stats(kept)
s_block = stats(blocked)
print(f"Baseline V12-fix SC longs: n={s_before['n']}, pnl={s_before['pnl']:+.1f}")
print(f"With new rule:             n={s_after['n']}, pnl={s_after['pnl']:+.1f}")
print(f"Blocked (saved):           n={s_block['n']}, pnl={s_block['pnl']:+.1f}")
print(f"Improvement:               {s_after['pnl']-s_before['pnl']:+.1f} pts ({100*(s_after['pnl']-s_before['pnl'])/abs(s_before['pnl']):.1f}%)")

# ============ OOS TEST: split by date, train = first half, test = second half ============
print()
print("=== OOS STABILITY TEST ===")
dates = sorted(set(t[12] for t in v12))
mid = dates[len(dates) // 2]
train = [t for t in v12 if t[12] <= mid]
test = [t for t in v12 if t[12] > mid]
print(f"Train dates: {dates[0]} to {mid} ({len(train)} trades)")
print(f"Test dates: {mid} to {dates[-1]} ({len(test)} trades)")

# Build same rule for each half
train_blocked = [t for t in train if t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 25]
test_blocked = [t for t in test if t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 25]
print(f"\nTRAIN half: AG-PURE+VIX>25 blocks {len(train_blocked)}t, pnl {stats(train_blocked)['pnl']:+.1f}")
print(f"TEST half:  AG-PURE+VIX>25 blocks {len(test_blocked)}t, pnl {stats(test_blocked)['pnl']:+.1f}")

# Impact per half
train_after = [t for t in train if not (t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 25)]
test_after = [t for t in test if not (t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 25)]
print(f"Train PnL: {stats(train)['pnl']:+.1f} -> {stats(train_after)['pnl']:+.1f} (delta {stats(train_after)['pnl']-stats(train)['pnl']:+.1f})")
print(f"Test PnL:  {stats(test)['pnl']:+.1f} -> {stats(test_after)['pnl']:+.1f} (delta {stats(test_after)['pnl']-stats(test)['pnl']:+.1f})")

# ============ What about AG-PURE + ANY VIX, or AG-PURE alone? ============
print()
print("=== COMPARE THRESHOLDS ===")
variants = [
    ('AG-PURE alone (no VIX threshold)', lambda t: t[3] == 'AG-PURE'),
    ('AG-PURE + VIX > 20', lambda t: t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 20),
    ('AG-PURE + VIX > 22', lambda t: t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 22),
    ('AG-PURE + VIX > 25', lambda t: t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 25),
    ('AG-PURE + VIX > 27', lambda t: t[3] == 'AG-PURE' and t[8] is not None and float(t[8]) > 27),
]
for name, fn in variants:
    blocked = [t for t in v12 if fn(t)]
    s = stats(blocked)
    improvement = -s['pnl']
    print(f"  {name:<40}: blocks {s['n']:>3}t, WR={s['wr']:>5.1f}%, pnl={s['pnl']:+7.1f}, saves {improvement:+7.1f}")

# Similar check for other high-VIX paradigms
print()
print("=== Other paradigms + VIX > 25 (comparable check) ===")
other_checks = ['BofA-LIS', 'GEX-LIS', 'AG-LIS', 'GEX-PURE', 'BOFA-PURE', 'AG-TARGET', 'GEX-TARGET', 'BOFA-MESSY']
for p in other_checks:
    hi = [t for t in v12 if t[3] == p and t[8] is not None and float(t[8]) > 25]
    if hi:
        s = stats(hi)
        print(f"  {p}+VIX>25: n={s['n']}, WR={s['wr']:.1f}%, pnl={s['pnl']:+.1f}")

# AG-PURE + alignment
print()
print("=== AG-PURE by alignment ===")
for a in [1, 2, 3]:
    tr = [t for t in ag_pure if t[7] == a]
    if tr:
        s = stats(tr)
        print(f"  AG-PURE align={a}: n={s['n']}, pnl={s['pnl']:+.1f}, WR={s['wr']:.1f}%")

# ============ Deeper: AG-PURE by time of day ============
print()
print("=== AG-PURE + VIX > 25 by time of day ===")
time_buckets = [(9, 10, '09:30-10:00'), (10, 11, '10:00-11:00'), (11, 12, '11:00-12:00'),
                (12, 14, '12:00-14:00'), (14, 16, '14:00-16:00')]
for lo_h, hi_h, label in time_buckets:
    tr = [t for t in agp_hi if lo_h <= t[10] < hi_h]
    if tr:
        s = stats(tr)
        print(f"  {label}: n={s['n']}, pnl={s['pnl']:+.1f}, WR={s['wr']:.1f}%")

# Save summary
import json
with open('_sc_long_agpure.json', 'w') as f:
    json.dump({
        'all_sc_longs_n': len(all_sc_longs),
        'v12_sc_longs_n': len(v12),
        'v12_pnl': stats(v12)['pnl'],
        'by_paradigm': {p: stats(tr) for p, tr in by_par.items()},
        'ag_pure_all': stats(ag_pure),
        'ag_pure_hi_vix': stats(agp_hi),
        'ag_pure_lo_vix': stats(agp_lo),
        'proposed_rule_blocks': {'n': s_block['n'], 'pnl': s_block['pnl'], 'wr': s_block['wr']},
        'after_rule': {'n': s_after['n'], 'pnl': s_after['pnl']},
        'train_blocked': {'n': len(train_blocked), 'pnl': stats(train_blocked)['pnl']},
        'test_blocked': {'n': len(test_blocked), 'pnl': stats(test_blocked)['pnl']},
        'ag_pure_monthly': {m: stats(tr) for m, tr in by_month.items()},
    }, f, indent=2, default=str)
print("\nSaved to _sc_long_agpure.json")
