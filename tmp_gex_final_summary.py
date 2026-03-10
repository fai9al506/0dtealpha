"""
GEX Long Force Alignment - Final Summary
Research only - does NOT modify any code or database
"""
import sqlalchemy as sa, pandas as pd, numpy as np

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

df = pd.read_sql("""
    SELECT id, ts, spot, lis, target, max_plus_gex, max_minus_gex, paradigm,
           outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
           greek_alignment
    FROM setup_log WHERE setup_name = 'GEX Long' ORDER BY ts
""", engine)

df['lis_dist'] = df['lis'] - df['spot']
df['neg_gex_dist'] = df['max_minus_gex'] - df['spot']
df['pos_gex_dist'] = df['max_plus_gex'] - df['spot']
df['target_dist'] = df['target'] - df['spot']
df['is_win'] = (df['outcome_result'] == 'WIN').astype(int)


def quality_score(row):
    score = 0
    ld = abs(row['lis_dist'])
    td = row['target_dist']
    pgd = row['pos_gex_dist']
    if ld <= 3:
        score += 3
    elif ld <= 5:
        score += 2
    elif ld <= 10:
        score += 1
    if 10 <= td < 25:
        score += 2
    elif 25 <= td < 40:
        score += 1
    if 15 <= pgd < 40:
        score += 2
    elif 10 <= pgd < 50:
        score += 1
    if row['greek_alignment'] >= 1:
        score += 2
    if row['greek_alignment'] >= 3:
        score += 1
    return score


df['quality'] = df.apply(quality_score, axis=1)

print("=" * 80)
print("COMPREHENSIVE FINAL RECOMMENDATION")
print("=" * 80)

# Best filter combos
filters = {
    '#1 align>=1 + tgt<50 + +gex<50': (
        (df['greek_alignment'] >= 1) & (df['target_dist'] < 50) & (df['pos_gex_dist'] < 50)
    ),
    '#2 align>=1 + tgt<30': (
        (df['greek_alignment'] >= 1) & (df['target_dist'] < 30)
    ),
    '#3 quality>=6': df['quality'] >= 6,
    '#4 align>=1 + lis<=5': (
        (df['greek_alignment'] >= 1) & (df['lis_dist'].abs() <= 5)
    ),
    '#5 align>=1 + lis<=5 + -gex>-10': (
        (df['greek_alignment'] >= 1) & (df['lis_dist'].abs() <= 5) & (df['neg_gex_dist'] >= -10)
    ),
    '#6 align>=1 (baseline)': df['greek_alignment'] >= 1,
    '#7 UNFILTERED': pd.Series(True, index=df.index),
}

print(f"\n{'Filter':<42s} {'N':>3s} {'W':>3s} {'L':>3s} {'WR%':>6s} {'PnL':>8s} {'PF':>6s} {'Avg':>6s}")
print("-" * 85)
for name, mask in filters.items():
    sub = df[mask]
    n = len(sub)
    w = sub['is_win'].sum()
    l = n - w
    wr = w / n * 100
    pnl = sub['outcome_pnl'].sum()
    gw = sub.loc[sub['outcome_pnl'] > 0, 'outcome_pnl'].sum()
    gl = abs(sub.loc[sub['outcome_pnl'] < 0, 'outcome_pnl'].sum())
    pf = gw / gl if gl > 0 else float('inf')
    avg = sub['outcome_pnl'].mean()
    print(f"  {name:<40s} {n:3d} {w:3d} {l:3d} {wr:5.1f}% {pnl:+7.1f} {pf:5.2f} {avg:+5.1f}")

# Paradigm breakdown
print("\n--- Paradigm Sub-Type Performance ---")
for p in sorted(df['paradigm'].unique()):
    sub = df[df['paradigm'] == p]
    n = len(sub)
    wr = sub['is_win'].mean() * 100
    pnl = sub['outcome_pnl'].sum()
    # with align>=1
    sub_a = sub[sub['greek_alignment'] >= 1]
    na = len(sub_a)
    wra = sub_a['is_win'].mean() * 100 if na > 0 else 0
    pnla = sub_a['outcome_pnl'].sum() if na > 0 else 0
    print(f"  {p:10s}: N={n:2d}, WR={wr:5.1f}%, PnL={pnl:+6.1f} | with align>=1: N={na:2d}, WR={wra:5.1f}%, PnL={pnla:+6.1f}")

# -GEX below spot: detailed breakdown
print("\n--- -GEX Below Spot: Does It Create Downside Risk? ---")
below = df[df['neg_gex_dist'] < 0].copy()
below['ngd_abs'] = -below['neg_gex_dist']

for lo, hi in [(0, 10), (10, 20), (20, 40), (40, 80)]:
    sub = below[(below['ngd_abs'] >= lo) & (below['ngd_abs'] < hi)]
    n = len(sub)
    if n == 0:
        continue
    wr = sub['is_win'].mean() * 100
    pnl = sub['outcome_pnl'].sum()
    avg_mxp = sub['outcome_max_profit'].mean()
    avg_mxl = sub['outcome_max_loss'].mean()
    # with align
    sa = sub[sub['greek_alignment'] >= 1]
    na = len(sa)
    wra = sa['is_win'].mean() * 100 if na > 0 else 0
    pnla = sa['outcome_pnl'].sum() if na > 0 else 0
    print(f"  [{lo:3d}-{hi:3d}) below: N={n:3d}, WR={wr:5.1f}%, PnL={pnl:+6.1f}, "
          f"AvgMFE={avg_mxp:+5.1f}, AvgMAE={avg_mxl:+5.1f} | "
          f"w/align: N={na:2d} WR={wra:5.1f}% PnL={pnla:+6.1f}")

above = df[df['neg_gex_dist'] >= 0]
print(f"  ABOVE spot: N={len(above):3d}, WR={above['is_win'].mean()*100:5.1f}%, PnL={above['outcome_pnl'].sum():+6.1f}")

# Overlap and trade lists for best combo
print("\n--- Trades in BEST COMBO (#1: align>=1 + tgt<50 + +gex<50) ---")
best_mask = (df['greek_alignment'] >= 1) & (df['target_dist'] < 50) & (df['pos_gex_dist'] < 50)
best = df[best_mask].sort_values('ts')
for _, r in best.iterrows():
    ts_str = str(r['ts'])[:16]
    print(f"  #{r['id']:3.0f} {ts_str} Q={r['quality']} {r['paradigm']:10s} "
          f"spot={r['spot']:7.1f} LIS={r['lis']:7.0f}({r['lis_dist']:+5.1f}) "
          f"T={r['target']:7.0f}({r['target_dist']:+5.1f}) "
          f"+GEX={r['max_plus_gex']:7.0f}({r['pos_gex_dist']:+5.1f}) "
          f"-GEX={r['max_minus_gex']:7.0f}({r['neg_gex_dist']:+5.1f}) "
          f"| {r['outcome_result']:7s} {r['outcome_pnl']:+5.1f}")

# Trades BLOCKED by #1 filter
print(f"\n--- Trades BLOCKED by #1 filter ---")
blocked = df[~best_mask].sort_values('ts')
print(f"  (total blocked: {len(blocked)}, all {blocked['outcome_result'].value_counts().to_dict()})")

# KEY INSIGHTS
print("\n" + "=" * 80)
print("KEY INSIGHTS")
print("=" * 80)

print("""
1. GREEK ALIGNMENT is the DOMINANT filter for GEX Long:
   - align=-1: 0% WR (13 trades, ALL losses = -104 pts)
   - align>=1: 41% WR (29 trades, near breakeven = +0.5 pts)
   Without align>=1 gate, GEX Long is a net loser.

2. TARGET DISTANCE is the second-most powerful filter:
   - target<30pt: 58% WR with align>=1 (12 trades, +39.6 pts)
   - target>=50pt: almost always loses
   Closer target = higher probability of reaching it.

3. +GEX DISTANCE acts as a quality gate:
   - +GEX<50pt above spot: 50% WR with align (16 trades, +37.7)
   - +GEX>=50pt above: too far to act as effective magnet

4. LIS PROXIMITY is a strong confirming factor:
   - |LIS dist|<=5: 50% WR with align (16 trades, +33.3)
   - LIS within 3pt: even better granularity

5. -GEX POSITION (PART 4 FINDING - COUNTERINTUITIVE):
   - -GEX 10-40pt below spot = DANGER ZONE (15-18% WR, worst bucket)
   - -GEX 0-10pt below = OK (43% WR, close enough to act as floor)
   - -GEX 40-80pt below = ALSO OK with align (60% WR, 10 trades +39.7)
     The theory: when -GEX is FAR below, it means the negative gamma
     zone is well-cleared, and with alignment support, price bounces
     off higher-level structures. The DANGER is the MIDDLE zone where
     -GEX is close enough to pull price down but not close enough
     to have already been passed.

6. FORCE SCORE (original binary) DOES NOT WORK:
   - f3 (+GEX above 10pt) and f4 (target above 10pt) always true
   - f5 (sandwich) and f6 (double magnet) almost never true
   - Score clusters at 2-3, no discrimination
   - CONTINUOUS quality score works better (Q>=6: 53% WR, +56 pts)

7. FORWARD-SIMULATED (442 moments, ES 1-min bars, 60min):
   - force>=5 perfect (3/3=100%) but only 3 samples
   - Most filter combos are NET NEGATIVE in forward sim
   - This is because GEX paradigm moments fire CONTINUOUSLY while
     paradigm persists, but real trades fire only on TRANSITIONS
   - Forward sim represents "what if you entered at every 2-min
     snapshot during GEX paradigm", not "first signal only"
   - Real trades have cooldown preventing re-entry, which is key

8. PARADIGM SUB-TYPE (with alignment>=1):
   - GEX-LIS: N=7, 57% WR, +15.1 (best sub-type)
   - GEX-MESSY: N=3, 33% WR, -9.5 (sample too small)
   - GEX-PURE: N=14, 43% WR, +3.8 (biggest bucket, marginal)
   - GEX-TARGET: N=5, 20% WR, -9.8 (worst with alignment)
""")

print("=" * 80)
print("RECOMMENDED ENTRY CRITERIA FOR GEX LONG")
print("=" * 80)
print("""
+-------------------------------------------------------------+
|              RECOMMENDED GEX LONG FILTER                     |
+-------------------------------------------------------------+
| GATE 1 (MANDATORY): greek_alignment >= 1                    |
|   Removes: 13 trades, saves 104 pts, blocks 0% WR bucket   |
|                                                             |
| GATE 2: target_distance < 50 pts                            |
|   Removes: 9 more, filters unreachable targets              |
|                                                             |
| GATE 3: +GEX_distance < 50 pts                              |
|   Removes: 8 more, +GEX too far = no effective magnet       |
|                                                             |
| COMBINED: N=12, WR=66.7%, PnL=+55.7, PF=2.74               |
| vs UNFILTERED: N=42, WR=28.6%, PnL=-103.5, PF=0.54         |
| IMPROVEMENT: +159.2 pts swing, +38% WR, PF 5x better       |
+-------------------------------------------------------------+

ALTERNATIVE (simpler, nearly as good):
  align>=1 + target<30: N=12, WR=58.3%, PnL=+39.6, PF=2.22

IMPORTANT CAVEATS:
  - Only 42 total trades, 12 in best combo (small sample)
  - Needs 30+ more trades to confirm statistical significance
  - Forward sim on all GEX moments shows most combos are negative,
    suggesting the setup detector cooldown/gate logic is important
  - Keep logging all GEX Long trades (even blocked ones) for data
""")
