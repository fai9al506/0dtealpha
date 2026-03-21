"""
Deep Analysis: What makes Skew Charm trades WIN vs LOSE?
Correlate with Volland data (charm, vanna, gamma, per-strike exposure)
"""
import os, json, statistics, re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "")
if not DB_URL:
    DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
if "postgresql://" in DB_URL and "postgresql+psycopg" not in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(DB_URL)

# ---- Pull ALL Skew Charm trades with outcomes ----
with engine.begin() as conn:
    trades = [dict(r) for r in conn.execute(text("""
        SELECT s.id, s.ts as ts_utc,
               s.ts AT TIME ZONE 'America/New_York' as ts_et,
               s.direction, s.grade, s.score, s.spot, s.target,
               s.outcome_result, s.outcome_pnl,
               s.outcome_max_profit, s.outcome_max_loss,
               s.outcome_first_event, s.outcome_elapsed_min,
               s.outcome_target_level, s.outcome_stop_level,
               s.greek_alignment, s.vix, s.overvix,
               s.support_score, s.upside_score, s.floor_cluster_score,
               s.target_cluster_score, s.rr_score,
               s.lis, s.paradigm, s.gap_to_lis,
               s.max_plus_gex, s.max_minus_gex,
               s.vanna_all, s.vanna_weekly, s.vanna_monthly,
               s.spot_vol_beta
        FROM setup_log s
        WHERE s.setup_name = 'Skew Charm'
          AND s.outcome_result IS NOT NULL
        ORDER BY s.ts ASC
    """)).mappings().all()]

print(f"Total SC trades: {len(trades)}")

def parse_dollar_str(s):
    """Parse dollar strings like '$7,298,110,681' or '-$200,000,000' to float"""
    if not s: return None
    s = str(s).strip()
    neg = '-' in s
    s = re.sub(r'[^0-9.]', '', s)
    try:
        v = float(s)
        return -v if neg else v
    except:
        return None

def parse_lis_range(s):
    """Parse LIS like '$5,700 - $5,728' to midpoint"""
    if not s: return None
    s = str(s)
    parts = re.findall(r'[\d,]+', s)
    if len(parts) >= 2:
        try:
            lo = float(parts[0].replace(',', ''))
            hi = float(parts[1].replace(',', ''))
            return (lo + hi) / 2
        except:
            return None
    elif len(parts) == 1:
        try: return float(parts[0].replace(',', ''))
        except: return None
    return None

# ---- For each trade, fetch closest volland snapshot + exposure points ----
print("\nFetching Volland data for each trade...")
with engine.begin() as conn:
    for t in trades:
        ts = t['ts_utc']  # Use raw UTC for DB comparisons

        # Volland snapshot (JSONB payload)
        vol = conn.execute(text("""
            SELECT ts as vol_ts,
                   payload
            FROM volland_snapshots
            WHERE ts BETWEEN :start AND :end
              AND (payload->>'exposure_points_saved')::int > 0
            ORDER BY ts DESC
            LIMIT 1
        """), {"start": ts - timedelta(minutes=5), "end": ts + timedelta(minutes=1)}).mappings().first()

        if vol and vol['payload']:
            p = vol['payload']
            stats = p.get('statistics', {})
            t['vol_paradigm'] = stats.get('paradigm')
            t['vol_lis'] = parse_lis_range(stats.get('lines_in_sand'))
            t['vol_agg_charm'] = stats.get('aggregatedCharm')
            t['vol_dd_hedging'] = stats.get('delta_decay_hedging')
            t['vol_dd_numeric'] = parse_dollar_str(stats.get('delta_decay_hedging'))
            svb = stats.get('spot_vol_beta', {})
            t['vol_svb_corr'] = svb.get('correlation') if isinstance(svb, dict) else None
            t['vol_target'] = stats.get('target')
            t['has_vol'] = True
        else:
            t['vol_paradigm'] = None
            t['vol_lis'] = None
            t['vol_agg_charm'] = None
            t['vol_dd_hedging'] = None
            t['vol_dd_numeric'] = None
            t['vol_svb_corr'] = None
            t['vol_target'] = None
            t['has_vol'] = False

        # Per-strike exposure points (columns: greek, strike, value)
        exp_pts = conn.execute(text("""
            SELECT greek, strike, value
            FROM volland_exposure_points
            WHERE ts_utc BETWEEN :start AND :end
              AND greek IN ('charm', 'vannaOI', 'vannaVol', 'gamma', 'gammaColor', 'deltaDecay')
            ORDER BY ts_utc DESC, greek, strike
            LIMIT 2000
        """), {"start": ts - timedelta(minutes=5), "end": ts + timedelta(minutes=1)}).mappings().all()

        exp_by_type = {}
        for ep in exp_pts:
            etype = ep['greek']
            exp_by_type.setdefault(etype, []).append({'strike': ep['strike'], 'points': ep['value']})
        t['exposure_points'] = exp_by_type

wins = [t for t in trades if t['outcome_result'] == 'WIN']
losses = [t for t in trades if t['outcome_result'] == 'LOSS']
expired = [t for t in trades if t['outcome_result'] == 'EXPIRED']

print(f"Enriched {len(trades)} trades")
print(f"  With volland snapshot: {sum(1 for t in trades if t['has_vol'])}")
print(f"  With exposure points: {sum(1 for t in trades if t['exposure_points'])}")

def safe_float(v):
    if v is None: return None
    try: return float(v)
    except: return None

def compare_groups(field_name, win_vals, loss_vals):
    w = [v for v in win_vals if v is not None]
    l = [v for v in loss_vals if v is not None]
    if not w and not l:
        return
    if len(w) < 2 and len(l) < 2:
        return
    w_avg = statistics.mean(w) if w else 0
    l_avg = statistics.mean(l) if l else 0
    w_med = statistics.median(w) if w else 0
    l_med = statistics.median(l) if l else 0
    diff = w_avg - l_avg
    sig = " ***" if abs(diff) > 0.5 * max(abs(w_avg), abs(l_avg), 0.01) else ""
    print(f"\n  {field_name}:{sig}")
    print(f"    WIN  (n={len(w):>3}): avg={w_avg:>10.2f}  med={w_med:>10.2f}")
    print(f"    LOSS (n={len(l):>3}): avg={l_avg:>10.2f}  med={l_med:>10.2f}")
    print(f"    DIFF: {diff:>+10.2f}")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 1: Setup Log Fields -- Wins vs Losses")
print("=" * 70)

compare_groups("Greek Alignment",
    [t['greek_alignment'] for t in wins],
    [t['greek_alignment'] for t in losses])

compare_groups("VIX",
    [t['vix'] for t in wins],
    [t['vix'] for t in losses])

compare_groups("Overvix (VIX - VIX3M)",
    [t['overvix'] for t in wins],
    [t['overvix'] for t in losses])

compare_groups("Score (total)",
    [t['score'] for t in wins],
    [t['score'] for t in losses])

compare_groups("Gap to LIS",
    [t['gap_to_lis'] for t in wins],
    [t['gap_to_lis'] for t in losses])

compare_groups("Spot Vol Beta",
    [t['spot_vol_beta'] for t in wins],
    [t['spot_vol_beta'] for t in losses])

compare_groups("Vanna All",
    [t['vanna_all'] for t in wins],
    [t['vanna_all'] for t in losses])

compare_groups("Support Score (Skew magnitude, 0-30)",
    [t['support_score'] for t in wins],
    [t['support_score'] for t in losses])

compare_groups("Upside Score (Charm strength, 0-25)",
    [t['upside_score'] for t in wins],
    [t['upside_score'] for t in losses])

compare_groups("Floor Cluster Score (Time-of-day, 0-15)",
    [t['floor_cluster_score'] for t in wins],
    [t['floor_cluster_score'] for t in losses])

compare_groups("Target Cluster Score (Paradigm, 0-15)",
    [t['target_cluster_score'] for t in wins],
    [t['target_cluster_score'] for t in losses])

compare_groups("RR Score (Skew level, 0-15)",
    [t['rr_score'] for t in wins],
    [t['rr_score'] for t in losses])

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 2: Volland Snapshot Data -- Wins vs Losses")
print("=" * 70)

compare_groups("DD Hedging Numeric ($)",
    [t['vol_dd_numeric'] for t in wins],
    [t['vol_dd_numeric'] for t in losses])

compare_groups("Aggregated Charm",
    [safe_float(t['vol_agg_charm']) for t in wins],
    [safe_float(t['vol_agg_charm']) for t in losses])

compare_groups("SVB Correlation",
    [safe_float(t['vol_svb_corr']) for t in wins],
    [safe_float(t['vol_svb_corr']) for t in losses])

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 3: Paradigm Context")
print("=" * 70)

paradigm_stats = {}
for t in trades:
    p = t.get('paradigm') or 'unknown'
    if p not in paradigm_stats:
        paradigm_stats[p] = {'total': 0, 'wins': 0, 'pnl': 0}
    paradigm_stats[p]['total'] += 1
    if t['outcome_result'] == 'WIN':
        paradigm_stats[p]['wins'] += 1
    paradigm_stats[p]['pnl'] += (t['outcome_pnl'] or 0)

print(f"\n  {'Paradigm':<25} | {'Total':>5} | {'Wins':>5} | {'WR%':>6} | {'PnL':>8}")
print(f"  {'-'*25}-+-{'-'*5}-+-{'-'*5}-+-{'-'*6}-+-{'-'*8}")
for p, s in sorted(paradigm_stats.items(), key=lambda x: -x[1]['pnl']):
    wr = 100 * s['wins'] / s['total'] if s['total'] else 0
    print(f"  {p:<25} | {s['total']:>5} | {s['wins']:>5} | {wr:>5.1f}% | {s['pnl']:>+8.1f}")

# Also check volland paradigm (real-time, may differ from setup_log)
print("\n  Volland real-time paradigm:")
vol_paradigm_stats = {}
for t in trades:
    p = t.get('vol_paradigm') or 'no_data'
    if p not in vol_paradigm_stats:
        vol_paradigm_stats[p] = {'total': 0, 'wins': 0, 'pnl': 0}
    vol_paradigm_stats[p]['total'] += 1
    if t['outcome_result'] == 'WIN':
        vol_paradigm_stats[p]['wins'] += 1
    vol_paradigm_stats[p]['pnl'] += (t['outcome_pnl'] or 0)

print(f"\n  {'Vol Paradigm':<25} | {'Total':>5} | {'Wins':>5} | {'WR%':>6} | {'PnL':>8}")
print(f"  {'-'*25}-+-{'-'*5}-+-{'-'*5}-+-{'-'*6}-+-{'-'*8}")
for p, s in sorted(vol_paradigm_stats.items(), key=lambda x: -x[1]['pnl']):
    wr = 100 * s['wins'] / s['total'] if s['total'] else 0
    print(f"  {p:<25} | {s['total']:>5} | {s['wins']:>5} | {wr:>5.1f}% | {s['pnl']:>+8.1f}")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 4: Per-Strike Exposure Near Spot")
print("=" * 70)

def analyze_exposure(trades_subset, label, etype='charm'):
    net_near_vals = []
    pos_near_vals = []
    neg_near_vals = []
    total_abs_vals = []
    for t in trades_subset:
        spot = t['spot']
        if not spot or etype not in t.get('exposure_points', {}):
            continue
        points = t['exposure_points'][etype]
        if not points:
            continue
        near = [p for p in points if abs(p['strike'] - spot) <= 15]
        net = sum(p['points'] for p in near)
        pos = sum(p['points'] for p in near if p['points'] > 0)
        neg = sum(p['points'] for p in near if p['points'] < 0)
        total_abs = sum(abs(p['points']) for p in points)
        net_near_vals.append(net)
        pos_near_vals.append(pos)
        neg_near_vals.append(neg)
        total_abs_vals.append(total_abs)
    if net_near_vals:
        print(f"\n  {label} - {etype} (within 15pts of spot):")
        print(f"    Net:  avg={statistics.mean(net_near_vals):>8.1f}  med={statistics.median(net_near_vals):>8.1f}  n={len(net_near_vals)}")
        print(f"    Pos:  avg={statistics.mean(pos_near_vals):>8.1f}  Neg: avg={statistics.mean(neg_near_vals):>8.1f}")

for etype in ['charm', 'vannaOI', 'vannaVol', 'gamma', 'deltaDecay']:
    def calc_net(t, et=etype):
        pts = t.get('exposure_points', {}).get(et, [])
        spot = float(t['spot'] or 0)
        return sum(float(p['points']) for p in pts if abs(float(p['strike']) - spot) <= 15)
    compare_groups(f"{etype} net near spot (15pts)",
        [calc_net(t) for t in wins if etype in t.get('exposure_points', {})],
        [calc_net(t) for t in losses if etype in t.get('exposure_points', {})])

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 5: Charm Direction Alignment")
print("=" * 70)

for direction in ['long', 'short']:
    dir_trades = [t for t in trades if t['direction'] == direction]
    print(f"\n  === {direction.upper()} ===")

    # Aggregated charm alignment
    aligned = []
    against = []
    for t in dir_trades:
        charm_val = safe_float(t.get('vol_agg_charm'))
        if charm_val is None:
            continue
        if direction == 'long':
            is_aligned = charm_val > 0
        else:
            is_aligned = charm_val < 0

        if is_aligned:
            aligned.append(t)
        else:
            against.append(t)

    for label, group in [('ALIGNED', aligned), ('AGAINST', against)]:
        if not group:
            continue
        g_wins = sum(1 for t in group if t['outcome_result'] == 'WIN')
        g_losses = sum(1 for t in group if t['outcome_result'] == 'LOSS')
        g_pnl = sum(t['outcome_pnl'] or 0 for t in group)
        wr = 100 * g_wins / len(group) if group else 0
        print(f"    Charm {label}: {len(group)} trades, {g_wins}W/{g_losses}L ({wr:.0f}% WR), {g_pnl:+.1f} pts")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 6: Direction + Time-of-Day Breakdown")
print("=" * 70)

for direction in ['long', 'short']:
    dir_trades = [t for t in trades if t['direction'] == direction]
    if not dir_trades:
        continue
    print(f"\n  === {direction.upper()} ===")
    print(f"  {'Hour':>6} | {'Total':>5} | {'Wins':>5} | {'Loss':>5} | {'WR%':>6} | {'PnL':>8}")
    print(f"  {'-'*6}-+-{'-'*5}-+-{'-'*5}-+-{'-'*5}-+-{'-'*6}-+-{'-'*8}")
    for hour in range(9, 16):
        h_trades = [t for t in dir_trades if t['ts_et'].hour == hour]
        if not h_trades:
            continue
        h_wins = sum(1 for t in h_trades if t['outcome_result'] == 'WIN')
        h_losses = sum(1 for t in h_trades if t['outcome_result'] == 'LOSS')
        h_pnl = sum(t['outcome_pnl'] or 0 for t in h_trades)
        wr = 100 * h_wins / len(h_trades)
        print(f"  {hour:>5}h | {len(h_trades):>5} | {h_wins:>5} | {h_losses:>5} | {wr:>5.1f}% | {h_pnl:>+8.1f}")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 7: Grade Breakdown")
print("=" * 70)

grade_stats = {}
for t in trades:
    g = t['grade']
    if g not in grade_stats:
        grade_stats[g] = {'total': 0, 'wins': 0, 'losses': 0, 'expired': 0, 'pnl': 0}
    grade_stats[g]['total'] += 1
    grade_stats[g][t['outcome_result'].lower()] = grade_stats[g].get(t['outcome_result'].lower(), 0) + 1
    grade_stats[g]['pnl'] += (t['outcome_pnl'] or 0)

print(f"\n  {'Grade':<8} | {'Total':>5} | {'Wins':>5} | {'Loss':>5} | {'Exp':>4} | {'WR%':>6} | {'PnL':>8} | {'AvgPnL':>7}")
print(f"  {'-'*8}-+-{'-'*5}-+-{'-'*5}-+-{'-'*5}-+-{'-'*4}-+-{'-'*6}-+-{'-'*8}-+-{'-'*7}")
for g in ['A+', 'A', 'A-Entry', 'B', 'C', 'LOG']:
    if g not in grade_stats:
        continue
    s = grade_stats[g]
    wr = 100 * s['wins'] / s['total'] if s['total'] else 0
    avg = s['pnl'] / s['total'] if s['total'] else 0
    print(f"  {g:<8} | {s['total']:>5} | {s['wins']:>5} | {s['losses']:>5} | {s['expired']:>4} | {wr:>5.1f}% | {s['pnl']:>+8.1f} | {avg:>+7.2f}")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 8: VIX Buckets")
print("=" * 70)

vix_buckets = [(0, 21), (21, 23), (23, 25), (25, 27), (27, 30), (30, 50)]
print(f"\n  {'VIX Range':<12} | {'Total':>5} | {'Wins':>5} | {'Loss':>5} | {'WR%':>6} | {'PnL':>8} | {'AvgPnL':>7}")
print(f"  {'-'*12}-+-{'-'*5}-+-{'-'*5}-+-{'-'*5}-+-{'-'*6}-+-{'-'*8}-+-{'-'*7}")
for lo, hi in vix_buckets:
    b = [t for t in trades if t.get('vix') and lo <= t['vix'] < hi]
    if not b:
        continue
    w = sum(1 for t in b if t['outcome_result'] == 'WIN')
    l = sum(1 for t in b if t['outcome_result'] == 'LOSS')
    pnl = sum(t['outcome_pnl'] or 0 for t in b)
    wr = 100 * w / len(b)
    avg = pnl / len(b)
    print(f"  {lo:>2}-{hi:<2}      | {len(b):>5} | {w:>5} | {l:>5} | {wr:>5.1f}% | {pnl:>+8.1f} | {avg:>+7.2f}")

# By direction
for direction in ['long', 'short']:
    print(f"\n  VIX Buckets - {direction.upper()} only:")
    for lo, hi in vix_buckets:
        b = [t for t in trades if t.get('vix') and lo <= t['vix'] < hi and t['direction'] == direction]
        if not b:
            continue
        w = sum(1 for t in b if t['outcome_result'] == 'WIN')
        l = sum(1 for t in b if t['outcome_result'] == 'LOSS')
        pnl = sum(t['outcome_pnl'] or 0 for t in b)
        wr = 100 * w / len(b)
        print(f"    {lo:>2}-{hi:<2}: {len(b):>3} trades, {w}W/{l}L ({wr:.0f}% WR), {pnl:+.1f} pts")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 9: LIS Position")
print("=" * 70)

for direction in ['long', 'short']:
    dir_trades = [t for t in trades if t['direction'] == direction]
    print(f"\n  === {direction.upper()} ===")

    above_lis = [t for t in dir_trades if t.get('gap_to_lis') and t['gap_to_lis'] > 5]
    near_lis = [t for t in dir_trades if t.get('gap_to_lis') is not None and abs(t['gap_to_lis']) <= 5]
    below_lis = [t for t in dir_trades if t.get('gap_to_lis') and t['gap_to_lis'] < -5]

    for label, group in [('Above LIS (>5)', above_lis), ('Near LIS (+-5)', near_lis), ('Below LIS (<-5)', below_lis)]:
        if not group:
            continue
        g_wins = sum(1 for t in group if t['outcome_result'] == 'WIN')
        g_losses = sum(1 for t in group if t['outcome_result'] == 'LOSS')
        g_pnl = sum(t['outcome_pnl'] or 0 for t in group)
        wr = 100 * g_wins / len(group) if group else 0
        avg_gap = statistics.mean([t['gap_to_lis'] for t in group])
        print(f"    {label}: {len(group)} trades, {g_wins}W/{g_losses}L ({wr:.0f}% WR), {g_pnl:+.1f} pts")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 10: Daily Performance (Loss Clustering)")
print("=" * 70)

day_stats = {}
for t in trades:
    d = str(t['ts_et'])[:10]
    if d not in day_stats:
        day_stats[d] = {'total': 0, 'wins': 0, 'losses': 0, 'expired': 0, 'pnl': 0, 'vix_avg': []}
    day_stats[d]['total'] += 1
    if t['outcome_result'] == 'WIN': day_stats[d]['wins'] += 1
    elif t['outcome_result'] == 'LOSS': day_stats[d]['losses'] += 1
    else: day_stats[d]['expired'] += 1
    day_stats[d]['pnl'] += (t['outcome_pnl'] or 0)
    if t.get('vix'): day_stats[d]['vix_avg'].append(t['vix'])

print(f"\n  {'Date':<12} | {'Total':>5} | {'W':>3} | {'L':>3} | {'E':>3} | {'WR%':>6} | {'PnL':>8} | {'VIX':>5}")
print(f"  {'-'*12}-+-{'-'*5}-+-{'-'*3}-+-{'-'*3}-+-{'-'*3}-+-{'-'*6}-+-{'-'*8}-+-{'-'*5}")
for d, s in sorted(day_stats.items()):
    wr = 100 * s['wins'] / s['total'] if s['total'] else 0
    avg_vix = statistics.mean(s['vix_avg']) if s['vix_avg'] else 0
    flag = " *** BAD" if s['pnl'] < -40 else ""
    print(f"  {d:<12} | {s['total']:>5} | {s['wins']:>3} | {s['losses']:>3} | {s['expired']:>3} | {wr:>5.1f}% | {s['pnl']:>+8.1f} | {avg_vix:>5.1f}{flag}")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 11: DD Hedging Direction Alignment")
print("=" * 70)

for direction in ['long', 'short']:
    dir_trades = [t for t in trades if t['direction'] == direction and t['vol_dd_numeric'] is not None]
    if not dir_trades:
        continue
    print(f"\n  === {direction.upper()} ===")

    # DD positive = bullish hedging, DD negative = bearish hedging
    # For longs: DD > 0 = aligned, DD < 0 = against
    # For shorts: DD < 0 = aligned, DD > 0 = against
    dd_aligned = []
    dd_against = []
    dd_neutral = []
    for t in dir_trades:
        dd = t['vol_dd_numeric']
        threshold = 200_000_000  # $200M
        if abs(dd) < threshold:
            dd_neutral.append(t)
        elif (direction == 'long' and dd > 0) or (direction == 'short' and dd < 0):
            dd_aligned.append(t)
        else:
            dd_against.append(t)

    for label, group in [('DD ALIGNED', dd_aligned), ('DD NEUTRAL', dd_neutral), ('DD AGAINST', dd_against)]:
        if not group:
            continue
        g_wins = sum(1 for t in group if t['outcome_result'] == 'WIN')
        g_losses = sum(1 for t in group if t['outcome_result'] == 'LOSS')
        g_pnl = sum(t['outcome_pnl'] or 0 for t in group)
        wr = 100 * g_wins / len(group)
        print(f"    {label}: {len(group)} trades, {g_wins}W/{g_losses}L ({wr:.0f}% WR), {g_pnl:+.1f} pts")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 12: Filter Combinations Testing")
print("=" * 70)

baseline_pnl = sum(t['outcome_pnl'] or 0 for t in trades)

def test_filter(name, condition_fn):
    passed = [t for t in trades if condition_fn(t)]
    blocked = [t for t in trades if not condition_fn(t)]
    if not passed or not blocked:
        return

    p_wins = sum(1 for t in passed if t['outcome_result'] == 'WIN')
    p_losses = sum(1 for t in passed if t['outcome_result'] == 'LOSS')
    p_pnl = sum(t['outcome_pnl'] or 0 for t in passed)
    b_wins = sum(1 for t in blocked if t['outcome_result'] == 'WIN')
    b_losses = sum(1 for t in blocked if t['outcome_result'] == 'LOSS')
    b_pnl = sum(t['outcome_pnl'] or 0 for t in blocked)

    wr_p = 100 * p_wins / len(passed) if passed else 0
    wr_b = 100 * b_wins / len(blocked) if blocked else 0
    improvement = -b_pnl  # removing blocked trades PnL

    flag = " *** GOOD" if b_pnl < -20 and b_wins < b_losses else ""
    print(f"\n  {name}:{flag}")
    print(f"    PASS:  {len(passed):>3} trades, {p_wins}W/{p_losses}L ({wr_p:.0f}% WR), {p_pnl:+.1f} pts")
    print(f"    BLOCK: {len(blocked):>3} trades, {b_wins}W/{b_losses}L ({wr_b:.0f}% WR), {b_pnl:+.1f} pts")
    print(f"    Effect: blocking removes {b_pnl:+.1f} pts -> new total = {p_pnl:+.1f} ({p_pnl - baseline_pnl:+.1f} vs baseline)")

test_filter("F1: VIX < 25",
    lambda t: t.get('vix') and t['vix'] < 25)

test_filter("F2: VIX < 27",
    lambda t: t.get('vix') and t['vix'] < 27)

test_filter("F3: Grade >= B (exclude C, LOG)",
    lambda t: t['grade'] not in ('C', 'LOG'))

test_filter("F4: Grade >= A-Entry",
    lambda t: t['grade'] in ('A+', 'A', 'A-Entry'))

test_filter("F5: Only before 14:00",
    lambda t: t['ts_et'].hour < 14)

test_filter("F6: Only before 15:00",
    lambda t: t['ts_et'].hour < 15)

test_filter("F7: Score >= 50",
    lambda t: (t['score'] or 0) >= 50)

test_filter("F8: Score >= 40",
    lambda t: (t['score'] or 0) >= 40)

test_filter("F9: Charm aligned with direction",
    lambda t: t['vol_agg_charm'] is None or (
        (safe_float(t['vol_agg_charm']) or 0) > 0 if t['direction'] == 'long'
        else (safe_float(t['vol_agg_charm']) or 0) < 0
    ))

test_filter("F10: DD aligned or neutral (not against)",
    lambda t: t['vol_dd_numeric'] is None or abs(t['vol_dd_numeric']) < 200_000_000 or (
        (t['direction'] == 'long' and t['vol_dd_numeric'] > 0) or
        (t['direction'] == 'short' and t['vol_dd_numeric'] < 0)
    ))

test_filter("F11: VIX < 25 + Grade >= B",
    lambda t: t.get('vix') and t['vix'] < 25 and t['grade'] not in ('C', 'LOG'))

test_filter("F12: Alignment >= 1 (abs)",
    lambda t: t.get('greek_alignment') is not None and abs(t['greek_alignment']) >= 1)

test_filter("F13: Skip grade C",
    lambda t: t['grade'] != 'C')

test_filter("F14: Skip grade C + LOG",
    lambda t: t['grade'] not in ('C', 'LOG'))

test_filter("F15: Exclude VIX 25-28 longs",
    lambda t: not (t['direction'] == 'long' and t.get('vix') and 25 <= t['vix'] < 28))

test_filter("F16: Exclude VIX > 25 longs only",
    lambda t: not (t['direction'] == 'long' and t.get('vix') and t['vix'] > 25))

test_filter("F17: Exclude VIX > 26 longs only",
    lambda t: not (t['direction'] == 'long' and t.get('vix') and t['vix'] > 26))

test_filter("F18: Exclude after 15:00",
    lambda t: t['ts_et'].hour < 15)

test_filter("F19: Support Score >= 15 (strong skew change)",
    lambda t: (t['support_score'] or 0) >= 15)

test_filter("F20: Upside Score >= 10 (strong charm)",
    lambda t: (t['upside_score'] or 0) >= 10)

# Combined filters
test_filter("F21: VIX<25 + no C/LOG + charm aligned",
    lambda t: t.get('vix') and t['vix'] < 25 and t['grade'] not in ('C', 'LOG') and (
        t['vol_agg_charm'] is None or (
            (safe_float(t['vol_agg_charm']) or 0) > 0 if t['direction'] == 'long'
            else (safe_float(t['vol_agg_charm']) or 0) < 0
        )
    ))

test_filter("F22: No C/LOG + no VIX>25 longs",
    lambda t: t['grade'] not in ('C', 'LOG') and not (t['direction'] == 'long' and t.get('vix') and t['vix'] > 25))

test_filter("F23: No C/LOG + no VIX>26 longs",
    lambda t: t['grade'] not in ('C', 'LOG') and not (t['direction'] == 'long' and t.get('vix') and t['vix'] > 26))

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 13: Time-to-Resolution")
print("=" * 70)

compare_groups("Elapsed Minutes",
    [t['outcome_elapsed_min'] for t in wins],
    [t['outcome_elapsed_min'] for t in losses])

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 14: SVB Correlation Breakdown")
print("=" * 70)

for direction in ['long', 'short']:
    dir_trades = [t for t in trades if t['direction'] == direction and t['vol_svb_corr'] is not None]
    if not dir_trades:
        continue
    print(f"\n  === {direction.upper()} ===")

    # SVB positive = positive correlation (SPX up, VIX up) = unusual
    # SVB negative = normal negative correlation
    for lo, hi, label in [(-1.0, -0.7, 'Strong negative'), (-0.7, -0.3, 'Moderate negative'),
                           (-0.3, 0.0, 'Weak negative'), (0.0, 0.3, 'Weak positive'),
                           (0.3, 0.7, 'Moderate positive'), (0.7, 1.01, 'Strong positive')]:
        b = [t for t in dir_trades if lo <= float(t['vol_svb_corr'] or 0) < hi]
        if not b:
            continue
        w = sum(1 for t in b if t['outcome_result'] == 'WIN')
        l = sum(1 for t in b if t['outcome_result'] == 'LOSS')
        pnl = sum(t['outcome_pnl'] or 0 for t in b)
        wr = 100 * w / len(b)
        print(f"    SVB [{lo:+.1f}, {hi:+.1f}): {len(b):>3} trades, {w}W/{l}L ({wr:.0f}% WR), {pnl:+.1f} pts")

# ===================================================================
print("\n" + "=" * 70)
print("ANALYSIS 15: Per-Strike Charm Support/Resistance Quality")
print("=" * 70)

# For each trade, calculate charm S/R metrics
for direction in ['long', 'short']:
    dir_wins = [t for t in wins if t['direction'] == direction and 'charm' in t.get('exposure_points', {})]
    dir_losses = [t for t in losses if t['direction'] == direction and 'charm' in t.get('exposure_points', {})]

    if not dir_wins and not dir_losses:
        continue

    print(f"\n  === {direction.upper()} ===")

    def charm_support_quality(t):
        """Measure charm support below spot (for longs) or resistance above (for shorts)"""
        spot = float(t['spot'] or 0)
        points = t['exposure_points'].get('charm', [])
        if not spot or not points:
            return None

        if direction == 'long':
            support = sum(float(p['points']) for p in points if float(p['strike']) <= spot and float(p['points']) > 0)
            resistance = sum(abs(float(p['points'])) for p in points if float(p['strike']) > spot and float(p['points']) < 0)
            return support - resistance
        else:
            resistance = sum(abs(float(p['points'])) for p in points if float(p['strike']) >= spot and float(p['points']) < 0)
            support = sum(float(p['points']) for p in points if float(p['strike']) < spot and float(p['points']) > 0)
            return resistance - support

    w_quality = [charm_support_quality(t) for t in dir_wins]
    l_quality = [charm_support_quality(t) for t in dir_losses]
    w_quality = [q for q in w_quality if q is not None]
    l_quality = [q for q in l_quality if q is not None]

    if w_quality and l_quality:
        print(f"    Charm S/R Quality (higher = more supportive):")
        print(f"      WIN:  avg={statistics.mean(w_quality):>8.1f}  med={statistics.median(w_quality):>8.1f}  n={len(w_quality)}")
        print(f"      LOSS: avg={statistics.mean(l_quality):>8.1f}  med={statistics.median(l_quality):>8.1f}  n={len(l_quality)}")
        print(f"      DIFF: {statistics.mean(w_quality) - statistics.mean(l_quality):>+8.1f}")

print("\n" + "=" * 70)
print("DONE")
print("=" * 70)
