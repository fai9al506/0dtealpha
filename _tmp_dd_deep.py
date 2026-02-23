"""Deep analysis of DD Exhaustion signals — what makes them WIN vs LOSS?
Look at charm, vanna, DD flow, paradigm, and conflict patterns."""
import os, sys, json
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    dd_trades = conn.execute(text("""
        SELECT id, setup_name, direction, grade, score,
               spot, target, outcome_target_level, outcome_stop_level,
               outcome_result, outcome_pnl, outcome_first_event,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min, ts,
               comments
        FROM setup_log
        WHERE setup_name = 'DD Exhaustion'
        ORDER BY ts
    """)).mappings().all()

    # Get volland snapshots — stats are in payload->'statistics'
    volland_snaps = conn.execute(text("""
        SELECT ts,
               payload->'statistics'->>'paradigm' as paradigm,
               payload->'statistics'->>'aggregatedCharm' as charm,
               payload->'statistics'->>'delta_decay_hedging' as dd,
               payload->'statistics'->'spot_vol_beta'->>'correlation' as svb_corr,
               payload->'statistics'->>'lines_in_sand' as lis
        FROM volland_snapshots
        WHERE ts >= '2026-02-11'
          AND payload->'statistics' IS NOT NULL
          AND payload->>'statistics' != '{}'
        ORDER BY ts
    """)).mappings().all()

print(f"DD Exhaustion: {len(dd_trades)} trades total", flush=True)
print(f"Volland snapshots with stats: {len(volland_snaps)}\n", flush=True)

def parse_dollar(s):
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace(',', '').replace('$', '')
    try:
        return float(s)
    except:
        return None

# Build volland timeline
vol_timeline = []
prev_dd = None
for v in volland_snaps:
    ts = v['ts'].replace(tzinfo=None) if hasattr(v['ts'], 'replace') else v['ts']
    dd_val = parse_dollar(v['dd'])
    charm_val = v['charm']
    # charm is stored as raw number (not dollar string) for aggregatedCharm
    try:
        charm_num = float(charm_val) if charm_val else None
    except:
        charm_num = parse_dollar(str(charm_val))

    # Track DD shift from previous cycle
    dd_shift = None
    if dd_val is not None and prev_dd is not None:
        dd_shift = dd_val - prev_dd
    prev_dd = dd_val

    svb = None
    try:
        svb = float(v['svb_corr']) if v['svb_corr'] else None
    except:
        pass

    vol_timeline.append({
        'ts': ts,
        'paradigm': v['paradigm'] or '',
        'dd_val': dd_val,
        'dd_shift': dd_shift,
        'charm_val': charm_num,
        'svb_corr': svb,
        'lis': v['lis'] or '',
    })

def find_volland_at(ts):
    ts = ts.replace(tzinfo=None) if hasattr(ts, 'replace') else ts
    best = None
    best_diff = float('inf')
    for v in vol_timeline:
        diff = abs((v['ts'] - ts).total_seconds())
        if diff < best_diff and diff < 300:
            best_diff = diff
            best = v
    return best

# Analyze each DD trade
print("=" * 140, flush=True)
print(f"{'#':>4s} {'Date':>5s} {'Time':>5s} {'Dir':>5s} {'Grade':>7s} {'Result':>7s} "
      f"{'PnL':>7s} {'MaxP':>6s} {'Min':>5s} "
      f"{'Paradigm':>12s} {'Charm':>14s} {'DD$':>14s} {'DD_Shift':>12s} {'SVB':>6s}", flush=True)
print("-" * 140, flush=True)

wins = []
losses = []
expired = []
all_dd = []

for r in dd_trades:
    fired = r['ts'].replace(tzinfo=None) if hasattr(r['ts'], 'replace') else r['ts']
    result = r['outcome_result']
    pnl = r['outcome_pnl'] or 0
    elapsed = r['outcome_elapsed_min'] or 0
    direction = 'long' if r['direction'].lower() in ('long', 'bullish') else 'short'
    grade = r['grade']
    max_profit = r['outcome_max_profit'] or 0

    vol = find_volland_at(fired)
    dd_val = vol['dd_val'] if vol else None
    dd_shift = vol['dd_shift'] if vol else None
    charm_val = vol['charm_val'] if vol else None
    paradigm = (vol['paradigm'] if vol else '')[:12]
    svb = vol['svb_corr'] if vol else None

    charm_str = f"${charm_val/1e6:+,.0f}M" if charm_val else '?'
    dd_str = f"${dd_val/1e6:+,.0f}M" if dd_val else '?'
    dd_shift_str = f"${dd_shift/1e6:+,.0f}M" if dd_shift else '?'
    svb_str = f"{svb:+.2f}" if svb else '?'

    entry = {
        'id': r['id'], 'fired': fired, 'direction': direction, 'grade': grade,
        'result': result, 'pnl': pnl, 'max_profit': max_profit, 'elapsed': elapsed,
        'dd_val': dd_val, 'dd_shift': dd_shift, 'charm_val': charm_val,
        'paradigm': paradigm, 'svb': svb, 'spot': r['spot'],
    }
    all_dd.append(entry)
    if result == 'WIN': wins.append(entry)
    elif result == 'LOSS': losses.append(entry)
    elif result == 'EXPIRED': expired.append(entry)

    res_str = result or 'OPEN'
    print(f"#{r['id']:>3d} {fired.strftime('%m/%d'):>5s} {fired.strftime('%H:%M'):>5s} "
          f"{direction:>5s} {grade:>7s} {res_str:>7s} "
          f"{pnl:>+7.1f} {max_profit:>+6.1f} {elapsed:>5.0f} "
          f"{paradigm:>12s} {charm_str:>14s} {dd_str:>14s} {dd_shift_str:>12s} {svb_str:>6s}", flush=True)

print(f"\n{'='*140}", flush=True)
resolved = [t for t in all_dd if t['result']]
print(f"TOTAL: {len(all_dd)} trades | {len(wins)}W / {len(losses)}L / {len(expired)} EXP / "
      f"{len(all_dd)-len(resolved)} open", flush=True)
total_pnl = sum(t['pnl'] for t in resolved)
print(f"Net P&L: {total_pnl:+.1f} pts\n", flush=True)

# WIN vs LOSS comparison
print("=" * 80, flush=True)
print("=== WIN vs LOSS CHARACTERISTICS ===\n", flush=True)

def analyze_group(group, label):
    if not group:
        print(f"{label}: no trades\n", flush=True)
        return
    dd_vals = [abs(t['dd_val']) for t in group if t['dd_val'] is not None]
    dd_shifts = [abs(t['dd_shift']) for t in group if t['dd_shift'] is not None]
    charm_vals = [t['charm_val'] for t in group if t['charm_val'] is not None]
    svbs = [t['svb'] for t in group if t['svb'] is not None]

    longs = [t for t in group if t['direction'] == 'long']
    shorts = [t for t in group if t['direction'] == 'short']
    grades = defaultdict(int)
    for t in group: grades[t['grade']] += 1
    paradigms = defaultdict(int)
    for t in group: paradigms[t['paradigm']] += 1

    # Signal alignment check: SHORT = DD>0 + charm<0, LONG = DD<0 + charm>0
    aligned = 0
    misaligned = 0
    for t in group:
        if t['dd_val'] is None or t['charm_val'] is None:
            continue
        if t['direction'] == 'short' and t['dd_val'] > 0 and t['charm_val'] < 0:
            aligned += 1
        elif t['direction'] == 'long' and t['dd_val'] < 0 and t['charm_val'] > 0:
            aligned += 1
        else:
            misaligned += 1

    avg_pnl = sum(t['pnl'] for t in group) / len(group)

    print(f"{label} ({len(group)} trades, avg PnL: {avg_pnl:+.1f}):", flush=True)
    print(f"  Direction: {len(longs)} long / {len(shorts)} short", flush=True)
    print(f"  Grades: {dict(grades)}", flush=True)
    print(f"  Paradigms: {dict(paradigms)}", flush=True)
    if dd_vals:
        print(f"  |DD| at signal: min=${min(dd_vals)/1e6:.0f}M avg=${sum(dd_vals)/len(dd_vals)/1e6:.0f}M max=${max(dd_vals)/1e6:.0f}M", flush=True)
    if dd_shifts:
        print(f"  |DD shift|: min=${min(dd_shifts)/1e6:.0f}M avg=${sum(dd_shifts)/len(dd_shifts)/1e6:.0f}M max=${max(dd_shifts)/1e6:.0f}M", flush=True)
    if charm_vals:
        charm_abs = [abs(c) for c in charm_vals]
        print(f"  |Charm| at signal: min=${min(charm_abs)/1e6:.0f}M avg=${sum(charm_abs)/len(charm_abs)/1e6:.0f}M max=${max(charm_abs)/1e6:.0f}M", flush=True)
    if svbs:
        print(f"  SpotVolBeta: min={min(svbs):.2f} avg={sum(svbs)/len(svbs):.2f} max={max(svbs):.2f}", flush=True)
    print(f"  Signal aligned (DD-charm divergence): {aligned}/{aligned+misaligned}", flush=True)
    print(flush=True)

analyze_group(wins, "WINS")
analyze_group(losses, "LOSSES")
analyze_group(expired, "EXPIRED")

# DD vs opposite DD deep dive
print("=" * 80, flush=True)
print("=== DD vs OPPOSITE DD CONFLICTS ===\n", flush=True)

dd_conflicts = []
for i, a in enumerate(all_dd):
    if not a['result']:
        continue
    for b in all_dd[i+1:]:
        if not b['result']:
            continue
        if a['direction'] == b['direction']:
            continue
        a_end = a['fired'] + timedelta(minutes=a['elapsed'])
        if b['fired'] < a_end:
            if a['spot'] and b['spot']:
                a_unreal = (b['spot'] - a['spot']) if a['direction'] == 'long' else (a['spot'] - b['spot'])
            else:
                a_unreal = None
            dd_conflicts.append({'first': a, 'second': b, 'a_unrealized': a_unreal})

print(f"Found {len(dd_conflicts)} DD vs opposite DD conflicts\n", flush=True)

for c in dd_conflicts:
    a, b = c['first'], c['second']
    a_end_str = (a['fired'] + timedelta(minutes=a['elapsed'])).strftime('%H:%M')
    b_end_str = (b['fired'] + timedelta(minutes=b['elapsed'])).strftime('%H:%M')
    a_unr = c['a_unrealized']

    if a['result'] == 'WIN' and b['result'] == 'LOSS':
        verdict = "1st RIGHT"
    elif a['result'] == 'LOSS' and b['result'] == 'WIN':
        verdict = "2nd RIGHT (reversal)"
    elif a['result'] == 'WIN' and b['result'] == 'WIN':
        verdict = "BOTH WIN"
    else:
        verdict = "BOTH LOSS"

    a_dd_str = f"${a['dd_val']/1e6:+,.0f}M" if a['dd_val'] else '?'
    b_dd_str = f"${b['dd_val']/1e6:+,.0f}M" if b['dd_val'] else '?'
    a_ch_str = f"${a['charm_val']/1e6:+,.0f}M" if a['charm_val'] else '?'
    b_ch_str = f"${b['charm_val']/1e6:+,.0f}M" if b['charm_val'] else '?'
    a_shift_str = f"${a['dd_shift']/1e6:+,.0f}M" if a['dd_shift'] else '?'
    b_shift_str = f"${b['dd_shift']/1e6:+,.0f}M" if b['dd_shift'] else '?'

    gap_min = (b['fired'] - a['fired']).total_seconds() / 60

    print(f"  {a['fired'].strftime('%m/%d')} | {verdict:20s} | gap={gap_min:.0f}min | A@B={a_unr:+.1f}pts" if a_unr else f"  {a['fired'].strftime('%m/%d')} | {verdict:20s} | gap={gap_min:.0f}min", flush=True)
    print(f"    1st: #{a['id']} {a['direction'].upper():5s} {a['fired'].strftime('%H:%M')}-{a_end_str} "
          f"{a['result']:7s} {a['pnl']:+.1f} | DD={a_dd_str} shift={a_shift_str} charm={a_ch_str} {a['paradigm']}", flush=True)
    print(f"    2nd: #{b['id']} {b['direction'].upper():5s} {b['fired'].strftime('%H:%M')}-{b_end_str} "
          f"{b['result']:7s} {b['pnl']:+.1f} | DD={b_dd_str} shift={b_shift_str} charm={b_ch_str} {b['paradigm']}", flush=True)
    if a['dd_val'] and b['dd_val']:
        dd_delta = b['dd_val'] - a['dd_val']
        print(f"    DD moved: {a_dd_str} -> {b_dd_str} (delta=${dd_delta/1e6:+,.0f}M)", flush=True)
    if a['charm_val'] and b['charm_val']:
        charm_delta = b['charm_val'] - a['charm_val']
        print(f"    Charm moved: {a_ch_str} -> {b_ch_str} (delta=${charm_delta/1e6:+,.0f}M)", flush=True)
    print(flush=True)

# Statistical patterns
print("=" * 80, flush=True)
print("=== KEY DISTINGUISHING FACTORS ===\n", flush=True)

# 1. DD amount at signal: does higher |DD| predict success?
print("1. DD Amount at Signal vs Outcome:", flush=True)
for bucket_label, lo, hi in [("<$1B", 0, 1e9), ("$1-3B", 1e9, 3e9), ("$3-5B", 3e9, 5e9), (">$5B", 5e9, 1e12)]:
    bucket_trades = [t for t in resolved if t['dd_val'] is not None and lo <= abs(t['dd_val']) < hi]
    if not bucket_trades:
        continue
    w = sum(1 for t in bucket_trades if t['result'] == 'WIN')
    l = sum(1 for t in bucket_trades if t['result'] == 'LOSS')
    avg_pnl = sum(t['pnl'] for t in bucket_trades) / len(bucket_trades) if bucket_trades else 0
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  |DD| {bucket_label:>6s}: {len(bucket_trades):>2d} trades, {w}W/{l}L, WR={wr:.0f}%, avg={avg_pnl:+.1f}pts", flush=True)

# 2. DD shift magnitude
print("\n2. DD Shift Magnitude at Signal vs Outcome:", flush=True)
for bucket_label, lo, hi in [("<$200M", 0, 2e8), ("$200-500M", 2e8, 5e8), ("$500M-1B", 5e8, 1e9), (">$1B", 1e9, 1e12)]:
    bucket_trades = [t for t in resolved if t['dd_shift'] is not None and lo <= abs(t['dd_shift']) < hi]
    if not bucket_trades:
        continue
    w = sum(1 for t in bucket_trades if t['result'] == 'WIN')
    l = sum(1 for t in bucket_trades if t['result'] == 'LOSS')
    avg_pnl = sum(t['pnl'] for t in bucket_trades) / len(bucket_trades) if bucket_trades else 0
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  |shift| {bucket_label:>10s}: {len(bucket_trades):>2d} trades, {w}W/{l}L, WR={wr:.0f}%, avg={avg_pnl:+.1f}pts", flush=True)

# 3. Charm alignment
print("\n3. Charm-DD Alignment vs Outcome:", flush=True)
aligned_trades = [t for t in resolved if t['dd_val'] and t['charm_val'] and
                  ((t['direction'] == 'short' and t['dd_val'] > 0 and t['charm_val'] < 0) or
                   (t['direction'] == 'long' and t['dd_val'] < 0 and t['charm_val'] > 0))]
misaligned_trades = [t for t in resolved if t['dd_val'] and t['charm_val'] and t not in aligned_trades]

for label, group in [("Aligned (correct divergence)", aligned_trades), ("Misaligned", misaligned_trades)]:
    if not group:
        continue
    w = sum(1 for t in group if t['result'] == 'WIN')
    l = sum(1 for t in group if t['result'] == 'LOSS')
    avg_pnl = sum(t['pnl'] for t in group) / len(group)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  {label}: {len(group)} trades, {w}W/{l}L, WR={wr:.0f}%, avg={avg_pnl:+.1f}pts", flush=True)

# 4. Paradigm
print("\n4. Paradigm at Signal vs Outcome:", flush=True)
by_para = defaultdict(list)
for t in resolved:
    by_para[t['paradigm'] or 'unknown'].append(t)
for para in sorted(by_para.keys()):
    group = by_para[para]
    w = sum(1 for t in group if t['result'] == 'WIN')
    l = sum(1 for t in group if t['result'] == 'LOSS')
    avg_pnl = sum(t['pnl'] for t in group) / len(group)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  {para:>12s}: {len(group):>2d} trades, {w}W/{l}L, WR={wr:.0f}%, avg={avg_pnl:+.1f}pts", flush=True)

# 5. Time of day
print("\n5. Time of Day (ET) vs Outcome:", flush=True)
for hour_label, lo_h, hi_h in [("10:00-11:00", 15, 16), ("11:00-12:00", 16, 17), ("12:00-13:00", 17, 18),
                                 ("13:00-14:00", 18, 19), ("14:00-15:30", 19, 20.5)]:
    bucket = [t for t in resolved if lo_h <= t['fired'].hour + t['fired'].minute/60 < hi_h]
    if not bucket:
        continue
    w = sum(1 for t in bucket if t['result'] == 'WIN')
    l = sum(1 for t in bucket if t['result'] == 'LOSS')
    avg_pnl = sum(t['pnl'] for t in bucket) / len(bucket)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  {hour_label}: {len(bucket):>2d} trades, {w}W/{l}L, WR={wr:.0f}%, avg={avg_pnl:+.1f}pts", flush=True)

# 6. SpotVolBeta correlation
print("\n6. SpotVol Beta Correlation vs Outcome:", flush=True)
for label, lo, hi in [("Strong neg (<-0.5)", -2, -0.5), ("Weak neg (-0.5 to 0)", -0.5, 0), ("Positive (>0)", 0, 2)]:
    bucket = [t for t in resolved if t['svb'] is not None and lo <= t['svb'] < hi]
    if not bucket:
        continue
    w = sum(1 for t in bucket if t['result'] == 'WIN')
    l = sum(1 for t in bucket if t['result'] == 'LOSS')
    avg_pnl = sum(t['pnl'] for t in bucket) / len(bucket)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  SVB {label}: {len(bucket):>2d} trades, {w}W/{l}L, WR={wr:.0f}%, avg={avg_pnl:+.1f}pts", flush=True)

# 7. Grade
print("\n7. Grade vs Outcome:", flush=True)
by_grade = defaultdict(list)
for t in resolved:
    by_grade[t['grade']].append(t)
for g in sorted(by_grade.keys()):
    group = by_grade[g]
    w = sum(1 for t in group if t['result'] == 'WIN')
    l = sum(1 for t in group if t['result'] == 'LOSS')
    avg_pnl = sum(t['pnl'] for t in group) / len(group)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  {g:>7s}: {len(group):>2d} trades, {w}W/{l}L, WR={wr:.0f}%, avg={avg_pnl:+.1f}pts", flush=True)

# 8. Was DD signal the FIRST of the day or a repeat?
print("\n8. First DD of Day vs Repeat:", flush=True)
seen_day_dir = set()
first_trades = []
repeat_trades = []
for t in all_dd:
    if not t['result']:
        continue
    day = t['fired'].strftime('%Y-%m-%d')
    key = f"{day}_{t['direction']}"
    if key not in seen_day_dir:
        seen_day_dir.add(key)
        first_trades.append(t)
    else:
        repeat_trades.append(t)

for label, group in [("First of day/direction", first_trades), ("Repeat same direction", repeat_trades)]:
    if not group:
        continue
    w = sum(1 for t in group if t['result'] == 'WIN')
    l = sum(1 for t in group if t['result'] == 'LOSS')
    avg_pnl = sum(t['pnl'] for t in group) / len(group)
    wr = w / (w + l) * 100 if (w + l) else 0
    print(f"  {label}: {len(group)} trades, {w}W/{l}L, WR={wr:.0f}%, avg={avg_pnl:+.1f}pts", flush=True)
