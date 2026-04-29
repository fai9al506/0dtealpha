"""TSRT ACTUAL vs theoretical: use real_trade_orders table as ground truth."""
import psycopg2, json
from collections import defaultdict
from datetime import datetime, timedelta

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
conn = psycopg2.connect(DB)
cur = conn.cursor()

# 1. Actual TSRT orders
cur.execute("""
SELECT r.setup_log_id, r.state, r.created_at,
       s.setup_name, s.direction, s.grade, s.paradigm, s.spot,
       s.outcome_result, s.outcome_pnl,
       s.ts as signal_ts,
       (s.ts AT TIME ZONE 'America/New_York')::date as d,
       s.greek_alignment, s.vix, s.overvix
FROM real_trade_orders r
LEFT JOIN setup_log s ON s.id = r.setup_log_id
ORDER BY r.created_at
""")
rows = cur.fetchall()
print(f"Total real trades: {len(rows)}")

# Build actual TSRT trade list with SPX-based PnL (from stop/target prices in state)
actual = []
for r in rows:
    setup_log_id, state, created, setup, dirx, grade, paradigm, spot, outcome, outcome_pnl, sig_ts, d, align, vix, ovx = r
    if not state: continue
    fill = state.get('fill_price')
    stop_fill = state.get('stop_fill_price')
    target = state.get('target_price')
    current_stop = state.get('current_stop')
    status = state.get('status')
    close_reason = state.get('close_reason')
    account = state.get('account_id')
    be_trig = state.get('be_triggered')
    max_fav = state.get('max_favorable')

    # Compute actual SPX PnL based on fills
    spx_pnl = None
    if fill and close_reason:
        if close_reason == 'stop_filled' and stop_fill:
            if dirx == 'short':
                spx_pnl = fill - stop_fill
            else:
                spx_pnl = stop_fill - fill
        elif close_reason == 'target_filled' and target:
            if dirx == 'short':
                spx_pnl = fill - target
            else:
                spx_pnl = target - fill
    actual.append({
        'setup_log_id': setup_log_id, 'ts': sig_ts, 'd': d, 'setup': setup,
        'direction': dirx, 'grade': grade, 'paradigm': paradigm,
        'account': account, 'fill': fill, 'status': status,
        'close_reason': close_reason, 'stop_fill': stop_fill, 'target': target,
        'spx_pnl': spx_pnl, 'outcome': outcome, 'outcome_pnl': outcome_pnl,
        'be_triggered': be_trig, 'max_favorable': max_fav,
    })

print(f"\nActual trade breakdown:")
print(f"  Date range: {actual[0]['d']} to {actual[-1]['d']}")
print(f"  By status:")
statuses = defaultdict(int)
for a in actual: statuses[a['status']] += 1
for s, n in statuses.items(): print(f"    {s}: {n}")

print(f"\n  By close_reason:")
reasons = defaultdict(int)
for a in actual:
    if a['close_reason']: reasons[a['close_reason']] += 1
for r, n in reasons.items(): print(f"    {r}: {n}")

print(f"\n  By setup/direction:")
by_setup = defaultdict(list)
for a in actual:
    if a['status'] == 'closed' and a['spx_pnl'] is not None:
        by_setup[(a['setup'], a['direction'])].append(a)
for (setup, dirx), lst in by_setup.items():
    pnl_sum = sum(a['spx_pnl'] for a in lst)
    w = sum(1 for a in lst if a['spx_pnl'] > 0)
    l = sum(1 for a in lst if a['spx_pnl'] < 0)
    print(f"    {setup} {dirx}: n={len(lst)}, PnL={pnl_sum:+.1f}, W={w}, L={l}")

# Total actual SPX PnL
closed = [a for a in actual if a['status'] == 'closed' and a['spx_pnl'] is not None]
total_actual_pnl = sum(a['spx_pnl'] for a in closed)
print(f"\n  TOTAL CLOSED: {len(closed)} trades, SPX-PnL {total_actual_pnl:+.1f} pts")

# SPX-to-$ translation: 1 MES = $5/point
total_actual_usd = total_actual_pnl * 5
print(f"  Approx $ value at 1 MES: ${total_actual_usd:+.1f}")

# By date (for later gap analysis)
by_date = defaultdict(lambda: {'actual_n': 0, 'actual_pnl': 0})
for a in closed:
    by_date[str(a['d'])]['actual_n'] += 1
    by_date[str(a['d'])]['actual_pnl'] += a['spx_pnl']

# 2. Now get theoretical V13 for same period (Mar 24 onwards)
start = actual[0]['d']
end = actual[-1]['d']
print(f"\n\n=== THEORETICAL V13 SAME PERIOD ({start} to {end}) ===")

cur.execute("""
SELECT id, ts, setup_name, grade, paradigm, spot, outcome_result, outcome_pnl,
       greek_alignment, vix, overvix, direction,
       EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York'))::int as h,
       EXTRACT(MINUTE FROM (ts AT TIME ZONE 'America/New_York'))::int as m,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN %s AND %s
  AND outcome_result IS NOT NULL AND spot IS NOT NULL
ORDER BY ts
""", (start, end))
raw = cur.fetchall()

def passes_v12fix(t):
    tid, ts, setup, grade, paradigm, spot, outcome, pnl, align, vix, ovx, dirx, h, m, d = t
    if setup in ("VIX Divergence", "IV Momentum", "Vanna Butterfly"): return False
    if setup == 'Skew Charm' and grade and grade in ('C', 'LOG'): return False
    if setup in ('Skew Charm', 'DD Exhaustion'):
        if (h == 14 and m >= 30) or h == 15: return False
    if setup == 'BofA Scalp' and ((h == 14 and m >= 30) or h >= 15): return False
    is_long = dirx in ('long', 'bullish')
    if is_long and paradigm == 'SIDIAL-EXTREME': return False
    if is_long:
        if align is None or align < 2: return False
        if setup == 'Skew Charm': return True
        vix_f = float(vix) if vix else None
        ovx_f = float(ovx) if ovx else -99
        if vix_f is not None and vix_f > 22 and ovx_f < 2: return False
        return True
    else:
        if setup in ('Skew Charm','DD Exhaustion') and paradigm == 'GEX-LIS': return False
        if setup == 'AG Short' and paradigm == 'AG-TARGET': return False
        if setup in ('Skew Charm', 'AG Short'): return True
        if setup == 'DD Exhaustion' and align != 0: return True
        return False

def tsrt_scope(t):
    setup, dirx = t[2], t[11]
    if setup == 'Skew Charm': return True
    if setup == 'AG Short' and dirx in ('short','bearish'): return True
    return False

v12 = [t for t in raw if passes_v12fix(t)]
tsrt_v12 = [t for t in v12 if tsrt_scope(t)]
print(f"TSRT-scope V12-fix (same period): {len(tsrt_v12)}")

# V13 apply (simplified — just get the flag, reuse logic)
def get_v13_gex(ts, spot):
    cur.execute("SELECT columns, rows FROM chain_snapshots WHERE ts <= %s AND ts >= %s - interval '3 minutes' AND spot IS NOT NULL ORDER BY ts DESC LIMIT 1", (ts, ts))
    r = cur.fetchone()
    if not r: return 0.0
    cols, rows = r
    try:
        s_i = cols.index('Strike'); c_oi = cols.index('Open Int'); c_g = cols.index('Gamma')
        p_g = cols.index('Gamma', c_g+1); p_oi = cols.index('Open Int', c_oi+1)
        mg = 0
        for row in rows:
            s = row[s_i]
            if s is None or float(s) <= float(spot): continue
            ng = float(row[c_g] or 0)*float(row[c_oi] or 0) - float(row[p_g] or 0)*float(row[p_oi] or 0)
            if ng > mg: mg = ng
        return mg
    except: return 0.0

def get_v13_dd(ts, spot):
    cur.execute("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT MAX(ABS(value::float)) FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc=(SELECT mts FROM lts) AND ABS(strike::float - %s) <= 10
    """, (ts, ts, float(spot)))
    r = cur.fetchone()
    return float(r[0]) if r and r[0] else 0.0

def get_vanna(ts, spot):
    cur.execute("""
    WITH lts AS (SELECT MAX(ts_utc) as mts FROM volland_exposure_points
                 WHERE greek='vanna' AND expiration_option='THIS_WEEK'
                   AND ts_utc <= %s AND ts_utc >= %s - interval '15 minutes')
    SELECT strike, value FROM volland_exposure_points
    WHERE greek='vanna' AND expiration_option='THIS_WEEK'
      AND ts_utc=(SELECT mts FROM lts) ORDER BY strike
    """, (ts, ts))
    pts = cur.fetchall()
    if not pts: return None, None
    near = [(float(s), float(v)) for s, v in pts if abs(float(s) - float(spot)) <= 50]
    if len(near) < 2: return None, None
    s0 = sorted(near); cr = []
    for i in range(1, len(s0)):
        x0, v0 = s0[i-1]; x1, v1 = s0[i]
        if (v0 > 0 and v1 < 0) or (v0 < 0 and v1 > 0):
            if v1-v0 != 0: cr.append(x0 + (-v0/(v1-v0))*(x1-x0))
    cs = None
    if cr:
        nearest = min(cr, key=lambda s: abs(s - float(spot)))
        cs = 'A' if nearest > float(spot) else 'B'
    pk = max(near, key=lambda x: abs(x[1]))[0]
    ps = 'A' if pk > float(spot) else 'B'
    return cs, ps

def v13_block(gex, dd, vc, vp, setup, dirx):
    if dirx in ('short','bearish') and setup in ('Skew Charm','DD Exhaustion'):
        if gex >= 75: return True
        if dd >= 3_000_000_000: return True
    if vc is not None:
        if dirx in ('short','bearish'):
            if setup == 'DD Exhaustion' and vc == 'A': return True
            if setup == 'Skew Charm' and vc == 'A' and vp == 'B': return True
            if setup == 'AG Short' and vc == 'B' and vp == 'A': return True
        if dirx in ('long','bullish'):
            if setup == 'Skew Charm' and vc == 'A' and vp == 'B': return True
    return False

print("Computing V13 on TSRT-scope signals...", flush=True)
v13_theo = []
for i, t in enumerate(tsrt_v12):
    gx = get_v13_gex(t[1], t[5])
    dd = get_v13_dd(t[1], t[5])
    vc, vp = get_vanna(t[1], t[5])
    if not v13_block(gx, dd, vc, vp, t[2], t[11]):
        v13_theo.append(t)
    if (i+1) % 40 == 0: print(f"  {i+1}/{len(tsrt_v12)}", flush=True)

theo_pnl = sum(float(t[7] or 0) for t in v13_theo)
print(f"\nTheoretical V13 TSRT ({start} to {end}):")
print(f"  Signals: {len(v13_theo)}, PnL: {theo_pnl:+.1f} pts")

# Slot-capped theoretical (1-slot per direction)
def simulate(signals, slot=1):
    active = []
    fired = []; skipped = []
    for t in signals:
        active = [(e, i) for (e, i) in active if e > t[1]]
        if len(active) < slot:
            # Use 60min default if no elapsed data available here
            end_ts = t[1] + timedelta(minutes=60)
            active.append((end_ts, t[0]))
            fired.append(t)
        else:
            skipped.append(t)
    return fired, skipped

longs_t = [t for t in v13_theo if t[11] in ('long','bullish')]
shorts_t = [t for t in v13_theo if t[11] in ('short','bearish')]
lf, ls = simulate(longs_t, 1)
sf, ss = simulate(shorts_t, 1)
theo_cap_fired = lf + sf
theo_cap_skipped = ls + ss
theo_cap_pnl = sum(float(t[7] or 0) for t in theo_cap_fired)
print(f"\nSlot-capped theoretical (1-slot per direction):")
print(f"  Fired: {len(theo_cap_fired)}, PnL: {theo_cap_pnl:+.1f}")
print(f"  Skipped: {len(theo_cap_skipped)}, missed PnL: {sum(float(t[7] or 0) for t in theo_cap_skipped):+.1f}")

# GAP ANALYSIS
print("\n" + "="*70)
print("GAP ANALYSIS: Actual TSRT vs Theoretical V13 (slot-capped, same period)")
print("="*70)
print(f"Actual SPX-PnL:             {total_actual_pnl:+.1f} pts ({len(closed)} trades) ${total_actual_usd:+.0f}")
print(f"Theoretical V13 (no cap):   {theo_pnl:+.1f} pts ({len(v13_theo)} trades)")
print(f"Theoretical V13 (1-cap):    {theo_cap_pnl:+.1f} pts ({len(theo_cap_fired)} trades)")
print(f"GAP vs 1-cap theoretical:   {total_actual_pnl - theo_cap_pnl:+.1f} pts")
print(f"GAP vs no-cap theoretical:  {total_actual_pnl - theo_pnl:+.1f} pts")

# Get matched trades (actual + theo)
actual_ids = {a['setup_log_id'] for a in closed}
theo_ids = {t[0] for t in theo_cap_fired}
matched = actual_ids & theo_ids
actual_only = actual_ids - theo_ids
theo_only = theo_ids - actual_ids
print(f"\nMatched setup_log_ids (both actual+theo): {len(matched)}")
print(f"Actual only (shouldn't have traded per V13): {len(actual_only)}")
print(f"Theo only (should have traded but didn't):   {len(theo_only)}")

# Compare per-matched trade: actual SPX PnL vs setup_log outcome_pnl
print("\nMATCHED TRADE COMPARISON (actual fill-PnL vs portal outcome_pnl):")
actual_idx = {a['setup_log_id']: a for a in closed}
theo_idx = {t[0]: t for t in v13_theo}
matched_actual = 0; matched_theo = 0; big_diffs = []
for tid in matched:
    a = actual_idx[tid]; t = theo_idx[tid]
    matched_actual += a['spx_pnl']
    t_pnl = float(t[7] or 0)
    matched_theo += t_pnl
    if abs(a['spx_pnl'] - t_pnl) > 3:
        big_diffs.append((str(a['d']), a['setup'], a['direction'], a['spx_pnl'], t_pnl, a['close_reason']))
print(f"  Matched actual sum: {matched_actual:+.1f}")
print(f"  Matched theo sum:   {matched_theo:+.1f}")
print(f"  Per-trade variance: {matched_actual - matched_theo:+.1f}")
print(f"  Big (>3pt) discrepancies: {len(big_diffs)}")
for bd in big_diffs[:10]:
    print(f"    {bd}")

# Save
summary = {
    'period': {'start': str(start), 'end': str(end)},
    'actual': {
        'n': len(closed), 'spx_pnl': total_actual_pnl,
        'usd_at_1mes': total_actual_usd,
        'by_setup': {f"{s}_{d}": {'n': len(lst), 'pnl': sum(a['spx_pnl'] for a in lst)} for (s,d), lst in by_setup.items()},
        'close_reasons': dict(reasons),
    },
    'theo_nocap': {'n': len(v13_theo), 'pnl': theo_pnl},
    'theo_1cap': {'n': len(theo_cap_fired), 'pnl': theo_cap_pnl, 'skipped': len(theo_cap_skipped)},
    'gaps': {
        'actual_vs_1cap': total_actual_pnl - theo_cap_pnl,
        'actual_vs_nocap': total_actual_pnl - theo_pnl,
    },
    'matched_count': len(matched),
    'actual_only_count': len(actual_only),
    'theo_only_count': len(theo_only),
    'matched_actual_pnl': matched_actual,
    'matched_theo_pnl': matched_theo,
    'big_discrepancies': [{'date':bd[0],'setup':bd[1],'dir':bd[2],'actual':bd[3],'theo':bd[4],'reason':bd[5]} for bd in big_diffs],
}
with open('_tsrt_actual.json', 'w') as f:
    json.dump(summary, f, indent=2, default=str)
print("\nSaved to _tsrt_actual.json")
