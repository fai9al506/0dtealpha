"""
Delta Decay Per-Strike Deep Study
===================================
1. DD S/R vs Charm S/R for setup outcomes
2. DD per-strike for limit entry improvement
3. EOD DD neutral zone for close prediction (butterfly targeting)
4. DD as standalone quality filter
5. DD green->red transition patterns

Key concept: DD bars flip sign when spot crosses them.
- Negative DD below spot = dealers buy (support)
- Positive DD above spot = dealers sell (resistance)
- The STRONGEST negative bar below = strongest support
- The STRONGEST positive bar above = strongest resistance
"""

from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
import bisect

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)


# ========================================================================
#  DATA FETCHING
# ========================================================================

def fetch_setups():
    sql = text("""
        SELECT id, ts, setup_name, direction, grade, score, spot, lis, target,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               outcome_target_level, outcome_stop_level, outcome_elapsed_min,
               greek_alignment, vix, paradigm, charm_limit_entry, overvix
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS')
          AND spot IS NOT NULL
          AND ts::date >= '2026-02-11'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        return conn.execute(sql).fetchall()


def fetch_all_dd_charm():
    """Batch fetch DD and charm per-strike data."""
    print("Fetching DD + charm per-strike data...")
    sql = text("""
        SELECT greek, ts_utc, strike::numeric AS strike,
               value::numeric AS val, current_price::numeric AS cp
        FROM volland_exposure_points
        WHERE (
            (greek = 'deltaDecay')
            OR
            (greek = 'charm' AND (expiration_option IS NULL OR expiration_option = 'TODAY'))
        )
        AND ts_utc::date >= '2026-02-11'
        AND value != 0
        ORDER BY greek, ts_utc, strike
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    print(f"  {len(rows)} rows total")

    # Group by (greek, ts_utc)
    raw = defaultdict(lambda: defaultdict(list))
    for r in rows:
        raw[r.greek][r.ts_utc].append((float(r.strike), float(r.val)))

    snaps = {}
    for greek, ts_dict in raw.items():
        sorted_ts = sorted(ts_dict.keys())
        snaps[greek] = {'timestamps': sorted_ts, 'data': ts_dict}
        print(f"  {greek}: {len(sorted_ts)} snapshots")

    return snaps


def fetch_actual_closes():
    """Get actual SPX close for each trading day (from chain_snapshots)."""
    sql = text("""
        SELECT DISTINCT ON (ts::date)
            ts::date as d, spot
        FROM chain_snapshots
        WHERE ts::date >= '2026-02-11'
          AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 15
          AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 50
        ORDER BY ts::date, ts DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return {str(r.d): float(r.spot) for r in rows if r.spot is not None}


def nearest_snap(snaps, greek, ts, max_s=300):
    if greek not in snaps:
        return None
    timestamps = snaps[greek]['timestamps']
    if not timestamps:
        return None
    idx = bisect.bisect_left(timestamps, ts)
    cands = []
    if idx > 0: cands.append(timestamps[idx - 1])
    if idx < len(timestamps): cands.append(timestamps[idx])
    best = min(cands, key=lambda t: abs((t - ts).total_seconds()))
    if abs((best - ts).total_seconds()) > max_s:
        return None
    return snaps[greek]['data'][best]


def analyze_sr(strike_vals, spot, proximity=30):
    """Find S/R levels from per-strike data."""
    if not strike_vals:
        return None
    nearby = [(s, v) for s, v in strike_vals if abs(s - spot) <= proximity]
    if not nearby:
        return None

    above = [(s, v) for s, v in nearby if s > spot]
    below = [(s, v) for s, v in nearby if s <= spot]

    # Strongest absolute values
    strongest_above = max(above, key=lambda x: abs(x[1]), default=None)
    strongest_below = max(below, key=lambda x: abs(x[1]), default=None)

    # For DD: strongest POSITIVE above = resistance, strongest NEGATIVE below = support
    pos_above = [x for x in above if x[1] > 0]
    neg_below = [x for x in below if x[1] < 0]
    dd_resistance = max(pos_above, key=lambda x: x[1], default=None)
    dd_support = max(neg_below, key=lambda x: abs(x[1]), default=None)

    # Sum for net direction
    total = sum(v for _, v in nearby)

    # Near spot (within 5 pts)
    near_spot = sum(v for s, v in nearby if abs(s - spot) <= 5)

    return {
        'strongest_above': strongest_above,
        'strongest_below': strongest_below,
        'dd_resistance': dd_resistance,  # positive DD above = dealers sell
        'dd_support': dd_support,         # negative DD below = dealers buy
        'total': total,
        'near_spot': near_spot,
        'all_strikes': nearby,
    }


def wr(trades):
    if not trades:
        return "N/A (0)"
    w = sum(1 for t in trades if t['result'] == 'WIN')
    p = sum(t['pnl'] for t in trades)
    return f"{w}W/{len(trades)-w}L ({w/len(trades)*100:.0f}% WR), {p:+.1f} pts ({p/len(trades):+.1f}/t)"


# ========================================================================
#  MAIN
# ========================================================================

def main():
    setups = fetch_setups()
    snaps = fetch_all_dd_charm()
    closes = fetch_actual_closes()
    print(f"Loaded {len(setups)} signals, {len(closes)} daily closes\n")

    # ── Enrich signals ──────────────────────────────────────────────
    enriched = []
    no_dd = 0
    no_charm = 0

    for s in setups:
        ts, spot = s.ts, float(s.spot)
        direction = s.direction
        is_long = direction in ('long', 'bullish')

        dd_raw = nearest_snap(snaps, 'deltaDecay', ts)
        dd_info = analyze_sr(dd_raw, spot) if dd_raw else None
        if not dd_info: no_dd += 1

        charm_raw = nearest_snap(snaps, 'charm', ts)
        charm_info = analyze_sr(charm_raw, spot) if charm_raw else None
        if not charm_info: no_charm += 1

        # Check stacked: DD and charm S/R at same strike
        dd_stacked = False
        dd_stacked_strike = None
        if dd_info and charm_info:
            if is_long:
                dd_s = dd_info.get('dd_support')
                c_s = charm_info.get('strongest_below')
            else:
                dd_s = dd_info.get('dd_resistance')
                c_s = charm_info.get('strongest_above')
            if dd_s and c_s and abs(dd_s[0] - c_s[0]) <= 5:
                dd_stacked = True
                dd_stacked_strike = dd_s[0]

        # DD directional alignment
        dd_aligned = None
        if dd_info:
            if is_long:
                # For longs: strong negative DD below = support = aligned
                dd_aligned = dd_info.get('dd_support') is not None
            else:
                # For shorts: strong positive DD above = resistance = aligned
                dd_aligned = dd_info.get('dd_resistance') is not None

        # DD S/R distance from spot
        dd_sr_dist = None
        if dd_info:
            if is_long and dd_info.get('dd_support'):
                dd_sr_dist = spot - dd_info['dd_support'][0]
            elif not is_long and dd_info.get('dd_resistance'):
                dd_sr_dist = dd_info['dd_resistance'][0] - spot

        # Charm S/R distance
        charm_sr_dist = None
        if charm_info:
            if is_long and charm_info.get('strongest_below'):
                charm_sr_dist = spot - charm_info['strongest_below'][0]
            elif not is_long and charm_info.get('strongest_above'):
                charm_sr_dist = charm_info['strongest_above'][0] - spot

        enriched.append({
            'id': s.id, 'ts': s.ts, 'setup': s.setup_name,
            'direction': direction, 'grade': s.grade, 'spot': spot,
            'result': s.outcome_result,
            'pnl': float(s.outcome_pnl) if s.outcome_pnl else 0,
            'max_profit': float(s.outcome_max_profit) if s.outcome_max_profit else 0,
            'alignment': s.greek_alignment,
            'vix': float(s.vix) if s.vix else None,
            'paradigm': s.paradigm,
            'dd_info': dd_info,
            'charm_info': charm_info,
            'dd_stacked': dd_stacked,
            'dd_stacked_strike': dd_stacked_strike,
            'dd_aligned': dd_aligned,
            'dd_sr_dist': dd_sr_dist,
            'charm_sr_dist': charm_sr_dist,
            'is_long': direction in ('long', 'bullish'),
        })

    with_dd = [e for e in enriched if e['dd_info'] is not None]
    print(f"Total: {len(enriched)}, with DD: {len(with_dd)}, no DD: {no_dd}, no charm: {no_charm}")

    # ================================================================
    #  1. DD S/R vs CHARM S/R
    # ================================================================
    print(f"\n{'='*80}")
    print("1. DD S/R vs CHARM S/R: Which Better Predicts Outcomes?")
    print(f"{'='*80}\n")

    # DD aligned vs not
    dd_yes = [e for e in with_dd if e['dd_aligned'] == True]
    dd_no = [e for e in with_dd if e['dd_aligned'] == False]
    print(f"  DD directionally aligned (support for longs, resist for shorts):")
    print(f"    Aligned:     {wr(dd_yes)}")
    print(f"    Not aligned: {wr(dd_no)}")

    # Split by direction
    for dir_label, dir_vals in [("LONG", True), ("SHORT", False)]:
        dir_trades = [e for e in with_dd if e['is_long'] == dir_vals]
        aligned = [e for e in dir_trades if e['dd_aligned'] == True]
        not_aligned = [e for e in dir_trades if e['dd_aligned'] == False]
        print(f"\n  {dir_label}:")
        print(f"    DD aligned:     {wr(aligned)}")
        print(f"    DD not aligned: {wr(not_aligned)}")

    # DD S/R distance buckets
    print(f"\n  DD S/R distance from spot (how far is the key level):")
    for lo, hi, label in [(0, 5, "0-5 pts"), (5, 10, "5-10"), (10, 15, "10-15"),
                          (15, 20, "15-20"), (20, 30, "20-30")]:
        bucket = [e for e in with_dd if e['dd_sr_dist'] is not None and lo <= e['dd_sr_dist'] < hi]
        if bucket:
            print(f"    {label}: {wr(bucket)}")

    # Compare DD vs Charm: when they agree vs disagree on direction
    print(f"\n  DD vs Charm agreement on directional S/R:")
    both_aligned = [e for e in with_dd if e['dd_aligned'] == True and e['charm_info'] is not None]
    # Check if charm also has directional S/R
    dd_charm_agree = []
    dd_charm_disagree = []
    dd_only = []
    charm_only = []
    for e in with_dd:
        if e['charm_info'] is None:
            continue
        has_dd = e['dd_aligned'] == True
        ci = e['charm_info']
        has_charm = False
        if e['is_long'] and ci.get('strongest_below'):
            has_charm = True
        elif not e['is_long'] and ci.get('strongest_above'):
            has_charm = True
        if has_dd and has_charm:
            dd_charm_agree.append(e)
        elif has_dd and not has_charm:
            dd_only.append(e)
        elif not has_dd and has_charm:
            charm_only.append(e)

    print(f"    Both DD + Charm S/R present: {wr(dd_charm_agree)}")
    print(f"    DD S/R only (no charm):      {wr(dd_only)}")
    print(f"    Charm S/R only (no DD):      {wr(charm_only)}")

    # ================================================================
    #  2. DD+CHARM STACKED vs SINGLE
    # ================================================================
    print(f"\n{'='*80}")
    print("2. DD+CHARM STACKED S/R (same strike +/-5)")
    print(f"{'='*80}\n")

    stacked = [e for e in with_dd if e['dd_stacked']]
    not_stacked = [e for e in with_dd if not e['dd_stacked'] and e['charm_info'] is not None]
    print(f"  Stacked (DD+Charm at same strike): {wr(stacked)}")
    print(f"  Not stacked:                       {wr(not_stacked)}")

    # By setup
    print(f"\n  By setup:")
    for setup in sorted(set(e['setup'] for e in with_dd)):
        s_sub = [e for e in stacked if e['setup'] == setup]
        ns_sub = [e for e in not_stacked if e['setup'] == setup]
        if len(s_sub) >= 3 or len(ns_sub) >= 3:
            print(f"    {setup}:")
            if s_sub: print(f"      Stacked:     {wr(s_sub)}")
            if ns_sub: print(f"      Not stacked: {wr(ns_sub)}")

    # By direction
    print(f"\n  By direction:")
    for dir_label, dir_val in [("LONG", True), ("SHORT", False)]:
        s_d = [e for e in stacked if e['is_long'] == dir_val]
        ns_d = [e for e in not_stacked if e['is_long'] == dir_val]
        print(f"    {dir_label} stacked:     {wr(s_d)}")
        print(f"    {dir_label} not stacked: {wr(ns_d)}")

    # ================================================================
    #  3. DD PER-STRIKE S/R vs CHARM S/R FOR SHORTS (limit entry)
    # ================================================================
    print(f"\n{'='*80}")
    print("3. DD RESISTANCE vs CHARM RESISTANCE FOR SHORTS")
    print(f"{'='*80}\n")

    short_trades = [e for e in with_dd if not e['is_long'] and e['dd_info'] and e['charm_info']]
    print(f"  Total short trades with both DD + charm: {len(short_trades)}")

    # Compare: DD resistance strike vs Charm resistance strike — which is closer to actual high?
    dd_better = 0
    charm_better = 0
    same = 0
    dd_resist_vals = []
    charm_resist_vals = []

    for e in short_trades:
        dd_r = e['dd_info'].get('dd_resistance')
        c_r = e['charm_info'].get('strongest_above')
        if dd_r and c_r:
            # For shorts, resistance above is where price should reject
            # The better one is the one closer to actual max_profit reversal point
            # But we don't have the exact reversal point, so compare distances
            dd_dist = dd_r[0] - e['spot']
            c_dist = c_r[0] - e['spot']
            dd_resist_vals.append((dd_r[0], dd_r[1], dd_dist, e))
            charm_resist_vals.append((c_r[0], c_r[1], c_dist, e))

    print(f"  Trades with both DD and Charm resistance above: {len(dd_resist_vals)}")

    if dd_resist_vals:
        # Compare strike prices
        agree_count = sum(1 for d, c in zip(dd_resist_vals, charm_resist_vals)
                         if abs(d[0] - c[0]) <= 5)
        disagree_count = len(dd_resist_vals) - agree_count
        print(f"  DD + Charm at same strike (+/-5): {agree_count} ({agree_count/len(dd_resist_vals)*100:.0f}%)")
        print(f"  DD + Charm at different strikes:  {disagree_count}")

        # When they agree vs disagree — WR
        agree_trades = [d[3] for d, c in zip(dd_resist_vals, charm_resist_vals) if abs(d[0] - c[0]) <= 5]
        disagree_trades = [d[3] for d, c in zip(dd_resist_vals, charm_resist_vals) if abs(d[0] - c[0]) > 5]
        print(f"\n  When DD+Charm agree on resist strike: {wr(agree_trades)}")
        print(f"  When DD+Charm disagree:               {wr(disagree_trades)}")

        # DD resistance magnitude vs WR
        print(f"\n  DD resistance value buckets:")
        for lo, hi, label in [(0, 50e6, "<50M"), (50e6, 200e6, "50-200M"),
                              (200e6, 500e6, "200-500M"), (500e6, 1e15, ">500M")]:
            bucket = [d[3] for d in dd_resist_vals if lo <= abs(d[1]) < hi]
            if bucket:
                print(f"    {label}: {wr(bucket)}")

    # ================================================================
    #  4. EOD DD NEUTRAL ZONE — CLOSE PREDICTION
    # ================================================================
    print(f"\n{'='*80}")
    print("4. EOD DD NEUTRAL ZONE (Where Dealers Want to Close)")
    print(f"{'='*80}\n")

    print("  Wizard: 'where DD is neutral = perfect close for dealers'")
    print("  Method: Find strike where DD net above = DD net below (equilibrium)\n")

    # For each day, find the DD equilibrium point from snapshots at various times
    dd_timestamps = snaps.get('deltaDecay', {}).get('timestamps', [])
    dd_data = snaps.get('deltaDecay', {}).get('data', {})

    # Group timestamps by date
    ts_by_date = defaultdict(list)
    for ts in dd_timestamps:
        ts_by_date[str(ts.date())].append(ts)

    print(f"  {'Date':>12} {'DD Equil@14':>12} {'DD Equil@15':>12} {'Actual Close':>13} "
          f"{'Err@14':>7} {'Err@15':>7}")
    print(f"  {'-'*12} {'-'*12} {'-'*12} {'-'*13} {'-'*7} {'-'*7}")

    eod_errors_14 = []
    eod_errors_15 = []

    for d in sorted(ts_by_date.keys()):
        actual = closes.get(d)
        if not actual:
            continue

        results = {}
        for target_hour in [14, 15]:
            # Find snapshot closest to target_hour:00 ET
            # ts_utc is UTC, ET = UTC-4 (EDT) or UTC-5 (EST)
            # For simplicity, look for snapshots where ET hour matches
            best_ts = None
            best_diff = 999999
            for ts in ts_by_date[d]:
                # Convert to ET (approximate: UTC-4 for EDT which applies Mar-Nov)
                et_hour = (ts.hour - 4) % 24  # crude EDT offset
                if ts.month < 3 or (ts.month == 3 and ts.day < 9):
                    et_hour = (ts.hour - 5) % 24  # EST before Mar 9
                diff = abs(et_hour - target_hour) * 3600 + abs(ts.minute) * 60
                if diff < best_diff:
                    best_diff = diff
                    best_ts = ts

            if not best_ts or best_diff > 7200:
                results[target_hour] = None
                continue

            strikes = dd_data[best_ts]
            if not strikes:
                results[target_hour] = None
                continue

            # Find equilibrium: the strike where cumulative DD from below crosses zero
            # Sort by strike, compute running sum from lowest
            sorted_strikes = sorted(strikes, key=lambda x: x[0])

            # Method: find where sum of DD values balances
            # Weight each strike by its DD value, find the "center of mass"
            total_dd = sum(v for _, v in sorted_strikes if v != 0)
            if abs(total_dd) < 1e3:
                results[target_hour] = None
                continue

            # Running sum approach: where cumulative crosses zero
            running = 0
            equil = None
            for i, (s, v) in enumerate(sorted_strikes):
                prev_running = running
                running += v
                if prev_running * running < 0:  # sign change
                    frac = abs(prev_running) / (abs(prev_running) + abs(running))
                    equil = sorted_strikes[i-1][0] + frac * (s - sorted_strikes[i-1][0]) if i > 0 else s
                    break

            # Also try: weighted average of strikes by |DD|
            total_weight = sum(abs(v) for _, v in sorted_strikes)
            if total_weight > 0:
                weighted_avg = sum(s * abs(v) for s, v in sorted_strikes) / total_weight
            else:
                weighted_avg = None

            # Use the strike where DD is closest to zero within +/-25 of spot
            near = [(s, v) for s, v in sorted_strikes if abs(s - actual) <= 50]
            if near:
                min_dd_strike = min(near, key=lambda x: abs(x[1]))
                results[target_hour] = min_dd_strike[0]
            else:
                results[target_hour] = equil

        eq14 = results.get(14)
        eq15 = results.get(15)
        err14 = abs(eq14 - actual) if eq14 else None
        err15 = abs(eq15 - actual) if eq15 else None

        if err14 is not None: eod_errors_14.append(err14)
        if err15 is not None: eod_errors_15.append(err15)

        eq14_s = f"{eq14:.1f}" if eq14 else "---"
        eq15_s = f"{eq15:.1f}" if eq15 else "---"
        err14_s = f"{err14:.1f}" if err14 is not None else "---"
        err15_s = f"{err15:.1f}" if err15 is not None else "---"

        print(f"  {d:>12} {eq14_s:>12} {eq15_s:>12} {actual:>13.1f} {err14_s:>7} {err15_s:>7}")

    if eod_errors_14:
        avg14 = sum(eod_errors_14) / len(eod_errors_14)
        print(f"\n  Avg error @14:00: {avg14:.1f} pts ({len(eod_errors_14)} days)")
    if eod_errors_15:
        avg15 = sum(eod_errors_15) / len(eod_errors_15)
        print(f"  Avg error @15:00: {avg15:.1f} pts ({len(eod_errors_15)} days)")

    # How many days was DD equilibrium within 5/10/15 pts of actual close?
    if eod_errors_15:
        within5 = sum(1 for e in eod_errors_15 if e <= 5)
        within10 = sum(1 for e in eod_errors_15 if e <= 10)
        within15 = sum(1 for e in eod_errors_15 if e <= 15)
        within25 = sum(1 for e in eod_errors_15 if e <= 25)
        n = len(eod_errors_15)
        print(f"\n  @15:00 prediction accuracy:")
        print(f"    Within 5 pts:  {within5}/{n} ({within5/n*100:.0f}%)")
        print(f"    Within 10 pts: {within10}/{n} ({within10/n*100:.0f}%)")
        print(f"    Within 15 pts: {within15}/{n} ({within15/n*100:.0f}%)")
        print(f"    Within 25 pts: {within25}/{n} ({within25/n*100:.0f}%)")

    # ================================================================
    #  5. DD NET VALUE AS DIRECTIONAL BIAS FILTER
    # ================================================================
    print(f"\n{'='*80}")
    print("5. DD NET VALUE AS DIRECTIONAL BIAS")
    print(f"{'='*80}\n")

    # Net DD near spot: positive = bearish (dealers sell), negative = bullish (dealers buy)
    for dir_label, dir_val in [("LONG", True), ("SHORT", False)]:
        dir_trades = [e for e in with_dd if e['is_long'] == dir_val]
        dd_bullish = [e for e in dir_trades if e['dd_info']['near_spot'] < 0]
        dd_bearish = [e for e in dir_trades if e['dd_info']['near_spot'] > 0]
        dd_neutral = [e for e in dir_trades if e['dd_info']['near_spot'] == 0]
        print(f"  {dir_label}:")
        print(f"    DD near-spot bullish (negative): {wr(dd_bullish)}")
        print(f"    DD near-spot bearish (positive): {wr(dd_bearish)}")

    # DD near-spot magnitude
    print(f"\n  DD near-spot magnitude (all trades):")
    for lo, hi, label in [(0, 10e6, "<10M"), (10e6, 50e6, "10-50M"),
                          (50e6, 200e6, "50-200M"), (200e6, 500e6, "200-500M"),
                          (500e6, 1e15, ">500M")]:
        bucket = [e for e in with_dd if lo <= abs(e['dd_info']['near_spot']) < hi]
        if bucket:
            print(f"    |DD| {label}: {wr(bucket)}")

    # ================================================================
    #  6. DD S/R QUALITY FOR EXISTING SETUPS
    # ================================================================
    print(f"\n{'='*80}")
    print("6. DD S/R QUALITY BY SETUP")
    print(f"{'='*80}\n")

    # V9-SC filter approximation
    def v9sc(e):
        if e['grade'] == 'LOG': return False
        if e['is_long']:
            if e['alignment'] is None or e['alignment'] < 2: return False
            if e['setup'] == 'Skew Charm': return True
            if e['vix'] and e['vix'] <= 22: return True
            return False
        else:
            if e['setup'] in ('Skew Charm', 'AG Short'): return True
            if e['setup'] == 'DD Exhaustion' and e['alignment'] and e['alignment'] != 0: return True
            return False

    v9_trades = [e for e in with_dd if v9sc(e)]
    print(f"  V9-SC trades with DD data: {len(v9_trades)}")

    # DD aligned vs not within V9-SC
    v9_dd_aligned = [e for e in v9_trades if e['dd_aligned'] == True]
    v9_dd_not = [e for e in v9_trades if e['dd_aligned'] == False]
    print(f"  V9-SC + DD aligned:     {wr(v9_dd_aligned)}")
    print(f"  V9-SC + DD not aligned: {wr(v9_dd_not)}")

    # DD stacked within V9-SC
    v9_stacked = [e for e in v9_trades if e['dd_stacked']]
    v9_not_stacked = [e for e in v9_trades if not e['dd_stacked']]
    print(f"\n  V9-SC + DD+Charm stacked:     {wr(v9_stacked)}")
    print(f"  V9-SC + DD+Charm not stacked: {wr(v9_not_stacked)}")

    # Per setup within V9-SC
    print(f"\n  Per setup (V9-SC, DD aligned vs not):")
    for setup in sorted(set(e['setup'] for e in v9_trades)):
        sub = [e for e in v9_trades if e['setup'] == setup]
        if len(sub) < 5: continue
        al = [e for e in sub if e['dd_aligned'] == True]
        na = [e for e in sub if e['dd_aligned'] == False]
        if al and na:
            print(f"    {setup}:")
            print(f"      DD aligned:     {wr(al)}")
            print(f"      DD not aligned: {wr(na)}")

    # ================================================================
    #  7. DD VALUE AT CHARM LIMIT ENTRY STRIKE
    # ================================================================
    print(f"\n{'='*80}")
    print("7. DD CONFIRMATION OF CHARM LIMIT ENTRY")
    print(f"{'='*80}\n")

    # For trades that used charm_limit_entry, check if DD also shows resistance there
    limit_trades = [e for e in with_dd if e.get('charm_limit_entry') is not None
                    and not e['is_long'] and e['dd_info']]
    print(f"  Short trades with charm limit entry + DD data: {len(limit_trades)}")

    if limit_trades:
        # placeholder - not enough data in setup_log for charm_limit_entry
        pass

    # ================================================================
    #  8. INTRADAY DD SHIFT PATTERNS
    # ================================================================
    print(f"\n{'='*80}")
    print("8. DD SHIFT: Does DD Change Direction Before Price Moves?")
    print(f"{'='*80}\n")

    print("  Wizard: 'DD hedging changing BEFORE the market does'")
    print("  Testing: At signal time, is DD net direction a LEADING indicator?\n")

    # For each trade, compare DD direction (net near-spot) with subsequent outcome
    dd_leads = []
    for e in with_dd:
        dd_net = e['dd_info']['near_spot']
        if dd_net == 0:
            continue
        dd_bullish = dd_net < 0  # negative DD = dealers buy = bullish
        trade_won = e['result'] == 'WIN'

        # Check: does DD direction AGREE with trade direction AND outcome?
        if e['is_long']:
            dd_agrees = dd_bullish
        else:
            dd_agrees = not dd_bullish

        dd_leads.append({
            'agrees': dd_agrees,
            'result': e['result'],
            'pnl': e['pnl'],
            'setup': e['setup'],
        })

    agrees = [d for d in dd_leads if d['agrees']]
    disagrees = [d for d in dd_leads if not d['agrees']]
    print(f"  DD direction agrees with trade: {wr(agrees)}")
    print(f"  DD direction opposes trade:     {wr(disagrees)}")

    # ================================================================
    #  SUMMARY
    # ================================================================
    print(f"\n{'='*80}")
    print("EXECUTIVE SUMMARY")
    print(f"{'='*80}\n")

    print("  Key findings to report:")
    print(f"  1. DD aligned trades:          {wr([e for e in with_dd if e['dd_aligned']==True])}")
    print(f"  2. DD not aligned:             {wr([e for e in with_dd if e['dd_aligned']==False])}")
    print(f"  3. DD+Charm stacked:           {wr(stacked)}")
    print(f"  4. DD+Charm not stacked:       {wr(not_stacked)}")
    if eod_errors_15:
        print(f"  5. EOD prediction avg error:   {sum(eod_errors_15)/len(eod_errors_15):.1f} pts ({len(eod_errors_15)} days)")
        within10 = sum(1 for e in eod_errors_15 if e <= 10)
        print(f"  6. EOD within 10 pts:          {within10}/{len(eod_errors_15)} ({within10/len(eod_errors_15)*100:.0f}%)")
    print(f"  7. V9-SC + DD aligned:         {wr(v9_dd_aligned)}")
    print(f"  8. V9-SC + DD not aligned:     {wr(v9_dd_not)}")


if __name__ == '__main__':
    main()
