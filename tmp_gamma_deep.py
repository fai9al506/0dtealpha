"""
Gamma Deep Analysis
====================
Builds on tmp_gamma_confluence.py batch-fetch approach.
Answers 7 specific deep-dive questions about gamma confluence.
"""

from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import datetime, timedelta
import bisect

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)

GAMMA_TIMEFRAMES = ['TODAY', 'THIS_WEEK', 'THIRTY_NEXT_DAYS', 'ALL']
PROXIMITY = 30  # strikes within +/-30 pts of spot


# ============================================================================
#  DATA FETCHING (same batch approach as tmp_gamma_confluence.py)
# ============================================================================

def fetch_setups():
    sql = text("""
        SELECT id, ts, setup_name, direction, grade, score, spot, lis, target,
               max_plus_gex, max_minus_gex, outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss, greek_alignment, vix, paradigm,
               charm_limit_entry, overvix
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS')
          AND spot IS NOT NULL
          AND ts::date >= '2026-02-05'
        ORDER BY ts
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    print(f"Fetched {len(rows)} setup signals with outcomes")
    return rows


def fetch_all_exposure_snapshots():
    """Batch-fetch ALL gamma + charm snapshots."""
    print("Fetching all gamma + charm exposure data (batch)...")
    sql = text("""
        SELECT greek, expiration_option, ts_utc, strike::numeric AS strike,
               value::numeric AS val, current_price::numeric AS cp
        FROM volland_exposure_points
        WHERE (
            (greek = 'gamma' AND expiration_option IN ('TODAY', 'THIS_WEEK', 'THIRTY_NEXT_DAYS', 'ALL'))
            OR
            (greek = 'charm' AND (expiration_option IS NULL OR expiration_option = 'TODAY'))
        )
        AND ts_utc::date >= '2026-02-05'
        AND value != 0
        ORDER BY greek, expiration_option, ts_utc, strike
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    print(f"  Fetched {len(rows)} exposure rows total")

    raw_groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        exp = r.expiration_option or 'TODAY'
        key = (r.greek, exp)
        raw_groups[key][r.ts_utc].append((float(r.strike), float(r.val)))

    snapshots = {}
    for key, ts_dict in raw_groups.items():
        sorted_ts = sorted(ts_dict.keys())
        snapshots[key] = {
            'timestamps': sorted_ts,
            'data': {ts: strikes for ts, strikes in ts_dict.items()},
        }

    for key, snap in snapshots.items():
        print(f"  {key[0]}/{key[1]}: {len(snap['timestamps'])} snapshots")

    return snapshots


def find_nearest_snapshot(snapshots, greek, exp_option, ts, max_delta_s=300):
    key = (greek, exp_option)
    if key not in snapshots:
        return None
    snap = snapshots[key]
    timestamps = snap['timestamps']
    if not timestamps:
        return None
    idx = bisect.bisect_left(timestamps, ts)
    candidates = []
    if idx > 0:
        candidates.append(timestamps[idx - 1])
    if idx < len(timestamps):
        candidates.append(timestamps[idx])
    best_ts = min(candidates, key=lambda t: abs((t - ts).total_seconds()))
    delta = abs((best_ts - ts).total_seconds())
    if delta > max_delta_s:
        return None
    return snap['data'][best_ts]


def analyze_strikes(strike_vals, spot):
    if not strike_vals:
        return None
    above = [(s, v) for s, v in strike_vals if s > spot]
    below = [(s, v) for s, v in strike_vals if s <= spot]
    strongest_above = max(above, key=lambda x: abs(x[1]), default=None)
    strongest_below = max(below, key=lambda x: abs(x[1]), default=None)
    total = sum(v for _, v in strike_vals)
    near_spot = [(s, v) for s, v in strike_vals if abs(s - spot) <= 5]
    near_spot_val = sum(v for _, v in near_spot) if near_spot else 0
    return {
        'strongest_above': strongest_above,
        'strongest_below': strongest_below,
        'total': total,
        'near_spot': near_spot_val,
        'n_strikes': len(strike_vals),
        'all_strikes': strike_vals,
    }


def compute_confluence(charm_info, gamma_info, spot, direction):
    if not charm_info or not gamma_info:
        return {'score': 0, 'flags': []}
    score = 0
    flags = []
    is_long = direction in ('long', 'bullish')

    if charm_info.get('strongest_above') and gamma_info.get('strongest_above'):
        c_strike = charm_info['strongest_above'][0]
        g_strike = gamma_info['strongest_above'][0]
        if abs(c_strike - g_strike) <= 5:
            score += 2
            flags.append(f"CONVERGE_ABOVE@{g_strike:.0f}")
            if not is_long:
                score += 1
                flags.append("RESIST_SHORT")
            else:
                score -= 1
                flags.append("RESIST_LONG")

    if charm_info.get('strongest_below') and gamma_info.get('strongest_below'):
        c_strike = charm_info['strongest_below'][0]
        g_strike = gamma_info['strongest_below'][0]
        if abs(c_strike - g_strike) <= 5:
            score += 2
            flags.append(f"CONVERGE_BELOW@{g_strike:.0f}")
            if is_long:
                score += 1
                flags.append("SUPPORT_LONG")
            else:
                score -= 1
                flags.append("SUPPORT_SHORT")

    gamma_net = gamma_info.get('total', 0)
    if gamma_net > 0 and is_long:
        score += 1
        flags.append("NET_G+_LONG")
    elif gamma_net < 0 and not is_long:
        score += 1
        flags.append("NET_G-_SHORT")

    near = gamma_info.get('near_spot', 0)
    if abs(near) > 1e6:
        if near > 0 and is_long:
            score += 1
            flags.append("SPOT_G+")
        elif near < 0 and not is_long:
            score += 1
            flags.append("SPOT_G-")

    charm_net = charm_info.get('total', 0)
    if charm_net > 0 and is_long:
        score += 1
        flags.append("CHARM+")
    elif charm_net < 0 and not is_long:
        score += 1
        flags.append("CHARM-")

    return {'score': score, 'flags': flags}


# ============================================================================
#  STATS HELPERS
# ============================================================================

def wr_stats(trades):
    if not trades:
        return "N/A (0 trades)"
    wins = sum(1 for t in trades if t['result'] == 'WIN')
    losses = len(trades) - wins
    total_pnl = sum(t['pnl'] for t in trades)
    avg_pnl = total_pnl / len(trades)
    wr = wins / len(trades) * 100
    return f"{wins}W/{losses}L ({wr:.0f}% WR), {total_pnl:+.1f} pts ({avg_pnl:+.1f}/trade)"


def detailed_stats(trades, label=""):
    """Print comprehensive stats for a bucket of trades."""
    if not trades:
        print(f"    {label}: N/A (0 trades)")
        return
    wins = sum(1 for t in trades if t['result'] == 'WIN')
    losses = len(trades) - wins
    total_pnl = sum(t['pnl'] for t in trades)
    avg_pnl = total_pnl / len(trades)
    wr = wins / len(trades) * 100

    # Trading days
    dates = sorted(set(str(t['ts'])[:10] for t in trades))
    n_days = len(dates)
    pnl_per_day = total_pnl / n_days if n_days > 0 else 0
    trades_per_day = len(trades) / n_days if n_days > 0 else 0

    # Max drawdown (running PnL)
    running = 0
    peak = 0
    max_dd = 0
    for t in sorted(trades, key=lambda x: x['ts']):
        running += t['pnl']
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    prefix = f"    {label}: " if label else "    "
    print(f"{prefix}{wins}W/{losses}L ({wr:.0f}% WR), {total_pnl:+.1f} pts total")
    print(f"      {len(trades)} trades over {n_days} days | {trades_per_day:.1f} trades/day | {pnl_per_day:+.1f} pts/day | {avg_pnl:+.1f} pts/trade | MaxDD: {max_dd:.1f}")


def max_drawdown(trades):
    """Return max drawdown value for a list of trades."""
    if not trades:
        return 0
    running = 0
    peak = 0
    max_dd = 0
    for t in sorted(trades, key=lambda x: x['ts']):
        running += t['pnl']
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd
    return max_dd


def is_long(e):
    return e['direction'] in ('long', 'bullish')


def is_short(e):
    return e['direction'] in ('short', 'bearish')


# ============================================================================
#  ENRICHMENT (same as original)
# ============================================================================

def enrich_signals(setups, snapshots):
    print(f"\n{'='*80}")
    print("Enriching signals with gamma+charm data...")
    print(f"{'='*80}\n")

    enriched = []
    no_gamma = 0
    no_charm = 0

    for s in setups:
        ts = s.ts
        spot = float(s.spot)
        direction = s.direction

        charm_strikes = find_nearest_snapshot(snapshots, 'charm', 'TODAY', ts)
        if charm_strikes:
            charm_strikes = [(st, v) for st, v in charm_strikes if abs(st - spot) <= PROXIMITY]
        charm_info = analyze_strikes(charm_strikes, spot) if charm_strikes else None
        if not charm_info:
            no_charm += 1

        gamma_by_tf = {}
        gamma_raw_by_tf = {}
        for tf in GAMMA_TIMEFRAMES:
            g_strikes = find_nearest_snapshot(snapshots, 'gamma', tf, ts)
            if g_strikes:
                g_filtered = [(st, v) for st, v in g_strikes if abs(st - spot) <= PROXIMITY]
            else:
                g_filtered = None
            gamma_raw_by_tf[tf] = g_filtered
            gamma_by_tf[tf] = analyze_strikes(g_filtered, spot) if g_filtered else None

        has_any_gamma = any(v is not None for v in gamma_by_tf.values())
        if not has_any_gamma:
            no_gamma += 1

        confluence_by_tf = {}
        for tf in GAMMA_TIMEFRAMES:
            if gamma_by_tf[tf]:
                confluence_by_tf[tf] = compute_confluence(charm_info, gamma_by_tf[tf], spot, direction)
            else:
                confluence_by_tf[tf] = {'score': 0, 'flags': []}

        best_tf = max(GAMMA_TIMEFRAMES, key=lambda tf: confluence_by_tf[tf]['score'])
        best_score = confluence_by_tf[best_tf]['score']
        tf_agreement = sum(1 for tf in GAMMA_TIMEFRAMES if confluence_by_tf[tf]['score'] > 0)

        # Compute stacked S/R info for TODAY gamma (for deep analysis)
        stacked_info = _compute_stacked_info(charm_info, gamma_by_tf, gamma_raw_by_tf, spot, direction)

        enriched.append({
            'id': s.id, 'ts': s.ts, 'setup': s.setup_name,
            'direction': direction, 'grade': s.grade, 'spot': spot,
            'result': s.outcome_result, 'pnl': float(s.outcome_pnl) if s.outcome_pnl else 0,
            'max_profit': float(s.outcome_max_profit) if s.outcome_max_profit else 0,
            'max_loss': float(s.outcome_max_loss) if s.outcome_max_loss else 0,
            'alignment': s.greek_alignment, 'vix': float(s.vix) if s.vix else None,
            'overvix': float(s.overvix) if s.overvix else None,
            'paradigm': s.paradigm,
            'charm_info': charm_info, 'gamma_by_tf': gamma_by_tf,
            'gamma_raw_by_tf': gamma_raw_by_tf,
            'confluence_by_tf': confluence_by_tf,
            'best_tf': best_tf, 'best_confluence': best_score,
            'tf_agreement': tf_agreement,
            'stacked_info': stacked_info,
        })

    with_gamma = [e for e in enriched if any(e['gamma_by_tf'][tf] is not None for tf in GAMMA_TIMEFRAMES)]
    print(f"Total signals: {len(enriched)}")
    print(f"No gamma data: {no_gamma} ({no_gamma*100/len(enriched):.0f}%)")
    print(f"No charm data: {no_charm} ({no_charm*100/len(enriched):.0f}%)")
    print(f"With gamma:    {len(with_gamma)}")

    return enriched, with_gamma


def _compute_stacked_info(charm_info, gamma_by_tf, gamma_raw_by_tf, spot, direction):
    """Compute detailed stacked S/R information for multiple proximity thresholds."""
    info = {}
    for tf in GAMMA_TIMEFRAMES:
        gi = gamma_by_tf[tf]
        ci = charm_info
        if not gi or not ci:
            info[tf] = None
            continue

        is_lng = direction in ('long', 'bullish')
        if is_lng:
            cs = ci.get('strongest_below')
            gs = gi.get('strongest_below')
        else:
            cs = ci.get('strongest_above')
            gs = gi.get('strongest_above')

        tf_info = {
            'charm_strike': cs,
            'gamma_strike': gs,
            'is_stacked': {},
            'strike_dist_from_spot': None,
            'gamma_value_at_stacked': None,
        }

        if cs and gs:
            gap = abs(cs[0] - gs[0])
            # Test multiple proximity thresholds
            for prox in [3, 5, 10, 15]:
                tf_info['is_stacked'][prox] = gap <= prox

            # Distance from spot to the gamma strike (the S/R level)
            tf_info['strike_dist_from_spot'] = abs(gs[0] - spot)
            tf_info['gamma_value_at_stacked'] = abs(gs[1])

            # Also find the exact gamma value at the charm strike
            # (for correlation analysis)
            raw = gamma_raw_by_tf.get(tf)
            if raw:
                charm_s = cs[0]
                exact_match = [v for s, v in raw if abs(s - charm_s) <= 1]
                if exact_match:
                    tf_info['gamma_at_charm_strike'] = abs(exact_match[0])
                else:
                    tf_info['gamma_at_charm_strike'] = None
            else:
                tf_info['gamma_at_charm_strike'] = None
        else:
            for prox in [3, 5, 10, 15]:
                tf_info['is_stacked'][prox] = False
            tf_info['gamma_at_charm_strike'] = None

        info[tf] = tf_info
    return info


# ============================================================================
#  V9-SC FILTER (replicated for comparison)
# ============================================================================

def passes_v9sc(e):
    """Replicate V9-SC filter logic."""
    setup = e['setup']
    direction = e['direction']
    alignment = e['alignment']
    vix = e['vix']
    overvix = e['overvix']

    if alignment is None:
        return True  # can't evaluate

    is_lng = direction in ('long', 'bullish')

    if is_lng:
        # Longs: alignment >= +2 AND (Skew Charm OR VIX <= 22 OR overvix >= +2)
        if alignment < 2:
            return False
        if setup == 'Skew Charm':
            return True
        if vix is not None and vix <= 22:
            return True
        if overvix is not None and overvix >= 2:
            return True
        return False
    else:
        # Shorts whitelist: SC (all), AG (all), DD (align!=0)
        if setup == 'Skew Charm':
            return True
        if setup == 'AG Short':
            return True
        if setup == 'DD Exhaustion':
            return alignment != 0
        # Others blocked if short
        return False


# ============================================================================
#  ANALYSIS SECTIONS
# ============================================================================

def analysis_1_pnl_context(with_gamma):
    """Show baseline PnL context for all analyses."""
    print(f"\n{'='*80}")
    print("0. BASELINE: Overall PnL Context")
    print(f"{'='*80}\n")

    dates = sorted(set(str(t['ts'])[:10] for t in with_gamma))
    n_days = len(dates)

    print(f"  Date range: {dates[0]} to {dates[-1]}")
    print(f"  Total trading days: {n_days}")
    print()

    detailed_stats(with_gamma, "ALL TRADES")
    print()

    longs = [e for e in with_gamma if is_long(e)]
    shorts = [e for e in with_gamma if is_short(e)]
    detailed_stats(longs, "LONGS")
    print()
    detailed_stats(shorts, "SHORTS")
    print()

    # Per-setup summary
    setup_names = sorted(set(e['setup'] for e in with_gamma))
    print("  Per-setup summary:")
    print(f"    {'Setup':>20} {'Trades':>6} {'WR':>5} {'PnL':>8} {'PnL/d':>7} {'PnL/t':>7} {'MaxDD':>7}")
    print(f"    {'-'*20} {'-'*6} {'-'*5} {'-'*8} {'-'*7} {'-'*7} {'-'*7}")
    for setup in setup_names:
        sub = [e for e in with_gamma if e['setup'] == setup]
        if not sub:
            continue
        wins = sum(1 for t in sub if t['result'] == 'WIN')
        pnl = sum(t['pnl'] for t in sub)
        wr = wins / len(sub) * 100
        sd = sorted(set(str(t['ts'])[:10] for t in sub))
        nd = len(sd)
        ppd = pnl / nd if nd > 0 else 0
        ppt = pnl / len(sub) if sub else 0
        mdd = max_drawdown(sub)
        print(f"    {setup:>20} {len(sub):>6} {wr:>4.0f}% {pnl:>+8.1f} {ppd:>+7.1f} {ppt:>+7.1f} {mdd:>7.1f}")


def analysis_2_stacked_sr_deep(with_gamma):
    """Stacked S/R deep dive."""
    print(f"\n{'='*80}")
    print("2. STACKED S/R DEEP DIVE (TODAY Gamma)")
    print(f"{'='*80}")

    # Only trades with both gamma and charm data for TODAY
    has_data = [e for e in with_gamma
                if e['gamma_by_tf']['TODAY'] is not None and e['charm_info'] is not None
                and e['stacked_info']['TODAY'] is not None]
    print(f"\n  Trades with TODAY gamma + charm: {len(has_data)}")

    # ---- 2a: Different proximity thresholds ----
    print(f"\n  --- 2a: Stacked vs Single by proximity threshold ---\n")
    print(f"    {'Prox':>5} {'Stacked':>8} {'Single':>8} {'Stack WR':>9} {'Single WR':>10} {'Stack PnL':>10} {'Single PnL':>11} {'Stack PnL/d':>12} {'Single PnL/d':>13}")
    print(f"    {'-'*5} {'-'*8} {'-'*8} {'-'*9} {'-'*10} {'-'*10} {'-'*11} {'-'*12} {'-'*13}")

    for prox in [3, 5, 10, 15]:
        stacked = [e for e in has_data if e['stacked_info']['TODAY']['is_stacked'].get(prox, False)]
        single = [e for e in has_data if not e['stacked_info']['TODAY']['is_stacked'].get(prox, False)]
        sw = sum(1 for t in stacked if t['result'] == 'WIN')
        sn_w = sum(1 for t in single if t['result'] == 'WIN')
        sp = sum(t['pnl'] for t in stacked)
        snp = sum(t['pnl'] for t in single)
        swr = sw / len(stacked) * 100 if stacked else 0
        snwr = sn_w / len(single) * 100 if single else 0
        sd = len(set(str(t['ts'])[:10] for t in stacked)) if stacked else 1
        snd = len(set(str(t['ts'])[:10] for t in single)) if single else 1
        print(f"    +/-{prox:<3} {len(stacked):>8} {len(single):>8} {swr:>8.0f}% {snwr:>9.0f}% {sp:>+10.1f} {snp:>+11.1f} {sp/sd:>+12.1f} {snp/snd:>+13.1f}")

    # ---- 2b: Stacked (prox=5) by each setup ----
    print(f"\n  --- 2b: Stacked (+/-5) by Setup ---\n")
    setup_names = sorted(set(e['setup'] for e in has_data))
    for setup in setup_names:
        sub = [e for e in has_data if e['setup'] == setup]
        if len(sub) < 3:
            continue
        stacked = [e for e in sub if e['stacked_info']['TODAY']['is_stacked'].get(5, False)]
        single = [e for e in sub if not e['stacked_info']['TODAY']['is_stacked'].get(5, False)]
        print(f"    {setup}:")
        detailed_stats(stacked, "Stacked")
        detailed_stats(single, "Single ")
        print()

    # ---- 2c: Trades per day: stacked vs single ----
    print(f"  --- 2c: Trades per day distribution ---\n")
    stacked_all = [e for e in has_data if e['stacked_info']['TODAY']['is_stacked'].get(5, False)]
    single_all = [e for e in has_data if not e['stacked_info']['TODAY']['is_stacked'].get(5, False)]

    by_date_stacked = defaultdict(int)
    by_date_single = defaultdict(int)
    for e in stacked_all:
        by_date_stacked[str(e['ts'])[:10]] += 1
    for e in single_all:
        by_date_single[str(e['ts'])[:10]] += 1

    all_dates = sorted(set(list(by_date_stacked.keys()) + list(by_date_single.keys())))
    if all_dates:
        avg_stacked = sum(by_date_stacked.values()) / len(all_dates)
        avg_single = sum(by_date_single.values()) / len(all_dates)
        print(f"    Avg stacked/day: {avg_stacked:.1f}")
        print(f"    Avg single/day:  {avg_single:.1f}")
        print(f"    Total days:      {len(all_dates)}")

    # ---- 2d: Strike distance from spot ----
    print(f"\n  --- 2d: Stacked S/R strike distance from spot ---\n")
    stacked_with_dist = [e for e in stacked_all if e['stacked_info']['TODAY']['strike_dist_from_spot'] is not None]
    if stacked_with_dist:
        dists = [e['stacked_info']['TODAY']['strike_dist_from_spot'] for e in stacked_with_dist]
        avg_dist = sum(dists) / len(dists)
        min_dist = min(dists)
        max_dist = max(dists)
        print(f"    Avg distance:  {avg_dist:.1f} pts")
        print(f"    Min distance:  {min_dist:.1f} pts")
        print(f"    Max distance:  {max_dist:.1f} pts")
        print()

        # WR by distance buckets
        for lo, hi, label in [(0, 5, "0-5 pts"), (5, 10, "5-10 pts"), (10, 15, "10-15 pts"),
                               (15, 20, "15-20 pts"), (20, 30, "20-30 pts")]:
            bucket = [e for e in stacked_with_dist if lo <= e['stacked_info']['TODAY']['strike_dist_from_spot'] < hi]
            if bucket:
                detailed_stats(bucket, f"Dist {label}")

    # ---- 2e: Gamma magnitude at stacked strike ----
    print(f"\n  --- 2e: Gamma magnitude at stacked strike (correlated with WR?) ---\n")
    stacked_with_val = [e for e in stacked_all if e['stacked_info']['TODAY']['gamma_value_at_stacked'] is not None]
    if stacked_with_val:
        for lo, hi, label in [(0, 5e6, "|val| < 5M"), (5e6, 20e6, "5-20M"),
                               (20e6, 50e6, "20-50M"), (50e6, 1e15, "> 50M")]:
            bucket = [e for e in stacked_with_val if lo <= e['stacked_info']['TODAY']['gamma_value_at_stacked'] < hi]
            if bucket:
                detailed_stats(bucket, label)


def analysis_3_longs_convergence(with_gamma):
    """Longs convergence filter analysis."""
    print(f"\n{'='*80}")
    print("3. LONGS CONVERGENCE FILTER")
    print(f"{'='*80}")

    def has_convergence(e):
        for tf in GAMMA_TIMEFRAMES:
            for f in e['confluence_by_tf'][tf].get('flags', []):
                if 'CONVERGE_ABOVE' in f or 'CONVERGE_BELOW' in f:
                    return True
        return False

    longs = [e for e in with_gamma if is_long(e)]
    print(f"\n  Total LONG trades with gamma: {len(longs)}")

    conv_longs = [e for e in longs if has_convergence(e)]
    no_conv_longs = [e for e in longs if not has_convergence(e)]

    # ---- 3a: Overall longs convergence ----
    print(f"\n  --- 3a: LONG trades with vs without convergence ---\n")
    detailed_stats(conv_longs, "WITH convergence   ")
    print()
    detailed_stats(no_conv_longs, "WITHOUT convergence")

    # ---- 3b: By setup x convergence ----
    print(f"\n  --- 3b: LONG by Setup x Convergence ---\n")
    setup_names = sorted(set(e['setup'] for e in longs))
    for setup in setup_names:
        sub = [e for e in longs if e['setup'] == setup]
        if len(sub) < 3:
            continue
        conv = [e for e in sub if has_convergence(e)]
        no_c = [e for e in sub if not has_convergence(e)]
        print(f"    {setup}:")
        if conv:
            w = sum(1 for t in conv if t['result'] == 'WIN')
            p = sum(t['pnl'] for t in conv)
            wr = w / len(conv) * 100
            print(f"      + convergence:  {w}W/{len(conv)-w}L ({wr:.0f}% WR) {p:+.1f} pts")
        if no_c:
            w = sum(1 for t in no_c if t['result'] == 'WIN')
            p = sum(t['pnl'] for t in no_c)
            wr = w / len(no_c) * 100
            print(f"      - convergence:  {w}W/{len(no_c)-w}L ({wr:.0f}% WR) {p:+.1f} pts")
        print()

    # ---- 3c: What if we blocked longs without convergence? ----
    print(f"  --- 3c: Impact of blocking longs without convergence ---\n")
    blocked = no_conv_longs
    blocked_pnl = sum(t['pnl'] for t in blocked)
    blocked_wins = sum(1 for t in blocked if t['result'] == 'WIN')
    blocked_losses = len(blocked) - blocked_wins
    print(f"    Trades blocked:  {len(blocked)} ({blocked_wins}W/{blocked_losses}L)")
    print(f"    PnL saved:       {-blocked_pnl:+.1f} pts (negative means we'd lose that PnL)")
    print(f"    Remaining longs: {len(conv_longs)} trades")
    if conv_longs:
        detailed_stats(conv_longs, "Remaining")

    # ---- 3d: Interaction with V9-SC ----
    print(f"\n  --- 3d: Interaction with V9-SC filter ---\n")
    blocked_by_v9sc = [e for e in blocked if not passes_v9sc(e)]
    blocked_not_v9sc = [e for e in blocked if passes_v9sc(e)]
    print(f"    No-convergence longs already blocked by V9-SC: {len(blocked_by_v9sc)}")
    print(f"    No-convergence longs NOT blocked by V9-SC:     {len(blocked_not_v9sc)}")
    if blocked_not_v9sc:
        pnl_new = sum(t['pnl'] for t in blocked_not_v9sc)
        w = sum(1 for t in blocked_not_v9sc if t['result'] == 'WIN')
        print(f"    These {len(blocked_not_v9sc)} trades: {w}W/{len(blocked_not_v9sc)-w}L, {pnl_new:+.1f} pts")
        print(f"    --> Blocking these would save {-pnl_new:+.1f} pts ABOVE V9-SC")
        print()
        print(f"    Setups of the newly-blockable trades:")
        for setup in sorted(set(e['setup'] for e in blocked_not_v9sc)):
            sub = [e for e in blocked_not_v9sc if e['setup'] == setup]
            w = sum(1 for t in sub if t['result'] == 'WIN')
            p = sum(t['pnl'] for t in sub)
            print(f"      {setup}: {len(sub)} trades ({w}W/{len(sub)-w}L, {p:+.1f} pts)")

    # ---- 3e: By VIX regime ----
    print(f"\n  --- 3e: LONG convergence by VIX regime ---\n")
    for vix_lo, vix_hi, label in [(0, 20, "VIX < 20"), (20, 25, "VIX 20-25"),
                                   (25, 30, "VIX 25-30"), (30, 100, "VIX > 30")]:
        vix_longs = [e for e in longs if e['vix'] is not None and vix_lo <= e['vix'] < vix_hi]
        if not vix_longs:
            continue
        conv = [e for e in vix_longs if has_convergence(e)]
        no_c = [e for e in vix_longs if not has_convergence(e)]
        print(f"    {label} ({len(vix_longs)} longs):")
        if conv:
            w = sum(1 for t in conv if t['result'] == 'WIN')
            p = sum(t['pnl'] for t in conv)
            wr = w / len(conv) * 100
            print(f"      + convergence: {w}W/{len(conv)-w}L ({wr:.0f}% WR) {p:+.1f} pts")
        if no_c:
            w = sum(1 for t in no_c if t['result'] == 'WIN')
            p = sum(t['pnl'] for t in no_c)
            wr = w / len(no_c) * 100
            print(f"      - convergence: {w}W/{len(no_c)-w}L ({wr:.0f}% WR) {p:+.1f} pts")
        print()


def analysis_4_timeframe_agreement(with_gamma):
    """Timeframe agreement deep dive."""
    print(f"\n{'='*80}")
    print("4. TIMEFRAME AGREEMENT DEEP DIVE")
    print(f"{'='*80}")

    # ---- 4a: Distribution check ----
    print(f"\n  --- 4a: Distribution of TF agreement ---\n")
    total = len(with_gamma)
    for n in range(5):
        bucket = [e for e in with_gamma if e['tf_agreement'] == n]
        pct = len(bucket) / total * 100 if total > 0 else 0
        print(f"    {n}/4 agree: {len(bucket):>4} trades ({pct:>5.1f}%)")
        if bucket:
            detailed_stats(bucket, f"  Stats")
        print()

    # ---- 4b: 4/4 agreement by setup ----
    print(f"  --- 4b: 4/4 agreement by setup ---\n")
    agree4 = [e for e in with_gamma if e['tf_agreement'] == 4]
    not_agree4 = [e for e in with_gamma if e['tf_agreement'] < 4]

    setup_names = sorted(set(e['setup'] for e in with_gamma))
    print(f"    {'Setup':>20} {'4/4 Trades':>10} {'4/4 WR':>7} {'4/4 PnL':>8} {'<4/4 Trades':>11} {'<4/4 WR':>8} {'<4/4 PnL':>9} {'WR Diff':>8}")
    print(f"    {'-'*20} {'-'*10} {'-'*7} {'-'*8} {'-'*11} {'-'*8} {'-'*9} {'-'*8}")
    for setup in setup_names:
        a4 = [e for e in agree4 if e['setup'] == setup]
        na4 = [e for e in not_agree4 if e['setup'] == setup]
        if not a4 and not na4:
            continue
        a4w = sum(1 for t in a4 if t['result'] == 'WIN')
        a4wr = a4w / len(a4) * 100 if a4 else 0
        a4pnl = sum(t['pnl'] for t in a4)
        na4w = sum(1 for t in na4 if t['result'] == 'WIN')
        na4wr = na4w / len(na4) * 100 if na4 else 0
        na4pnl = sum(t['pnl'] for t in na4)
        diff = a4wr - na4wr if a4 and na4 else 0
        print(f"    {setup:>20} {len(a4):>10} {a4wr:>6.0f}% {a4pnl:>+8.1f} {len(na4):>11} {na4wr:>7.0f}% {na4pnl:>+9.1f} {diff:>+7.0f}%")

    # ---- 4c: Partial agreement breakdown ----
    print(f"\n  --- 4c: Partial agreement (1-3/4) breakdown ---\n")
    partial = [e for e in with_gamma if 1 <= e['tf_agreement'] <= 3]
    if partial:
        print(f"    {len(partial)} trades with partial agreement")
        for setup in sorted(set(e['setup'] for e in partial)):
            sub = [e for e in partial if e['setup'] == setup]
            if sub:
                w = sum(1 for t in sub if t['result'] == 'WIN')
                p = sum(t['pnl'] for t in sub)
                wr = w / len(sub) * 100
                print(f"      {setup}: {len(sub)} trades, {w}W/{len(sub)-w}L ({wr:.0f}% WR), {p:+.1f} pts")

        # Which TF disagrees?
        print(f"\n    Which timeframes disagree in partial cases?")
        for tf in GAMMA_TIMEFRAMES:
            disagree = [e for e in partial if e['confluence_by_tf'][tf]['score'] <= 0]
            print(f"      {tf} has zero/neg confluence: {len(disagree)}/{len(partial)} ({len(disagree)/len(partial)*100:.0f}%)")

    # ---- 4d: TODAY + ALL only ----
    print(f"\n  --- 4d: TODAY + ALL agreement (2 key timeframes) ---\n")
    today_pos = [e for e in with_gamma if e['confluence_by_tf']['TODAY']['score'] > 0]
    all_pos = [e for e in with_gamma if e['confluence_by_tf']['ALL']['score'] > 0]
    both_pos = [e for e in with_gamma
                if e['confluence_by_tf']['TODAY']['score'] > 0
                and e['confluence_by_tf']['ALL']['score'] > 0]
    neither = [e for e in with_gamma
               if e['confluence_by_tf']['TODAY']['score'] <= 0
               and e['confluence_by_tf']['ALL']['score'] <= 0]
    one_only = [e for e in with_gamma
                if (e['confluence_by_tf']['TODAY']['score'] > 0) !=
                   (e['confluence_by_tf']['ALL']['score'] > 0)]

    print(f"    TODAY + ALL both positive: {len(both_pos)}")
    detailed_stats(both_pos, "Both positive")
    print()
    print(f"    Only one positive:         {len(one_only)}")
    detailed_stats(one_only, "One only     ")
    print()
    print(f"    Neither positive:          {len(neither)}")
    detailed_stats(neither, "Neither      ")
    print()

    # Compare to 4/4
    print(f"    Comparison: 4/4 agreement = {len(agree4)} trades vs TODAY+ALL = {len(both_pos)} trades")
    print(f"    Would filtering on TODAY+ALL only miss any 4/4 trades? {len(agree4) - len([e for e in agree4 if e in both_pos])}")


def analysis_5_per_day(with_gamma):
    """Per-day stacked vs single analysis."""
    print(f"\n{'='*80}")
    print("5. PER-DAY ANALYSIS: Stacked vs Single (TODAY +/-5)")
    print(f"{'='*80}")

    has_data = [e for e in with_gamma
                if e['gamma_by_tf']['TODAY'] is not None and e['charm_info'] is not None
                and e['stacked_info']['TODAY'] is not None]

    by_date = defaultdict(lambda: {'stacked': [], 'single': []})
    for e in has_data:
        d = str(e['ts'])[:10]
        if e['stacked_info']['TODAY']['is_stacked'].get(5, False):
            by_date[d]['stacked'].append(e)
        else:
            by_date[d]['single'].append(e)

    print(f"\n  {'Date':>12} {'Stacked':>8} {'S.WR':>5} {'S.PnL':>8} {'Single':>7} {'Sn.WR':>6} {'Sn.PnL':>8} {'Net':>7}")
    print(f"  {'-'*12} {'-'*8} {'-'*5} {'-'*8} {'-'*7} {'-'*6} {'-'*8} {'-'*7}")

    wrongly_filtered_count = 0
    wrongly_filtered_pnl = 0

    for d in sorted(by_date.keys()):
        st = by_date[d]['stacked']
        sn = by_date[d]['single']
        sw = sum(1 for t in st if t['result'] == 'WIN')
        sp = sum(t['pnl'] for t in st)
        snw = sum(1 for t in sn if t['result'] == 'WIN')
        snp = sum(t['pnl'] for t in sn)
        swr = sw / len(st) * 100 if st else 0
        snwr = snw / len(sn) * 100 if sn else 0
        net = sp + snp

        print(f"  {d:>12} {len(st):>8} {swr:>4.0f}% {sp:>+8.1f} {len(sn):>7} {snwr:>5.0f}% {snp:>+8.1f} {net:>+7.1f}")

        # Check if filtering single would remove winners
        single_wins = [e for e in sn if e['result'] == 'WIN']
        for w in single_wins:
            if w['pnl'] > 5:  # significant winner
                wrongly_filtered_count += 1
                wrongly_filtered_pnl += w['pnl']

    print()
    print(f"  Days where filtering single removes significant winners (>5 pts):")
    print(f"    Count: {wrongly_filtered_count}, Total PnL lost: {wrongly_filtered_pnl:+.1f} pts")

    # Show the significant single winners
    sig_single_wins = [e for e in has_data
                       if not e['stacked_info']['TODAY']['is_stacked'].get(5, False)
                       and e['result'] == 'WIN' and e['pnl'] > 5]
    if sig_single_wins:
        print(f"\n  Single (non-stacked) trades with PnL > 5 that would be filtered:")
        print(f"    {'ID':>6} {'Date':>12} {'Setup':>18} {'Dir':>6} {'PnL':>7} {'Setup':>18}")
        for e in sorted(sig_single_wins, key=lambda x: -x['pnl']):
            print(f"    {e['id']:>6} {str(e['ts'])[:10]:>12} {e['setup']:>18} {e['direction']:>6} {e['pnl']:>+7.1f}")


def analysis_6_directional_gamma(with_gamma):
    """Direction-specific gamma meaning."""
    print(f"\n{'='*80}")
    print("6. DIRECTION-SPECIFIC GAMMA MEANING")
    print(f"{'='*80}")

    has_today = [e for e in with_gamma if e['gamma_by_tf']['TODAY'] is not None]

    # ---- 6a: SHORT trades + strong positive gamma ABOVE spot (dealer resistance) ----
    print(f"\n  --- 6a: SHORT trades + gamma ABOVE spot (dealer resistance) ---\n")
    shorts = [e for e in has_today if is_short(e)]
    print(f"    Total short trades with TODAY gamma: {len(shorts)}")

    for lo, hi, label in [(0, 5e6, "|gamma| < 5M"), (5e6, 20e6, "5-20M"),
                           (20e6, 50e6, "20-50M"), (50e6, 1e15, "> 50M")]:
        bucket = [e for e in shorts
                  if e['gamma_by_tf']['TODAY'].get('strongest_above') is not None
                  and lo <= abs(e['gamma_by_tf']['TODAY']['strongest_above'][1]) < hi]
        if bucket:
            # Positive gamma above = dealer resistance (helps shorts)
            pos_bucket = [e for e in bucket if e['gamma_by_tf']['TODAY']['strongest_above'][1] > 0]
            neg_bucket = [e for e in bucket if e['gamma_by_tf']['TODAY']['strongest_above'][1] < 0]
            print(f"    {label} (total {len(bucket)}):")
            if pos_bucket:
                w = sum(1 for t in pos_bucket if t['result'] == 'WIN')
                p = sum(t['pnl'] for t in pos_bucket)
                wr = w / len(pos_bucket) * 100
                print(f"      + gamma above (resistance): {w}W/{len(pos_bucket)-w}L ({wr:.0f}% WR) {p:+.1f} pts")
            if neg_bucket:
                w = sum(1 for t in neg_bucket if t['result'] == 'WIN')
                p = sum(t['pnl'] for t in neg_bucket)
                wr = w / len(neg_bucket) * 100
                print(f"      - gamma above (no resist):  {w}W/{len(neg_bucket)-w}L ({wr:.0f}% WR) {p:+.1f} pts")

    # ---- 6b: LONG trades + strong positive gamma BELOW spot (dealer support) ----
    print(f"\n  --- 6b: LONG trades + gamma BELOW spot (dealer support) ---\n")
    longs_today = [e for e in has_today if is_long(e)]
    print(f"    Total long trades with TODAY gamma: {len(longs_today)}")

    for lo, hi, label in [(0, 5e6, "|gamma| < 5M"), (5e6, 20e6, "5-20M"),
                           (20e6, 50e6, "20-50M"), (50e6, 1e15, "> 50M")]:
        bucket = [e for e in longs_today
                  if e['gamma_by_tf']['TODAY'].get('strongest_below') is not None
                  and lo <= abs(e['gamma_by_tf']['TODAY']['strongest_below'][1]) < hi]
        if bucket:
            pos_bucket = [e for e in bucket if e['gamma_by_tf']['TODAY']['strongest_below'][1] > 0]
            neg_bucket = [e for e in bucket if e['gamma_by_tf']['TODAY']['strongest_below'][1] < 0]
            print(f"    {label} (total {len(bucket)}):")
            if pos_bucket:
                w = sum(1 for t in pos_bucket if t['result'] == 'WIN')
                p = sum(t['pnl'] for t in pos_bucket)
                wr = w / len(pos_bucket) * 100
                print(f"      + gamma below (support):  {w}W/{len(pos_bucket)-w}L ({wr:.0f}% WR) {p:+.1f} pts")
            if neg_bucket:
                w = sum(1 for t in neg_bucket if t['result'] == 'WIN')
                p = sum(t['pnl'] for t in neg_bucket)
                wr = w / len(neg_bucket) * 100
                print(f"      - gamma below (no supp):  {w}W/{len(neg_bucket)-w}L ({wr:.0f}% WR) {p:+.1f} pts")

    # ---- 6c: Standalone directional filters ----
    print(f"\n  --- 6c: Standalone directional gamma filter ---\n")
    print(f"    Filter: SHORT only when +gamma above spot (resistance helps)")
    shorts_pos_above = [e for e in shorts
                        if e['gamma_by_tf']['TODAY'].get('strongest_above') is not None
                        and e['gamma_by_tf']['TODAY']['strongest_above'][1] > 0]
    shorts_no_pos_above = [e for e in shorts
                           if e['gamma_by_tf']['TODAY'].get('strongest_above') is None
                           or e['gamma_by_tf']['TODAY']['strongest_above'][1] <= 0]
    print(f"      Pass:  {wr_stats(shorts_pos_above)}")
    print(f"      Block: {wr_stats(shorts_no_pos_above)}")

    print()
    print(f"    Filter: LONG only when +gamma below spot (support helps)")
    longs_pos_below = [e for e in longs_today
                       if e['gamma_by_tf']['TODAY'].get('strongest_below') is not None
                       and e['gamma_by_tf']['TODAY']['strongest_below'][1] > 0]
    longs_no_pos_below = [e for e in longs_today
                          if e['gamma_by_tf']['TODAY'].get('strongest_below') is None
                          or e['gamma_by_tf']['TODAY']['strongest_below'][1] <= 0]
    print(f"      Pass:  {wr_stats(longs_pos_below)}")
    print(f"      Block: {wr_stats(longs_no_pos_below)}")

    # ---- 6d: Combined directional filter ----
    print(f"\n  --- 6d: Combined directional filter (pass only with aligned gamma) ---\n")
    passed = shorts_pos_above + longs_pos_below
    blocked = shorts_no_pos_above + longs_no_pos_below
    detailed_stats(passed, "PASS (aligned gamma) ")
    print()
    detailed_stats(blocked, "BLOCK (no alignment) ")


def analysis_7_gamma_value_thresholds(with_gamma):
    """Gamma value thresholds at stacked strike."""
    print(f"\n{'='*80}")
    print("7. GAMMA VALUE THRESHOLDS")
    print(f"{'='*80}")

    # ---- 7a: Absolute gamma at stacked strike (TODAY) ----
    print(f"\n  --- 7a: |Gamma| at stacked strike (TODAY, +/-5 proximity) ---\n")

    has_data = [e for e in with_gamma
                if e['stacked_info'].get('TODAY') is not None
                and e['stacked_info']['TODAY']['is_stacked'].get(5, False)
                and e['stacked_info']['TODAY']['gamma_value_at_stacked'] is not None]

    if not has_data:
        print("    No stacked trades with gamma values found.")
    else:
        vals = [e['stacked_info']['TODAY']['gamma_value_at_stacked'] for e in has_data]
        print(f"    Stacked trades with gamma values: {len(has_data)}")
        print(f"    Gamma value range: {min(vals)/1e6:.1f}M to {max(vals)/1e6:.1f}M")
        print(f"    Median: {sorted(vals)[len(vals)//2]/1e6:.1f}M")
        print()

        for lo, hi, label in [(0, 5e6, "|val| < 5M"), (5e6, 20e6, "5-20M"),
                               (20e6, 50e6, "20-50M"), (50e6, 1e15, "> 50M")]:
            bucket = [e for e in has_data if lo <= e['stacked_info']['TODAY']['gamma_value_at_stacked'] < hi]
            if bucket:
                detailed_stats(bucket, label)
        print()

    # ---- 7b: Near-spot gamma (within +/-5 of spot) by magnitude ----
    print(f"\n  --- 7b: Near-spot gamma (+/-5 from spot) magnitude ---\n")
    has_today = [e for e in with_gamma if e['gamma_by_tf']['TODAY'] is not None]
    for lo, hi, label in [(0, 1e6, "< 1M"), (1e6, 5e6, "1-5M"),
                           (5e6, 10e6, "5-10M"), (10e6, 25e6, "10-25M"),
                           (25e6, 50e6, "25-50M"), (50e6, 1e15, "> 50M")]:
        bucket = [e for e in has_today if lo <= abs(e['gamma_by_tf']['TODAY'].get('near_spot', 0)) < hi]
        if bucket:
            detailed_stats(bucket, f"Near-spot |gamma| {label}")

    # ---- 7c: Gamma at stacked strike correlation with MFE ----
    print(f"\n  --- 7c: Gamma magnitude at stacked strike vs max profit (MFE) ---\n")
    if has_data:
        # Sort by gamma value and split into quartiles
        sorted_by_val = sorted(has_data, key=lambda e: e['stacked_info']['TODAY']['gamma_value_at_stacked'])
        q_size = len(sorted_by_val) // 4
        if q_size > 0:
            quartiles = [
                ("Q1 (lowest gamma)", sorted_by_val[:q_size]),
                ("Q2", sorted_by_val[q_size:2*q_size]),
                ("Q3", sorted_by_val[2*q_size:3*q_size]),
                ("Q4 (highest gamma)", sorted_by_val[3*q_size:]),
            ]
            print(f"    {'Quartile':>22} {'Trades':>7} {'WR':>5} {'PnL':>8} {'Avg MFE':>8} {'Avg PnL':>8}")
            print(f"    {'-'*22} {'-'*7} {'-'*5} {'-'*8} {'-'*8} {'-'*8}")
            for qlabel, qdata in quartiles:
                w = sum(1 for t in qdata if t['result'] == 'WIN')
                p = sum(t['pnl'] for t in qdata)
                avg_mfe = sum(t['max_profit'] for t in qdata) / len(qdata) if qdata else 0
                avg_pnl = p / len(qdata) if qdata else 0
                wr = w / len(qdata) * 100 if qdata else 0
                gamma_range = (qdata[0]['stacked_info']['TODAY']['gamma_value_at_stacked'] / 1e6,
                               qdata[-1]['stacked_info']['TODAY']['gamma_value_at_stacked'] / 1e6)
                print(f"    {qlabel:>22} {len(qdata):>7} {wr:>4.0f}% {p:>+8.1f} {avg_mfe:>+8.1f} {avg_pnl:>+8.1f}  [{gamma_range[0]:.1f}-{gamma_range[1]:.1f}M]")


def analysis_all_tf_comparison(with_gamma):
    """Compare stacked S/R across all timeframes."""
    print(f"\n{'='*80}")
    print("BONUS: STACKED S/R ACROSS ALL TIMEFRAMES (+/-5)")
    print(f"{'='*80}\n")

    for tf in GAMMA_TIMEFRAMES:
        has_data = [e for e in with_gamma
                    if e['gamma_by_tf'][tf] is not None and e['charm_info'] is not None
                    and e['stacked_info'].get(tf) is not None]
        if not has_data:
            continue

        stacked = [e for e in has_data if e['stacked_info'][tf]['is_stacked'].get(5, False)]
        single = [e for e in has_data if not e['stacked_info'][tf]['is_stacked'].get(5, False)]

        sw = sum(1 for t in stacked if t['result'] == 'WIN')
        sp = sum(t['pnl'] for t in stacked)
        swr = sw / len(stacked) * 100 if stacked else 0
        snw = sum(1 for t in single if t['result'] == 'WIN')
        snp = sum(t['pnl'] for t in single)
        snwr = snw / len(single) * 100 if single else 0

        print(f"  {tf}:")
        print(f"    Stacked: {len(stacked):>4} trades, {swr:.0f}% WR, {sp:+.1f} pts ({sp/len(stacked) if stacked else 0:+.1f}/trade)")
        print(f"    Single:  {len(single):>4} trades, {snwr:.0f}% WR, {snp:+.1f} pts ({snp/len(single) if single else 0:+.1f}/trade)")
        print(f"    WR diff: {swr - snwr:+.1f}%")
        print()


# ============================================================================
#  MAIN
# ============================================================================

def main():
    setups = fetch_setups()
    if not setups:
        return

    snapshots = fetch_all_exposure_snapshots()
    enriched, with_gamma = enrich_signals(setups, snapshots)

    if not with_gamma:
        print("No gamma data found! Exiting.")
        return

    # Run all analyses
    analysis_1_pnl_context(with_gamma)
    analysis_2_stacked_sr_deep(with_gamma)
    analysis_3_longs_convergence(with_gamma)
    analysis_4_timeframe_agreement(with_gamma)
    analysis_5_per_day(with_gamma)
    analysis_6_directional_gamma(with_gamma)
    analysis_7_gamma_value_thresholds(with_gamma)
    analysis_all_tf_comparison(with_gamma)

    # ---- Final summary ----
    print(f"\n{'='*80}")
    print("EXECUTIVE SUMMARY")
    print(f"{'='*80}\n")

    # Key findings
    has_data = [e for e in with_gamma
                if e['gamma_by_tf']['TODAY'] is not None and e['charm_info'] is not None
                and e['stacked_info']['TODAY'] is not None]

    stacked = [e for e in has_data if e['stacked_info']['TODAY']['is_stacked'].get(5, False)]
    single = [e for e in has_data if not e['stacked_info']['TODAY']['is_stacked'].get(5, False)]

    sw = sum(1 for t in stacked if t['result'] == 'WIN')
    swr = sw / len(stacked) * 100 if stacked else 0
    sp = sum(t['pnl'] for t in stacked)
    snw = sum(1 for t in single if t['result'] == 'WIN')
    snwr = snw / len(single) * 100 if single else 0
    snp = sum(t['pnl'] for t in single)

    def has_convergence(e):
        for tf in GAMMA_TIMEFRAMES:
            for f in e['confluence_by_tf'][tf].get('flags', []):
                if 'CONVERGE' in f:
                    return True
        return False

    longs = [e for e in with_gamma if is_long(e)]
    conv_longs = [e for e in longs if has_convergence(e)]
    no_conv_longs = [e for e in longs if not has_convergence(e)]

    agree4 = [e for e in with_gamma if e['tf_agreement'] == 4]
    partial = [e for e in with_gamma if 1 <= e['tf_agreement'] <= 3]

    print("  1. STACKED S/R (TODAY +/-5):")
    print(f"     Stacked: {swr:.0f}% WR, {sp:+.1f} pts ({len(stacked)} trades)")
    print(f"     Single:  {snwr:.0f}% WR, {snp:+.1f} pts ({len(single)} trades)")
    print(f"     --> Stacked is {swr-snwr:+.0f}% WR better, {sp-snp:+.1f} pts better")
    print()

    print("  2. LONG CONVERGENCE FILTER:")
    ncl_pnl = sum(t['pnl'] for t in no_conv_longs)
    cl_pnl = sum(t['pnl'] for t in conv_longs)
    ncl_wr = sum(1 for t in no_conv_longs if t['result'] == 'WIN') / len(no_conv_longs) * 100 if no_conv_longs else 0
    cl_wr = sum(1 for t in conv_longs if t['result'] == 'WIN') / len(conv_longs) * 100 if conv_longs else 0
    print(f"     With convergence:    {cl_wr:.0f}% WR, {cl_pnl:+.1f} pts ({len(conv_longs)} trades)")
    print(f"     Without convergence: {ncl_wr:.0f}% WR, {ncl_pnl:+.1f} pts ({len(no_conv_longs)} trades)")
    print(f"     Blocking no-conv longs saves {-ncl_pnl:+.1f} pts")
    print()

    print("  3. TIMEFRAME AGREEMENT:")
    print(f"     4/4 agree: {len(agree4)} trades ({len(agree4)/len(with_gamma)*100:.0f}% of all)")
    print(f"     1-3/4:     {len(partial)} trades ({len(partial)/len(with_gamma)*100:.0f}% of all)")
    print(f"     --> {len(agree4)/len(with_gamma)*100:.0f}% of trades have 4/4. Partial is tiny sample.")
    print()


if __name__ == '__main__':
    main()
