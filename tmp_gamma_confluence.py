"""
Gamma + Charm Confluence Backtest (v2 — batch queries)
======================================================
Batch-fetches ALL gamma + charm per-strike data, then joins in-memory.
"""

from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import datetime, timedelta
import bisect

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)

GAMMA_TIMEFRAMES = ['TODAY', 'THIS_WEEK', 'THIRTY_NEXT_DAYS', 'ALL']
PROXIMITY = 30  # strikes within ±30 pts of spot


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
    """Batch-fetch ALL gamma + charm snapshots. Group by (greek, exp_option, ts_utc)."""
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

    # Group: key = (greek, exp_option) → list of (ts_utc, [(strike, val)]) snapshots
    # First group by (greek, exp_option, ts_utc)
    raw_groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        exp = r.expiration_option or 'TODAY'  # charm NULL → TODAY
        key = (r.greek, exp)
        raw_groups[key][r.ts_utc].append((float(r.strike), float(r.val)))

    # Convert to sorted list of (ts_utc, strikes_list) for binary search
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
    """Find the nearest snapshot to ts within max_delta_s seconds."""
    key = (greek, exp_option)
    if key not in snapshots:
        return None

    snap = snapshots[key]
    timestamps = snap['timestamps']
    if not timestamps:
        return None

    # Binary search for closest timestamp
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
    """From per-strike data, find strongest resistance/support, net, near-spot."""
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
    }


def compute_confluence(charm_info, gamma_info, spot, direction):
    """Score gamma+charm confluence for a given direction."""
    if not charm_info or not gamma_info:
        return {'score': 0, 'flags': []}

    score = 0
    flags = []
    is_long = direction in ('long', 'bullish')

    # 1. Same-strike convergence above spot
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

    # 2. Same-strike convergence below spot
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

    # 3. Net gamma aligns with direction
    gamma_net = gamma_info.get('total', 0)
    if gamma_net > 0 and is_long:
        score += 1
        flags.append("NET_G+_LONG")
    elif gamma_net < 0 and not is_long:
        score += 1
        flags.append("NET_G-_SHORT")

    # 4. Near-spot gamma significance
    near = gamma_info.get('near_spot', 0)
    if abs(near) > 1e6:
        if near > 0 and is_long:
            score += 1
            flags.append("SPOT_G+")
        elif near < 0 and not is_long:
            score += 1
            flags.append("SPOT_G-")

    # 5. Charm net direction
    charm_net = charm_info.get('total', 0)
    if charm_net > 0 and is_long:
        score += 1
        flags.append("CHARM+")
    elif charm_net < 0 and not is_long:
        score += 1
        flags.append("CHARM-")

    return {'score': score, 'flags': flags}


def wr_stats(trades):
    if not trades:
        return "N/A (0 trades)"
    wins = sum(1 for t in trades if t['result'] == 'WIN')
    total_pnl = sum(t['pnl'] for t in trades)
    avg_pnl = total_pnl / len(trades)
    wr = wins / len(trades) * 100
    return f"{wins}W/{len(trades)-wins}L ({wr:.0f}% WR), {total_pnl:+.1f} pts ({avg_pnl:+.1f}/trade)"


def main():
    setups = fetch_setups()
    if not setups:
        return

    snapshots = fetch_all_exposure_snapshots()

    # ── Enrich each signal ──────────────────────────────────────────────
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

        # Find nearest charm snapshot
        charm_strikes = find_nearest_snapshot(snapshots, 'charm', 'TODAY', ts)
        # Filter to ±PROXIMITY of spot
        if charm_strikes:
            charm_strikes = [(st, v) for st, v in charm_strikes if abs(st - spot) <= PROXIMITY]
        charm_info = analyze_strikes(charm_strikes, spot) if charm_strikes else None
        if not charm_info:
            no_charm += 1

        # Find nearest gamma for each timeframe
        gamma_by_tf = {}
        for tf in GAMMA_TIMEFRAMES:
            g_strikes = find_nearest_snapshot(snapshots, 'gamma', tf, ts)
            if g_strikes:
                g_strikes = [(st, v) for st, v in g_strikes if abs(st - spot) <= PROXIMITY]
            gamma_by_tf[tf] = analyze_strikes(g_strikes, spot) if g_strikes else None

        has_any_gamma = any(v is not None for v in gamma_by_tf.values())
        if not has_any_gamma:
            no_gamma += 1

        # Confluence per timeframe
        confluence_by_tf = {}
        for tf in GAMMA_TIMEFRAMES:
            if gamma_by_tf[tf]:
                confluence_by_tf[tf] = compute_confluence(charm_info, gamma_by_tf[tf], spot, direction)
            else:
                confluence_by_tf[tf] = {'score': 0, 'flags': []}

        best_tf = max(GAMMA_TIMEFRAMES, key=lambda tf: confluence_by_tf[tf]['score'])
        best_score = confluence_by_tf[best_tf]['score']
        tf_agreement = sum(1 for tf in GAMMA_TIMEFRAMES if confluence_by_tf[tf]['score'] > 0)

        enriched.append({
            'id': s.id, 'ts': s.ts, 'setup': s.setup_name,
            'direction': direction, 'grade': s.grade, 'spot': spot,
            'result': s.outcome_result, 'pnl': float(s.outcome_pnl) if s.outcome_pnl else 0,
            'max_profit': float(s.outcome_max_profit) if s.outcome_max_profit else 0,
            'alignment': s.greek_alignment, 'vix': float(s.vix) if s.vix else None,
            'paradigm': s.paradigm,
            'charm_info': charm_info, 'gamma_by_tf': gamma_by_tf,
            'confluence_by_tf': confluence_by_tf,
            'best_tf': best_tf, 'best_confluence': best_score,
            'tf_agreement': tf_agreement,
        })

    with_gamma = [e for e in enriched if any(e['gamma_by_tf'][tf] is not None for tf in GAMMA_TIMEFRAMES)]
    print(f"Total signals: {len(enriched)}")
    print(f"No gamma data: {no_gamma} ({no_gamma*100/len(enriched):.0f}%)")
    print(f"No charm data: {no_charm} ({no_charm*100/len(enriched):.0f}%)")
    print(f"With gamma:    {len(with_gamma)}")

    if not with_gamma:
        print("No gamma data found! Exiting.")
        return

    # ════════════════════════════════════════════════════════════════════
    #  ANALYSIS
    # ════════════════════════════════════════════════════════════════════

    # ── 1. Overall confluence impact ────────────────────────────────────
    print(f"\n{'='*80}")
    print("1. OVERALL: Best Confluence Score Impact")
    print(f"{'='*80}\n")

    for threshold in range(-2, 8):
        bucket = [e for e in with_gamma if e['best_confluence'] == threshold]
        if bucket:
            print(f"  Score = {threshold:+d}: {wr_stats(bucket)}")

    print()
    low = [e for e in with_gamma if e['best_confluence'] <= 0]
    med = [e for e in with_gamma if 1 <= e['best_confluence'] <= 2]
    high = [e for e in with_gamma if e['best_confluence'] >= 3]
    print(f"  Low  (<=0):  {wr_stats(low)}")
    print(f"  Med  (1-2):  {wr_stats(med)}")
    print(f"  High (>=3):  {wr_stats(high)}")

    # ── 2. Timeframe agreement ──────────────────────────────────────────
    print(f"\n{'='*80}")
    print("2. TIMEFRAME AGREEMENT (# with positive confluence)")
    print(f"{'='*80}\n")

    for n in range(5):
        bucket = [e for e in with_gamma if e['tf_agreement'] == n]
        if bucket:
            print(f"  {n}/4 agree: {wr_stats(bucket)}")

    # ── 3. Per-timeframe ────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("3. WHICH GAMMA TIMEFRAME IS MOST PREDICTIVE?")
    print(f"{'='*80}\n")

    for tf in GAMMA_TIMEFRAMES:
        has_tf = [e for e in with_gamma if e['gamma_by_tf'][tf] is not None]
        if not has_tf:
            print(f"  {tf}: No data")
            continue
        pos = [e for e in has_tf if e['confluence_by_tf'][tf]['score'] > 0]
        neg = [e for e in has_tf if e['confluence_by_tf'][tf]['score'] <= 0]
        print(f"  {tf} ({len(has_tf)} signals):")
        print(f"    Positive confluence: {wr_stats(pos)}")
        print(f"    Zero/Neg confluence: {wr_stats(neg)}")
        if pos and neg and len(pos) >= 3 and len(neg) >= 3:
            pos_wr = sum(1 for t in pos if t['result'] == 'WIN') / len(pos) * 100
            neg_wr = sum(1 for t in neg if t['result'] == 'WIN') / len(neg) * 100
            print(f"    WR Delta: {pos_wr - neg_wr:+.1f}%")
        print()

    # ── 4. Per-setup breakdown ──────────────────────────────────────────
    print(f"\n{'='*80}")
    print("4. PER-SETUP BREAKDOWN")
    print(f"{'='*80}\n")

    setup_names = sorted(set(e['setup'] for e in with_gamma))
    for setup in setup_names:
        subset = [e for e in with_gamma if e['setup'] == setup]
        if len(subset) < 3:
            continue

        print(f"  -- {setup} ({len(subset)} trades) --")
        print(f"    ALL:        {wr_stats(subset)}")

        hi = [e for e in subset if e['best_confluence'] >= 2]
        lo = [e for e in subset if e['best_confluence'] < 2]
        if hi and lo:
            print(f"    Conf >= 2:  {wr_stats(hi)}")
            print(f"    Conf < 2:   {wr_stats(lo)}")
            hi_wr = sum(1 for t in hi if t['result'] == 'WIN') / len(hi) * 100
            lo_wr = sum(1 for t in lo if t['result'] == 'WIN') / len(lo) * 100
            print(f"    WR Delta:   {hi_wr - lo_wr:+.1f}%")

        # Best timeframe
        best_deltas = {}
        for tf in GAMMA_TIMEFRAMES:
            has_tf = [e for e in subset if e['gamma_by_tf'][tf] is not None]
            if len(has_tf) < 5:
                continue
            pos = [e for e in has_tf if e['confluence_by_tf'][tf]['score'] > 0]
            neg = [e for e in has_tf if e['confluence_by_tf'][tf]['score'] <= 0]
            if len(pos) >= 2 and len(neg) >= 2:
                pos_wr = sum(1 for t in pos if t['result'] == 'WIN') / len(pos) * 100
                neg_wr = sum(1 for t in neg if t['result'] == 'WIN') / len(neg) * 100
                best_deltas[tf] = pos_wr - neg_wr
        if best_deltas:
            best_tf = max(best_deltas, key=best_deltas.get)
            print(f"    Best TF:    {best_tf} (WR delta: {best_deltas[best_tf]:+.1f}%)")
        print()

    # ── 5. Same-strike convergence ──────────────────────────────────────
    print(f"\n{'='*80}")
    print("5. SAME-STRIKE CONVERGENCE (Gamma+Charm at Same Level)")
    print(f"{'='*80}\n")

    def has_flag(e, pattern):
        for tf in GAMMA_TIMEFRAMES:
            for f in e['confluence_by_tf'][tf].get('flags', []):
                if pattern in f:
                    return True
        return False

    conv_above = [e for e in with_gamma if has_flag(e, 'CONVERGE_ABOVE')]
    conv_below = [e for e in with_gamma if has_flag(e, 'CONVERGE_BELOW')]
    conv_any = [e for e in with_gamma if has_flag(e, 'CONVERGE_ABOVE') or has_flag(e, 'CONVERGE_BELOW')]
    no_conv = [e for e in with_gamma if not has_flag(e, 'CONVERGE_ABOVE') and not has_flag(e, 'CONVERGE_BELOW')]

    print(f"  Any convergence:       {wr_stats(conv_any)}")
    print(f"  Converge ABOVE spot:   {wr_stats(conv_above)}")
    print(f"  Converge BELOW spot:   {wr_stats(conv_below)}")
    print(f"  No convergence:        {wr_stats(no_conv)}")

    print()
    print("  ── By Direction ──")
    for dir_label in ['long', 'short']:
        dir_trades = [e for e in with_gamma if e['direction'] in
                      (['long', 'bullish'] if dir_label == 'long' else ['short', 'bearish'])]
        conv = [e for e in dir_trades if has_flag(e, 'CONVERGE_ABOVE') or has_flag(e, 'CONVERGE_BELOW')]
        no_c = [e for e in dir_trades if not has_flag(e, 'CONVERGE_ABOVE') and not has_flag(e, 'CONVERGE_BELOW')]
        print(f"  {dir_label.upper()} + convergence:    {wr_stats(conv)}")
        print(f"  {dir_label.upper()} + no convergence: {wr_stats(no_c)}")

    # ── 6. Gamma as standalone direction filter ─────────────────────────
    print(f"\n{'='*80}")
    print("6. GAMMA NET DIRECTION vs TRADE DIRECTION")
    print(f"{'='*80}\n")

    for tf in GAMMA_TIMEFRAMES:
        has_tf = [e for e in with_gamma if e['gamma_by_tf'][tf] is not None]
        if not has_tf:
            continue
        aligned = []
        opposed = []
        for e in has_tf:
            net = e['gamma_by_tf'][tf]['total']
            is_long = e['direction'] in ('long', 'bullish')
            if (net > 0 and is_long) or (net < 0 and not is_long):
                aligned.append(e)
            else:
                opposed.append(e)
        print(f"  {tf}:")
        print(f"    Aligned:  {wr_stats(aligned)}")
        print(f"    Opposed:  {wr_stats(opposed)}")
        print()

    # ── 7. Near-spot gamma magnitude ────────────────────────────────────
    print(f"\n{'='*80}")
    print("7. NEAR-SPOT GAMMA MAGNITUDE")
    print(f"{'='*80}\n")

    for tf in ['TODAY', 'ALL']:
        has_tf = [e for e in with_gamma if e['gamma_by_tf'][tf] is not None]
        if not has_tf:
            continue
        print(f"  {tf}:")
        for lo, hi, label in [
            (0, 1e6, "Weak (<1M)"),
            (1e6, 5e6, "Medium (1-5M)"),
            (5e6, 50e6, "Strong (5-50M)"),
            (50e6, 1e15, "Very Strong (>50M)")
        ]:
            bucket = [e for e in has_tf if lo <= abs(e['gamma_by_tf'][tf].get('near_spot', 0)) < hi]
            if bucket:
                print(f"    {label}: {wr_stats(bucket)}")
        print()

    # ── 8. Greek Alignment + Gamma Confluence ───────────────────────────
    print(f"\n{'='*80}")
    print("8. GREEK ALIGNMENT + GAMMA CONFLUENCE")
    print(f"{'='*80}\n")

    for a_lo, a_hi, a_label in [(-3, -1, "Neg align"), (0, 0, "Neutral"), (1, 3, "Pos align")]:
        ab = [e for e in with_gamma if e['alignment'] is not None and a_lo <= e['alignment'] <= a_hi]
        if not ab:
            continue
        hi = [e for e in ab if e['best_confluence'] >= 2]
        lo = [e for e in ab if e['best_confluence'] < 2]
        print(f"  {a_label}:")
        if hi:
            print(f"    + High gamma conf: {wr_stats(hi)}")
        if lo:
            print(f"    + Low gamma conf:  {wr_stats(lo)}")

    # ── 9. Stacked S/R (charm+gamma at same directional level) ──────────
    print(f"\n{'='*80}")
    print("9. STACKED S/R: Charm+Gamma at Same Directional Level")
    print(f"{'='*80}\n")

    for tf in ['TODAY', 'ALL']:
        has_data = [e for e in with_gamma
                    if e['gamma_by_tf'][tf] is not None and e['charm_info'] is not None]
        if not has_data:
            continue
        print(f"  ── {tf} gamma ──")
        stacked = []
        single = []
        none_sr = []
        for e in has_data:
            ci = e['charm_info']
            gi = e['gamma_by_tf'][tf]
            is_long = e['direction'] in ('long', 'bullish')
            if is_long:
                cs = ci.get('strongest_below')
                gs = gi.get('strongest_below')
            else:
                cs = ci.get('strongest_above')
                gs = gi.get('strongest_above')
            if cs and gs and abs(cs[0] - gs[0]) <= 5:
                stacked.append(e)
            elif cs or gs:
                single.append(e)
            else:
                none_sr.append(e)

        print(f"    Stacked (same ±5 strike): {wr_stats(stacked)}")
        print(f"    Single S/R only:          {wr_stats(single)}")
        print(f"    No S/R found:             {wr_stats(none_sr)}")
        print()

    # ── 10. Top confluence trades ───────────────────────────────────────
    print(f"\n{'='*80}")
    print("10. TOP 25 HIGHEST CONFLUENCE TRADES")
    print(f"{'='*80}\n")

    top = sorted(with_gamma, key=lambda e: e['best_confluence'], reverse=True)[:25]
    print(f"  {'ID':>6} {'Date':>12} {'Setup':>18} {'Dir':>6} {'Spot':>8} {'Res':>5} {'PnL':>7} {'Conf':>4} {'TF':>12} {'Flags'}")
    print(f"  {'─'*6} {'─'*12} {'─'*18} {'─'*6} {'─'*8} {'─'*5} {'─'*7} {'─'*4} {'─'*12} {'─'*40}")
    for e in top:
        flags = e['confluence_by_tf'][e['best_tf']]['flags']
        print(f"  {e['id']:>6} {str(e['ts'])[:10]:>12} {e['setup']:>18} {e['direction']:>6} "
              f"{e['spot']:>8.1f} {e['result']:>5} {e['pnl']:>+7.1f} {e['best_confluence']:>4} "
              f"{e['best_tf']:>12} {', '.join(flags[:4])}")

    # ── 11. Data availability ───────────────────────────────────────────
    print(f"\n{'='*80}")
    print("11. DATA AVAILABILITY BY DATE")
    print(f"{'='*80}\n")

    by_date = defaultdict(lambda: {'total': 0, 'gamma': 0})
    for e in enriched:
        d = str(e['ts'])[:10]
        by_date[d]['total'] += 1
        if any(e['gamma_by_tf'][tf] is not None for tf in GAMMA_TIMEFRAMES):
            by_date[d]['gamma'] += 1

    print(f"  {'Date':>12} {'Total':>6} {'Gamma':>6} {'Cov%':>5}")
    for d in sorted(by_date.keys()):
        t = by_date[d]['total']
        g = by_date[d]['gamma']
        print(f"  {d:>12} {t:>6} {g:>6} {g/t*100:>5.0f}%")


if __name__ == '__main__':
    main()
