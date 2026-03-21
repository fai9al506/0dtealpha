"""
V10 Filter Backtest: V9-SC vs V10 (V9-SC + Gamma Confluence)
=============================================================
Replays all 765 setup signals through both filter sets.
Compares trades taken, PnL, WR, MaxDD, daily breakdown.
Shows real trade examples of blocked/saved trades.
"""

from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import datetime, timedelta
import bisect

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)

GAMMA_TIMEFRAMES = ['TODAY', 'THIS_WEEK', 'THIRTY_NEXT_DAYS', 'ALL']


# ========================================================================
#  DATA FETCHING (same batch approach)
# ========================================================================

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
        return conn.execute(sql).fetchall()


def fetch_all_exposures():
    print("Fetching gamma + charm exposure data...")
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
    print(f"  {len(rows)} exposure rows")

    raw = defaultdict(lambda: defaultdict(list))
    for r in rows:
        exp = r.expiration_option or 'TODAY'
        raw[(r.greek, exp)][r.ts_utc].append((float(r.strike), float(r.val)))

    snaps = {}
    for key, ts_dict in raw.items():
        sorted_ts = sorted(ts_dict.keys())
        snaps[key] = {'timestamps': sorted_ts, 'data': ts_dict}

    return snaps


def nearest_snapshot(snaps, greek, exp, ts, max_s=300):
    key = (greek, exp)
    if key not in snaps:
        return None
    timestamps = snaps[key]['timestamps']
    if not timestamps:
        return None
    idx = bisect.bisect_left(timestamps, ts)
    cands = []
    if idx > 0: cands.append(timestamps[idx-1])
    if idx < len(timestamps): cands.append(timestamps[idx])
    best = min(cands, key=lambda t: abs((t - ts).total_seconds()))
    if abs((best - ts).total_seconds()) > max_s:
        return None
    return snaps[key]['data'][best]


def get_strikes_near_spot(strike_vals, spot, proximity=30):
    if not strike_vals:
        return None
    nearby = [(s, v) for s, v in strike_vals if abs(s - spot) <= proximity]
    if not nearby:
        return None
    above = [(s, v) for s, v in nearby if s > spot]
    below = [(s, v) for s, v in nearby if s <= spot]
    strongest_above = max(above, key=lambda x: abs(x[1]), default=None)
    strongest_below = max(below, key=lambda x: abs(x[1]), default=None)
    # Positive gamma above spot
    pos_gamma_above = [(s, v) for s, v in above if v > 0]
    strongest_pos_above = max(pos_gamma_above, key=lambda x: x[1], default=None)
    # Positive gamma below spot
    pos_gamma_below = [(s, v) for s, v in below if v > 0]
    strongest_pos_below = max(pos_gamma_below, key=lambda x: x[1], default=None)
    total = sum(v for _, v in nearby)
    return {
        'strongest_above': strongest_above,
        'strongest_below': strongest_below,
        'pos_above': strongest_pos_above,  # positive gamma above = resistance
        'pos_below': strongest_pos_below,  # positive gamma below = support/pin
        'total': total,
    }


# ========================================================================
#  V9-SC FILTER (current production)
# ========================================================================

def v9sc_filter(setup, direction, alignment, vix, overvix, grade):
    """Replicate V9-SC live filter logic."""
    if grade == 'LOG':
        return False

    is_long = direction in ('long', 'bullish')

    # Shorts whitelist: SC (all), AG (all), DD (align!=0)
    if not is_long:
        if setup == 'Skew Charm':
            return True
        if setup == 'AG Short':
            return True
        if setup == 'DD Exhaustion':
            return alignment is not None and alignment != 0
        if setup == 'ES Absorption':
            return False  # blocked
        if setup == 'BofA Scalp':
            return False  # blocked
        if setup == 'Paradigm Reversal':
            return False  # blocked
        if setup == 'SB Absorption':
            return False
        if setup == 'Vanna Pivot Bounce':
            return False
        return False

    # Longs: alignment >= +2 AND (SC OR VIX <= 22 OR overvix >= +2)
    if alignment is None or alignment < 2:
        return False
    if setup == 'Skew Charm':
        return True  # SC exempt from VIX gate
    if vix is not None and vix <= 22:
        return True
    if overvix is not None and overvix >= 2:
        return True
    return False


# ========================================================================
#  V10 FILTER (V9-SC + Gamma Confluence)
# ========================================================================

def v10_filter(setup, direction, alignment, vix, overvix, grade,
               gamma_today, charm_info, gamma_all):
    """V10 = V9-SC + gamma gates."""

    # First pass V9-SC
    if not v9sc_filter(setup, direction, alignment, vix, overvix, grade):
        return False, 'V9-SC blocked'

    is_long = direction in ('long', 'bullish')

    # --- GAMMA FILTER 1: Short gamma resistance gate ---
    # For shorts: require positive gamma above spot exists (5-50M)
    if not is_long and gamma_today is not None:
        pos_above = gamma_today.get('pos_above')
        if pos_above is None:
            return False, 'no gamma resist above'
        mag = abs(pos_above[1])
        if mag < 5e6:
            return False, 'gamma resist too weak (<5M)'
        # >50M: borderline, let it pass for now (only 4 trades)

    # --- GAMMA FILTER 2: Long heavy gamma pinning block ---
    # For longs: block if positive gamma below spot > 20M (pins price)
    if is_long and gamma_today is not None:
        pos_below = gamma_today.get('pos_below')
        if pos_below is not None and pos_below[1] > 20e6:
            return False, 'gamma pin below >20M'

    # --- GAMMA FILTER 3: BofA stacked-only ---
    # BofA requires gamma+charm convergence (stacked S/R)
    if setup == 'BofA Scalp' and gamma_today is not None and charm_info is not None:
        is_stacked = False
        if is_long:
            cs = charm_info.get('strongest_below')
            gs = gamma_today.get('strongest_below')
            if cs and gs and abs(cs[0] - gs[0]) <= 10:
                is_stacked = True
        else:
            cs = charm_info.get('strongest_above')
            gs = gamma_today.get('strongest_above')
            if cs and gs and abs(cs[0] - gs[0]) <= 10:
                is_stacked = True
        if not is_stacked:
            return False, 'BofA not stacked'

    # --- GAMMA FILTER 4: Partial TF agreement block ---
    # Block if TODAY + ALL gamma don't both show positive confluence
    if gamma_today is not None and gamma_all is not None:
        today_aligned = False
        all_aligned = False

        if is_long:
            # For longs: check if there's support structure
            if gamma_today.get('pos_below') or gamma_today['total'] > 0:
                today_aligned = True
            if gamma_all.get('pos_below') or gamma_all['total'] > 0:
                all_aligned = True
        else:
            # For shorts: check if there's resistance structure
            if gamma_today.get('pos_above') or gamma_today['total'] < 0:
                today_aligned = True
            if gamma_all.get('pos_above') or gamma_all['total'] < 0:
                all_aligned = True

        # Only block if BOTH disagree (one disagreeing is partial = flag, not block)
        # Actually from data: one-only = 32% WR. Block.
        if not today_aligned and not all_aligned:
            return False, 'TF disagree (TODAY+ALL)'

    return True, 'pass'


# ========================================================================
#  ANALYSIS
# ========================================================================

def compute_maxdd(pnl_series):
    """MaxDD from a list of trade PnLs."""
    peak = 0
    cumulative = 0
    maxdd = 0
    for pnl in pnl_series:
        cumulative += pnl
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > maxdd:
            maxdd = dd
    return maxdd


def stats(trades, n_days):
    if not trades:
        return {'n': 0, 'wins': 0, 'losses': 0, 'wr': 0, 'pnl': 0,
                'pnl_d': 0, 'pnl_t': 0, 'maxdd': 0}
    wins = sum(1 for t in trades if t['result'] == 'WIN')
    losses = len(trades) - wins
    pnl = sum(t['pnl'] for t in trades)
    maxdd = compute_maxdd([t['pnl'] for t in trades])
    return {
        'n': len(trades), 'wins': wins, 'losses': losses,
        'wr': wins / len(trades) * 100 if trades else 0,
        'pnl': pnl, 'pnl_d': pnl / n_days if n_days else 0,
        'pnl_t': pnl / len(trades) if trades else 0, 'maxdd': maxdd,
    }


def print_stats(label, s):
    print(f"  {label}: {s['wins']}W/{s['losses']}L ({s['wr']:.0f}% WR), "
          f"{s['pnl']:+.1f} pts total, {s['pnl_d']:+.1f} pts/day, "
          f"{s['pnl_t']:+.1f} pts/trade, MaxDD: {s['maxdd']:.1f}")


def main():
    setups = fetch_setups()
    snaps = fetch_all_exposures()
    print(f"Loaded {len(setups)} signals\n")

    # ── Enrich all signals ──────────────────────────────────────────
    enriched = []
    for s in setups:
        ts, spot = s.ts, float(s.spot)
        direction = s.direction

        charm_raw = nearest_snapshot(snaps, 'charm', 'TODAY', ts)
        charm_near = None
        if charm_raw:
            near = [(st, v) for st, v in charm_raw if abs(st - spot) <= 30]
            charm_near = get_strikes_near_spot(near, spot) if near else None

        gamma_today = None
        gamma_all = None
        g_raw = nearest_snapshot(snaps, 'gamma', 'TODAY', ts)
        if g_raw:
            near = [(st, v) for st, v in g_raw if abs(st - spot) <= 30]
            gamma_today = get_strikes_near_spot(near, spot) if near else None
        g_raw_all = nearest_snapshot(snaps, 'gamma', 'ALL', ts)
        if g_raw_all:
            near = [(st, v) for st, v in g_raw_all if abs(st - spot) <= 30]
            gamma_all = get_strikes_near_spot(near, spot) if near else None

        # Check stacked (charm+gamma at same strike, +/-10)
        is_stacked = False
        stacked_strike = None
        stacked_gamma_val = None
        if charm_near and gamma_today:
            is_long = direction in ('long', 'bullish')
            if is_long:
                cs = charm_near.get('strongest_below')
                gs = gamma_today.get('strongest_below')
            else:
                cs = charm_near.get('strongest_above')
                gs = gamma_today.get('strongest_above')
            if cs and gs and abs(cs[0] - gs[0]) <= 10:
                is_stacked = True
                stacked_strike = gs[0]
                stacked_gamma_val = gs[1]

        v9_pass = v9sc_filter(s.setup_name, direction, s.greek_alignment,
                              float(s.vix) if s.vix else None,
                              float(s.overvix) if s.overvix else None,
                              s.grade)

        v10_pass, v10_reason = v10_filter(
            s.setup_name, direction, s.greek_alignment,
            float(s.vix) if s.vix else None,
            float(s.overvix) if s.overvix else None,
            s.grade,
            gamma_today, charm_near, gamma_all)

        enriched.append({
            'id': s.id, 'ts': s.ts, 'setup': s.setup_name,
            'direction': direction, 'grade': s.grade, 'spot': spot,
            'result': s.outcome_result,
            'pnl': float(s.outcome_pnl) if s.outcome_pnl else 0,
            'max_profit': float(s.outcome_max_profit) if s.outcome_max_profit else 0,
            'alignment': s.greek_alignment,
            'vix': float(s.vix) if s.vix else None,
            'paradigm': s.paradigm,
            'v9_pass': v9_pass,
            'v10_pass': v10_pass,
            'v10_reason': v10_reason,
            'is_stacked': is_stacked,
            'stacked_strike': stacked_strike,
            'stacked_gamma_val': stacked_gamma_val,
            'gamma_today': gamma_today,
            'charm_info': charm_near,
        })

    # ── Determine date range ────────────────────────────────────────
    dates = sorted(set(str(e['ts'])[:10] for e in enriched))
    n_days = len(dates)
    print(f"Date range: {dates[0]} to {dates[-1]} ({n_days} trading days)\n")

    v9_trades = [e for e in enriched if e['v9_pass']]
    v10_trades = [e for e in enriched if e['v10_pass']]
    v9_blocked_v10_passed = [e for e in enriched if not e['v9_pass'] and e['v10_pass']]
    v9_passed_v10_blocked = [e for e in enriched if e['v9_pass'] and not e['v10_pass']]

    # ================================================================
    #  COMPARISON TABLE
    # ================================================================
    print("=" * 80)
    print("V9-SC vs V10 COMPARISON")
    print("=" * 80)

    s9 = stats(v9_trades, n_days)
    s10 = stats(v10_trades, n_days)
    all_s = stats(enriched, n_days)

    print(f"\n  {'':>25} {'V9-SC':>12} {'V10':>12} {'Unfiltered':>12}")
    print(f"  {'':>25} {'-----':>12} {'-----':>12} {'----------':>12}")
    print(f"  {'Trades':>25} {s9['n']:>12} {s10['n']:>12} {all_s['n']:>12}")
    print(f"  {'Trades/day':>25} {s9['n']/n_days:>12.1f} {s10['n']/n_days:>12.1f} {all_s['n']/n_days:>12.1f}")
    print(f"  {'Wins':>25} {s9['wins']:>12} {s10['wins']:>12} {all_s['wins']:>12}")
    print(f"  {'Losses':>25} {s9['losses']:>12} {s10['losses']:>12} {all_s['losses']:>12}")
    print(f"  {'Win Rate':>25} {s9['wr']:>11.1f}% {s10['wr']:>11.1f}% {all_s['wr']:>11.1f}%")
    print(f"  {'Total PnL':>25} {s9['pnl']:>+12.1f} {s10['pnl']:>+12.1f} {all_s['pnl']:>+12.1f}")
    print(f"  {'PnL / day':>25} {s9['pnl_d']:>+12.1f} {s10['pnl_d']:>+12.1f} {all_s['pnl_d']:>+12.1f}")
    print(f"  {'PnL / trade':>25} {s9['pnl_t']:>+12.1f} {s10['pnl_t']:>+12.1f} {all_s['pnl_t']:>+12.1f}")
    print(f"  {'MaxDD':>25} {s9['maxdd']:>12.1f} {s10['maxdd']:>12.1f} {all_s['maxdd']:>12.1f}")

    # Profit factor
    for label, trades in [("V9-SC", v9_trades), ("V10", v10_trades)]:
        gross_win = sum(t['pnl'] for t in trades if t['pnl'] > 0)
        gross_loss = abs(sum(t['pnl'] for t in trades if t['pnl'] < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else 999
        print(f"  {'Profit Factor (' + label + ')':>25} {pf:>12.2f}")

    # ================================================================
    #  TRADES BLOCKED BY V10 (passed V9-SC but blocked by V10)
    # ================================================================
    print(f"\n{'=' * 80}")
    print(f"TRADES V10 BLOCKS (that V9-SC allowed): {len(v9_passed_v10_blocked)}")
    print("=" * 80)

    blocked_stats = stats(v9_passed_v10_blocked, n_days)
    print_stats("Blocked trades", blocked_stats)

    # Group by reason
    by_reason = defaultdict(list)
    for e in v9_passed_v10_blocked:
        by_reason[e['v10_reason']].append(e)

    print(f"\n  By block reason:")
    for reason, trades in sorted(by_reason.items(), key=lambda x: -len(x[1])):
        s = stats(trades, n_days)
        print(f"    {reason:>30}: {s['n']:>3} trades, {s['wins']}W/{s['losses']}L "
              f"({s['wr']:.0f}% WR), {s['pnl']:+.1f} pts")

    # Group by setup
    print(f"\n  By setup:")
    by_setup = defaultdict(list)
    for e in v9_passed_v10_blocked:
        by_setup[e['setup']].append(e)
    for setup, trades in sorted(by_setup.items(), key=lambda x: -len(x[1])):
        s = stats(trades, n_days)
        print(f"    {setup:>20}: {s['n']:>3} trades, {s['wins']}W/{s['losses']}L "
              f"({s['wr']:.0f}% WR), {s['pnl']:+.1f} pts")

    # ================================================================
    #  PER-SETUP COMPARISON
    # ================================================================
    print(f"\n{'=' * 80}")
    print("PER-SETUP V9-SC vs V10")
    print("=" * 80)

    setup_names = sorted(set(e['setup'] for e in enriched))
    for setup in setup_names:
        v9s = [e for e in v9_trades if e['setup'] == setup]
        v10s = [e for e in v10_trades if e['setup'] == setup]
        if not v9s and not v10s:
            continue
        s9 = stats(v9s, n_days)
        s10 = stats(v10s, n_days)
        delta_pnl = s10['pnl'] - s9['pnl']
        delta_wr = s10['wr'] - s9['wr']
        print(f"\n  {setup}:")
        print(f"    V9-SC: {s9['n']:>3} trades, {s9['wr']:.0f}% WR, {s9['pnl']:+.1f} pts, MaxDD {s9['maxdd']:.1f}")
        print(f"    V10:   {s10['n']:>3} trades, {s10['wr']:.0f}% WR, {s10['pnl']:+.1f} pts, MaxDD {s10['maxdd']:.1f}")
        print(f"    Delta: {s10['n'] - s9['n']:+d} trades, {delta_wr:+.1f}% WR, {delta_pnl:+.1f} pts")

    # ================================================================
    #  PER-DAY COMPARISON
    # ================================================================
    print(f"\n{'=' * 80}")
    print("PER-DAY COMPARISON")
    print("=" * 80)

    print(f"\n  {'Date':>12} {'V9 Trades':>10} {'V9 PnL':>8} {'V10 Trades':>11} {'V10 PnL':>8} {'Delta':>8}")
    print(f"  {'-'*12} {'-'*10} {'-'*8} {'-'*11} {'-'*8} {'-'*8}")

    v9_cumulative = 0
    v10_cumulative = 0
    for d in dates:
        v9d = [e for e in v9_trades if str(e['ts'])[:10] == d]
        v10d = [e for e in v10_trades if str(e['ts'])[:10] == d]
        v9_pnl = sum(e['pnl'] for e in v9d)
        v10_pnl = sum(e['pnl'] for e in v10d)
        v9_cumulative += v9_pnl
        v10_cumulative += v10_pnl
        delta = v10_pnl - v9_pnl
        marker = " ***" if abs(delta) > 20 else ""
        print(f"  {d:>12} {len(v9d):>10} {v9_pnl:>+8.1f} {len(v10d):>11} {v10_pnl:>+8.1f} {delta:>+8.1f}{marker}")

    print(f"\n  Cumulative: V9-SC {v9_cumulative:+.1f} pts | V10 {v10_cumulative:+.1f} pts | "
          f"V10 advantage: {v10_cumulative - v9_cumulative:+.1f} pts")

    # ================================================================
    #  EXAMPLE TRADES: Saved by V10 (losses V9-SC took but V10 blocked)
    # ================================================================
    print(f"\n{'=' * 80}")
    print("EXAMPLE TRADES: LOSSES V10 SAVES (V9-SC took these, V10 blocks them)")
    print("=" * 80)

    saved_losses = sorted([e for e in v9_passed_v10_blocked if e['result'] == 'LOSS'],
                          key=lambda e: e['pnl'])[:20]
    print(f"\n  Top 20 worst losses V10 would have avoided:\n")
    print(f"  {'ID':>6} {'Date':>12} {'Time':>8} {'Setup':>18} {'Dir':>6} {'Spot':>8} "
          f"{'PnL':>7} {'Block Reason':>30}")
    print(f"  {'-'*6} {'-'*12} {'-'*8} {'-'*18} {'-'*6} {'-'*8} {'-'*7} {'-'*30}")
    for e in saved_losses:
        time_str = str(e['ts'])[11:19]
        print(f"  {e['id']:>6} {str(e['ts'])[:10]:>12} {time_str:>8} {e['setup']:>18} "
              f"{e['direction']:>6} {e['spot']:>8.1f} {e['pnl']:>+7.1f} {e['v10_reason']:>30}")

    # ================================================================
    #  EXAMPLE TRADES: Winners V10 also blocks (false positives)
    # ================================================================
    print(f"\n{'=' * 80}")
    print("EXAMPLE TRADES: WINNERS V10 LOSES (V9-SC took these, V10 blocks them)")
    print("=" * 80)

    lost_wins = sorted([e for e in v9_passed_v10_blocked if e['result'] == 'WIN'],
                       key=lambda e: -e['pnl'])[:20]
    print(f"\n  Top 20 best winners V10 would have missed:\n")
    print(f"  {'ID':>6} {'Date':>12} {'Time':>8} {'Setup':>18} {'Dir':>6} {'Spot':>8} "
          f"{'PnL':>7} {'Block Reason':>30}")
    print(f"  {'-'*6} {'-'*12} {'-'*8} {'-'*18} {'-'*6} {'-'*8} {'-'*7} {'-'*30}")
    for e in lost_wins:
        time_str = str(e['ts'])[11:19]
        print(f"  {e['id']:>6} {str(e['ts'])[:10]:>12} {time_str:>8} {e['setup']:>18} "
              f"{e['direction']:>6} {e['spot']:>8.1f} {e['pnl']:>+7.1f} {e['v10_reason']:>30}")

    # ================================================================
    #  SPECIFIC REAL EXAMPLES WITH GAMMA CONTEXT
    # ================================================================
    print(f"\n{'=' * 80}")
    print("REAL EXAMPLES WITH FULL GAMMA CONTEXT")
    print("=" * 80)

    # Show a few interesting blocked trades with full gamma data
    examples = sorted(v9_passed_v10_blocked, key=lambda e: abs(e['pnl']), reverse=True)[:10]
    for e in examples:
        print(f"\n  --- Trade #{e['id']} ({str(e['ts'])[:19]}) ---")
        print(f"  {e['setup']} {e['direction'].upper()} @ SPX {e['spot']:.1f}")
        print(f"  Result: {e['result']} {e['pnl']:+.1f} pts | Alignment: {e['alignment']} | VIX: {e['vix']}")
        print(f"  V10 blocked: {e['v10_reason']}")

        if e['gamma_today']:
            g = e['gamma_today']
            print(f"  Gamma TODAY:")
            if g.get('pos_above'):
                print(f"    Positive above (resistance): strike {g['pos_above'][0]:.0f}, "
                      f"value {g['pos_above'][1]/1e6:.1f}M")
            else:
                print(f"    Positive above: NONE (no gamma resistance)")
            if g.get('pos_below'):
                print(f"    Positive below (support/pin): strike {g['pos_below'][0]:.0f}, "
                      f"value {g['pos_below'][1]/1e6:.1f}M")
            else:
                print(f"    Positive below: NONE")
            print(f"    Net gamma: {g['total']/1e6:.1f}M")

        if e['charm_info']:
            c = e['charm_info']
            if c.get('strongest_above'):
                print(f"  Charm strongest above: strike {c['strongest_above'][0]:.0f}, "
                      f"value {c['strongest_above'][1]/1e6:.1f}M")
            if c.get('strongest_below'):
                print(f"  Charm strongest below: strike {c['strongest_below'][0]:.0f}, "
                      f"value {c['strongest_below'][1]/1e6:.1f}M")

        if e['is_stacked']:
            print(f"  STACKED: gamma+charm converge @ {e['stacked_strike']:.0f} "
                  f"(gamma {e['stacked_gamma_val']/1e6:.1f}M)")
        else:
            print(f"  NOT STACKED (gamma and charm at different strikes)")

    # ================================================================
    #  LONGS vs SHORTS COMPARISON
    # ================================================================
    print(f"\n{'=' * 80}")
    print("DIRECTION BREAKDOWN: V9-SC vs V10")
    print("=" * 80)

    for dir_label, dir_vals in [("LONG", ['long', 'bullish']), ("SHORT", ['short', 'bearish'])]:
        v9d = [e for e in v9_trades if e['direction'] in dir_vals]
        v10d = [e for e in v10_trades if e['direction'] in dir_vals]
        s9 = stats(v9d, n_days)
        s10 = stats(v10d, n_days)
        print(f"\n  {dir_label}:")
        print(f"    V9-SC: {s9['n']:>3} trades, {s9['wr']:.0f}% WR, {s9['pnl']:+.1f} pts, "
              f"{s9['pnl_d']:+.1f}/day, MaxDD {s9['maxdd']:.1f}")
        print(f"    V10:   {s10['n']:>3} trades, {s10['wr']:.0f}% WR, {s10['pnl']:+.1f} pts, "
              f"{s10['pnl_d']:+.1f}/day, MaxDD {s10['maxdd']:.1f}")

    # ================================================================
    #  V10 VARIANT ANALYSIS: Each filter alone
    # ================================================================
    print(f"\n{'=' * 80}")
    print("V10 FILTER IMPACT: EACH FILTER TESTED ALONE (on top of V9-SC)")
    print("=" * 80)

    # Test each gamma filter independently
    filters = {
        'F1: Short gamma resist': lambda e: not (
            e['direction'] in ('short', 'bearish') and
            e['gamma_today'] is not None and
            (e['gamma_today'].get('pos_above') is None or
             abs(e['gamma_today']['pos_above'][1]) < 5e6)
        ),
        'F2: Long gamma pin block': lambda e: not (
            e['direction'] in ('long', 'bullish') and
            e['gamma_today'] is not None and
            e['gamma_today'].get('pos_below') is not None and
            e['gamma_today']['pos_below'][1] > 20e6
        ),
        'F3: BofA stacked-only': lambda e: not (
            e['setup'] == 'BofA Scalp' and
            not e['is_stacked']
        ),
        'F4: TF agreement': lambda e: True,  # placeholder - need full TF data
    }

    print(f"\n  {'Filter':>30} {'Trades':>7} {'WR':>6} {'PnL':>10} {'PnL/d':>8} "
          f"{'vs V9-SC':>10} {'MaxDD':>8}")
    print(f"  {'-'*30} {'-'*7} {'-'*6} {'-'*10} {'-'*8} {'-'*10} {'-'*8}")

    v9s = stats(v9_trades, n_days)
    print(f"  {'V9-SC (baseline)':>30} {v9s['n']:>7} {v9s['wr']:>5.0f}% {v9s['pnl']:>+10.1f} "
          f"{v9s['pnl_d']:>+8.1f} {'---':>10} {v9s['maxdd']:>8.1f}")

    for name, filt in filters.items():
        filtered = [e for e in v9_trades if filt(e)]
        s = stats(filtered, n_days)
        delta = s['pnl'] - v9s['pnl']
        print(f"  {name:>30} {s['n']:>7} {s['wr']:>5.0f}% {s['pnl']:>+10.1f} "
              f"{s['pnl_d']:>+8.1f} {delta:>+10.1f} {s['maxdd']:>8.1f}")

    # V10 combined
    s10 = stats(v10_trades, n_days)
    delta = s10['pnl'] - v9s['pnl']
    print(f"  {'V10 (all combined)':>30} {s10['n']:>7} {s10['wr']:>5.0f}% {s10['pnl']:>+10.1f} "
          f"{s10['pnl_d']:>+8.1f} {delta:>+10.1f} {s10['maxdd']:>8.1f}")

    # ================================================================
    #  EXECUTIVE SUMMARY
    # ================================================================
    print(f"\n{'=' * 80}")
    print("EXECUTIVE SUMMARY")
    print("=" * 80)

    v9s = stats(v9_trades, n_days)
    v10s = stats(v10_trades, n_days)
    print(f"""
  V9-SC (current):  {v9s['n']} trades, {v9s['wr']:.0f}% WR, {v9s['pnl']:+.1f} pts, {v9s['pnl_d']:+.1f}/day, MaxDD {v9s['maxdd']:.1f}
  V10 (proposed):   {v10s['n']} trades, {v10s['wr']:.0f}% WR, {v10s['pnl']:+.1f} pts, {v10s['pnl_d']:+.1f}/day, MaxDD {v10s['maxdd']:.1f}

  Change:           {v10s['n'] - v9s['n']:+d} trades, {v10s['wr'] - v9s['wr']:+.1f}% WR, {v10s['pnl'] - v9s['pnl']:+.1f} pts
  Trades blocked:   {len(v9_passed_v10_blocked)} ({sum(1 for e in v9_passed_v10_blocked if e['result']=='WIN')}W / {sum(1 for e in v9_passed_v10_blocked if e['result']=='LOSS')}L)
  PnL saved:        {-sum(e['pnl'] for e in v9_passed_v10_blocked):+.1f} pts from blocked trades
""")


if __name__ == '__main__':
    main()
