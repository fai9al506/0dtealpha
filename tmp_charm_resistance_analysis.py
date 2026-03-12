"""
Charm Resistance Filter Analysis
=================================
Tests whether blocking LONG trades when big positive charm exposure exists
above spot (within 20 pts) improves performance.

Logic: positive charm above spot = dealer selling resistance = headwind for longs
Reverse: negative charm below spot = dealer buying support = headwind for shorts
"""

import psycopg2
import psycopg2.extras
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from collections import defaultdict

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

def get_conn():
    return psycopg2.connect(DB_URL)

def is_long(direction):
    return direction.lower() in ('long', 'bullish')

def is_short(direction):
    return direction.lower() in ('short', 'bearish')

def round_to_5(x):
    """Round to nearest 5 (SPX strike grid)."""
    return round(x / 5) * 5

def fmt_m(val):
    """Format number in millions."""
    if val is None:
        return "N/A"
    v = float(val)
    if abs(v) >= 1e9:
        return f"{v/1e9:+.2f}B"
    return f"{v/1e6:+.1f}M"

def compute_stats(trades):
    """Compute win rate, total PnL, profit factor from list of (outcome_result, outcome_pnl) tuples."""
    if not trades:
        return {"count": 0, "wr": 0, "pnl": 0, "pf": 0, "avg_pnl": 0}

    n = len(trades)
    wins = sum(1 for r, _ in trades if r in ('WIN', 'WIN_TRAIL'))
    total_pnl = sum(float(p) for _, p in trades if p is not None)
    gross_win = sum(float(p) for r, p in trades if p is not None and float(p) > 0)
    gross_loss = abs(sum(float(p) for r, p in trades if p is not None and float(p) < 0))

    wr = wins / n * 100 if n > 0 else 0
    pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
    avg = total_pnl / n if n > 0 else 0

    return {"count": n, "wr": wr, "pnl": total_pnl, "pf": pf, "avg_pnl": avg}

def print_stats(label, stats):
    pf_str = f"{stats['pf']:.2f}" if stats['pf'] != float('inf') else "inf"
    print(f"  {label:40s} | {stats['count']:4d} trades | WR {stats['wr']:5.1f}% | PnL {stats['pnl']:+8.1f} | PF {pf_str:>6s} | Avg {stats['avg_pnl']:+.1f}")


def main():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # =========================================================================
    # STEP 1: Today's signals (2026-03-11)
    # =========================================================================
    print("=" * 100)
    print("STEP 1: TODAY'S SIGNALS (2026-03-11) WITH CHARM ABOVE/BELOW SPOT")
    print("=" * 100)

    cur.execute("""
        SELECT id, ts, setup_name, direction, grade, score, spot,
               outcome_result, outcome_pnl, greek_alignment
        FROM setup_log
        WHERE ts::date = '2026-03-11'
        ORDER BY ts
    """)
    today_signals = cur.fetchall()

    for sig in today_signals:
        sig_id = sig['id']
        sig_ts = sig['ts']
        sig_name = sig['setup_name']
        sig_dir = sig['direction']
        spot = float(sig['spot'])
        spot_rounded = round_to_5(spot)

        print(f"\n--- Signal #{sig_id}: {sig_name} {sig_dir} @ {spot:.2f} | "
              f"Grade={sig['grade']} | Align={sig['greek_alignment']} | "
              f"Outcome={sig['outcome_result']} {sig['outcome_pnl']}")
        print(f"    Time: {sig_ts}")

        # Find closest charm snapshot BEFORE signal time (within 5 min)
        cur.execute("""
            SELECT DISTINCT ts_utc
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc <= %s
              AND ts_utc >= %s - interval '5 minutes'
            ORDER BY ts_utc DESC
            LIMIT 1
        """, (sig_ts, sig_ts))
        snap_row = cur.fetchone()

        if not snap_row:
            print("    [NO CHARM SNAPSHOT FOUND WITHIN 5 MIN BEFORE SIGNAL]")
            continue

        snap_ts = snap_row['ts_utc']

        # Get charm at strikes above spot (spot to spot+20)
        if is_long(sig_dir):
            strike_lo = spot_rounded
            strike_hi = spot_rounded + 20
            zone_label = f"ABOVE spot ({strike_lo}-{strike_hi})"
        else:
            strike_lo = spot_rounded - 20
            strike_hi = spot_rounded
            zone_label = f"BELOW spot ({strike_lo}-{strike_hi})"

        cur.execute("""
            SELECT strike, value
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc = %s
              AND strike >= %s AND strike <= %s
            ORDER BY strike
        """, (snap_ts, strike_lo, strike_hi))

        charm_rows = cur.fetchall()

        if not charm_rows:
            print(f"    [NO CHARM POINTS IN ZONE {zone_label}]")
            continue

        total_charm = sum(float(r['value']) for r in charm_rows)
        max_charm = max(float(r['value']) for r in charm_rows)
        min_charm = min(float(r['value']) for r in charm_rows)

        print(f"    Charm snapshot: {snap_ts}")
        print(f"    Zone: {zone_label}")
        for r in charm_rows:
            bar = "+" * min(50, int(abs(float(r['value'])) / 50_000_000)) if float(r['value']) > 0 else "-" * min(50, int(abs(float(r['value'])) / 50_000_000))
            print(f"      Strike {int(r['strike']):5d}: {fmt_m(r['value']):>10s}  {bar}")

        print(f"    SUM charm in zone: {fmt_m(total_charm)}")
        print(f"    MAX single bar:    {fmt_m(max_charm)}")

        if is_long(sig_dir):
            # Would block if big positive charm above
            would_block_sum = total_charm > 200_000_000
            would_block_max = max_charm > 100_000_000
            print(f"    FILTER (sum>200M): {'BLOCK' if would_block_sum else 'PASS'}")
            print(f"    FILTER (max>100M): {'BLOCK' if would_block_max else 'PASS'}")
        else:
            # Would block if big negative charm below
            would_block_sum = total_charm < -200_000_000
            would_block_min = min_charm < -100_000_000
            print(f"    FILTER (sum<-200M): {'BLOCK' if would_block_sum else 'PASS'}")
            print(f"    FILTER (min<-100M): {'BLOCK' if would_block_min else 'PASS'}")

    # =========================================================================
    # STEP 2: Historical backtest — LONG trades
    # =========================================================================
    print("\n" + "=" * 100)
    print("STEP 2: HISTORICAL BACKTEST — CHARM RESISTANCE FILTER FOR LONG TRADES")
    print("=" * 100)

    cur.execute("""
        SELECT id, ts, setup_name, direction, spot, outcome_result, outcome_pnl, greek_alignment
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND direction IN ('long', 'bullish')
        ORDER BY ts
    """)
    long_trades = cur.fetchall()
    print(f"\nTotal LONG trades with outcomes: {len(long_trades)}")

    # For each long trade, find charm above spot
    long_results = []  # (trade, charm_sum, charm_max)
    skipped = 0

    for trade in long_trades:
        spot = float(trade['spot'])
        spot_rounded = round_to_5(spot)
        trade_ts = trade['ts']

        # Find closest charm snapshot before signal
        cur.execute("""
            SELECT DISTINCT ts_utc
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc <= %s
              AND ts_utc >= %s - interval '5 minutes'
            ORDER BY ts_utc DESC
            LIMIT 1
        """, (trade_ts, trade_ts))
        snap_row = cur.fetchone()

        if not snap_row:
            skipped += 1
            continue

        snap_ts = snap_row['ts_utc']

        # Charm above spot (spot to spot+20)
        cur.execute("""
            SELECT strike, value
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc = %s
              AND strike >= %s AND strike <= %s
            ORDER BY strike
        """, (snap_ts, spot_rounded, spot_rounded + 20))

        charm_rows = cur.fetchall()

        if not charm_rows:
            skipped += 1
            continue

        charm_sum = sum(float(r['value']) for r in charm_rows)
        charm_max = max(float(r['value']) for r in charm_rows)

        long_results.append({
            'trade': trade,
            'charm_sum': charm_sum,
            'charm_max': charm_max,
        })

    print(f"Matched with charm data: {len(long_results)} (skipped {skipped} — no charm snapshot)")

    if long_results:
        # Baseline (all)
        all_trades = [(r['trade']['outcome_result'], r['trade']['outcome_pnl']) for r in long_results]
        all_stats = compute_stats(all_trades)
        print(f"\n  --- BASELINE (all matched LONG trades) ---")
        print_stats("All LONG trades", all_stats)

        # Test SUM thresholds (block if charm_sum > threshold)
        sum_thresholds = [50_000_000, 100_000_000, 200_000_000, 500_000_000, 1_000_000_000]
        print(f"\n  --- SUM OF CHARM ABOVE SPOT (0 to +20 pts) THRESHOLDS ---")
        print(f"  Block LONG if sum of positive charm above spot > threshold\n")

        for thresh in sum_thresholds:
            passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                      for r in long_results if r['charm_sum'] <= thresh]
            blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                       for r in long_results if r['charm_sum'] > thresh]

            p_stats = compute_stats(passed)
            b_stats = compute_stats(blocked)

            print(f"  Threshold: charm_sum > {fmt_m(thresh):>8s}")
            print_stats(f"    PASSED (took trade)", p_stats)
            print_stats(f"    BLOCKED (would skip)", b_stats)
            pnl_saved = b_stats['pnl']
            print(f"    --> PnL saved by blocking: {-pnl_saved:+.1f} pts" if pnl_saved < 0 else f"    --> PnL LOST by blocking: {-pnl_saved:+.1f} pts")
            print()

        # Test MAX single-bar thresholds
        max_thresholds = [20_000_000, 50_000_000, 100_000_000, 200_000_000, 500_000_000]
        print(f"\n  --- MAX SINGLE CHARM BAR ABOVE SPOT (0 to +20 pts) THRESHOLDS ---")
        print(f"  Block LONG if any single charm bar above spot > threshold\n")

        for thresh in max_thresholds:
            passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                      for r in long_results if r['charm_max'] <= thresh]
            blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                       for r in long_results if r['charm_max'] > thresh]

            p_stats = compute_stats(passed)
            b_stats = compute_stats(blocked)

            print(f"  Threshold: charm_max > {fmt_m(thresh):>8s}")
            print_stats(f"    PASSED (took trade)", p_stats)
            print_stats(f"    BLOCKED (would skip)", b_stats)
            pnl_saved = b_stats['pnl']
            print(f"    --> PnL saved by blocking: {-pnl_saved:+.1f} pts" if pnl_saved < 0 else f"    --> PnL LOST by blocking: {-pnl_saved:+.1f} pts")
            print()

        # Breakdown by setup type
        print(f"\n  --- BREAKDOWN BY SETUP TYPE (using charm_sum > 200M filter) ---\n")
        by_setup = defaultdict(list)
        for r in long_results:
            by_setup[r['trade']['setup_name']].append(r)

        for setup_name, results in sorted(by_setup.items()):
            print(f"  {setup_name}:")
            all_t = [(r['trade']['outcome_result'], r['trade']['outcome_pnl']) for r in results]
            passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                      for r in results if r['charm_sum'] <= 200_000_000]
            blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                       for r in results if r['charm_sum'] > 200_000_000]

            print_stats(f"    All", compute_stats(all_t))
            print_stats(f"    Passed (sum<=200M)", compute_stats(passed))
            print_stats(f"    Blocked (sum>200M)", compute_stats(blocked))
            print()

        # Show distribution of charm_sum values
        print(f"\n  --- CHARM SUM DISTRIBUTION (LONG trades, above spot 0-20) ---")
        sums = sorted([r['charm_sum'] for r in long_results])
        percentiles = [10, 25, 50, 75, 90]
        for p in percentiles:
            idx = int(len(sums) * p / 100)
            print(f"    P{p:2d}: {fmt_m(sums[idx])}")

        # Show blocked trades detail (sum > 200M)
        print(f"\n  --- DETAIL: LONG TRADES WITH CHARM SUM > 200M (would be blocked) ---")
        blocked_detail = [r for r in long_results if r['charm_sum'] > 200_000_000]
        blocked_detail.sort(key=lambda r: r['charm_sum'], reverse=True)
        for r in blocked_detail[:30]:
            t = r['trade']
            print(f"    #{t['id']:4d} {str(t['ts'])[:16]} {t['setup_name']:20s} spot={float(t['spot']):8.1f} "
                  f"charm_sum={fmt_m(r['charm_sum']):>10s} max={fmt_m(r['charm_max']):>10s} "
                  f"=> {t['outcome_result']:10s} {float(t['outcome_pnl']):+6.1f} align={t['greek_alignment']}")

    # =========================================================================
    # STEP 3: Today's charm landscape (12:50-13:30 ET = 17:50-18:30 UTC)
    # =========================================================================
    print("\n" + "=" * 100)
    print("STEP 3: TODAY'S CHARM LANDSCAPE (around 12:50-13:30 ET)")
    print("=" * 100)

    # Find snapshots in that window
    cur.execute("""
        SELECT DISTINCT ts_utc
        FROM volland_exposure_points
        WHERE greek = 'charm'
          AND ts_utc >= '2026-03-11 17:50:00+00'
          AND ts_utc <= '2026-03-11 18:30:00+00'
        ORDER BY ts_utc
    """)
    snap_times = [r['ts_utc'] for r in cur.fetchall()]

    if not snap_times:
        # Try broader range
        cur.execute("""
            SELECT DISTINCT ts_utc
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc::date = '2026-03-11'
            ORDER BY ts_utc
        """)
        all_snaps = [r['ts_utc'] for r in cur.fetchall()]
        print(f"\n  No snapshots in 12:50-13:30 ET window. Available today: {len(all_snaps)} snapshots")
        if all_snaps:
            # Pick one from the middle of the day
            mid_idx = len(all_snaps) // 2
            snap_times = [all_snaps[mid_idx]]
            print(f"  Using midday snapshot: {snap_times[0]}")

    for snap_ts in snap_times[:3]:  # Show up to 3 snapshots
        # Get current_price from any row in this snapshot
        cur.execute("""
            SELECT current_price FROM volland_exposure_points
            WHERE greek = 'charm' AND ts_utc = %s
            LIMIT 1
        """, (snap_ts,))
        cp = float(cur.fetchone()['current_price'])
        cp_rounded = round_to_5(cp)

        print(f"\n  Snapshot: {snap_ts} (ET: {snap_ts - timedelta(hours=5)}) | SPX: {cp:.2f}")

        cur.execute("""
            SELECT strike, value
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc = %s
              AND strike >= %s AND strike <= %s
            ORDER BY strike
        """, (snap_ts, cp_rounded - 30, cp_rounded + 30))

        rows = cur.fetchall()
        print(f"  {'Strike':>8s} {'Value':>12s}  {'Bar (50M scale)':>50s}")
        print(f"  {'-'*8} {'-'*12}  {'-'*50}")
        for r in rows:
            strike = int(r['strike'])
            val = float(r['value'])
            bar_len = min(50, int(abs(val) / 50_000_000))
            if val > 0:
                bar = " " * 25 + "|" + "█" * bar_len
            else:
                bar = " " * max(0, 25 - bar_len) + "█" * bar_len + "|"

            marker = " <-- SPOT" if strike == cp_rounded else ""
            print(f"  {strike:8d} {fmt_m(val):>12s}  {bar}{marker}")

    # =========================================================================
    # STEP 4: SHORT trades — charm support below spot
    # =========================================================================
    print("\n" + "=" * 100)
    print("STEP 4: HISTORICAL BACKTEST — CHARM SUPPORT FILTER FOR SHORT TRADES")
    print("=" * 100)
    print("Logic: Block SHORT if big NEGATIVE charm below spot (= dealer buying support)")

    cur.execute("""
        SELECT id, ts, setup_name, direction, spot, outcome_result, outcome_pnl, greek_alignment
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND direction IN ('short', 'bearish')
        ORDER BY ts
    """)
    short_trades = cur.fetchall()
    print(f"\nTotal SHORT trades with outcomes: {len(short_trades)}")

    short_results = []
    skipped = 0

    for trade in short_trades:
        spot = float(trade['spot'])
        spot_rounded = round_to_5(spot)
        trade_ts = trade['ts']

        cur.execute("""
            SELECT DISTINCT ts_utc
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc <= %s
              AND ts_utc >= %s - interval '5 minutes'
            ORDER BY ts_utc DESC
            LIMIT 1
        """, (trade_ts, trade_ts))
        snap_row = cur.fetchone()

        if not snap_row:
            skipped += 1
            continue

        snap_ts = snap_row['ts_utc']

        # Charm below spot (spot-20 to spot)
        cur.execute("""
            SELECT strike, value
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc = %s
              AND strike >= %s AND strike <= %s
            ORDER BY strike
        """, (snap_ts, spot_rounded - 20, spot_rounded))

        charm_rows = cur.fetchall()

        if not charm_rows:
            skipped += 1
            continue

        charm_sum = sum(float(r['value']) for r in charm_rows)
        charm_min = min(float(r['value']) for r in charm_rows)

        short_results.append({
            'trade': trade,
            'charm_sum': charm_sum,
            'charm_min': charm_min,
        })

    print(f"Matched with charm data: {len(short_results)} (skipped {skipped})")

    if short_results:
        all_trades = [(r['trade']['outcome_result'], r['trade']['outcome_pnl']) for r in short_results]
        all_stats = compute_stats(all_trades)
        print(f"\n  --- BASELINE (all matched SHORT trades) ---")
        print_stats("All SHORT trades", all_stats)

        # Test SUM thresholds (block if charm_sum < -threshold, i.e. big negative = support)
        sum_thresholds = [50_000_000, 100_000_000, 200_000_000, 500_000_000, 1_000_000_000]
        print(f"\n  --- SUM OF CHARM BELOW SPOT (spot-20 to spot) THRESHOLDS ---")
        print(f"  Block SHORT if sum of charm below spot < -threshold (big negative = support)\n")

        for thresh in sum_thresholds:
            passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                      for r in short_results if r['charm_sum'] >= -thresh]
            blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                       for r in short_results if r['charm_sum'] < -thresh]

            p_stats = compute_stats(passed)
            b_stats = compute_stats(blocked)

            print(f"  Threshold: charm_sum < {fmt_m(-thresh):>8s}")
            print_stats(f"    PASSED (took trade)", p_stats)
            print_stats(f"    BLOCKED (would skip)", b_stats)
            pnl_saved = b_stats['pnl']
            print(f"    --> PnL saved by blocking: {-pnl_saved:+.1f} pts" if pnl_saved < 0 else f"    --> PnL LOST by blocking: {-pnl_saved:+.1f} pts")
            print()

        # Test MIN single-bar thresholds
        min_thresholds = [20_000_000, 50_000_000, 100_000_000, 200_000_000, 500_000_000]
        print(f"\n  --- MIN SINGLE CHARM BAR BELOW SPOT THRESHOLDS ---")
        print(f"  Block SHORT if any single charm bar below spot < -threshold\n")

        for thresh in min_thresholds:
            passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                      for r in short_results if r['charm_min'] >= -thresh]
            blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                       for r in short_results if r['charm_min'] < -thresh]

            p_stats = compute_stats(passed)
            b_stats = compute_stats(blocked)

            print(f"  Threshold: charm_min < {fmt_m(-thresh):>8s}")
            print_stats(f"    PASSED (took trade)", p_stats)
            print_stats(f"    BLOCKED (would skip)", b_stats)
            pnl_saved = b_stats['pnl']
            print(f"    --> PnL saved by blocking: {-pnl_saved:+.1f} pts" if pnl_saved < 0 else f"    --> PnL LOST by blocking: {-pnl_saved:+.1f} pts")
            print()

        # Breakdown by setup type
        print(f"\n  --- BREAKDOWN BY SETUP TYPE (using charm_sum < -200M filter) ---\n")
        by_setup = defaultdict(list)
        for r in short_results:
            by_setup[r['trade']['setup_name']].append(r)

        for setup_name, results in sorted(by_setup.items()):
            print(f"  {setup_name}:")
            all_t = [(r['trade']['outcome_result'], r['trade']['outcome_pnl']) for r in results]
            passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                      for r in results if r['charm_sum'] >= -200_000_000]
            blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                       for r in results if r['charm_sum'] < -200_000_000]

            print_stats(f"    All", compute_stats(all_t))
            print_stats(f"    Passed (sum>=-200M)", compute_stats(passed))
            print_stats(f"    Blocked (sum<-200M)", compute_stats(blocked))
            print()

        # Distribution
        print(f"\n  --- CHARM SUM DISTRIBUTION (SHORT trades, below spot 0-20) ---")
        sums = sorted([r['charm_sum'] for r in short_results])
        percentiles = [10, 25, 50, 75, 90]
        for p in percentiles:
            idx = int(len(sums) * p / 100)
            print(f"    P{p:2d}: {fmt_m(sums[idx])}")

        # Show blocked trades detail
        print(f"\n  --- DETAIL: SHORT TRADES WITH CHARM SUM < -200M (would be blocked) ---")
        blocked_detail = [r for r in short_results if r['charm_sum'] < -200_000_000]
        blocked_detail.sort(key=lambda r: r['charm_sum'])
        for r in blocked_detail[:30]:
            t = r['trade']
            print(f"    #{t['id']:4d} {str(t['ts'])[:16]} {t['setup_name']:20s} spot={float(t['spot']):8.1f} "
                  f"charm_sum={fmt_m(r['charm_sum']):>10s} min={fmt_m(r['charm_min']):>10s} "
                  f"=> {t['outcome_result']:10s} {float(t['outcome_pnl']):+6.1f} align={t['greek_alignment']}")

    # =========================================================================
    # COMBINED SUMMARY
    # =========================================================================
    print("\n" + "=" * 100)
    print("COMBINED SUMMARY: CHARM WALL FILTER")
    print("=" * 100)

    if long_results and short_results:
        # Best threshold candidates
        print("\n  Evaluating combined filter: Block LONG if charm_sum above spot > T, Block SHORT if charm_sum below spot < -T\n")

        for thresh in [100_000_000, 200_000_000, 500_000_000, 1_000_000_000]:
            long_passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                           for r in long_results if r['charm_sum'] <= thresh]
            long_blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                            for r in long_results if r['charm_sum'] > thresh]
            short_passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                            for r in short_results if r['charm_sum'] >= -thresh]
            short_blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                             for r in short_results if r['charm_sum'] < -thresh]

            all_passed = long_passed + short_passed
            all_blocked = long_blocked + short_blocked

            print(f"  Threshold: ±{fmt_m(thresh)}")
            print_stats(f"    ALL PASSED", compute_stats(all_passed))
            print_stats(f"    ALL BLOCKED", compute_stats(all_blocked))

            orig_pnl = compute_stats([(r['trade']['outcome_result'], r['trade']['outcome_pnl']) for r in long_results + short_results])['pnl']
            new_pnl = compute_stats(all_passed)['pnl']
            print(f"    Original PnL: {orig_pnl:+.1f} | Filtered PnL: {new_pnl:+.1f} | Delta: {new_pnl - orig_pnl:+.1f}")
            print()

    # =========================================================================
    # ALSO: Test the POSITIVE charm above = resistance for ALL directions
    # (checking if there's a pattern regardless of direction)
    # =========================================================================
    print("\n" + "=" * 100)
    print("BONUS: NET CHARM PROFILE AROUND SPOT FOR ALL TRADES")
    print("=" * 100)
    print("  For each trade, compute net charm in 5-pt zones around spot")
    print("  Shows whether charm profile predicts direction\n")

    # Merge all results
    all_results = []
    for r in long_results:
        all_results.append({**r, 'is_long': True})
    for r in short_results:
        all_results.append({**r, 'is_long': False})

    # For each, get the full charm profile ±30 around spot
    wins_above = []
    losses_above = []
    wins_below = []
    losses_below = []

    for r in all_results:
        is_win = r['trade']['outcome_result'] in ('WIN', 'WIN_TRAIL')
        if r['is_long']:
            # For longs: charm above matters
            if is_win:
                wins_above.append(r.get('charm_sum', 0))
            else:
                losses_above.append(r.get('charm_sum', 0))
        else:
            if is_win:
                wins_below.append(r.get('charm_sum', 0))
            else:
                losses_below.append(r.get('charm_sum', 0))

    if wins_above and losses_above:
        print(f"  LONG trades — charm sum above spot:")
        print(f"    Winners  (n={len(wins_above):3d}): avg {fmt_m(sum(wins_above)/len(wins_above))}, median {fmt_m(sorted(wins_above)[len(wins_above)//2])}")
        print(f"    Losers   (n={len(losses_above):3d}): avg {fmt_m(sum(losses_above)/len(losses_above))}, median {fmt_m(sorted(losses_above)[len(losses_above)//2])}")

    if wins_below and losses_below:
        print(f"  SHORT trades — charm sum below spot:")
        print(f"    Winners  (n={len(wins_below):3d}): avg {fmt_m(sum(wins_below)/len(wins_below))}, median {fmt_m(sorted(wins_below)[len(wins_below)//2])}")
        print(f"    Losers   (n={len(losses_below):3d}): avg {fmt_m(sum(losses_below)/len(losses_below))}, median {fmt_m(sorted(losses_below)[len(losses_below)//2])}")

    conn.close()


if __name__ == "__main__":
    main()
