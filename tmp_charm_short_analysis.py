"""
Charm Support Filter Analysis — SHORT trades
Tests blocking SHORT trades when big negative charm exists below spot.
Also shows the EOD charm explosion pattern.
"""

import psycopg2
import psycopg2.extras
from decimal import Decimal
from collections import defaultdict

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

def fmt_m(val):
    if val is None: return "N/A"
    v = float(val)
    if abs(v) >= 1e9: return f"{v/1e9:+.2f}B"
    return f"{v/1e6:+.1f}M"

def compute_stats(trades):
    if not trades: return {"count": 0, "wr": 0, "pnl": 0, "pf": 0, "avg_pnl": 0}
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

def round_to_5(x):
    return round(x / 5) * 5

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # =========================================================================
    # STEP 4: SHORT trades — charm support below spot
    # =========================================================================
    print("=" * 100)
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

        # Test SUM thresholds
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
    print("COMBINED SUMMARY: CHARM WALL FILTER (LONG + SHORT)")
    print("=" * 100)

    # Re-fetch long trades for combined analysis
    cur.execute("""
        SELECT id, ts, setup_name, direction, spot, outcome_result, outcome_pnl, greek_alignment
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND direction IN ('long', 'bullish')
        ORDER BY ts
    """)
    long_trades = cur.fetchall()

    long_results = []
    for trade in long_trades:
        spot = float(trade['spot'])
        spot_rounded = round_to_5(spot)
        trade_ts = trade['ts']
        cur.execute("""
            SELECT DISTINCT ts_utc FROM volland_exposure_points
            WHERE greek = 'charm' AND ts_utc <= %s AND ts_utc >= %s - interval '5 minutes'
            ORDER BY ts_utc DESC LIMIT 1
        """, (trade_ts, trade_ts))
        snap_row = cur.fetchone()
        if not snap_row: continue
        snap_ts = snap_row['ts_utc']
        cur.execute("""
            SELECT strike, value FROM volland_exposure_points
            WHERE greek = 'charm' AND ts_utc = %s AND strike >= %s AND strike <= %s ORDER BY strike
        """, (snap_ts, spot_rounded, spot_rounded + 20))
        charm_rows = cur.fetchall()
        if not charm_rows: continue
        charm_sum = sum(float(r['value']) for r in charm_rows)
        charm_max = max(float(r['value']) for r in charm_rows)
        long_results.append({'trade': trade, 'charm_sum': charm_sum, 'charm_max': charm_max})

    print(f"\n  Matched: {len(long_results)} LONG + {len(short_results)} SHORT = {len(long_results)+len(short_results)} total\n")

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

        all_orig = [(r['trade']['outcome_result'], r['trade']['outcome_pnl']) for r in long_results + short_results]

        print(f"  Threshold: +/-{fmt_m(thresh)}")
        print_stats(f"    ALL PASSED", compute_stats(all_passed))
        print_stats(f"    ALL BLOCKED", compute_stats(all_blocked))

        orig_pnl = compute_stats(all_orig)['pnl']
        new_pnl = compute_stats(all_passed)['pnl']
        blocked_pnl = compute_stats(all_blocked)['pnl']
        print(f"    Original PnL: {orig_pnl:+.1f} | Filtered PnL: {new_pnl:+.1f} | Blocked PnL: {blocked_pnl:+.1f} | Delta: {new_pnl - orig_pnl:+.1f}")
        print()

    # =========================================================================
    # BONUS: Charm magnitude vs time of day
    # =========================================================================
    print("\n" + "=" * 100)
    print("BONUS: CHARM MAGNITUDE BY TIME OF DAY")
    print("=" * 100)
    print("  Showing how charm exposure scales through the day\n")

    # Pick a few representative dates
    cur.execute("""
        SELECT DISTINCT ts_utc::date as d FROM volland_exposure_points
        WHERE greek = 'charm' AND ts_utc::date >= '2026-03-03'
        ORDER BY d DESC LIMIT 5
    """)
    dates = [r['d'] for r in cur.fetchall()]

    for d in dates[:3]:
        print(f"\n  Date: {d}")
        cur.execute("""
            SELECT ts_utc,
                   MAX(ABS(value)) as max_abs_val,
                   SUM(CASE WHEN value > 0 THEN value ELSE 0 END) as total_pos,
                   SUM(CASE WHEN value < 0 THEN value ELSE 0 END) as total_neg
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc::date = %s
              AND strike > (SELECT current_price - 50 FROM volland_exposure_points WHERE greek = 'charm' AND ts_utc::date = %s LIMIT 1)
              AND strike < (SELECT current_price + 50 FROM volland_exposure_points WHERE greek = 'charm' AND ts_utc::date = %s LIMIT 1)
            GROUP BY ts_utc
            ORDER BY ts_utc
        """, (d, d, d))
        rows = cur.fetchall()

        # Sample every ~10th row to keep output manageable
        step = max(1, len(rows) // 15)
        for i in range(0, len(rows), step):
            r = rows[i]
            ts = r['ts_utc']
            et = ts.hour - 5 if ts.hour >= 5 else ts.hour + 19  # rough UTC to ET
            max_v = float(r['max_abs_val']) / 1e6
            pos = float(r['total_pos']) / 1e6
            neg = float(r['total_neg']) / 1e6
            print(f"    {str(ts)[11:16]} UTC ({et:02d}:xx ET) | Max bar: {max_v:+10.1f}M | Sum+: {pos:+10.1f}M | Sum-: {neg:+10.1f}M")

    # =========================================================================
    # WINNER/LOSER charm comparison
    # =========================================================================
    print("\n" + "=" * 100)
    print("BONUS: CHARM SUM — WINNERS vs LOSERS")
    print("=" * 100)

    # LONG trades
    long_wins = [r for r in long_results if r['trade']['outcome_result'] in ('WIN', 'WIN_TRAIL')]
    long_losses = [r for r in long_results if r['trade']['outcome_result'] not in ('WIN', 'WIN_TRAIL')]

    if long_wins and long_losses:
        w_sums = sorted([r['charm_sum'] for r in long_wins])
        l_sums = sorted([r['charm_sum'] for r in long_losses])
        print(f"\n  LONG trades (charm sum above spot, 0-20 pts):")
        print(f"    Winners  (n={len(w_sums):3d}): P25={fmt_m(w_sums[len(w_sums)//4])} P50={fmt_m(w_sums[len(w_sums)//2])} P75={fmt_m(w_sums[3*len(w_sums)//4])} Avg={fmt_m(sum(w_sums)/len(w_sums))}")
        print(f"    Losers   (n={len(l_sums):3d}): P25={fmt_m(l_sums[len(l_sums)//4])} P50={fmt_m(l_sums[len(l_sums)//2])} P75={fmt_m(l_sums[3*len(l_sums)//4])} Avg={fmt_m(sum(l_sums)/len(l_sums))}")

    # SHORT trades
    short_wins = [r for r in short_results if r['trade']['outcome_result'] in ('WIN', 'WIN_TRAIL')]
    short_losses = [r for r in short_results if r['trade']['outcome_result'] not in ('WIN', 'WIN_TRAIL')]

    if short_wins and short_losses:
        w_sums = sorted([r['charm_sum'] for r in short_wins])
        l_sums = sorted([r['charm_sum'] for r in short_losses])
        print(f"\n  SHORT trades (charm sum below spot, spot-20 to spot):")
        print(f"    Winners  (n={len(w_sums):3d}): P25={fmt_m(w_sums[len(w_sums)//4])} P50={fmt_m(w_sums[len(w_sums)//2])} P75={fmt_m(w_sums[3*len(w_sums)//4])} Avg={fmt_m(sum(w_sums)/len(w_sums))}")
        print(f"    Losers   (n={len(l_sums):3d}): P25={fmt_m(l_sums[len(l_sums)//4])} P50={fmt_m(l_sums[len(l_sums)//2])} P75={fmt_m(l_sums[3*len(l_sums)//4])} Avg={fmt_m(sum(l_sums)/len(l_sums))}")

    # =========================================================================
    # TIME FILTER: Only check trades AFTER 3pm ET (when charm explodes)
    # =========================================================================
    print("\n" + "=" * 100)
    print("BONUS: CHARM FILTER — LATE DAY ONLY (after 3pm ET = 20:00 UTC)")
    print("=" * 100)
    print("  Charm grows exponentially near close. Testing filter only on late trades.\n")

    from datetime import time
    late_long = [r for r in long_results if r['trade']['ts'].hour >= 20]  # 3pm+ ET = 20:00+ UTC
    early_long = [r for r in long_results if r['trade']['ts'].hour < 20]

    print(f"  LONG trades before 3pm ET: {len(early_long)}, after 3pm ET: {len(late_long)}")

    if late_long:
        for thresh in [100_000_000, 200_000_000, 500_000_000]:
            passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                      for r in late_long if r['charm_sum'] <= thresh]
            blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                       for r in late_long if r['charm_sum'] > thresh]
            p_stats = compute_stats(passed)
            b_stats = compute_stats(blocked)

            print(f"\n  Late LONG, charm_sum > {fmt_m(thresh)}:")
            print_stats(f"    PASSED", p_stats)
            print_stats(f"    BLOCKED", b_stats)

    late_short = [r for r in short_results if r['trade']['ts'].hour >= 20]
    early_short = [r for r in short_results if r['trade']['ts'].hour < 20]

    print(f"\n  SHORT trades before 3pm ET: {len(early_short)}, after 3pm ET: {len(late_short)}")

    if late_short:
        for thresh in [100_000_000, 200_000_000, 500_000_000]:
            passed = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                      for r in late_short if r['charm_sum'] >= -thresh]
            blocked = [(r['trade']['outcome_result'], r['trade']['outcome_pnl'])
                       for r in late_short if r['charm_sum'] < -thresh]
            p_stats = compute_stats(passed)
            b_stats = compute_stats(blocked)

            print(f"\n  Late SHORT, charm_sum < {fmt_m(-thresh)}:")
            print_stats(f"    PASSED", p_stats)
            print_stats(f"    BLOCKED", b_stats)

    conn.close()
    print("\n\nDONE.")


if __name__ == "__main__":
    main()
