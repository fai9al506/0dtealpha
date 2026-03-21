"""
Backtest: GEX LIS Velocity — would signals blocked by gap>5 have been winners
if we allowed them based on rapid LIS convergence?

Approach:
1. Query volland_snapshots for all GEX paradigm periods
2. Cross-reference with chain_snapshots for SPX spot at each timestamp
3. Find cases where gap was 5-12 pts AND LIS was surging (velocity >= 25)
4. Simulate forward outcome using chain_snapshots price data (SL=8, T=10, trail)
"""
import psycopg2
import psycopg2.extras
import re
from datetime import datetime, timedelta
from collections import deque

DATABASE_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"


def parse_lis(lis_str):
    """Parse LIS string like '$6,569' or '$6,923 - $6,943' into float."""
    if not lis_str:
        return None
    cleaned = str(lis_str).replace("$", "").replace(",", "")
    matches = re.findall(r"[\d.]+", cleaned)
    return float(matches[0]) if matches else None


def parse_target(target_str):
    """Parse target string like '$6,718'."""
    if not target_str:
        return None
    cleaned = str(target_str).replace("$", "").replace(",", "")
    matches = re.findall(r"[\d.]+", cleaned)
    return float(matches[0]) if matches else None


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Step 1: Get all volland snapshots with GEX paradigm
    print("Querying volland_snapshots for GEX paradigm periods...")
    cur.execute("""
        SELECT
            ts,
            payload->'statistics'->>'paradigm' as paradigm,
            payload->'statistics'->>'lines_in_sand' as lis_str,
            payload->'statistics'->>'target' as target_str,
            payload->>'current_price' as current_price
        FROM volland_snapshots
        WHERE payload->'statistics'->>'paradigm' ILIKE '%%GEX%%'
          AND payload->'statistics'->>'lines_in_sand' IS NOT NULL
          AND ts >= '2026-02-01'
        ORDER BY ts ASC
    """)
    gex_rows = cur.fetchall()
    print(f"  Found {len(gex_rows)} GEX snapshots")

    # Step 2: Build LIS velocity timeline and find candidate signals
    # Group by date, track LIS velocity within each day
    candidates = []
    lis_buffer = deque(maxlen=20)
    last_date = None

    for row in gex_rows:
        ts = row['ts']
        day = ts.date()
        paradigm = row['paradigm']
        lis = parse_lis(row['lis_str'])
        target = parse_target(row['target_str'])
        spot_raw = row['current_price']

        if lis is None or spot_raw is None:
            continue
        spot = float(spot_raw)

        # Daily reset
        if last_date != day:
            lis_buffer.clear()
            last_date = day

        lis_buffer.append((ts, lis))

        # Only during market hours (9:30-16:00 ET)
        t = ts.hour * 60 + ts.minute  # UTC offset ~5hrs, but ts is already in DB timezone
        # Skip if less than 3 readings
        if len(lis_buffer) < 3:
            continue

        # Calculate velocity
        values = [v for _, v in lis_buffer]
        lis_move = values[-1] - values[0]

        # Block toxic subtypes (same as production code)
        paradigm_upper = paradigm.upper() if paradigm else ""
        if "TARGET" in paradigm_upper or "MESSY" in paradigm_upper:
            continue

        # Gap check: find cases where gap is 5-12 (blocked before, allowed with velocity)
        gap = abs(spot - lis)

        # Velocity gap bonus
        if lis_move >= 80:
            gap_bonus = 7
        elif lis_move >= 50:
            gap_bonus = 5
        elif lis_move >= 25:
            gap_bonus = 3
        else:
            gap_bonus = 0

        adjusted_max_gap = 5 + gap_bonus

        # We want: gap > 5 (would have been blocked) AND gap <= adjusted_max_gap (now allowed)
        if gap > 5 and gap <= adjusted_max_gap and lis_move >= 25:
            candidates.append({
                'ts': ts,
                'date': day,
                'spot': spot,
                'lis': lis,
                'target': target,
                'gap': round(gap, 2),
                'lis_move': round(lis_move, 2),
                'gap_bonus': gap_bonus,
                'paradigm': paradigm,
                'n_readings': len(values),
            })

    print(f"  Found {len(candidates)} candidate signals (gap 5-12, velocity >= 25)")

    if not candidates:
        print("\nNo candidates found. This means either:")
        print("  - GEX paradigm periods didn't have rapid LIS movement with gap 5-12")
        print("  - Or all rapid LIS moves had gap <= 5 (already allowed)")
        conn.close()
        return

    # Step 3: Deduplicate — keep first signal per date (cooldown)
    seen_dates = set()
    unique_candidates = []
    for c in candidates:
        key = (c['date'], c['paradigm'])
        if key not in seen_dates:
            seen_dates.add(key)
            unique_candidates.append(c)

    print(f"  After cooldown dedup: {len(unique_candidates)} unique signals")

    # Step 4: Simulate forward outcomes using chain_snapshots
    print("\nSimulating outcomes (SL=8, T=10, trail BE@8/act=10/gap=5)...")
    results = []

    for c in unique_candidates:
        entry_ts = c['ts']
        entry_spot = c['spot']
        sl = 8
        target_pts = 10
        direction = 'long'  # GEX Long is always long

        stop_level = entry_spot - sl
        target_level = entry_spot + target_pts

        # Trail params
        be_trigger = 8  # move stop to BE when profit >= 8
        trail_activation = 10
        trail_gap = 5
        trail_stop = stop_level  # starts at initial stop

        # Query forward prices (every 30s for up to 2 hours)
        cur.execute("""
            SELECT ts, spot
            FROM chain_snapshots
            WHERE ts > %s AND ts <= %s + interval '3 hours'
              AND spot IS NOT NULL
            ORDER BY ts ASC
        """, (entry_ts, entry_ts))
        forward = cur.fetchall()

        if not forward:
            results.append({**c, 'outcome': 'NO_DATA', 'pnl': 0, 'max_profit': 0, 'max_loss': 0, 'elapsed_min': 0})
            continue

        outcome = 'EXPIRED'
        pnl = 0
        max_profit = 0
        max_loss = 0
        elapsed_min = 0

        for fwd in forward:
            fwd_ts = fwd[0]
            fwd_spot = fwd[1]
            elapsed_min = (fwd_ts - entry_ts).total_seconds() / 60

            profit = fwd_spot - entry_spot
            max_profit = max(max_profit, profit)
            max_loss = min(max_loss, profit)

            # Trail logic
            if max_profit >= trail_activation:
                new_trail = entry_spot + (max_profit - trail_gap)
                trail_stop = max(trail_stop, new_trail)
            elif max_profit >= be_trigger:
                trail_stop = max(trail_stop, entry_spot)

            # Check stop hit
            if fwd_spot <= trail_stop:
                outcome = 'LOSS' if trail_stop < entry_spot else 'WIN'
                pnl = trail_stop - entry_spot
                break

            # Check target hit
            if fwd_spot >= target_level:
                outcome = 'WIN'
                pnl = target_pts
                break

            # 2-hour timeout
            if elapsed_min >= 120:
                outcome = 'EXPIRED'
                pnl = profit
                break

        results.append({
            **c,
            'outcome': outcome,
            'pnl': round(pnl, 2),
            'max_profit': round(max_profit, 2),
            'max_loss': round(max_loss, 2),
            'elapsed_min': round(elapsed_min, 1),
        })

    # Step 5: Print results
    print("\n" + "=" * 100)
    print(f"{'Date':<12} {'Time':<8} {'Paradigm':<12} {'Spot':>8} {'LIS':>8} {'Gap':>6} {'Vel':>6} {'Bonus':>6} {'Outcome':<8} {'PnL':>7} {'MFE':>7} {'MAE':>7}")
    print("-" * 100)

    total_pnl = 0
    wins = 0
    losses = 0
    expired = 0
    no_data = 0

    for r in results:
        ts_str = r['ts'].strftime('%H:%M')
        print(f"{r['date']}  {ts_str:<8} {r['paradigm']:<12} {r['spot']:>8.1f} {r['lis']:>8.1f} {r['gap']:>6.1f} {r['lis_move']:>+6.0f} {r['gap_bonus']:>+6} {r['outcome']:<8} {r['pnl']:>+7.1f} {r['max_profit']:>+7.1f} {r['max_loss']:>+7.1f}")

        if r['outcome'] == 'NO_DATA':
            no_data += 1
            continue

        total_pnl += r['pnl']
        if r['outcome'] == 'WIN':
            wins += 1
        elif r['outcome'] == 'LOSS':
            losses += 1
        else:
            expired += 1

    total = wins + losses + expired
    print("=" * 100)
    print(f"\nSUMMARY:")
    print(f"  Signals: {total} ({no_data} no data)")
    print(f"  Wins: {wins} | Losses: {losses} | Expired: {expired}")
    if total > 0:
        print(f"  Win Rate: {wins/total*100:.0f}%")
        print(f"  Total PnL: {total_pnl:+.1f} pts")
        print(f"  Avg PnL: {total_pnl/total:+.1f} pts/trade")

    # Also show what WOULD have happened — signals that had gap <= 5 (already allowed)
    # to compare the velocity signals against normal ones
    print("\n\nCOMPARISON: Existing GEX Long signals (gap <= 5) from setup_log:")
    cur.execute("""
        SELECT
            ts::date as trade_date,
            ts::time as trade_time,
            paradigm, spot, lis, gap_to_lis,
            outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE setup_name = 'GEX Long'
          AND outcome_result IS NOT NULL
          AND ts >= '2026-02-01'
        ORDER BY ts ASC
    """)
    existing = cur.fetchall()

    ex_wins = sum(1 for r in existing if r['outcome_result'] == 'WIN')
    ex_losses = sum(1 for r in existing if r['outcome_result'] == 'LOSS')
    ex_expired = sum(1 for r in existing if r['outcome_result'] not in ('WIN', 'LOSS'))
    ex_total = len(existing)
    ex_pnl = sum(r['outcome_pnl'] or 0 for r in existing)

    if ex_total > 0:
        print(f"  Signals: {ex_total}")
        print(f"  Wins: {ex_wins} | Losses: {ex_losses} | Expired: {ex_expired}")
        print(f"  Win Rate: {ex_wins/ex_total*100:.0f}%")
        print(f"  Total PnL: {ex_pnl:+.1f} pts")
        print(f"  Avg PnL: {ex_pnl/ex_total:+.1f} pts/trade")
    else:
        print("  No existing GEX Long signals found")

    conn.close()


if __name__ == "__main__":
    main()
