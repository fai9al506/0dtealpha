"""
DEEP Backtest: LIS Velocity as a standalone signal.

Core thesis: Rapid LIS movement = dealer positioning shift = price follows.

Tests:
1. ALL LIS velocity events during GEX paradigm (not just gap > 5)
2. Forward price action at multiple horizons (15/30/60/120 min)
3. SL=8/T=10 outcome simulation with trail
4. Segmented by velocity magnitude, gap size, paradigm subtype
5. Comparison: velocity signals vs no-velocity GEX signals
6. MFE/MAE distribution
"""
import psycopg2
import psycopg2.extras
import re
from datetime import datetime, timedelta, time as dtime
from collections import deque, defaultdict

DATABASE_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"


def parse_lis(lis_str):
    if not lis_str:
        return None
    cleaned = str(lis_str).replace("$", "").replace(",", "")
    matches = re.findall(r"[\d.]+", cleaned)
    return float(matches[0]) if matches else None


def parse_target(target_str):
    if not target_str:
        return None
    cleaned = str(target_str).replace("$", "").replace(",", "")
    matches = re.findall(r"[\d.]+", cleaned)
    return float(matches[0]) if matches else None


def simulate_outcome(cur, entry_ts, entry_spot, direction="long", sl=8, target_pts=10):
    """Simulate forward outcome with trail (BE@8, act=10, gap=5)."""
    cur.execute("""
        SELECT ts, spot FROM chain_snapshots
        WHERE ts > %s AND ts <= %s + interval '3 hours'
          AND spot IS NOT NULL
        ORDER BY ts ASC
    """, (entry_ts, entry_ts))
    forward = cur.fetchall()

    if not forward:
        return {'outcome': 'NO_DATA', 'pnl': 0, 'mfe': 0, 'mae': 0, 'elapsed': 0}

    sign = 1 if direction == "long" else -1
    trail_stop = entry_spot - sign * sl
    outcome = 'EXPIRED'
    pnl = 0
    mfe = 0
    mae = 0
    elapsed = 0

    for fwd_ts, fwd_spot in forward:
        elapsed = (fwd_ts - entry_ts).total_seconds() / 60
        profit = sign * (fwd_spot - entry_spot)
        mfe = max(mfe, profit)
        mae = min(mae, profit)

        # Trail logic
        if profit >= 10:  # trail activation
            new_trail = entry_spot + sign * (mfe - 5)
            if direction == "long":
                trail_stop = max(trail_stop, new_trail)
            else:
                trail_stop = min(trail_stop, new_trail)
        elif profit >= 8:  # BE trigger
            if direction == "long":
                trail_stop = max(trail_stop, entry_spot)
            else:
                trail_stop = min(trail_stop, entry_spot)

        # Check stop
        if direction == "long" and fwd_spot <= trail_stop:
            pnl = trail_stop - entry_spot
            outcome = 'WIN' if pnl >= 0 else 'LOSS'
            break
        elif direction == "short" and fwd_spot >= trail_stop:
            pnl = entry_spot - trail_stop
            outcome = 'WIN' if pnl >= 0 else 'LOSS'
            break

        # Check target
        if profit >= target_pts:
            outcome = 'WIN'
            pnl = target_pts
            break

        # 2-hour timeout
        if elapsed >= 120:
            pnl = profit
            outcome = 'EXPIRED'
            break

    return {'outcome': outcome, 'pnl': round(pnl, 2), 'mfe': round(mfe, 2),
            'mae': round(mae, 2), 'elapsed': round(elapsed, 1)}


def get_forward_returns(cur, entry_ts, entry_spot, horizons=[15, 30, 60, 120]):
    """Get forward returns at multiple horizons."""
    results = {}
    for h in horizons:
        target_ts = entry_ts + timedelta(minutes=h)
        cur.execute("""
            SELECT spot FROM chain_snapshots
            WHERE ts >= %s - interval '2 minutes' AND ts <= %s + interval '2 minutes'
              AND spot IS NOT NULL
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts - %s))) ASC
            LIMIT 1
        """, (target_ts, target_ts, target_ts))
        row = cur.fetchone()
        if row:
            results[f'{h}m'] = round(row[0] - entry_spot, 2)
        else:
            results[f'{h}m'] = None
    return results


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_session(readonly=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # -- Step 1: Get ALL volland snapshots with GEX paradigm --
    print("=" * 110)
    print("DEEP BACKTEST: LIS Velocity Signal")
    print("Thesis: Rapid LIS movement = dealer repositioning = price follows")
    print("=" * 110)

    print("\nQuerying all GEX paradigm snapshots...")
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

    # -- Step 2: Build velocity timeline, categorize ALL snapshots --
    lis_buffer = deque(maxlen=20)
    last_date = None

    velocity_signals = []      # velocity >= 25
    no_velocity_signals = []   # velocity < 25 (baseline comparison)

    # Cooldown: min 10 min between signals on same day
    last_signal_ts = {}

    for row in gex_rows:
        ts = row['ts']
        day = ts.date()
        paradigm = row['paradigm'] or ""
        lis = parse_lis(row['lis_str'])
        target = parse_target(row['target_str'])
        spot_raw = row['current_price']

        if lis is None or spot_raw is None:
            continue
        spot = float(spot_raw)

        # Block toxic subtypes
        p_upper = paradigm.upper()
        if "TARGET" in p_upper or "MESSY" in p_upper:
            continue

        # Daily reset
        if last_date != day:
            lis_buffer.clear()
            last_date = day

        lis_buffer.append((ts, lis))

        # Market hours filter (ET) — ts is in UTC, convert
        # March 2026 is EDT (UTC-4)
        et_hour = (ts.hour - 4) % 24  # rough EDT conversion
        et_min = ts.minute
        et_time = et_hour * 60 + et_min
        if et_time < 9 * 60 + 30 or et_time > 16 * 60:
            continue

        if len(lis_buffer) < 3:
            continue

        # Calculate velocity
        values = [v for _, v in lis_buffer]
        lis_move = values[-1] - values[0]
        gap = abs(spot - lis)

        # Cooldown check (10 min between signals)
        cooldown_key = day
        if cooldown_key in last_signal_ts:
            if (ts - last_signal_ts[cooldown_key]).total_seconds() < 600:
                continue

        signal = {
            'ts': ts,
            'date': day,
            'et_time': f"{et_hour:02d}:{et_min:02d}",
            'spot': spot,
            'lis': lis,
            'target': target,
            'gap': round(gap, 2),
            'lis_move': round(lis_move, 2),
            'paradigm': paradigm,
            'n_readings': len(values),
            'lis_start': values[0],
            'lis_end': values[-1],
        }

        if lis_move >= 25:
            velocity_signals.append(signal)
            last_signal_ts[cooldown_key] = ts
        elif gap <= 5 and len(no_velocity_signals) < 200:
            # Baseline: normal GEX signals (gap <= 5, no velocity)
            no_velocity_signals.append(signal)
            last_signal_ts[cooldown_key] = ts

    print(f"  Velocity signals (LIS move >= 25): {len(velocity_signals)}")
    print(f"  Baseline signals (gap <= 5, no velocity): {len(no_velocity_signals)}")

    # -- Step 3: Simulate outcomes for ALL signals --
    print("\nSimulating outcomes...")

    def process_signals(signals, label):
        for i, s in enumerate(signals):
            # Outcome simulation
            outcome = simulate_outcome(cur, s['ts'], s['spot'])
            s.update(outcome)
            # Forward returns
            fwd = get_forward_returns(cur, s['ts'], s['spot'])
            s.update(fwd)
            if (i + 1) % 20 == 0:
                print(f"  [{label}] {i+1}/{len(signals)}...")

    process_signals(velocity_signals, "velocity")
    process_signals(no_velocity_signals, "baseline")

    # -- Step 4: Print detailed velocity signal results --
    print("\n" + "=" * 130)
    print("VELOCITY SIGNALS — All LIS surge events (move >= 25 pts)")
    print("=" * 130)
    print(f"{'Date':<12} {'ET':<6} {'Paradigm':<10} {'Spot':>7} {'LIS':>7} {'Gap':>5} {'LIS Move':>9} {'LIS Path':<20} {'Out':<8} {'PnL':>6} {'MFE':>6} {'MAE':>6} {'15m':>6} {'30m':>6} {'60m':>6}")
    print("-" * 130)

    for s in velocity_signals:
        lis_path = f"{s['lis_start']:.0f}->{s['lis_end']:.0f}"
        r15 = f"{s.get('15m', 0):+.1f}" if s.get('15m') is not None else "  n/a"
        r30 = f"{s.get('30m', 0):+.1f}" if s.get('30m') is not None else "  n/a"
        r60 = f"{s.get('60m', 0):+.1f}" if s.get('60m') is not None else "  n/a"
        print(f"{s['date']}  {s['et_time']:<6} {s['paradigm']:<10} {s['spot']:>7.1f} {s['lis']:>7.0f} {s['gap']:>5.1f} {s['lis_move']:>+9.0f} {lis_path:<20} {s['outcome']:<8} {s['pnl']:>+6.1f} {s['mfe']:>+6.1f} {s['mae']:>+6.1f} {r15:>6} {r30:>6} {r60:>6}")

    # -- Step 5: Summary stats --
    def calc_stats(signals, label):
        valid = [s for s in signals if s['outcome'] != 'NO_DATA']
        if not valid:
            print(f"\n{label}: No valid signals")
            return

        wins = sum(1 for s in valid if s['outcome'] == 'WIN')
        losses = sum(1 for s in valid if s['outcome'] == 'LOSS')
        expired = sum(1 for s in valid if s['outcome'] == 'EXPIRED')
        total = len(valid)
        total_pnl = sum(s['pnl'] for s in valid)
        avg_mfe = sum(s['mfe'] for s in valid) / total
        avg_mae = sum(s['mae'] for s in valid) / total

        # Forward returns
        fwd_stats = {}
        for h in ['15m', '30m', '60m', '120m']:
            vals = [s[h] for s in valid if s.get(h) is not None]
            if vals:
                pos = sum(1 for v in vals if v > 0)
                fwd_stats[h] = {
                    'avg': sum(vals) / len(vals),
                    'med': sorted(vals)[len(vals)//2],
                    'pos_pct': pos / len(vals) * 100,
                    'n': len(vals),
                }

        print(f"\n{'-' * 60}")
        print(f"  {label}")
        print(f"{'-' * 60}")
        print(f"  Trades: {total} ({wins}W / {losses}L / {expired}E)")
        print(f"  Win Rate: {wins/total*100:.0f}%")
        print(f"  Total PnL: {total_pnl:+.1f} pts")
        print(f"  Avg PnL: {total_pnl/total:+.1f} pts/trade")
        print(f"  Avg MFE: {avg_mfe:+.1f} pts | Avg MAE: {avg_mae:+.1f} pts")

        if fwd_stats:
            print(f"\n  Forward returns (directional accuracy):")
            for h, st in fwd_stats.items():
                print(f"    {h:>4}: avg {st['avg']:+.1f} | median {st['med']:+.1f} | {st['pos_pct']:.0f}% positive ({st['n']} obs)")

        return valid

    print("\n" + "=" * 110)
    print("COMPARISON: Velocity vs No-Velocity GEX Signals")
    print("=" * 110)

    vel_valid = calc_stats(velocity_signals, "VELOCITY SIGNALS (LIS surge >= 25 pts)")
    base_valid = calc_stats(no_velocity_signals, "BASELINE (normal GEX, gap <= 5, no velocity)")

    # -- Step 6: Segment velocity by magnitude --
    if vel_valid:
        print("\n" + "=" * 110)
        print("VELOCITY SEGMENTATION BY MAGNITUDE")
        print("=" * 110)

        for label, min_vel, max_vel in [
            ("Low velocity (25-49 pts)", 25, 50),
            ("Medium velocity (50-79 pts)", 50, 80),
            ("High velocity (80+ pts)", 80, 9999),
        ]:
            seg = [s for s in vel_valid if min_vel <= s['lis_move'] < max_vel]
            if seg:
                wins = sum(1 for s in seg if s['outcome'] == 'WIN')
                total = len(seg)
                pnl = sum(s['pnl'] for s in seg)
                avg_mfe = sum(s['mfe'] for s in seg) / total
                fwd30 = [s['30m'] for s in seg if s.get('30m') is not None]
                fwd30_avg = sum(fwd30) / len(fwd30) if fwd30 else 0
                fwd30_pos = sum(1 for v in fwd30 if v > 0) / len(fwd30) * 100 if fwd30 else 0
                print(f"\n  {label}: {total} trades | WR {wins/total*100:.0f}% | PnL {pnl:+.1f} | MFE {avg_mfe:+.1f} | 30m: {fwd30_avg:+.1f} ({fwd30_pos:.0f}% pos)")

                for s in seg:
                    lis_path = f"{s['lis_start']:.0f}->{s['lis_end']:.0f}"
                    print(f"    {s['date']} {s['et_time']} | {s['spot']:.0f} gap={s['gap']:.1f} | {lis_path} ({s['lis_move']:+.0f}) | {s['outcome']} {s['pnl']:+.1f} | MFE {s['mfe']:+.1f}")

    # -- Step 7: Segment by gap size --
    if vel_valid:
        print("\n" + "=" * 110)
        print("VELOCITY SEGMENTATION BY GAP SIZE")
        print("=" * 110)

        for label, min_gap, max_gap in [
            ("Tight gap (0-5 pts) — would fire WITHOUT velocity", 0, 5.01),
            ("Medium gap (5-8 pts) — needs velocity bonus", 5.01, 8.01),
            ("Wide gap (8-12 pts) — needs strong velocity", 8.01, 15),
        ]:
            seg = [s for s in vel_valid if min_gap <= s['gap'] < max_gap]
            if seg:
                wins = sum(1 for s in seg if s['outcome'] == 'WIN')
                total = len(seg)
                pnl = sum(s['pnl'] for s in seg)
                avg_mfe = sum(s['mfe'] for s in seg) / total
                print(f"\n  {label}: {total} trades | WR {wins/total*100:.0f}% | PnL {pnl:+.1f} | MFE {avg_mfe:+.1f}")
                for s in seg:
                    print(f"    {s['date']} {s['et_time']} | gap={s['gap']:.1f} vel={s['lis_move']:+.0f} | {s['outcome']} {s['pnl']:+.1f} MFE {s['mfe']:+.1f}")

    # -- Step 8: Combined existing + velocity signals --
    if vel_valid and base_valid:
        print("\n" + "=" * 110)
        print("COMBINED IMPACT: What if velocity was always active?")
        print("=" * 110)

        # Velocity signals that ONLY fire because of velocity (gap > 5)
        new_signals = [s for s in vel_valid if s['gap'] > 5]
        existing_only = base_valid

        new_wins = sum(1 for s in new_signals if s['outcome'] == 'WIN')
        new_total = len(new_signals)
        new_pnl = sum(s['pnl'] for s in new_signals)

        ex_wins = sum(1 for s in existing_only if s['outcome'] == 'WIN')
        ex_total = len(existing_only)
        ex_pnl = sum(s['pnl'] for s in existing_only)

        # Also velocity signals at gap <= 5 (already would fire, but with velocity context)
        vel_tight = [s for s in vel_valid if s['gap'] <= 5]
        vt_wins = sum(1 for s in vel_tight if s['outcome'] == 'WIN')
        vt_total = len(vel_tight)
        vt_pnl = sum(s['pnl'] for s in vel_tight)

        print(f"\n  Existing GEX Long (gap<=5, no vel): {ex_total} trades, {ex_wins/ex_total*100:.0f}% WR, {ex_pnl:+.1f} pts")
        if vt_total:
            print(f"  Velocity + tight gap (gap<=5):       {vt_total} trades, {vt_wins/vt_total*100:.0f}% WR, {vt_pnl:+.1f} pts")
        if new_total:
            print(f"  NEW velocity signals (gap 5-12):     {new_total} trades, {new_wins/new_total*100:.0f}% WR, {new_pnl:+.1f} pts")

        combined_total = ex_total + new_total
        combined_pnl = ex_pnl + new_pnl
        combined_wins = ex_wins + new_wins
        if combined_total:
            print(f"\n  COMBINED (existing + new velocity):  {combined_total} trades, {combined_wins/combined_total*100:.0f}% WR, {combined_pnl:+.1f} pts")
            print(f"  Delta from velocity feature:         {new_pnl:+.1f} pts from {new_total} additional trades")

    # -- Step 9: LIS velocity as a FILTER on existing signals --
    if vel_valid and base_valid:
        print("\n" + "=" * 110)
        print("VELOCITY AS A QUALITY FILTER")
        print("Could velocity predict which existing signals win?")
        print("=" * 110)

        # Combine all signals, check if having velocity improves WR
        all_tight = [s for s in vel_valid if s['gap'] <= 5] + base_valid

        vel_tight_wins = sum(1 for s in all_tight if s['lis_move'] >= 25 and s['outcome'] == 'WIN')
        vel_tight_total = sum(1 for s in all_tight if s['lis_move'] >= 25)
        novel_tight_wins = sum(1 for s in all_tight if s['lis_move'] < 25 and s['outcome'] == 'WIN')
        novel_tight_total = sum(1 for s in all_tight if s['lis_move'] < 25)

        if vel_tight_total:
            print(f"\n  Gap <= 5 WITH velocity (>= 25):    {vel_tight_total} trades, {vel_tight_wins/vel_tight_total*100:.0f}% WR")
        if novel_tight_total:
            print(f"  Gap <= 5 WITHOUT velocity (< 25):  {novel_tight_total} trades, {novel_tight_wins/novel_tight_total*100:.0f}% WR")

    conn.close()
    print("\n" + "=" * 110)
    print("DONE")
    print("=" * 110)


if __name__ == "__main__":
    main()
