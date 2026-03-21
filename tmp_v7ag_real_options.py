"""V7+AG March Backtest using REAL SPXW option prices from chain_snapshots.
chain_snapshots schema: columns=JSONB (column names), rows=JSONB (list of strike arrays).
Column order: [call_Vol, call_OI, call_IV, call_Gamma, call_Delta, call_BID, call_BID_QTY,
               call_ASK, call_ASK_QTY, call_LAST, Strike, put_LAST, put_ASK, put_ASK_QTY,
               put_BID, put_BID_QTY, put_Delta, put_Gamma, put_IV, put_OI, put_Volume]
Indices:       0        1        2       3           4          5            6
               7          8           9        10       11        12          13
               14         15         16        17       18      19       20
Newer rows (25 cols) add: call_Theta(21), call_Vega(22), put_Theta(23), put_Vega(24)
"""
import sys, io, json, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import timedelta

# Column indices
C_DELTA = 4
C_ASK   = 7
C_BID   = 5
STRIKE  = 10
P_DELTA = 16
P_ASK   = 12
P_BID   = 14

e = create_engine(os.environ['DATABASE_URL'])

with e.connect() as conn:
    trades = conn.execute(text("""
        SELECT id, setup_name, direction, grade, spot, ts,
               outcome_result, outcome_pnl, outcome_elapsed_min,
               greek_alignment
        FROM setup_log
        WHERE ts >= '2026-03-01' AND ts < '2026-04-01'
          AND outcome_result IS NOT NULL
        ORDER BY ts
    """)).mappings().all()

    def passes_v7ag(r):
        setup, dirn, align = r['setup_name'], r['direction'], r['greek_alignment']
        if dirn in ('long', 'bullish'):
            return align is not None and align >= 2
        else:
            if setup == 'Skew Charm': return True
            elif setup == 'AG Short': return True
            elif setup == 'DD Exhaustion': return align is not None and align != 0
            else: return False

    filtered = [r for r in trades if passes_v7ag(r)]
    print(f"V7+AG March trades: {len(filtered)}")

    results = []
    no_entry = 0
    no_exit = 0

    for r in filtered:
        ts_entry = r['ts']
        spot_entry = r['spot']
        dirn = r['direction']
        is_long = dirn in ('long', 'bullish')
        elapsed = r['outcome_elapsed_min'] or 0
        pnl_pts = r['outcome_pnl'] or 0
        opt_type = 'call' if is_long else 'put'

        # Find nearest chain snapshot to entry time (within 2 min)
        entry_snap = conn.execute(text("""
            SELECT ts, rows
            FROM chain_snapshots
            WHERE ts BETWEEN :t1 AND :t2
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts)))
            LIMIT 1
        """), {
            "t1": ts_entry - timedelta(minutes=2),
            "t2": ts_entry + timedelta(minutes=2),
            "ts": ts_entry
        }).mappings().first()

        if not entry_snap:
            no_entry += 1
            continue

        strikes = entry_snap['rows']
        if isinstance(strikes, str):
            strikes = json.loads(strikes)

        # Find ~0.30 delta strike
        best_strike = None
        best_delta_diff = 999
        best_ask = None

        for sd in strikes:
            if len(sd) < 17:
                continue
            strike_val = sd[STRIKE]
            if opt_type == 'call':
                delta = sd[C_DELTA]
                ask = sd[C_ASK]
            else:
                delta = sd[P_DELTA]
                ask = sd[P_ASK]

            try:
                delta = float(delta)
                ask = float(ask)
            except (TypeError, ValueError):
                continue
            if ask <= 0:
                continue

            diff = abs(abs(delta) - 0.30)
            if diff < best_delta_diff:
                best_delta_diff = diff
                best_strike = strike_val
                best_ask = ask

        if best_strike is None or best_ask is None:
            no_entry += 1
            continue

        # Find exit snapshot
        ts_exit = ts_entry + timedelta(minutes=elapsed) if elapsed > 0 else ts_entry + timedelta(minutes=5)

        exit_snap = conn.execute(text("""
            SELECT ts, rows
            FROM chain_snapshots
            WHERE ts BETWEEN :t1 AND :t2
            ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts)))
            LIMIT 1
        """), {
            "t1": ts_exit - timedelta(minutes=2),
            "t2": ts_exit + timedelta(minutes=2),
            "ts": ts_exit
        }).mappings().first()

        if not exit_snap:
            no_exit += 1
            continue

        exit_strikes = exit_snap['rows']
        if isinstance(exit_strikes, str):
            exit_strikes = json.loads(exit_strikes)

        exit_bid = None
        for sd in exit_strikes:
            if len(sd) < 17:
                continue
            if float(sd[STRIKE]) == best_strike:
                try:
                    if opt_type == 'call':
                        exit_bid = float(sd[C_BID])
                    else:
                        exit_bid = float(sd[P_BID])
                except (TypeError, ValueError):
                    pass
                break

        if exit_bid is None:
            no_exit += 1
            continue

        option_pnl = (exit_bid - best_ask) * 100  # per contract

        results.append({
            'id': r['id'],
            'setup_name': r['setup_name'],
            'direction': dirn,
            'ts': ts_entry,
            'spot': spot_entry,
            'strike': best_strike,
            'opt_type': opt_type,
            'entry_ask': best_ask,
            'exit_bid': exit_bid,
            'option_pnl': option_pnl,
            'portal_pnl': pnl_pts,
            'portal_result': r['outcome_result'],
            'elapsed': elapsed,
            'alignment': r['greek_alignment'],
        })

    print(f"Matched with real option prices: {len(results)}")
    print(f"No entry snapshot: {no_entry}, No exit snapshot: {no_exit}")
    print()

    # ===== OPTIONS DAILY STATEMENT =====
    by_day = defaultdict(lambda: {'trades': 0, 'opt_pnl': 0, 'pts': 0})
    for r in results:
        day = r['ts'].strftime('%Y-%m-%d')
        d = by_day[day]
        d['trades'] += 1
        d['opt_pnl'] += r['option_pnl']
        d['pts'] += r['portal_pnl']

    START = 50000
    days = sorted(by_day.keys())

    print("=" * 62)
    print("  OPTIONS ACCOUNT (1 SPXW contract, REAL prices)")
    print("=" * 62)
    print(f"  {'Day':<4s} {'Date':<12s} {'Capital':>12s} {'P&L':>10s}")
    print(f"  {'-'*4} {'-'*12} {'-'*12} {'-'*10}")
    print(f"  {'0':<4s} {'Start':<12s} {'$50,000':>12s} {'$0':>10s}")
    bal = START
    for i, day in enumerate(days):
        d = by_day[day]
        bal += d['opt_pnl']
        print(f"  {i+1:<4d} {day:<12s} ${bal:>11,.0f} ${d['opt_pnl']:>+9,.0f}")
    total_opt = sum(d['opt_pnl'] for d in by_day.values())
    print(f"  {'-'*4} {'-'*12} {'-'*12} {'-'*10}")
    print(f"  {'':4s} {'TOTAL':<12s} ${bal:>11,.0f} ${total_opt:>+9,.0f}")
    print(f"  Return: {(bal-START)/START*100:+.1f}%")
    print()

    # ===== FUTURES DAILY STATEMENT =====
    sorted_r = sorted(filtered, key=lambda x: x['ts'])
    fut_by_day = defaultdict(float)
    in_trade = False
    current_end = None
    for r in sorted_r:
        ts = r['ts']
        elapsed = r['outcome_elapsed_min'] or 0
        day = ts.strftime('%Y-%m-%d')
        if in_trade and current_end and ts >= current_end:
            in_trade = False
        if in_trade:
            continue
        fut_by_day[day] += r['outcome_pnl'] or 0
        in_trade = True
        current_end = ts + timedelta(minutes=max(elapsed, 1))

    print("=" * 62)
    print("  FUTURES ACCOUNT (10 MES, single-position, $50/pt)")
    print("=" * 62)
    print(f"  {'Day':<4s} {'Date':<12s} {'Capital':>12s} {'P&L':>10s}")
    print(f"  {'-'*4} {'-'*12} {'-'*12} {'-'*10}")
    print(f"  {'0':<4s} {'Start':<12s} {'$50,000':>12s} {'$0':>10s}")
    fbal = START
    for i, day in enumerate(days):
        pts = fut_by_day.get(day, 0)
        pnl = pts * 50
        fbal += pnl
        print(f"  {i+1:<4d} {day:<12s} ${fbal:>11,.0f} ${pnl:>+9,.0f}")
    ftotal = sum(v * 50 for v in fut_by_day.values())
    print(f"  {'-'*4} {'-'*12} {'-'*12} {'-'*10}")
    print(f"  {'':4s} {'TOTAL':<12s} ${fbal:>11,.0f} ${ftotal:>+9,.0f}")
    print(f"  Return: {(fbal-START)/START*100:+.1f}%")
    print()

    # ===== SAMPLE TRADES =====
    print("=" * 95)
    print("  SAMPLE TRADES (first 15) for verification")
    print("=" * 95)
    print(f"  {'Time':<14s} {'Setup':<16s} {'Type':<5s} {'K':>7s} {'Ask':>7s} {'Bid':>7s} {'Opt$':>8s} {'Pts':>7s} {'Result':<7s}")
    print(f"  {'-'*14} {'-'*16} {'-'*5} {'-'*7} {'-'*7} {'-'*7} {'-'*8} {'-'*7} {'-'*7}")
    for r in results[:15]:
        print(f"  {r['ts'].strftime('%m/%d %H:%M'):<14s} {r['setup_name']:<16s} {r['opt_type'].upper():<5s} {r['strike']:>7.0f} ${r['entry_ask']:>6.2f} ${r['exit_bid']:>6.2f} ${r['option_pnl']:>+7.0f} {r['portal_pnl']:>+7.1f} {r['portal_result']:<7s}")
