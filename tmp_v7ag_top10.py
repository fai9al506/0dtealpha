"""V7+AG filter comparison — find best ~10 trades/day filter with real option prices."""
import sys, io, json, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import timedelta

C_DELTA, C_ASK, C_BID, STRIKE, P_DELTA, P_ASK, P_BID = 4, 7, 5, 10, 16, 12, 14
e = create_engine(os.environ['DATABASE_URL'])

with e.connect() as conn:
    trades = conn.execute(text("""
        SELECT id, setup_name, direction, grade, spot, ts,
               outcome_result, outcome_pnl, outcome_elapsed_min,
               greek_alignment, score
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

    def get_option_pnl(r):
        ts_entry = r['ts']
        is_long = r['direction'] in ('long', 'bullish')
        elapsed = r['outcome_elapsed_min'] or 0
        opt_type = 'call' if is_long else 'put'
        entry_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_entry - timedelta(minutes=2), "t2": ts_entry + timedelta(minutes=2), "ts": ts_entry}).mappings().first()
        if not entry_snap:
            return None, None, None, None
        strikes = entry_snap['rows']
        if isinstance(strikes, str):
            strikes = json.loads(strikes)
        best_strike, best_ask, best_dd = None, None, 999
        for sd in strikes:
            if len(sd) < 17:
                continue
            try:
                delta = float(sd[C_DELTA] if opt_type == 'call' else sd[P_DELTA])
                ask = float(sd[C_ASK] if opt_type == 'call' else sd[P_ASK])
            except (TypeError, ValueError):
                continue
            if ask <= 0:
                continue
            diff = abs(abs(delta) - 0.30)
            if diff < best_dd:
                best_dd = diff
                best_strike, best_ask = sd[STRIKE], ask
        if not best_strike:
            return None, None, None, None
        ts_exit = ts_entry + timedelta(minutes=elapsed) if elapsed > 0 else ts_entry + timedelta(minutes=5)
        exit_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_exit - timedelta(minutes=2), "t2": ts_exit + timedelta(minutes=2), "ts": ts_exit}).mappings().first()
        if not exit_snap:
            return None, None, None, None
        exit_strikes = exit_snap['rows']
        if isinstance(exit_strikes, str):
            exit_strikes = json.loads(exit_strikes)
        exit_bid = None
        for sd in exit_strikes:
            if len(sd) < 17:
                continue
            if float(sd[STRIKE]) == best_strike:
                try:
                    exit_bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
                except (TypeError, ValueError):
                    pass
                break
        if exit_bid is None:
            return None, None, None, None
        return (exit_bid - best_ask) * 100, best_ask, exit_bid, best_strike

    print("Loading real option prices...", flush=True)
    enriched = []
    for r in filtered:
        opt_pnl, ask, bid, strike = get_option_pnl(r)
        if opt_pnl is None:
            continue
        enriched.append({**dict(r), 'opt_pnl': opt_pnl, 'opt_ask': ask, 'opt_bid': bid, 'opt_strike': strike})
    print(f"Got prices for {len(enriched)} trades")
    print()

    days_count = len(set(r['ts'].strftime('%Y-%m-%d') for r in enriched))

    def eval_filter(name, tlist):
        if not tlist:
            print(f"  {name:<45s}    0t")
            return
        by_day = defaultdict(list)
        for t in tlist:
            by_day[t['ts'].strftime('%Y-%m-%d')].append(t)
        total_pnl = sum(t['opt_pnl'] for t in tlist)
        avg_per_day = len(tlist) / days_count
        wins = sum(1 for t in tlist if t['opt_pnl'] > 0)
        wr = wins / len(tlist) * 100
        worst = min((sum(t['opt_pnl'] for t in by_day[d]) for d in by_day), default=0)
        max_cash = max((sum(t['opt_ask'] * 100 for t in by_day[d]) for d in by_day), default=0)
        print(f"  {name:<45s} {len(tlist):>3d}t {avg_per_day:>5.1f}/d WR:{wr:>5.1f}% PnL:${total_pnl:>+8,.0f} Worst:${worst:>+7,.0f} MaxCash:${max_cash:>7,.0f}")

    print("=" * 130)
    print("FILTER COMPARISON (real SPXW prices / divide by 10 for 1 SPY)")
    print("=" * 130)

    eval_filter("ALL V7+AG (baseline)", enriched)
    print()

    # By setup
    eval_filter("Skew Charm only", [t for t in enriched if t['setup_name'] == 'Skew Charm'])
    eval_filter("SC + DD", [t for t in enriched if t['setup_name'] in ('Skew Charm', 'DD Exhaustion')])
    eval_filter("SC + DD + AG", [t for t in enriched if t['setup_name'] in ('Skew Charm', 'DD Exhaustion', 'AG Short')])
    print()

    # By grade
    eval_filter("A+ only", [t for t in enriched if t['grade'] == 'A+'])
    eval_filter("A+ or A", [t for t in enriched if t['grade'] in ('A+', 'A')])
    print()

    # Combos targeting ~10/day
    eval_filter("SC(all) + DD(A+/A)", [t for t in enriched if t['setup_name'] == 'Skew Charm' or (t['setup_name'] == 'DD Exhaustion' and t['grade'] in ('A+', 'A'))])
    eval_filter("SC(all) + AG(all) + DD(A+/A)", [t for t in enriched if t['setup_name'] in ('Skew Charm', 'AG Short') or (t['setup_name'] == 'DD Exhaustion' and t['grade'] in ('A+', 'A'))])
    eval_filter("SC(all) + DD(A+) + AG(A+)", [t for t in enriched if t['setup_name'] == 'Skew Charm' or (t['setup_name'] in ('DD Exhaustion', 'AG Short') and t['grade'] == 'A+')])
    eval_filter("SC(all) + AG(all)", [t for t in enriched if t['setup_name'] in ('Skew Charm', 'AG Short')])
    print()

    # Score-based
    eval_filter("Score >= 70", [t for t in enriched if t['score'] is not None and t['score'] >= 70])
    eval_filter("Score >= 60", [t for t in enriched if t['score'] is not None and t['score'] >= 60])
    eval_filter("Score >= 50", [t for t in enriched if t['score'] is not None and t['score'] >= 50])
    print()

    # Time filters
    eval_filter("Before noon ET (UTC<17)", [t for t in enriched if t['ts'].hour < 17])
    eval_filter("10:00-14:00 ET (UTC 15-19)", [t for t in enriched if 15 <= t['ts'].hour < 19])
    print()

    # ===== BEST FILTER DAILY STATEMENT =====
    # Pick the best ~10/day filter
    best_filters = {
        "SC(all) + DD(A+/A)": [t for t in enriched if t['setup_name'] == 'Skew Charm' or (t['setup_name'] == 'DD Exhaustion' and t['grade'] in ('A+', 'A'))],
        "SC(all) + AG(all)": [t for t in enriched if t['setup_name'] in ('Skew Charm', 'AG Short')],
        "SC only": [t for t in enriched if t['setup_name'] == 'Skew Charm'],
    }

    for fname, best in best_filters.items():
        by_day = defaultdict(list)
        for t in best:
            by_day[t['ts'].strftime('%Y-%m-%d')].append(t)

        print("=" * 70)
        print(f"  DAILY STATEMENT: {fname} (1 SPY = SPXW/10)")
        print("=" * 70)
        START = 5000
        bal = START
        print(f"  {'Day':<4s} {'Date':<12s} {'#':>3s} {'Capital':>10s} {'P&L':>10s} {'Cash':>8s}")
        print(f"  {'-'*4} {'-'*12} {'-'*3} {'-'*10} {'-'*10} {'-'*8}")
        print(f"  {'0':<4s} {'Start':<12s} {'':>3s} {'$5,000':>10s} {'':>10s} {'':>8s}")
        for i, day in enumerate(sorted(by_day.keys())):
            tday = by_day[day]
            pnl = sum(t['opt_pnl'] for t in tday) / 10  # divide by 10 for SPY
            cash = sum(t['opt_ask'] * 100 for t in tday) / 10
            bal += pnl
            print(f"  {i+1:<4d} {day:<12s} {len(tday):>3d} ${bal:>9,.0f} ${pnl:>+9,.0f} ${cash:>7,.0f}")
        total = sum(t['opt_pnl'] for t in best) / 10
        print(f"  {'-'*4} {'-'*12} {'-'*3} {'-'*10} {'-'*10}")
        print(f"  {'':4s} {'TOTAL':<12s} {len(best):>3d} ${bal:>9,.0f} ${total:>+9,.0f}")
        print(f"  Return: {(bal-START)/START*100:+.1f}%")
        print()
