"""V8 Capital — correct calculation: trades × avg premium per day."""
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
               greek_alignment, score, vix, overvix
        FROM setup_log
        WHERE ts >= '2026-03-01' AND ts < '2026-03-14'
          AND outcome_result IS NOT NULL
        ORDER BY ts
    """)).mappings().all()

    def passes_v8(r):
        setup, dirn, align = r['setup_name'], r['direction'], r['greek_alignment']
        vix_val, ov_val = r['vix'], r['overvix']
        if dirn in ('long', 'bullish'):
            if align is None or align < 2: return False
            if vix_val is not None and vix_val > 26:
                ov = ov_val if ov_val is not None else -99
                if ov < 2: return False
            return True
        else:
            if setup == 'Skew Charm': return True
            elif setup == 'AG Short': return True
            elif setup == 'DD Exhaustion': return align is not None and align != 0
            else: return False

    v8_trades = [r for r in trades if passes_v8(r)]

    def get_option_prices(r):
        ts_entry = r['ts']
        is_long = r['direction'] in ('long', 'bullish')
        elapsed = r['outcome_elapsed_min'] or 0
        opt_type = 'call' if is_long else 'put'
        entry_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 "
            "ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_entry - timedelta(minutes=2),
            "t2": ts_entry + timedelta(minutes=2),
            "ts": ts_entry}).mappings().first()
        if not entry_snap: return None
        strikes = entry_snap['rows']
        if isinstance(strikes, str): strikes = json.loads(strikes)
        best_strike, best_ask, best_delta, best_dd = None, None, None, 999
        for sd in strikes:
            if len(sd) < 17: continue
            try:
                delta = float(sd[C_DELTA] if opt_type == 'call' else sd[P_DELTA])
                ask = float(sd[C_ASK] if opt_type == 'call' else sd[P_ASK])
            except (TypeError, ValueError): continue
            if ask <= 0: continue
            diff = abs(abs(delta) - 0.30)
            if diff < best_dd:
                best_dd = diff
                best_strike = float(sd[STRIKE])
                best_ask = ask
                best_delta = abs(delta)
        if not best_strike: return None
        ts_exit = ts_entry + timedelta(minutes=elapsed) if elapsed > 0 else ts_entry + timedelta(minutes=5)
        exit_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 "
            "ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_exit - timedelta(minutes=2),
            "t2": ts_exit + timedelta(minutes=2),
            "ts": ts_exit}).mappings().first()
        if not exit_snap: return None
        exit_strikes = exit_snap['rows']
        if isinstance(exit_strikes, str): exit_strikes = json.loads(exit_strikes)
        for sd in exit_strikes:
            if len(sd) < 17: continue
            try:
                if float(sd[STRIKE]) == best_strike:
                    exit_bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
                    return {
                        'strike': best_strike, 'entry_ask': best_ask,
                        'exit_bid': exit_bid, 'entry_delta': best_delta,
                        'pnl_per_contract': (exit_bid - best_ask) * 100,
                        'capital_per_trade': best_ask * 100,
                        'elapsed_min': elapsed, 'opt_type': opt_type,
                    }
            except (TypeError, ValueError): continue
        return None

    print("Loading option prices...", flush=True)
    enriched = []
    for r in v8_trades:
        opt = get_option_prices(r)
        if opt is None: continue
        enriched.append({**dict(r), **opt})
    print(f"V8: {len(enriched)} trades\n")

    # ========== DAILY CAPITAL: trades × premium ==========
    daily = defaultdict(list)
    for t in enriched:
        daily[t['ts'].strftime('%Y-%m-%d')].append(t)

    print("=" * 80)
    print(" DAILY CAPITAL NEEDED (SPX: trades × entry premium × $100 multiplier)")
    print("=" * 80)
    print(f"\n  {'Date':<12s} {'Trades':>6s} {'Avg Prem':>9s} {'Capital Needed':>15s} {'Day P&L':>10s} {'Net Return':>10s} {'ROC':>7s}")
    print(f"  {'-'*12} {'-'*6} {'-'*9} {'-'*15} {'-'*10} {'-'*10} {'-'*7}")

    day_data = []
    for d in sorted(daily.keys()):
        dt = daily[d]
        num_trades = len(dt)
        avg_premium = sum(t['entry_ask'] for t in dt) / num_trades
        total_capital = sum(t['capital_per_trade'] for t in dt)  # trades × premium × 100
        day_pnl = sum(t['pnl_per_contract'] for t in dt)
        # Net return = cash returned at end of day (capital + pnl)
        net_return = total_capital + day_pnl
        roc = day_pnl / total_capital * 100 if total_capital > 0 else 0

        day_data.append({
            'date': d, 'trades': num_trades, 'avg_premium': avg_premium,
            'capital': total_capital, 'pnl': day_pnl, 'net_return': net_return, 'roc': roc
        })
        print(f"  {d:<12s} {num_trades:>6d} ${avg_premium:>8.2f} ${total_capital:>14,.0f} ${day_pnl:>+9,.0f} ${net_return:>9,.0f} {roc:>+6.1f}%")

    max_capital_day = max(day_data, key=lambda x: x['capital'])
    avg_capital = sum(d['capital'] for d in day_data) / len(day_data)
    total_pnl = sum(d['pnl'] for d in day_data)
    num_days = len(day_data)

    print(f"\n  SPX Summary:")
    print(f"    Max capital needed (1 day):  ${max_capital_day['capital']:>,.0f} ({max_capital_day['date']}, {max_capital_day['trades']} trades)")
    print(f"    Avg capital needed/day:      ${avg_capital:>,.0f}")
    print(f"    Total P&L (10 days):         ${total_pnl:>+,.0f}")
    print(f"    Avg daily P&L:               ${total_pnl/num_days:>+,.0f}")

    # ========== SPY EQUIVALENT ==========
    print(f"\n{'='*80}")
    print(f" SPY EQUIVALENT (SPX / 10)")
    print(f"{'='*80}")
    print(f"\n  SPY option premium ~ SPX / 10, same $100 multiplier")
    print(f"\n  {'Date':<12s} {'Trades':>6s} {'SPY Cap Needed':>15s} {'SPY P&L':>10s} {'Cash Back':>10s}")
    print(f"  {'-'*12} {'-'*6} {'-'*15} {'-'*10} {'-'*10}")

    spy_day_data = []
    for dd in day_data:
        spy_cap = dd['capital'] * 0.1
        spy_pnl = dd['pnl'] * 0.1
        spy_back = spy_cap + spy_pnl
        spy_day_data.append({**dd, 'spy_cap': spy_cap, 'spy_pnl': spy_pnl, 'spy_back': spy_back})
        print(f"  {dd['date']:<12s} {dd['trades']:>6d} ${spy_cap:>14,.0f} ${spy_pnl:>+9,.0f} ${spy_back:>9,.0f}")

    spy_max = max(spy_day_data, key=lambda x: x['spy_cap'])
    spy_avg_cap = sum(d['spy_cap'] for d in spy_day_data) / len(spy_day_data)
    spy_total_pnl = sum(d['spy_pnl'] for d in spy_day_data)

    print(f"\n  SPY Summary (1 contract per signal):")
    print(f"    Max capital needed (1 day):  ${spy_max['spy_cap']:>,.0f} ({spy_max['date']}, {spy_max['trades']} trades)")
    print(f"    Avg capital needed/day:      ${spy_avg_cap:>,.0f}")
    print(f"    Total P&L (10 days):         ${spy_total_pnl:>+,.0f}")
    print(f"    Avg daily P&L:               ${spy_total_pnl/num_days:>+,.0f}")

    # ========== T+1 SETTLEMENT ==========
    print(f"\n{'='*80}")
    print(f" T+1 SETTLEMENT ANALYSIS")
    print(f"{'='*80}")
    print(f"\n  With T+1: yesterday's proceeds aren't available yet.")
    print(f"  Account needs: today's capital + any shortfall from yesterday.")
    print(f"  Worst case: max single day capital (all trades buy fresh options).\n")

    # Actually with T+1, the key insight is:
    # Day 1: you spend $X on options. At EOD, options close. You get back $Y.
    # Day 2: You have (Account - X + Y) available BUT Y hasn't settled yet.
    # So on Day 2 you need another $X from unsettled cash.
    # Effectively: you need max(day's capital) from your account.
    # If Day 1 was a loss, your settled balance is lower.
    # Conservative: fund account with max_capital + worst_day_loss buffer

    print(f"  Scenario: 1 SPY contract per signal")
    print(f"")
    print(f"  Max capital day:     ${spy_max['spy_cap']:>,.0f} (need this much cash available)")
    worst_spy_pnl = min(d['spy_pnl'] for d in spy_day_data)
    print(f"  Worst day loss:      ${worst_spy_pnl:>+,.0f}")
    print(f"")
    # Two consecutive worst days
    consecutive_pairs = []
    sorted_days = sorted(spy_day_data, key=lambda x: x['date'])
    for i in range(len(sorted_days) - 1):
        pair_cap = sorted_days[i]['spy_cap'] + sorted_days[i+1]['spy_cap']
        pair_pnl = sorted_days[i]['spy_pnl'] + sorted_days[i+1]['spy_pnl']
        consecutive_pairs.append({
            'd1': sorted_days[i]['date'], 'd2': sorted_days[i+1]['date'],
            'cap': pair_cap, 'pnl': pair_pnl,
            'unsettled': sorted_days[i]['spy_cap']  # Day 1 capital not yet settled
        })
    worst_pair = max(consecutive_pairs, key=lambda x: x['cap'])
    print(f"  Worst 2-day capital need:")
    print(f"    {worst_pair['d1']} + {worst_pair['d2']}: ${worst_pair['cap']:>,.0f}")
    print(f"    (Day 1 cash ${sorted_days[consecutive_pairs.index(worst_pair)]['spy_cap']:,.0f} unsettled on Day 2)")
    print(f"")

    # Account sizing
    print(f"  ACCOUNT SIZING (1 SPY per signal):")
    print(f"  ----------------------------------------")
    # Method: max daily capital + buffer for losses
    min_acct = spy_max['spy_cap']
    comfortable = spy_max['spy_cap'] + abs(worst_spy_pnl)
    conservative = spy_max['spy_cap'] * 1.5 + abs(worst_spy_pnl)
    print(f"    Minimum (just covers max day):    ${min_acct:>,.0f}")
    print(f"    Comfortable (+ worst day buffer): ${comfortable:>,.0f}")
    print(f"    Conservative (1.5x + buffer):     ${conservative:>,.0f}")
    print(f"")
    print(f"  MONTHLY PROJECTIONS:")
    monthly = spy_total_pnl / num_days * 21
    print(f"    Monthly P&L (21 trading days):    ${monthly:>+,.0f}")
    print(f"    Monthly ROI (on comfortable):     {monthly/comfortable*100:+.0f}%")
    print(f"    Monthly ROI (on conservative):    {monthly/conservative*100:+.0f}%")

    # ========== SCALING TABLE ==========
    print(f"\n{'='*80}")
    print(f" SCALING TABLE — SPY contracts per signal")
    print(f"{'='*80}")
    print(f"\n  {'Qty':>4s} {'Max Day Cap':>12s} {'Worst Day':>10s} {'Acct Needed':>12s} {'Monthly P&L':>12s} {'Monthly ROI':>12s}")
    print(f"  {'-'*4} {'-'*12} {'-'*10} {'-'*12} {'-'*12} {'-'*12}")
    for qty in [1, 2, 3, 5, 10]:
        q_max_cap = spy_max['spy_cap'] * qty
        q_worst = worst_spy_pnl * qty
        q_acct = q_max_cap + abs(q_worst)
        q_monthly = monthly * qty
        q_roi = q_monthly / q_acct * 100 if q_acct > 0 else 0
        print(f"  {qty:>4d} ${q_max_cap:>11,.0f} ${q_worst:>+9,.0f} ${q_acct:>11,.0f} ${q_monthly:>+11,.0f} {q_roi:>+11.0f}%")

print("\nDone.")
