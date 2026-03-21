"""V8 Capital Analysis — daily capital usage, Mar 4 deep-dive, T+1 settlement."""
import sys, io, json, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import timedelta, datetime

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
        vix_val = r['vix']
        ov_val = r['overvix']
        if dirn in ('long', 'bullish'):
            if align is None or align < 2:
                return False
            if vix_val is not None and vix_val > 26:
                ov = ov_val if ov_val is not None else -99
                if ov < 2:
                    return False
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
        if not entry_snap:
            return None
        strikes = entry_snap['rows']
        if isinstance(strikes, str):
            strikes = json.loads(strikes)

        best_strike, best_ask, best_bid, best_delta, best_dd = None, None, None, None, 999
        for sd in strikes:
            if len(sd) < 17:
                continue
            try:
                delta = float(sd[C_DELTA] if opt_type == 'call' else sd[P_DELTA])
                ask = float(sd[C_ASK] if opt_type == 'call' else sd[P_ASK])
                bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
            except (TypeError, ValueError):
                continue
            if ask <= 0:
                continue
            diff = abs(abs(delta) - 0.30)
            if diff < best_dd:
                best_dd = diff
                best_strike = float(sd[STRIKE])
                best_ask = ask
                best_bid = bid
                best_delta = abs(delta)

        if not best_strike or best_ask is None:
            return None

        ts_exit = ts_entry + timedelta(minutes=elapsed) if elapsed > 0 else ts_entry + timedelta(minutes=5)
        exit_snap = conn.execute(text(
            "SELECT rows FROM chain_snapshots WHERE ts BETWEEN :t1 AND :t2 "
            "ORDER BY ABS(EXTRACT(EPOCH FROM (ts - :ts))) LIMIT 1"
        ), {"t1": ts_exit - timedelta(minutes=2),
            "t2": ts_exit + timedelta(minutes=2),
            "ts": ts_exit}).mappings().first()
        if not exit_snap:
            return None
        exit_strikes = exit_snap['rows']
        if isinstance(exit_strikes, str):
            exit_strikes = json.loads(exit_strikes)

        for sd in exit_strikes:
            if len(sd) < 17:
                continue
            try:
                if float(sd[STRIKE]) == best_strike:
                    exit_bid = float(sd[C_BID] if opt_type == 'call' else sd[P_BID])
                    return {
                        'strike': best_strike,
                        'entry_delta': best_delta,
                        'entry_ask': best_ask,
                        'entry_bid': best_bid,
                        'exit_bid': exit_bid,
                        'pnl_per_contract': (exit_bid - best_ask) * 100,
                        'pnl_pct': (exit_bid - best_ask) / best_ask * 100 if best_ask > 0 else 0,
                        'opt_type': opt_type,
                        'elapsed_min': elapsed,
                        'capital_used': best_ask * 100,  # $100 multiplier per SPX contract
                    }
            except (TypeError, ValueError):
                continue
        return None

    print("Loading option prices...", flush=True)
    enriched = []
    for r in v8_trades:
        opt = get_option_prices(r)
        if opt is None:
            continue
        enriched.append({**dict(r), **opt})

    print(f"V8: {len(enriched)} trades with real prices\n")

    # ========== PART 1: MAR 4 DEEP DIVE ==========
    print("=" * 70)
    print(" MARCH 4 DEEP DIVE — Why -$4,325?")
    print("=" * 70)

    mar4 = [t for t in enriched if t['ts'].strftime('%Y-%m-%d') == '2026-03-04']
    mar4_wins = [t for t in mar4 if t['pnl_per_contract'] > 0]
    mar4_losses = [t for t in mar4 if t['pnl_per_contract'] <= 0]
    print(f"\n  Total trades: {len(mar4)} ({len(mar4_wins)}W / {len(mar4_losses)}L)")
    print(f"  Total P&L: ${sum(t['pnl_per_contract'] for t in mar4):,.0f}")
    print(f"  Winners total: ${sum(t['pnl_per_contract'] for t in mar4_wins):,.0f}")
    print(f"  Losers total: ${sum(t['pnl_per_contract'] for t in mar4_losses):,.0f}")

    print(f"\n  Every trade on Mar 4:")
    mar4_sorted = sorted(mar4, key=lambda x: x['ts'])
    for t in mar4_sorted:
        time_str = t['ts'].strftime('%H:%M')
        result = "WIN" if t['pnl_per_contract'] > 0 else "LOSS"
        print(f"    {time_str} #{t['id']:<5d} {t['setup_name']:<22s} {t['direction']:<6s} "
              f"align={t['greek_alignment']:>+3d}  entry=${t['entry_ask']:.2f} exit=${t['exit_bid']:.2f}  "
              f"${t['pnl_per_contract']:>+7,.0f} ({t['pnl_pct']:>+5.0f}%)  {t['elapsed_min']:.0f}min  "
              f"spot={t['spot']:.1f} VIX={t['vix']}")

    # Check what SPX did on Mar 4
    print(f"\n  Setup breakdown Mar 4:")
    by_setup = defaultdict(list)
    for t in mar4:
        by_setup[t['setup_name']].append(t)
    for sn, st in sorted(by_setup.items()):
        sw = sum(1 for t in st if t['pnl_per_contract'] > 0)
        sp = sum(t['pnl_per_contract'] for t in st)
        dirs = defaultdict(int)
        for t in st:
            dirs[t['direction']] += 1
        dir_str = ", ".join(f"{d}:{c}" for d, c in dirs.items())
        print(f"    {sn:<22s} {len(st):>2d}t  {sw}W  ${sp:>+7,.0f}  ({dir_str})")

    # Check if any filter could help
    print(f"\n  Could filters help?")
    # By time of day
    by_hour = defaultdict(list)
    for t in mar4:
        by_hour[t['ts'].hour].append(t)
    for h in sorted(by_hour.keys()):
        ht = by_hour[h]
        hp = sum(t['pnl_per_contract'] for t in ht)
        hw = sum(1 for t in ht if t['pnl_per_contract'] > 0)
        print(f"    Hour {h:02d}: {len(ht):>2d}t  {hw}W  ${hp:>+7,.0f}")

    # By alignment
    print(f"\n  By alignment on Mar 4:")
    by_align = defaultdict(list)
    for t in mar4:
        by_align[t['greek_alignment']].append(t)
    for a in sorted(by_align.keys()):
        at = by_align[a]
        ap = sum(t['pnl_per_contract'] for t in at)
        aw = sum(1 for t in at if t['pnl_per_contract'] > 0)
        print(f"    align={a:>+3d}: {len(at):>2d}t  {aw}W  ${ap:>+7,.0f}")

    # Loss severity
    print(f"\n  Loss severity distribution (Mar 4):")
    for t in sorted(mar4_losses, key=lambda x: x['pnl_per_contract']):
        print(f"    #{t['id']:<5d} {t['setup_name']:<22s} ${t['pnl_per_contract']:>+7,.0f} ({t['pnl_pct']:>+5.0f}%)  held {t['elapsed_min']:.0f}min  entry=${t['entry_ask']:.2f}")

    # ========== PART 2: DAILY CAPITAL USAGE ==========
    print(f"\n{'='*70}")
    print(f" DAILY CAPITAL USAGE (V8, 1 SPX contract per trade)")
    print(f"{'='*70}")

    daily = defaultdict(list)
    for t in enriched:
        daily[t['ts'].strftime('%Y-%m-%d')].append(t)

    print(f"\n  {'Date':<12s} {'Trades':>6s} {'PnL':>9s} {'Tot Premium':>12s} {'Max Simult':>11s} {'Peak Cap':>10s} {'ROC':>7s}")
    print(f"  {'-'*12} {'-'*6} {'-'*9} {'-'*12} {'-'*11} {'-'*10} {'-'*7}")

    total_capital_days = []
    for d in sorted(daily.keys()):
        dt = daily[d]
        day_pnl = sum(t['pnl_per_contract'] for t in dt)
        total_premium = sum(t['capital_used'] for t in dt)  # total capital deployed across all trades

        # Calculate max simultaneous positions (overlapping trades)
        events = []
        for t in dt:
            entry_time = t['ts']
            exit_time = entry_time + timedelta(minutes=t['elapsed_min']) if t['elapsed_min'] > 0 else entry_time + timedelta(minutes=5)
            events.append(('open', entry_time, t['capital_used']))
            events.append(('close', exit_time, t['capital_used']))
        events.sort(key=lambda x: (x[1], 0 if x[0] == 'close' else 1))

        peak_capital = 0
        current_capital = 0
        max_simultaneous = 0
        current_open = 0
        for ev_type, ev_time, ev_cap in events:
            if ev_type == 'open':
                current_capital += ev_cap
                current_open += 1
            else:
                current_capital -= ev_cap
                current_open -= 1
            if current_capital > peak_capital:
                peak_capital = current_capital
            if current_open > max_simultaneous:
                max_simultaneous = current_open

        roc = day_pnl / peak_capital * 100 if peak_capital > 0 else 0
        total_capital_days.append({
            'date': d, 'pnl': day_pnl, 'total_premium': total_premium,
            'peak_capital': peak_capital, 'max_simult': max_simultaneous, 'roc': roc,
            'trades': len(dt)
        })
        print(f"  {d:<12s} {len(dt):>6d} ${day_pnl:>+8,.0f} ${total_premium:>11,.0f} {max_simultaneous:>11d} ${peak_capital:>9,.0f} {roc:>+6.1f}%")

    avg_peak = sum(d['peak_capital'] for d in total_capital_days) / len(total_capital_days)
    max_peak = max(d['peak_capital'] for d in total_capital_days)
    avg_pnl = sum(d['pnl'] for d in total_capital_days) / len(total_capital_days)
    worst_day = min(d['pnl'] for d in total_capital_days)

    print(f"\n  Summary (SPX, 1 contract per trade):")
    print(f"    Avg peak capital/day:  ${avg_peak:>,.0f}")
    print(f"    Max peak capital/day:  ${max_peak:>,.0f}")
    print(f"    Avg daily P&L:         ${avg_pnl:>+,.0f}")
    print(f"    Worst day:             ${worst_day:>+,.0f}")

    # ========== PART 3: T+1 SETTLEMENT & SPY SIZING ==========
    print(f"\n{'='*70}")
    print(f" T+1 SETTLEMENT — SPY ACCOUNT SIZING")
    print(f"{'='*70}")

    # SPY options = ~SPX/10 in price, same $100 multiplier
    spy_factor = 0.1
    print(f"\n  SPY option premium ~ SPX / 10")
    print(f"  SPY multiplier = $100 (same as SPX)")
    print(f"\n  T+1 means: today's closed trades settle tomorrow.")
    print(f"  So you need enough capital for TODAY's new trades + YESTERDAY's unsettled cash.")
    print(f"  Worst case: 2 bad days in a row = need capital for both days simultaneously.\n")

    print(f"  {'Contracts':>10s} {'Avg Peak/day':>13s} {'Max Peak/day':>13s} {'2-Day Worst':>12s} {'Monthly P&L':>12s} {'Acct Needed':>12s}")
    print(f"  {'-'*10} {'-'*13} {'-'*13} {'-'*12} {'-'*12} {'-'*12}")

    for qty in [1, 2, 3, 5, 10]:
        spy_avg_peak = avg_peak * spy_factor * qty
        spy_max_peak = max_peak * spy_factor * qty
        spy_worst = abs(worst_day) * spy_factor * qty
        spy_monthly = avg_pnl * spy_factor * qty * 21
        # T+1: need max_peak for today + yesterday's capital might not have settled
        # Conservative: 2x max peak + worst day loss buffer
        acct_needed = spy_max_peak * 2 + spy_worst
        print(f"  {qty:>10d} ${spy_avg_peak:>12,.0f} ${spy_max_peak:>12,.0f} ${spy_worst:>11,.0f} ${spy_monthly:>+11,.0f} ${acct_needed:>11,.0f}")

    # ========== PART 4: REALISTIC 1-SPY PLAN ==========
    print(f"\n{'='*70}")
    print(f" REALISTIC 1-SPY PLAN")
    print(f"{'='*70}")

    spy_avg_peak_1 = avg_peak * spy_factor
    spy_max_peak_1 = max_peak * spy_factor
    spy_monthly_1 = avg_pnl * spy_factor * 21
    spy_worst_1 = abs(worst_day) * spy_factor

    print(f"\n  1 SPY contract per signal:")
    print(f"    Avg option premium:     ${sum(t['entry_ask'] for t in enriched)/len(enriched) * spy_factor * 100:,.0f}")
    print(f"    Avg peak capital/day:   ${spy_avg_peak_1:,.0f}")
    print(f"    Max peak capital/day:   ${spy_max_peak_1:,.0f}")
    print(f"    Worst single day:       -${spy_worst_1:,.0f}")
    print(f"    Avg daily P&L:          ${avg_pnl * spy_factor:+,.0f}")
    print(f"    Monthly P&L:            ${spy_monthly_1:+,.0f}")
    print(f"    Monthly ROI:            {spy_monthly_1 / (spy_max_peak_1 * 2 + spy_worst_1) * 100:+.0f}%")
    print(f"")
    print(f"  RECOMMENDED ACCOUNT SIZE (1 SPY, T+1):")
    print(f"    Minimum (tight):        ${spy_max_peak_1 * 1.5 + spy_worst_1:,.0f}")
    print(f"    Comfortable:            ${spy_max_peak_1 * 2 + spy_worst_1:,.0f}")
    print(f"    Conservative:           ${spy_max_peak_1 * 2.5 + spy_worst_1 * 2:,.0f}")

    # ========== PART 5: CAN WE REDUCE LOSSES? ==========
    print(f"\n{'='*70}")
    print(f" LOSS ANALYSIS — Can we reduce bad days?")
    print(f"{'='*70}")

    # Look at all losing days
    losing_days = [d for d in total_capital_days if d['pnl'] < 0]
    print(f"\n  Losing days: {len(losing_days)} out of {len(total_capital_days)}")
    for d in sorted(losing_days, key=lambda x: x['pnl']):
        day_trades = daily[d['date']]
        longs = [t for t in day_trades if t['direction'] in ('long', 'bullish')]
        shorts = [t for t in day_trades if t['direction'] in ('short', 'bearish')]
        long_pnl = sum(t['pnl_per_contract'] for t in longs)
        short_pnl = sum(t['pnl_per_contract'] for t in shorts)
        long_wr = sum(1 for t in longs if t['pnl_per_contract'] > 0) / len(longs) * 100 if longs else 0
        short_wr = sum(1 for t in shorts if t['pnl_per_contract'] > 0) / len(shorts) * 100 if shorts else 0
        print(f"\n  {d['date']}  Total: ${d['pnl']:>+,.0f}  ({d['trades']} trades)")
        print(f"    Longs:  {len(longs):>2d}t  {long_wr:.0f}% WR  ${long_pnl:>+,.0f}")
        print(f"    Shorts: {len(shorts):>2d}t  {short_wr:.0f}% WR  ${short_pnl:>+,.0f}")

        # Which setups lost?
        by_setup = defaultdict(list)
        for t in day_trades:
            by_setup[t['setup_name']].append(t)
        for sn, st in sorted(by_setup.items(), key=lambda x: sum(t['pnl_per_contract'] for t in x[1])):
            sp = sum(t['pnl_per_contract'] for t in st)
            sw = sum(1 for t in st if t['pnl_per_contract'] > 0)
            dirs = [t['direction'] for t in st]
            print(f"      {sn:<22s} {len(st):>2d}t  {sw}W  ${sp:>+7,.0f}  dirs={dirs}")

    # Test: what if we limited max trades per day?
    print(f"\n  What if we limit trades per day?")
    for max_trades in [5, 10, 15, 20, 999]:
        total = 0
        for d in sorted(daily.keys()):
            dt = sorted(daily[d], key=lambda x: x['ts'])[:max_trades]
            total += sum(t['pnl_per_contract'] for t in dt)
        label = f"Max {max_trades}/day" if max_trades < 999 else "No limit"
        print(f"    {label:<15s}: ${total:>+10,.0f}")

    # Test: what if we stop trading after X losses in a day?
    print(f"\n  What if we stop after N consecutive losses?")
    for max_consec in [3, 4, 5, 999]:
        total = 0
        for d in sorted(daily.keys()):
            dt = sorted(daily[d], key=lambda x: x['ts'])
            consec_losses = 0
            stopped = False
            for t in dt:
                if stopped:
                    continue
                total += t['pnl_per_contract']
                if t['pnl_per_contract'] <= 0:
                    consec_losses += 1
                    if consec_losses >= max_consec:
                        stopped = True
                else:
                    consec_losses = 0
        label = f"Stop@{max_consec} losses" if max_consec < 999 else "No stop"
        print(f"    {label:<18s}: ${total:>+10,.0f}")

    # Test: what if we stop after daily loss exceeds $X?
    print(f"\n  What if we stop after daily P&L drops below -$X?")
    for max_loss in [500, 1000, 1500, 2000, 99999]:
        total = 0
        for d in sorted(daily.keys()):
            dt = sorted(daily[d], key=lambda x: x['ts'])
            day_cum = 0
            for t in dt:
                if day_cum < -max_loss:
                    continue
                day_cum += t['pnl_per_contract']
                total += t['pnl_per_contract']
        label = f"Stop@-${max_loss}" if max_loss < 99999 else "No stop"
        print(f"    {label:<18s}: ${total:>+10,.0f}")

print("\nDone.")
