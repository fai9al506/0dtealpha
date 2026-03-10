"""
Single-Position Realistic Trading Simulator v2
================================================
Replays all setup_log entries chronologically enforcing single position.
Uses actual DB columns: ts, outcome_result, outcome_pnl, outcome_elapsed_min, greek_alignment, etc.
"""

import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
from collections import defaultdict

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

# ET offset (simplification: use -5 for EST)
ET_OFFSET = timedelta(hours=-5)

def to_et(dt):
    """Convert UTC datetime to ET (approximation: EST=-5)."""
    if dt is None:
        return None
    if dt.tzinfo:
        return dt.astimezone(timezone(ET_OFFSET))
    return dt + ET_OFFSET

def et_date(dt):
    """Get ET date from UTC datetime."""
    return to_et(dt).date() if dt else None

def fetch_trades():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id, ts, setup_name, direction, grade, score, spot, paradigm,
               outcome_result, outcome_pnl, outcome_elapsed_min,
               outcome_max_profit, outcome_max_loss,
               greek_alignment, vanna_all, spot_vol_beta,
               lis, target, max_plus_gex, max_minus_gex
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Compute ET date and estimated outcome_at for each trade
    for r in rows:
        r['trade_date'] = et_date(r['ts'])
        r['direction_norm'] = r['direction'].capitalize()  # 'long' -> 'Long'
        # Estimate outcome time
        if r['outcome_elapsed_min'] is not None:
            r['outcome_at'] = r['ts'] + timedelta(minutes=r['outcome_elapsed_min'])
        else:
            # EXPIRED trades without elapsed — assume resolved at ~15:57 ET
            r['outcome_at'] = r['ts'] + timedelta(minutes=60)  # conservative 60min
    return rows


def greek_optimal_filter(t):
    """
    OPTIMAL Greek filter from Analysis #8:
    1. Charm alignment gate (encoded in greek_alignment)
    2. GEX Long needs alignment >= +1
    3. AG Short blocked at alignment -3
    4. DD Exhaustion blocks weak-negative SVB
    """
    setup = t['setup_name']
    alignment = t.get('greek_alignment')
    svb = t.get('spot_vol_beta')
    direction = t['direction_norm']

    # Greek alignment encodes: charm(±1) + vanna(±1) + gex(±1)
    # Charm alignment: if alignment exists and direction info available
    # From the analysis: charm aligned = charm supports trade direction
    # We can approximate: if alignment <= 0, at least charm likely opposed
    # But the analysis specifically says charm_aligned is the #1 filter
    #
    # Since we don't have a separate charm column, we use:
    # - alignment >= +1 means at least 2 of 3 Greeks support (very likely charm included)
    # - alignment <= -1 means at least 2 oppose
    # - alignment 0 means mixed
    #
    # The OPTIMAL filter from Analysis #8:
    # Rule 1: Charm must align (we'll use alignment >= 0 as proxy, since charm is the dominant component)
    # Rule 2: GEX Long alignment >= +1
    # Rule 3: AG Short alignment != -3
    # Rule 4: DD SVB filter

    if alignment is not None:
        # Rule 2: GEX Long needs alignment >= +1
        if setup == 'GEX Long' and alignment < 1:
            return False, "gex_low_alignment"

        # Rule 3: AG Short blocked at -3
        if setup == 'AG Short' and alignment == -3:
            return False, "ag_alignment_neg3"

        # Rule 1: General charm opposition filter
        # From Analysis #8: charm opposed trades have 38.8% WR vs 58.1% aligned
        # alignment < 0 is a decent proxy for "charm likely opposed"
        # But to avoid over-filtering, only block at alignment <= -1
        # (alignment 0 is ambiguous)
        if alignment <= -1 and setup not in ('DD Exhaustion',):
            # DD Exhaustion is contrarian — don't filter by alignment direction
            return False, "alignment_negative"

    # Rule 4: DD SVB filter
    if setup == 'DD Exhaustion':
        if svb is not None and -0.5 <= svb < 0:
            return False, "dd_weak_svb"

    return True, "passed"


def simulate(trades, contract_qty=2, contract_type='ES',
             use_greek_filter=True, slippage_pts=0.25,
             commission_per_ct=2.16, allow_reversal=True):
    """
    Single-position simulation with proper time ordering.

    For each filtered signal:
    - If no position: open it, track using log's outcome
    - If same direction open: skip
    - If opposite direction: check if current position resolved before this signal
      - If yes: close current with log outcome, then open new
      - If no: reversal — close current at this signal's spot, open new
    """
    pt_value = 50.0 if contract_type == 'ES' else 5.0
    value_per_pt = pt_value * contract_qty
    commission_rt = commission_per_ct * contract_qty
    slip_cost = slippage_pts * 2  # entry + exit

    # Filter
    filtered = []
    skipped_filter = 0
    filter_reasons = defaultdict(int)
    for t in trades:
        if t['grade'] == 'LOG':
            continue
        if use_greek_filter:
            passed, reason = greek_optimal_filter(t)
            if not passed:
                skipped_filter += 1
                filter_reasons[reason] += 1
                continue
        filtered.append(t)

    # Simulate
    position = None
    results = []
    skipped_pos = 0
    total_commissions = 0.0
    daily_pnl = defaultdict(float)
    daily_count = defaultdict(int)

    def close_position(pos, pnl_pts, exit_reason, result_override=None):
        """Record a closed position."""
        nonlocal total_commissions
        adj_pnl = pnl_pts - slip_cost
        pnl_dollar = adj_pnl * value_per_pt - commission_rt
        total_commissions += commission_rt
        date = pos['trade_date']
        daily_pnl[date] += pnl_dollar
        daily_count[date] += 1

        if result_override:
            result = result_override
        else:
            result = 'WIN' if adj_pnl > 0 else 'LOSS'

        results.append({
            'id': pos['id'],
            'setup': pos['setup_name'],
            'direction': pos['direction_norm'],
            'entry': pos['spot'],
            'pnl_pts': round(adj_pnl, 1),
            'pnl_dollar': round(pnl_dollar, 2),
            'result': result,
            'exit_reason': exit_reason,
            'date': str(date),
            'ts': str(to_et(pos['ts'])),
        })

    for t in filtered:
        if position is not None:
            # Did current position resolve before this signal?
            if position['outcome_at'] and t['ts'] >= position['outcome_at']:
                # Current position already resolved naturally
                close_position(position, position['outcome_pnl'] or 0, 'log_outcome', position['outcome_result'])
                position = None

        if position is not None:
            # Position still open — same or opposite direction?
            if t['direction_norm'] == position['direction_norm']:
                skipped_pos += 1
                continue

            if not allow_reversal:
                skipped_pos += 1
                continue

            # REVERSAL: close current at new signal's spot
            if position['direction_norm'] == 'Long':
                rev_pnl = t['spot'] - position['spot']
            else:
                rev_pnl = position['spot'] - t['spot']

            close_position(position, rev_pnl, f'reversal->{t["setup_name"]}')
            position = None

        # Open new position
        if position is None:
            position = dict(t)  # copy

    # Close final position
    if position is not None:
        close_position(position, position['outcome_pnl'] or 0, 'log_outcome_final', position['outcome_result'])

    # Stats
    wins = sum(1 for r in results if r['result'] == 'WIN')
    losses = sum(1 for r in results if r['result'] == 'LOSS')
    expired = sum(1 for r in results if r['result'] == 'EXPIRED')
    total = len(results)
    total_pnl_pts = sum(r['pnl_pts'] for r in results)
    total_pnl_dollar = sum(r['pnl_dollar'] for r in results)

    # Equity curve
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    equity = []
    for date in sorted(daily_pnl.keys()):
        cum += daily_pnl[date]
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
        equity.append({
            'date': str(date),
            'daily': round(daily_pnl[date], 2),
            'cum': round(cum, 2),
            'trades': daily_count[date],
            'dd': round(dd, 2),
        })

    # Per-setup
    setup_stats = defaultdict(lambda: {'n': 0, 'w': 0, 'pnl_pts': 0.0, 'pnl_$': 0.0})
    for r in results:
        s = setup_stats[r['setup']]
        s['n'] += 1
        if r['result'] == 'WIN': s['w'] += 1
        s['pnl_pts'] += r['pnl_pts']
        s['pnl_$'] += r['pnl_dollar']

    trading_days = len(equity)
    winning_days = sum(1 for e in equity if e['daily'] > 0)

    # Gross pnl (before costs) for profit factor
    gross_wins = sum(r['pnl_dollar'] for r in results if r['pnl_dollar'] > 0)
    gross_losses = abs(sum(r['pnl_dollar'] for r in results if r['pnl_dollar'] < 0))

    return {
        'total': total, 'wins': wins, 'losses': losses, 'expired': expired,
        'wr': round(wins / max(total, 1) * 100, 1),
        'pnl_pts': round(total_pnl_pts, 1),
        'pnl_$': round(total_pnl_dollar, 2),
        'commissions': round(total_commissions, 2),
        'avg_pts': round(total_pnl_pts / max(total, 1), 1),
        'avg_daily_$': round(total_pnl_dollar / max(trading_days, 1), 2),
        'days': trading_days, 'win_days': winning_days,
        'win_day_pct': round(winning_days / max(trading_days, 1) * 100, 1),
        'max_dd_$': round(max_dd, 2),
        'pf': round(gross_wins / max(gross_losses, 1), 2),
        'skipped_filter': skipped_filter,
        'skipped_pos': skipped_pos,
        'filter_reasons': dict(filter_reasons),
        'qty': contract_qty, 'type': contract_type,
        'equity': equity,
        'setup_stats': {k: dict(v) for k, v in setup_stats.items()},
        'trades': results,
    }


def report(r, label):
    print(f"\n{'='*72}")
    print(f"  {label}")
    print(f"  {r['qty']} {r['type']} | ${50 if r['type']=='ES' else 5}/pt | Slippage 0.25pt | Commission $2.16/ct")
    print(f"{'='*72}")

    print(f"\n  Signals total:        {r['total'] + r['skipped_filter'] + r['skipped_pos']}")
    print(f"  Skipped (Greek):      {r['skipped_filter']}")
    if r.get('filter_reasons'):
        for reason, cnt in sorted(r['filter_reasons'].items(), key=lambda x: -x[1]):
            print(f"    - {reason}: {cnt}")
    print(f"  Skipped (in position):{r['skipped_pos']}")
    print(f"  Trades executed:      {r['total']}")
    print(f"  W / L / E:            {r['wins']} / {r['losses']} / {r['expired']}")
    print(f"  Win Rate:             {r['wr']}%")
    print(f"  Profit Factor:        {r['pf']}")
    print(f"  Total PnL (pts):      {r['pnl_pts']:+.1f}")
    print(f"  Total PnL ($):        ${r['pnl_$']:+,.2f}")
    print(f"  Commissions paid:     ${r['commissions']:,.2f}")
    print(f"  Avg PnL/trade (pts):  {r['avg_pts']:+.1f}")
    print(f"  Trading days:         {r['days']}")
    print(f"  Avg daily PnL ($):    ${r['avg_daily_$']:+,.2f}")
    print(f"  Winning days:         {r['win_days']}/{r['days']} ({r['win_day_pct']}%)")
    print(f"  Max Drawdown ($):     ${r['max_dd_$']:,.2f}")

    monthly = r['avg_daily_$'] * 21
    yearly = monthly * 12
    print(f"\n  *** MONTHLY PROJECTION (21 days) ***")
    print(f"  Monthly:  ${monthly:+,.0f}")
    print(f"  Yearly:   ${yearly:+,.0f}")

    print(f"\n  --- PER SETUP ---")
    print(f"  {'Setup':<20} {'N':>4} {'W':>4} {'WR':>6} {'Pts':>8} {'$':>12}")
    print(f"  {'-'*58}")
    for name, s in sorted(r['setup_stats'].items(), key=lambda x: -x[1]['pnl_$']):
        wr = s['w'] / max(s['n'], 1) * 100
        print(f"  {name:<20} {s['n']:>4} {s['w']:>4} {wr:>5.1f}% {s['pnl_pts']:>+7.1f} ${s['pnl_$']:>+10,.2f}")

    print(f"\n  --- DAILY EQUITY ---")
    print(f"  {'Date':<12} {'#':>3} {'Daily $':>11} {'Cum $':>12} {'DD $':>10}")
    print(f"  {'-'*50}")
    for e in r['equity']:
        print(f"  {e['date']:<12} {e['trades']:>3} ${e['daily']:>+9,.2f} ${e['cum']:>+10,.2f} ${e['dd']:>8,.2f}")

    print(f"\n  --- ALL TRADES ({r['total']}) ---")
    print(f"  {'ID':>4} {'Date':<11} {'Setup':<18} {'Dir':<6} {'Pts':>7} {'$':>10} {'Result':<8} {'Exit'}")
    print(f"  {'-'*85}")
    for tr in r['trades']:
        print(f"  {tr['id']:>4} {tr['date']:<11} {tr['setup']:<18} {tr['direction']:<6} {tr['pnl_pts']:>+6.1f} ${tr['pnl_dollar']:>+8,.2f} {tr['result']:<8} {tr['exit_reason']}")


def main():
    print("Fetching trades from production DB...")
    trades = fetch_trades()
    print(f"Loaded {len(trades)} trades ({min(t['trade_date'] for t in trades)} to {max(t['trade_date'] for t in trades)})")

    # Distribution
    by_setup = defaultdict(int)
    for t in trades:
        by_setup[t['setup_name']] += 1
    print("\nDistribution:")
    for s, c in sorted(by_setup.items(), key=lambda x: -x[1]):
        print(f"  {s}: {c}")

    # ===== SCENARIOS =====

    # 1. 2 ES with Greek filter + reversals
    r1 = simulate(trades, 2, 'ES', use_greek_filter=True, allow_reversal=True)
    report(r1, "SCENARIO 1: 2 ES + Greek Filter + Reversals")

    # 2. 4 ES with Greek filter + reversals
    r2 = simulate(trades, 4, 'ES', use_greek_filter=True, allow_reversal=True)
    report(r2, "SCENARIO 2: 4 ES + Greek Filter + Reversals")

    # 3. 2 ES with Greek filter, NO reversals (wait for resolution)
    r3 = simulate(trades, 2, 'ES', use_greek_filter=True, allow_reversal=False)
    report(r3, "SCENARIO 3: 2 ES + Greek Filter + NO Reversals")

    # 4. 2 ES NO filter + reversals (baseline)
    r4 = simulate(trades, 2, 'ES', use_greek_filter=False, allow_reversal=True)
    report(r4, "SCENARIO 4: 2 ES + NO Filter (Baseline)")

    # 5. 4 ES NO filter + reversals
    r5 = simulate(trades, 4, 'ES', use_greek_filter=False, allow_reversal=True)
    report(r5, "SCENARIO 5: 4 ES + NO Filter (Baseline)")

    # ===== COMPARISON =====
    print(f"\n{'='*90}")
    print(f"  COMPARISON SUMMARY")
    print(f"{'='*90}")
    print(f"  {'Scenario':<45} {'N':>4} {'WR':>6} {'Pts':>8} {'Monthly $':>12} {'DD $':>10} {'PF':>5}")
    print(f"  {'-'*92}")
    for label, r in [
        ("2 ES + Greek + Reversals", r1),
        ("4 ES + Greek + Reversals", r2),
        ("2 ES + Greek + No Reversals", r3),
        ("2 ES + No Filter (Baseline)", r4),
        ("4 ES + No Filter (Baseline)", r5),
    ]:
        m = r['avg_daily_$'] * 21
        print(f"  {label:<45} {r['total']:>4} {r['wr']:>5.1f}% {r['pnl_pts']:>+7.1f} ${m:>+10,.0f} ${r['max_dd_$']:>8,.0f} {r['pf']:>5.2f}")

    # Theoretical comparison
    print(f"\n  Log-based theoretical (all signals, no position limit):")
    print(f"  {'2 ES Greek Filter (Analysis #8)':<45} {'176':>4} {'60.8':>5}% {'+602.4':>7}  ${'74,419':>10}  ${'3,502':>8}")
    print(f"  {'4 ES Greek Filter (Analysis #8)':<45} {'176':>4} {'60.8':>5}% {'+602.4':>7}  ${'148,838':>10}  ${'7,004':>8}")

    # Discount
    print(f"\n  REALITY vs THEORY:")
    for label, r, theory in [
        ("2 ES Greek", r1, 74419),
        ("4 ES Greek", r2, 148838),
    ]:
        m = r['avg_daily_$'] * 21
        pct = m / theory * 100 if theory else 0
        print(f"  {label}: ${m:+,.0f}/mo = {pct:.1f}% of theoretical ${theory:,}")


if __name__ == '__main__':
    main()
