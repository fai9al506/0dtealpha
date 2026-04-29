"""OHLC-based trail parameter sweep for SC and AG trades."""
import sqlalchemy as sa
from collections import defaultdict

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = sa.create_engine(DB)

def load_ohlc():
    """Load all OHLC bars into dict keyed by trade_date."""
    with engine.connect() as c:
        rows = c.execute(sa.text("""
            SELECT trade_date, ts AT TIME ZONE 'America/New_York' as ts_et,
                   bar_open, bar_high, bar_low, bar_close
            FROM spx_ohlc_1m
            ORDER BY ts
        """)).fetchall()
    bars = defaultdict(list)
    for r in rows:
        bars[str(r[0])].append({
            'ts': r[1], 'o': float(r[2]), 'h': float(r[3]),
            'l': float(r[4]), 'c': float(r[5])
        })
    return bars

def load_trades(setup_name):
    """Load trades with outcomes, excluding Mar 26."""
    with engine.connect() as c:
        rows = c.execute(sa.text("""
            SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, direction, grade, spot,
                   outcome_result, COALESCE(outcome_pnl,0), COALESCE(outcome_max_profit,0),
                   COALESCE(outcome_max_loss,0), vix, COALESCE(greek_alignment,0),
                   COALESCE(overvix, 0), score
            FROM setup_log
            WHERE setup_name = :name AND outcome_result IS NOT NULL
              AND ts::date != '2026-03-26'
            ORDER BY id
        """), {'name': setup_name}).fetchall()
    trades = []
    for r in rows:
        trades.append({
            'id': r[0], 'ts': r[1], 'dir': r[2], 'grade': r[3], 'spot': float(r[4]),
            'db_result': r[5], 'db_pnl': float(r[6]), 'db_mfe': float(r[7]),
            'db_mae': float(r[8]), 'vix': r[9], 'align': int(r[10]),
            'overvix': float(r[11]), 'score': float(r[12]),
            'date': str(r[1])[:10]
        })
    return trades

def simulate_trade(trade, bars_for_day, sl, be_trigger, activation, gap):
    """Simulate a single trade with given trail params. Returns (result, pnl, mfe)."""
    entry = trade['spot']
    is_long = trade['dir'] == 'long'
    entry_ts = trade['ts']

    # Find bars AFTER entry
    trade_bars = [b for b in bars_for_day if b['ts'] > entry_ts]
    if not trade_bars:
        return 'NO_BARS', 0, 0

    stop = entry - sl if is_long else entry + sl
    be_active = False
    trail_active = False
    trail_stop = None
    max_profit = 0

    for bar in trade_bars:
        h, l = bar['h'], bar['l']

        if is_long:
            # Check stop/trail stop first (conservative)
            current_stop = trail_stop if trail_active else (entry if be_active else stop)
            if l <= current_stop:
                pnl = current_stop - entry
                return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1), round(max_profit, 1)

            # Update MFE
            bar_profit = h - entry
            if bar_profit > max_profit:
                max_profit = bar_profit

            # BE trigger
            if not be_active and max_profit >= be_trigger:
                be_active = True

            # Trail activation
            if not trail_active and max_profit >= activation:
                trail_active = True
                trail_stop = entry + max_profit - gap
            elif trail_active and (entry + max_profit - gap) > trail_stop:
                trail_stop = entry + max_profit - gap
        else:
            # Short
            current_stop = trail_stop if trail_active else (entry if be_active else stop)
            if h >= current_stop:
                pnl = entry - current_stop
                return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1), round(max_profit, 1)

            bar_profit = entry - l
            if bar_profit > max_profit:
                max_profit = bar_profit

            if not be_active and max_profit >= be_trigger:
                be_active = True

            if not trail_active and max_profit >= activation:
                trail_active = True
                trail_stop = entry - max_profit + gap
            elif trail_active and (entry - max_profit + gap) < trail_stop:
                trail_stop = entry - max_profit + gap

    # EOD: close at last bar close
    pnl = (trade_bars[-1]['c'] - entry) if is_long else (entry - trade_bars[-1]['c'])
    return 'EXPIRED', round(pnl, 1), round(max_profit, 1)


def passes_v12(trade, setup_name):
    """Approximate V12 filter check."""
    d = trade['dir']
    align = trade['align']
    vix = trade['vix'] or 99
    overvix = trade['overvix']
    grade = trade['grade']

    if setup_name == 'Skew Charm':
        if grade not in ('A+', 'A', 'B'):
            return False
        return True

    if setup_name == 'AG Short':
        if d == 'short':
            return True
        if align >= 2 and (vix <= 22 or overvix >= 2):
            return True
        return False
    return True


def run_sweep(setup_name, all_bars, trades, sl_range, act_range, gap_range, be_trigger=10):
    """Run parameter sweep."""
    valid_trades = [t for t in trades if t['date'] in all_bars and len(all_bars[t['date']]) > 10]
    print(f"\n{'='*70}")
    print(f"  {setup_name} TRAIL PARAMETER SWEEP")
    print(f"  {len(valid_trades)} trades with OHLC (of {len(trades)} total), excl Mar 26")
    print(f"  BE trigger fixed at {be_trigger} pts")
    print(f"{'='*70}")

    v12_trades = [t for t in valid_trades if passes_v12(t, setup_name)]
    print(f"  V12 filtered: {len(v12_trades)} trades")

    # Show date range and excluded
    if valid_trades:
        dates = sorted(set(t['date'] for t in valid_trades))
        print(f"  Date range: {dates[0]} to {dates[-1]} ({len(dates)} days)")
    excluded = len(trades) - len(valid_trades)
    if excluded:
        excl_ids = [t['id'] for t in trades if t['date'] not in all_bars or len(all_bars.get(t['date'], [])) <= 10]
        print(f"  Excluded (no OHLC): {excluded} trades, IDs: {excl_ids[:10]}")

    results = []
    for sl in sl_range:
        for act in act_range:
            for g in gap_range:
                if g >= act:
                    continue

                for label, subset in [("ALL", valid_trades), ("V12", v12_trades)]:
                    wins, losses, expired = 0, 0, 0
                    total_pnl = 0
                    max_dd = 0
                    running_pnl = 0
                    pnl_list = []

                    for t in subset:
                        day_bars = all_bars[t['date']]
                        result, pnl, mfe = simulate_trade(t, day_bars, sl, be_trigger, act, g)
                        if result == 'NO_BARS':
                            continue

                        if result == 'WIN':
                            wins += 1
                        elif result == 'LOSS':
                            losses += 1
                        else:
                            if pnl > 0:
                                wins += 1
                            elif pnl < 0:
                                losses += 1
                            else:
                                expired += 1

                        total_pnl += pnl
                        running_pnl += pnl
                        if running_pnl < max_dd:
                            max_dd = running_pnl
                        pnl_list.append(pnl)

                    n = wins + losses + expired
                    if n == 0:
                        continue
                    wr = wins / n * 100
                    avg = total_pnl / n
                    gross_w = sum(p for p in pnl_list if p > 0)
                    gross_l = abs(sum(p for p in pnl_list if p < 0))
                    pf = gross_w / gross_l if gross_l > 0 else 999

                    results.append({
                        'label': label, 'sl': sl, 'act': act, 'gap': g,
                        'n': n, 'wins': wins, 'losses': losses, 'expired': expired,
                        'wr': wr, 'pnl': total_pnl, 'avg': avg, 'pf': pf, 'maxdd': max_dd
                    })

    for label in ['ALL', 'V12']:
        subset = [r for r in results if r['label'] == label]
        subset.sort(key=lambda x: x['pnl'], reverse=True)
        print(f"\n--- {label}: Top 15 by PnL ---")
        print(f"{'SL':>4} {'ACT':>4} {'GAP':>4} | {'N':>4} {'W':>3} {'L':>3} {'E':>2} | {'WR':>5} {'PnL':>8} {'Avg':>6} {'PF':>5} {'MaxDD':>7}")
        print("-" * 70)
        for r in subset[:15]:
            print(f"{r['sl']:4.0f} {r['act']:4.0f} {r['gap']:4.0f} | {r['n']:4d} {r['wins']:3d} {r['losses']:3d} {r['expired']:2d} | {r['wr']:5.1f} {r['pnl']:+8.1f} {r['avg']:+6.1f} {r['pf']:5.2f} {r['maxdd']:+7.1f}")

        for r in subset:
            r['risk_adj'] = r['pnl'] / abs(r['maxdd']) if r['maxdd'] < 0 else 999
        subset.sort(key=lambda x: x['risk_adj'], reverse=True)
        print(f"\n--- {label}: Top 10 by Risk-Adjusted (PnL/MaxDD) ---")
        print(f"{'SL':>4} {'ACT':>4} {'GAP':>4} | {'N':>4} {'W':>3} {'L':>3} {'E':>2} | {'WR':>5} {'PnL':>8} {'PF':>5} {'MaxDD':>7} {'P/DD':>6}")
        print("-" * 70)
        for r in subset[:10]:
            print(f"{r['sl']:4.0f} {r['act']:4.0f} {r['gap']:4.0f} | {r['n']:4d} {r['wins']:3d} {r['losses']:3d} {r['expired']:2d} | {r['wr']:5.1f} {r['pnl']:+8.1f} {r['pf']:5.2f} {r['maxdd']:+7.1f} {r['risk_adj']:6.2f}")

    # DEPLOYED vs PREVIOUS comparison
    print(f"\n--- DEPLOYED vs PREVIOUS ---")
    for r in results:
        if r['label'] == 'V12':
            if setup_name == 'Skew Charm':
                if r['sl'] == 14 and r['act'] == 10 and r['gap'] == 5:
                    print(f"  CURRENT (SL=14 ACT=10 GAP=5): {r['n']}t, {r['wr']:.1f}% WR, {r['pnl']:+.1f} PnL, PF={r['pf']:.2f}, MaxDD={r['maxdd']:+.1f}")
                if r['sl'] == 14 and r['act'] == 10 and r['gap'] == 8:
                    print(f"  PREVIOUS(SL=14 ACT=10 GAP=8): {r['n']}t, {r['wr']:.1f}% WR, {r['pnl']:+.1f} PnL, PF={r['pf']:.2f}, MaxDD={r['maxdd']:+.1f}")
                if r['sl'] == 20 and r['act'] == 10 and r['gap'] == 8:
                    print(f"  OLD     (SL=20 ACT=10 GAP=8): {r['n']}t, {r['wr']:.1f}% WR, {r['pnl']:+.1f} PnL, PF={r['pf']:.2f}, MaxDD={r['maxdd']:+.1f}")
            if setup_name == 'AG Short':
                if r['sl'] == 14 and r['act'] == 12 and r['gap'] == 5:
                    print(f"  CURRENT (SL=14 ACT=12 GAP=5): {r['n']}t, {r['wr']:.1f}% WR, {r['pnl']:+.1f} PnL, PF={r['pf']:.2f}, MaxDD={r['maxdd']:+.1f}")
                if r['sl'] == 14 and r['act'] == 15 and r['gap'] == 5:
                    print(f"  PREV    (SL=14 ACT=15 GAP=5): {r['n']}t, {r['wr']:.1f}% WR, {r['pnl']:+.1f} PnL, PF={r['pf']:.2f}, MaxDD={r['maxdd']:+.1f}")
                if r['sl'] == 14 and r['act'] == 10 and r['gap'] == 5:
                    print(f"  ALT     (SL=14 ACT=10 GAP=5): {r['n']}t, {r['wr']:.1f}% WR, {r['pnl']:+.1f} PnL, PF={r['pf']:.2f}, MaxDD={r['maxdd']:+.1f}")


# ---- MAIN ----
if __name__ == '__main__':
    print("Loading OHLC bars...")
    all_bars = load_ohlc()
    print(f"Loaded {sum(len(v) for v in all_bars.values())} bars across {len(all_bars)} days")

    print("\nLoading trades...")
    sc_trades = load_trades('Skew Charm')
    ag_trades = load_trades('AG Short')
    print(f"SC: {len(sc_trades)} trades, AG: {len(ag_trades)} trades")

    # SC sweep
    run_sweep('Skew Charm', all_bars, sc_trades,
              sl_range=[10, 12, 14, 16, 20],
              act_range=[8, 10, 12, 15],
              gap_range=[3, 5, 8])

    # AG sweep
    run_sweep('AG Short', all_bars, ag_trades,
              sl_range=[10, 12, 14, 16, 20],
              act_range=[8, 10, 12, 15, 20],
              gap_range=[3, 5, 8, 10])
