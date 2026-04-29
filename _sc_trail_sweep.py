"""
SC Trail Optimization - Full Parameter Sweep
Uses 5-min SPX OHLC bars to simulate different trail configurations.
Sweeps: SL (10-20), Activation (6-16), Gap (3-10)
Applies V12 filter (grade A+/A/B, time gates, paradigm, alignment).
"""
import csv, sys
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict

# Fix Windows encoding
sys.stdout.reconfigure(encoding='utf-8')

# -- Load 5-min OHLC bars --
bars = []
with open('exports/spx_5min_ohlc.csv', 'r') as f:
    for r in csv.DictReader(f):
        ts_utc = datetime.utcfromtimestamp(int(r['time']))
        ts_et = ts_utc - timedelta(hours=4)  # UTC-4 (EDT)
        bars.append({
            'ts': ts_et,
            'open': float(r['open']),
            'high': float(r['high']),
            'low': float(r['low']),
            'close': float(r['close']),
        })
bars.sort(key=lambda x: x['ts'])
print(f"Loaded {len(bars)} 5-min bars ({bars[0]['ts'].date()} to {bars[-1]['ts'].date()})")

# -- Load SC trades from fresh DB export --
with open('exports/sc_trades_full.csv', 'r', encoding='utf-8') as f:
    all_trades = list(csv.DictReader(f))
print(f"SC trades with outcomes: {len(all_trades)}")


# -- V12 filter (SC rules) --
def passes_v12_sc(t):
    grade = t['grade'].strip()
    if grade not in ('A+', 'A', 'B'):
        return False
    direction = t['direction'].strip()
    is_long = direction == 'long'

    # Time gates
    try:
        ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
    except:
        return False
    tt = ts.time()
    if dtime(14, 30) <= tt < dtime(15, 0):
        return False
    if tt >= dtime(15, 30):
        return False

    if is_long:
        try:
            align = int(float(t['greek_alignment'])) if t.get('greek_alignment') else 0
        except:
            align = 0
        if align < 2:
            return False
        return True  # SC longs VIX exempt
    else:
        paradigm = t.get('paradigm', '').strip()
        if paradigm == 'GEX-LIS':
            return False
        return True


v12_trades = [t for t in all_trades if passes_v12_sc(t)]
print(f"SC V12 A+/A/B: {len(v12_trades)}")

# Filter to dates with OHLC coverage
bar_start = bars[0]['ts'].date()
v12_trades = [t for t in v12_trades
              if datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S').date() >= bar_start]
print(f"SC V12 with OHLC coverage: {len(v12_trades)}")

# Also run on ALL trades (unfiltered A+/A/B) for comparison
all_ab = [t for t in all_trades if t['grade'].strip() in ('A+', 'A', 'B')]
all_ab = [t for t in all_ab
          if datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S').date() >= bar_start]
print(f"SC A+/A/B (no V12 filter, OHLC coverage): {len(all_ab)}")


def market_close(dt):
    return dt.replace(hour=16, minute=0, second=0, microsecond=0)


# -- Core simulation --
def simulate_trade(entry, ts_start, is_long, sl, be_trigger, activation, gap):
    stop = entry - sl if is_long else entry + sl
    trail_stop = stop
    close_time = market_close(ts_start)
    max_fav = 0.0
    max_adv = 0.0

    sim_bars = [b for b in bars if b['ts'] >= ts_start and b['ts'] <= close_time]

    for b in sim_bars:
        if is_long:
            fav = b['high'] - entry
            adverse = b['low'] - entry
        else:
            fav = entry - b['low']
            adverse = entry - b['high']

        if fav > max_fav:
            max_fav = fav
        if adverse < max_adv:
            max_adv = adverse

        # Trail logic
        new_stop = None
        if max_fav >= activation:
            if is_long:
                new_stop = entry + (max_fav - gap)
            else:
                new_stop = entry - (max_fav - gap)
        elif max_fav >= be_trigger:
            new_stop = entry  # breakeven

        if new_stop is not None:
            if is_long and new_stop > trail_stop:
                trail_stop = new_stop
            elif not is_long and new_stop < trail_stop:
                trail_stop = new_stop

        # Check stop hit
        if is_long and b['low'] <= trail_stop:
            pnl = trail_stop - entry
            return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1), max_fav, max_adv
        if not is_long and b['high'] >= trail_stop:
            pnl = entry - trail_stop
            return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1), max_fav, max_adv

    # EXPIRED at market close
    last_close = sim_bars[-1]['close'] if sim_bars else entry
    raw_pnl = (last_close - entry) if is_long else (entry - last_close)
    pnl = round(raw_pnl, 1)
    outcome = 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED')
    return outcome, pnl, max_fav, max_adv


# -- Parameter sweep --
SL_RANGE = [10, 12, 14, 16, 18, 20]
ACT_RANGE = [8, 10, 12, 14, 16]
GAP_RANGE = [3, 4, 5, 6, 7, 8, 10]

# Pre-parse trades
def parse_trades(trade_list):
    parsed = []
    for t in trade_list:
        ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
        spot = float(t['spot'])
        is_long = t['direction'].strip() == 'long'
        parsed.append({
            'id': t['id'], 'ts': ts, 'spot': spot, 'is_long': is_long,
            'grade': t['grade'].strip(), 'dir': t['direction'].strip(),
            'date': ts.date(),
            'db_outcome': t['outcome_result'],
            'db_pnl': float(t['outcome_pnl']) if t['outcome_pnl'] else 0,
            'db_mfe': float(t['outcome_max_profit']) if t['outcome_max_profit'] else 0,
        })
    return parsed

parsed_v12 = parse_trades(v12_trades)
parsed_all_ab = parse_trades(all_ab)

# Run sweep on V12 trades
def run_sweep(parsed_trades, label):
    print(f"\n{'='*105}")
    print(f"  PARAMETER SWEEP: {label} ({len(parsed_trades)} trades)")
    print(f"{'='*105}")

    results = []
    total_combos = len(SL_RANGE) * len(ACT_RANGE) * len(GAP_RANGE)
    done = 0

    for sl in SL_RANGE:
        for act in ACT_RANGE:
            be = min(act, 10)
            for gap in GAP_RANGE:
                if gap >= act:
                    continue

                wins, losses, expired = 0, 0, 0
                total_pnl = 0.0
                equity = 0.0
                peak = 0.0
                max_dd = 0.0
                win_pnls = []
                loss_pnls = []
                daily_pnl = defaultdict(float)
                streak = 0
                max_losing = 0

                for tr in parsed_trades:
                    outcome, pnl, mfe, mae = simulate_trade(
                        tr['spot'], tr['ts'], tr['is_long'],
                        sl, be, act, gap
                    )

                    if outcome == 'WIN':
                        wins += 1
                        win_pnls.append(pnl)
                        streak = 0
                    elif outcome == 'LOSS':
                        losses += 1
                        loss_pnls.append(pnl)
                        streak += 1
                        max_losing = max(max_losing, streak)
                    else:
                        expired += 1
                        if pnl < 0:
                            streak += 1
                            max_losing = max(max_losing, streak)
                        else:
                            streak = 0

                    total_pnl += pnl
                    daily_pnl[tr['date']] += pnl

                    equity += pnl
                    if equity > peak:
                        peak = equity
                    dd = peak - equity
                    if dd > max_dd:
                        max_dd = dd

                total = wins + losses + expired
                if total == 0:
                    continue
                wr = wins / total * 100
                avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0
                avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0
                pf = abs(sum(win_pnls) / sum(loss_pnls)) if loss_pnls and sum(loss_pnls) != 0 else 999
                ratio = total_pnl / max_dd if max_dd > 0 else 999
                losing_days = sum(1 for d, p in daily_pnl.items() if p < 0)
                total_days = len(daily_pnl)
                green_pct = (total_days - losing_days) / total_days * 100 if total_days else 0

                results.append({
                    'sl': sl, 'be': be, 'act': act, 'gap': gap,
                    'trades': total, 'wins': wins, 'losses': losses, 'expired': expired,
                    'wr': wr, 'pnl': total_pnl, 'max_dd': max_dd, 'ratio': ratio,
                    'avg_win': avg_win, 'avg_loss': avg_loss, 'pf': pf,
                    'max_losing': max_losing,
                    'losing_days': losing_days, 'total_days': total_days, 'green_pct': green_pct,
                })

                done += 1

    print(f"  Tested {done} valid combos")

    # Current params baseline
    current = [r for r in results if r['sl'] == 14 and r['act'] == 10 and r['gap'] == 8]
    if current:
        c = current[0]
        print(f"\n  >>> CURRENT (SL=14, BE=10, ACT=10, GAP=8):")
        print(f"      PnL: {c['pnl']:+.1f}  WR: {c['wr']:.1f}%  MaxDD: {c['max_dd']:.1f}  PnL/DD: {c['ratio']:.2f}  PF: {c['pf']:.2f}")
        print(f"      AvgWin: {c['avg_win']:+.1f}  AvgLoss: {c['avg_loss']:+.1f}  MaxLS: {c['max_losing']}  GreenDays: {c['green_pct']:.0f}%")

    def print_table(header, data, limit=20):
        print(f"\n  {header}")
        print(f"  {'SL':>3} {'ACT':>4} {'GAP':>4} | {'PnL':>8} {'WR':>6} {'MaxDD':>7} {'PnL/DD':>7} {'PF':>6} | {'AvgW':>6} {'AvgL':>6} | {'MLS':>3} {'Grn%':>5} | vs Current")
        print(f"  {'---':>3} {'----':>4} {'----':>4} | {'--------':>8} {'------':>6} {'-------':>7} {'-------':>7} {'------':>6} | {'------':>6} {'------':>6} | {'---':>3} {'-----':>5} | ----------")
        for r in data[:limit]:
            delta = r['pnl'] - c['pnl'] if current else 0
            dd_delta = r['max_dd'] - c['max_dd'] if current else 0
            tag = " <-- CURRENT" if r['sl'] == 14 and r['act'] == 10 and r['gap'] == 8 else ""
            print(f"  {r['sl']:>3} {r['act']:>4} {r['gap']:>4} | {r['pnl']:>+8.1f} {r['wr']:>5.1f}% {r['max_dd']:>7.1f} {r['ratio']:>7.2f} {r['pf']:>6.2f} | {r['avg_win']:>+6.1f} {r['avg_loss']:>+6.1f} | {r['max_losing']:>3} {r['green_pct']:>4.0f}% | {delta:>+6.1f} PnL {dd_delta:>+5.1f} DD{tag}")

    # TOP 20 by PnL
    print_table("TOP 20 BY TOTAL PnL:", sorted(results, key=lambda x: -x['pnl']))

    # TOP 20 by PnL/DD
    print_table("TOP 20 BY PnL/DD RATIO:", sorted(results, key=lambda x: -x['ratio']))

    # TOP 20 lowest MaxDD (PnL > 300)
    filtered = [r for r in results if r['pnl'] > 300]
    print_table("TOP 20 LOWEST DRAWDOWN (PnL > +300):", sorted(filtered, key=lambda x: x['max_dd']))

    # GAP sensitivity (SL=14, ACT=10 fixed)
    print(f"\n  GAP SENSITIVITY (SL=14, ACT=10 fixed):")
    gap_results = sorted([r for r in results if r['sl'] == 14 and r['act'] == 10], key=lambda x: x['gap'])
    print(f"  {'GAP':>4} | {'PnL':>8} {'WR':>6} {'MaxDD':>7} {'PnL/DD':>7} {'PF':>6} | {'AvgW':>6} {'AvgL':>6} | vs Current")
    print(f"  {'----':>4} | {'--------':>8} {'------':>6} {'-------':>7} {'-------':>7} {'------':>6} | {'------':>6} {'------':>6} | ----------")
    for r in gap_results:
        delta = r['pnl'] - c['pnl'] if current else 0
        tag = " <-- CURRENT" if r['gap'] == 8 else ""
        print(f"  {r['gap']:>4} | {r['pnl']:>+8.1f} {r['wr']:>5.1f}% {r['max_dd']:>7.1f} {r['ratio']:>7.2f} {r['pf']:>6.2f} | {r['avg_win']:>+6.1f} {r['avg_loss']:>+6.1f} | {delta:>+6.1f}{tag}")

    # ACT sensitivity (SL=14, GAP=5 fixed)
    print(f"\n  ACTIVATION SENSITIVITY (SL=14, GAP=5 fixed):")
    act_results = sorted([r for r in results if r['sl'] == 14 and r['gap'] == 5], key=lambda x: x['act'])
    print(f"  {'ACT':>4} | {'PnL':>8} {'WR':>6} {'MaxDD':>7} {'PnL/DD':>7} {'PF':>6} | {'AvgW':>6} {'AvgL':>6} | {'W':>3} {'L':>3} {'E':>3}")
    print(f"  {'----':>4} | {'--------':>8} {'------':>6} {'-------':>7} {'-------':>7} {'------':>6} | {'------':>6} {'------':>6} | {'---':>3} {'---':>3} {'---':>3}")
    for r in act_results:
        print(f"  {r['act']:>4} | {r['pnl']:>+8.1f} {r['wr']:>5.1f}% {r['max_dd']:>7.1f} {r['ratio']:>7.02f} {r['pf']:>6.02f} | {r['avg_win']:>+6.1f} {r['avg_loss']:>+6.1f} | {r['wins']:>3} {r['losses']:>3} {r['expired']:>3}")

    # SL sensitivity (ACT=10, GAP=5 fixed)
    print(f"\n  SL SENSITIVITY (ACT=10, GAP=5 fixed):")
    sl_results = sorted([r for r in results if r['act'] == 10 and r['gap'] == 5], key=lambda x: x['sl'])
    print(f"  {'SL':>3} | {'PnL':>8} {'WR':>6} {'MaxDD':>7} {'PnL/DD':>7} {'PF':>6} | {'AvgW':>6} {'AvgL':>6} | {'W':>3} {'L':>3} {'E':>3}")
    print(f"  {'---':>3} | {'--------':>8} {'------':>6} {'-------':>7} {'-------':>7} {'------':>6} | {'------':>6} {'------':>6} | {'---':>3} {'---':>3} {'---':>3}")
    for r in sl_results:
        print(f"  {r['sl']:>3} | {r['pnl']:>+8.1f} {r['wr']:>5.1f}% {r['max_dd']:>7.1f} {r['ratio']:>7.02f} {r['pf']:>6.02f} | {r['avg_win']:>+6.1f} {r['avg_loss']:>+6.1f} | {r['wins']:>3} {r['losses']:>3} {r['expired']:>3}")

    return results


# -- MFE analysis --
def mfe_analysis(parsed_trades, label):
    print(f"\n{'='*105}")
    print(f"  MFE ANALYSIS: {label}")
    print(f"{'='*105}")

    data = []
    for tr in parsed_trades:
        outcome, pnl, mfe, mae = simulate_trade(tr['spot'], tr['ts'], tr['is_long'], 14, 10, 10, 8)
        data.append({'outcome': outcome, 'pnl': pnl, 'mfe': mfe, 'mae': mae,
                     'grade': tr['grade'], 'db_mfe': tr['db_mfe']})

    winners = [d for d in data if d['outcome'] == 'WIN']
    losers = [d for d in data if d['outcome'] == 'LOSS']
    expired_t = [d for d in data if d['outcome'] not in ('WIN', 'LOSS')]

    print(f"\n  WINNERS ({len(winners)}):")
    if winners:
        avg_mfe = sum(d['mfe'] for d in winners) / len(winners)
        avg_pnl = sum(d['pnl'] for d in winners) / len(winners)
        print(f"    Avg MFE: {avg_mfe:+.1f} pts")
        print(f"    Avg PnL: {avg_pnl:+.1f} pts (capture: {avg_pnl/avg_mfe*100:.0f}%)")
        print(f"    Wasted: {avg_mfe - avg_pnl:.1f} pts/trade avg left on table")

        # MFE buckets
        buckets = [(10, 15), (15, 20), (20, 30), (30, 50), (50, 200)]
        for lo, hi in buckets:
            b = [d for d in winners if lo <= d['mfe'] < hi]
            if b:
                avg_p = sum(d['pnl'] for d in b) / len(b)
                avg_m = sum(d['mfe'] for d in b) / len(b)
                print(f"    MFE {lo:>2}-{hi:<3}: {len(b):>3} trades, avg PnL {avg_p:+.1f}, avg MFE {avg_m:.1f}, capture {avg_p/avg_m*100:.0f}%")

    print(f"\n  LOSERS ({len(losers)}):")
    if losers:
        avg_mfe_l = sum(d['mfe'] for d in losers) / len(losers)
        avg_mae_l = sum(d['mae'] for d in losers) / len(losers)
        print(f"    Avg MFE before loss: {avg_mfe_l:+.1f} pts")
        print(f"    Avg MAE: {avg_mae_l:+.1f} pts")
        print(f"    MFE >= 10 (touched activation): {sum(1 for d in losers if d['mfe'] >= 10)}")
        print(f"    MFE >= 8: {sum(1 for d in losers if d['mfe'] >= 8)}")
        print(f"    MFE < 3 (never had a chance): {sum(1 for d in losers if d['mfe'] < 3)}")

    if expired_t:
        print(f"\n  EXPIRED ({len(expired_t)}):")
        avg_p = sum(d['pnl'] for d in expired_t) / len(expired_t)
        avg_m = sum(d['mfe'] for d in expired_t) / len(expired_t)
        print(f"    Avg PnL: {avg_p:+.1f}, Avg MFE: {avg_m:+.1f}")

    # Per-grade MFE
    print(f"\n  PER-GRADE MFE:")
    for grade in ['A+', 'A', 'B']:
        g_w = [d for d in winners if d['grade'] == grade]
        g_l = [d for d in losers if d['grade'] == grade]
        if g_w or g_l:
            n = len(g_w) + len(g_l) + len([d for d in expired_t if d['grade'] == grade])
            w_mfe = sum(d['mfe'] for d in g_w) / len(g_w) if g_w else 0
            w_pnl = sum(d['pnl'] for d in g_w) / len(g_w) if g_w else 0
            print(f"    {grade:>3}: {n:>3} trades, {len(g_w)}W/{len(g_l)}L, WR {len(g_w)/n*100:.0f}%, AvgMFE(W) {w_mfe:.1f}, AvgCapture {w_pnl:.1f}")


# -- Run everything --
v12_results = run_sweep(parsed_v12, "V12 FILTERED SC (A+/A/B)")
mfe_analysis(parsed_v12, "V12 FILTERED")

# Per-grade with current vs suggested best
print(f"\n{'='*105}")
print(f"  PER-GRADE COMPARISON: Current (SL14/ACT10/GAP8) vs Candidate (SL14/ACT12/GAP5)")
print(f"{'='*105}")
for grade in ['A+', 'A', 'B']:
    grade_trades = [tr for tr in parsed_v12 if tr['grade'] == grade]
    if not grade_trades:
        continue

    for params_label, sl, be, act, gap in [("Current  ", 14, 10, 10, 8), ("Candidate", 14, 10, 12, 5)]:
        w, l, e, pnl_sum = 0, 0, 0, 0.0
        eq, pk, mdd = 0.0, 0.0, 0.0
        for tr in grade_trades:
            outcome, pnl, _, _ = simulate_trade(tr['spot'], tr['ts'], tr['is_long'], sl, be, act, gap)
            if outcome == 'WIN': w += 1
            elif outcome == 'LOSS': l += 1
            else: e += 1
            pnl_sum += pnl
            eq += pnl
            if eq > pk: pk = eq
            dd = pk - eq
            if dd > mdd: mdd = dd

        n = len(grade_trades)
        print(f"  {grade:>3} {params_label}: {n:>3}t  {w}W/{l}L/{e}E  WR {w/n*100:.0f}%  PnL {pnl_sum:>+7.1f}  MaxDD {mdd:.1f}")

print(f"\n{'='*105}")
print(f"  SWEEP COMPLETE")
print(f"{'='*105}")
