"""SB2 Absorption gate backtest: AND vs OR (vol/delta)"""
import os
from sqlalchemy import create_engine, text
from collections import defaultdict

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway")
engine = create_engine(DATABASE_URL)

# Get all bars
with engine.begin() as conn:
    all_bars = conn.execute(text("""
        SELECT trade_date, bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume, bar_delta, ts_start, ts_end
        FROM es_range_bars
        WHERE source = 'rithmic' AND range_pts = 5.0 AND status = 'closed'
        ORDER BY trade_date, bar_idx
    """)).fetchall()

cols = ['trade_date','bar_idx','bar_open','bar_high','bar_low','bar_close','bar_volume','bar_delta','ts_start','ts_end']
bars_by_date = {}
for row in all_bars:
    d = {cols[i]: row[i] for i in range(len(cols))}
    td = d['trade_date']
    if td not in bars_by_date:
        bars_by_date[td] = []
    bars_by_date[td].append(d)

print(f"Trading days: {len(bars_by_date)}, Total bars: {len(all_bars)}")


def sim_sb2(bars_list, vol_mult, delta_mult, recovery_pct, gate_mode='AND'):
    signals = []
    last_bull_idx = -100
    last_bear_idx = -100
    cooldown = 10

    for i in range(22, len(bars_list)):
        bar_n = bars_list[i]
        bar_n1 = bars_list[i-1]

        # Time gate: block after 15:00 ET
        ts = bar_n.get('ts_end')
        if ts and hasattr(ts, 'hour'):
            if ts.hour > 15 or (ts.hour == 15 and ts.minute >= 0):
                continue

        # Lookback
        lookback = bars_list[i-22:i-2]
        vols = [b['bar_volume'] for b in lookback if b['bar_volume'] > 0]
        deltas = [abs(b['bar_delta']) for b in lookback if b['bar_delta'] != 0]
        if not vols or not deltas:
            continue
        avg_vol = sum(vols) / len(vols)
        avg_delta = sum(deltas) / len(deltas)

        flush_vol = bar_n1['bar_volume']
        flush_delta = bar_n1['bar_delta']

        vol_pass = avg_vol > 0 and flush_vol >= avg_vol * vol_mult
        delta_pass = avg_delta > 0 and abs(flush_delta) >= avg_delta * delta_mult

        if gate_mode == 'AND':
            if not (vol_pass and delta_pass):
                continue
        elif gate_mode == 'OR':
            if not (vol_pass or delta_pass):
                continue
        elif gate_mode == 'DELTA_ONLY':
            if not delta_pass:
                continue

        flush_move = bar_n1['bar_close'] - bar_n1['bar_open']
        flush_range = abs(flush_move)
        if flush_range < 0.5:
            continue

        rec_close = bar_n['bar_close']
        if flush_delta < 0 and flush_move < 0:
            rec_amt = rec_close - bar_n1['bar_close']
            if rec_amt <= 0 or rec_amt / flush_range < recovery_pct:
                continue
            direction = 'bullish'
        elif flush_delta > 0 and flush_move > 0:
            rec_amt = bar_n1['bar_close'] - rec_close
            if rec_amt <= 0 or rec_amt / flush_range < recovery_pct:
                continue
            direction = 'bearish'
        else:
            continue

        idx = bar_n['bar_idx']
        if direction == 'bullish' and idx - last_bull_idx < cooldown:
            continue
        if direction == 'bearish' and idx - last_bear_idx < cooldown:
            continue

        if direction == 'bullish':
            last_bull_idx = idx
        else:
            last_bear_idx = idx

        signals.append({
            'date': bar_n['trade_date'],
            'idx': idx,
            'direction': direction,
            'es_entry': bar_n['bar_close'],
            'vol_ratio': flush_vol / avg_vol if avg_vol > 0 else 0,
            'delta_ratio': abs(flush_delta) / avg_delta if avg_delta > 0 else 0,
        })

    return signals


def forward_sim(signals, bars_list, sl=8, target=12):
    results = []
    bar_map = {b['bar_idx']: b for b in bars_list}
    max_idx = max(b['bar_idx'] for b in bars_list) if bars_list else 0

    for sig in signals:
        entry = sig['es_entry']
        is_long = sig['direction'] == 'bullish'
        stop = entry - sl if is_long else entry + sl
        target_lvl = entry + target if is_long else entry - target

        trail_stop = stop
        trail_peak = 0.0
        pnl = None

        for idx in range(sig['idx'] + 1, max_idx + 1):
            if idx not in bar_map:
                continue
            bar = bar_map[idx]

            if is_long:
                fav = bar['bar_high'] - entry
                unfav_price = bar['bar_low']
            else:
                fav = entry - bar['bar_low']
                unfav_price = bar['bar_high']

            if fav > trail_peak:
                trail_peak = fav

            # Trail: BE@10, activation=20, gap=10
            if trail_peak >= 20:
                new_lock = max(trail_peak - 10, 0)
                if is_long:
                    new_stop = entry + new_lock
                    if new_stop > trail_stop:
                        trail_stop = new_stop
                else:
                    new_stop = entry - new_lock
                    if new_stop < trail_stop:
                        trail_stop = new_stop
            elif trail_peak >= 10:
                if is_long and entry > trail_stop:
                    trail_stop = entry
                elif not is_long and entry < trail_stop:
                    trail_stop = entry

            # Check stop
            if is_long and bar['bar_low'] <= trail_stop:
                pnl = trail_stop - entry
                break
            elif not is_long and bar['bar_high'] >= trail_stop:
                pnl = entry - trail_stop
                break

            # Check target
            if is_long and bar['bar_high'] >= target_lvl:
                pnl = target
                break
            elif not is_long and bar['bar_low'] <= target_lvl:
                pnl = target
                break

        if pnl is None:
            pnl = 0

        results.append({
            **sig,
            'pnl': round(pnl, 1),
            'result': 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED'),
            'max_fav': round(trail_peak, 1),
        })

    return results


def run_scenario(name, mode, vm, dm):
    all_results = []
    for td in bars_by_date:
        bars = bars_by_date[td]
        if len(bars) < 25:
            continue
        sigs = sim_sb2(bars, vm, dm, 0.60, gate_mode=mode)
        if sigs:
            res = forward_sim(sigs, bars, sl=8, target=12)
            all_results.extend(res)

    wins = sum(1 for r in all_results if r['result'] == 'WIN')
    losses = sum(1 for r in all_results if r['result'] == 'LOSS')
    total = len(all_results)
    wr = wins / total * 100 if total > 0 else 0
    total_pnl = sum(r['pnl'] for r in all_results)

    peak = 0
    running = 0
    max_dd = 0
    for r in sorted(all_results, key=lambda x: (str(x['date']), x['idx'])):
        running += r['pnl']
        if running > peak:
            peak = running
        dd = running - peak
        if dd < max_dd:
            max_dd = dd

    gross_win = sum(r['pnl'] for r in all_results if r['pnl'] > 0)
    gross_loss = abs(sum(r['pnl'] for r in all_results if r['pnl'] < 0))
    pf = gross_win / gross_loss if gross_loss > 0 else 999

    days = len(set(str(r['date']) for r in all_results)) or 1
    pnl_per_day = total_pnl / days

    return {
        'name': name, 'total': total, 'wins': wins, 'losses': losses,
        'wr': wr, 'pnl': total_pnl, 'pnl_per_day': pnl_per_day,
        'max_dd': max_dd, 'pf': pf, 'results': all_results, 'days': days,
    }


# Run all scenarios
scenarios = [
    ('CURRENT (vol>=1.2x AND dlt>=1.0x)', 'AND', 1.2, 1.0),
    ('OR (vol>=1.2x OR dlt>=1.5x)', 'OR', 1.2, 1.5),
    ('OR (vol>=1.2x OR dlt>=1.2x)', 'OR', 1.2, 1.2),
    ('OR (vol>=1.0x OR dlt>=1.2x)', 'OR', 1.0, 1.2),
    ('DELTA_ONLY (dlt>=1.5x)', 'DELTA_ONLY', 0, 1.5),
    ('DELTA_ONLY (dlt>=1.2x)', 'DELTA_ONLY', 0, 1.2),
    ('DELTA_ONLY (dlt>=1.0x)', 'DELTA_ONLY', 0, 1.0),
]

print()
print(f"{'Scenario':<40} {'#Sig':>5} {'WR':>6} {'PnL':>8} {'PnL/d':>7} {'MaxDD':>7} {'PF':>5} {'Days':>5}")
print('-' * 90)

best = None
for name, mode, vm, dm in scenarios:
    s = run_scenario(name, mode, vm, dm)
    print(f"{s['name']:<40} {s['total']:>5} {s['wr']:>5.1f}% {s['pnl']:>+7.1f} {s['pnl_per_day']:>+6.1f} {s['max_dd']:>+6.1f} {s['pf']:>5.2f} {s['days']:>5}")
    if best is None or s['pnl'] > best['pnl']:
        best = s

# Show per-day for best OR scenario
print()
or_result = run_scenario('OR (vol>=1.2x OR dlt>=1.5x)', 'OR', 1.2, 1.5)
print(f"=== OR (vol>=1.2x OR dlt>=1.5x) — {or_result['total']} signals, {or_result['days']} days ===")
by_date = defaultdict(list)
for r in or_result['results']:
    by_date[str(r['date'])].append(r)
for d in sorted(by_date.keys())[-15:]:
    trades = by_date[d]
    pnl = sum(t['pnl'] for t in trades)
    w = sum(1 for t in trades if t['result'] == 'WIN')
    l = sum(1 for t in trades if t['result'] == 'LOSS')
    print(f"  {d}: {len(trades)}t  {w}W/{l}L  {pnl:+.1f} pts")

# Compare: what CURRENT missed that OR catches
print()
current = run_scenario('CURRENT', 'AND', 1.2, 1.0)
current_keys = set((str(r['date']), r['idx']) for r in current['results'])
or_keys = set((str(r['date']), r['idx']) for r in or_result['results'])
new_trades = [r for r in or_result['results'] if (str(r['date']), r['idx']) not in current_keys]
print(f"=== NEW trades OR catches that CURRENT misses: {len(new_trades)} ===")
new_wins = sum(1 for r in new_trades if r['result'] == 'WIN')
new_losses = sum(1 for r in new_trades if r['result'] == 'LOSS')
new_pnl = sum(r['pnl'] for r in new_trades)
print(f"  {new_wins}W/{new_losses}L, {new_pnl:+.1f} pts")
for r in new_trades[-10:]:
    print(f"  {r['date']} #{r['idx']:3d} {r['direction']:>7} ES={r['es_entry']:.2f} vol={r['vol_ratio']:.1f}x dlt={r['delta_ratio']:.1f}x -> {r['result']} {r['pnl']:+.1f}")
