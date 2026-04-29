"""SB2 Absorption gate backtest v2: with time filter + OR gate combos"""
import os
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import time as dtime

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway")
engine = create_engine(DATABASE_URL)

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

# Load Volland SVB data per date (nearest snapshot to each bar timestamp)
# We'll cache SVB per date as time-series for lookup
volland_by_date = {}
with engine.begin() as conn:
    vrows = conn.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as ts_et,
               date(ts AT TIME ZONE 'America/New_York') as td,
               (payload->'statistics'->>'spotVolBeta')::float as svb,
               payload->'statistics'->>'paradigm' as paradigm
        FROM volland_snapshots
        WHERE payload->>'error_event' IS NULL
          AND payload->'statistics' IS NOT NULL
          AND (payload->'statistics'->>'spotVolBeta') IS NOT NULL
        ORDER BY ts
    """)).fetchall()
    for r in vrows:
        td = r[1]
        if td not in volland_by_date:
            volland_by_date[td] = []
        volland_by_date[td].append({
            'ts': r[0], 'svb': r[2], 'paradigm': r[3]
        })
print(f"Volland days with SVB: {len(volland_by_date)}")


def get_volland_at(td, ts):
    """Get nearest Volland snapshot SVB/paradigm at given timestamp."""
    vsnaps = volland_by_date.get(td, [])
    if not vsnaps:
        return None, None
    # Find nearest before ts
    best = None
    for v in vsnaps:
        if v['ts'] <= ts:
            best = v
        else:
            break
    if best:
        return best['svb'], best['paradigm']
    return None, None


def sim_sb2(bars_list, vol_mult, delta_mult, recovery_pct, gate_mode='AND',
            time_start=(9,45), time_end=(15,0), min_flush_range=2.0, cooldown=10,
            svb_filter=False):
    signals = []
    last_bull_idx = -100
    last_bear_idx = -100

    for i in range(22, len(bars_list)):
        bar_n = bars_list[i]
        bar_n1 = bars_list[i-1]

        # Time gate
        ts = bar_n.get('ts_end')
        if ts and hasattr(ts, 'hour'):
            t = (ts.hour, ts.minute)
            if t < time_start or t >= time_end:
                continue
        else:
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

        flush_move = bar_n1['bar_close'] - bar_n1['bar_open']
        flush_range = abs(flush_move)
        if flush_range < min_flush_range:
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

        # SVB filter: block if negative (market dislocation)
        if svb_filter:
            td = bar_n['trade_date']
            svb, paradigm = get_volland_at(td, ts)
            if svb is not None and svb < 0:
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
            'recovery': rec_amt / flush_range,
            'flush_range': flush_range,
            'ts': ts,
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
            else:
                fav = entry - bar['bar_low']

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


def run_scenario(name, mode, vm, dm, rec=0.60, time_start=(9,45), time_end=(15,0),
                 min_flush=2.0, cooldown=10, sl=8, target=12, svb_filter=False):
    all_results = []
    for td in bars_by_date:
        bars = bars_by_date[td]
        if len(bars) < 25:
            continue
        sigs = sim_sb2(bars, vm, dm, rec, gate_mode=mode,
                       time_start=time_start, time_end=time_end,
                       min_flush_range=min_flush, cooldown=cooldown,
                       svb_filter=svb_filter)
        if sigs:
            res = forward_sim(sigs, bars, sl=sl, target=target)
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


# ═══════════════════════════════════════════════════════════
# PHASE 1: Time filter impact (9:45-15:00 vs current 9:30-15:00)
# ═══════════════════════════════════════════════════════════
print("\n" + "="*95)
print("PHASE 1: Time filter (all use current AND gate, vol>=1.2x AND dlt>=1.0x)")
print("="*95)
print(f"{'Scenario':<45} {'#Sig':>5} {'S/d':>4} {'WR':>6} {'PnL':>8} {'PnL/d':>7} {'MaxDD':>7} {'PF':>5}")
print('-'*95)

for name, ts, te in [
    ('9:30-15:00 (current)', (9,30), (15,0)),
    ('9:45-15:00', (9,45), (15,0)),
    ('10:00-15:00', (10,0), (15,0)),
    ('10:00-14:30', (10,0), (14,30)),
    ('9:45-14:30', (9,45), (14,30)),
]:
    s = run_scenario(name, 'AND', 1.2, 1.0, time_start=ts, time_end=te)
    spd = s['total'] / s['days'] if s['days'] > 0 else 0
    print(f"{s['name']:<45} {s['total']:>5} {spd:>3.1f} {s['wr']:>5.1f}% {s['pnl']:>+7.1f} {s['pnl_per_day']:>+6.1f} {s['max_dd']:>+6.1f} {s['pf']:>5.2f}")


# ═══════════════════════════════════════════════════════════
# PHASE 2: Gate mode comparison (with 9:45-15:00 time filter)
# ═══════════════════════════════════════════════════════════
print("\n" + "="*95)
print("PHASE 2: Gate mode (all 9:45-15:00 ET)")
print("="*95)
print(f"{'Scenario':<45} {'#Sig':>5} {'S/d':>4} {'WR':>6} {'PnL':>8} {'PnL/d':>7} {'MaxDD':>7} {'PF':>5}")
print('-'*95)

for name, mode, vm, dm in [
    ('AND vol>=1.2x dlt>=1.0x (current)', 'AND', 1.2, 1.0),
    ('OR vol>=1.2x OR dlt>=1.5x', 'OR', 1.2, 1.5),
    ('OR vol>=1.2x OR dlt>=2.0x', 'OR', 1.2, 2.0),
    ('OR vol>=1.5x OR dlt>=1.5x', 'OR', 1.5, 1.5),
    ('OR vol>=1.5x OR dlt>=2.0x', 'OR', 1.5, 2.0),
]:
    s = run_scenario(name, mode, vm, dm, time_start=(9,45), time_end=(15,0))
    spd = s['total'] / s['days'] if s['days'] > 0 else 0
    print(f"{s['name']:<45} {s['total']:>5} {spd:>3.1f} {s['wr']:>5.1f}% {s['pnl']:>+7.1f} {s['pnl_per_day']:>+6.1f} {s['max_dd']:>+6.1f} {s['pf']:>5.2f}")


# ═══════════════════════════════════════════════════════════
# PHASE 3: Additional filters on best gate mode
# ═══════════════════════════════════════════════════════════
print("\n" + "="*95)
print("PHASE 3: Additional filters (OR vol>=1.2x OR dlt>=1.5x, 9:45-15:00)")
print("="*95)
print(f"{'Scenario':<45} {'#Sig':>5} {'S/d':>4} {'WR':>6} {'PnL':>8} {'PnL/d':>7} {'MaxDD':>7} {'PF':>5}")
print('-'*95)

for name, rec, mf, cd in [
    ('rec>=60% flush>=2.0 cd=10 (current)', 0.60, 2.0, 10),
    ('rec>=70% flush>=2.0 cd=10', 0.70, 2.0, 10),
    ('rec>=80% flush>=2.0 cd=10', 0.80, 2.0, 10),
    ('rec>=60% flush>=3.0 cd=10', 0.60, 3.0, 10),
    ('rec>=60% flush>=4.0 cd=10', 0.60, 4.0, 10),
    ('rec>=70% flush>=3.0 cd=10', 0.70, 3.0, 10),
    ('rec>=80% flush>=3.0 cd=10', 0.80, 3.0, 10),
    ('rec>=60% flush>=2.0 cd=15', 0.60, 2.0, 15),
    ('rec>=60% flush>=2.0 cd=20', 0.60, 2.0, 20),
    ('rec>=70% flush>=3.0 cd=15', 0.70, 3.0, 15),
    ('rec>=70% flush>=3.0 cd=20', 0.70, 3.0, 20),
]:
    s = run_scenario(name, 'OR', 1.2, 1.5, rec=rec, min_flush=mf, cooldown=cd,
                     time_start=(9,45), time_end=(15,0))
    spd = s['total'] / s['days'] if s['days'] > 0 else 0
    print(f"{s['name']:<45} {s['total']:>5} {spd:>3.1f} {s['wr']:>5.1f}% {s['pnl']:>+7.1f} {s['pnl_per_day']:>+6.1f} {s['max_dd']:>+6.1f} {s['pf']:>5.2f}")


# ═══════════════════════════════════════════════════════════
# PHASE 4: SL/Target combos on best filter combo
# ═══════════════════════════════════════════════════════════
print("\n" + "="*95)
print("PHASE 4: SL/Target combos (OR 1.2x/1.5x, 9:45-15:00, rec>=70%, flush>=3.0, cd=15)")
print("="*95)
print(f"{'Scenario':<45} {'#Sig':>5} {'S/d':>4} {'WR':>6} {'PnL':>8} {'PnL/d':>7} {'MaxDD':>7} {'PF':>5}")
print('-'*95)

for name, sl, tgt in [
    ('SL=8 T=10', 8, 10),
    ('SL=8 T=12 (current)', 8, 12),
    ('SL=8 T=15', 8, 15),
    ('SL=10 T=10', 10, 10),
    ('SL=10 T=12', 10, 12),
    ('SL=10 T=15', 10, 15),
    ('SL=6 T=10', 6, 10),
    ('SL=6 T=12', 6, 12),
]:
    s = run_scenario(name, 'OR', 1.2, 1.5, rec=0.70, min_flush=3.0, cooldown=15,
                     time_start=(9,45), time_end=(15,0), sl=sl, target=tgt)
    spd = s['total'] / s['days'] if s['days'] > 0 else 0
    print(f"{s['name']:<45} {s['total']:>5} {spd:>3.1f} {s['wr']:>5.1f}% {s['pnl']:>+7.1f} {s['pnl_per_day']:>+6.1f} {s['max_dd']:>+6.1f} {s['pf']:>5.2f}")


# ═══════════════════════════════════════════════════════════
# BEST CONFIG: per-day breakdown
# ═══════════════════════════════════════════════════════════
print("\n" + "="*95)
print("BEST CONFIG — per-day breakdown")
print("="*95)

# Pick the best from phase 3 that has reasonable signal count
best = run_scenario('BEST', 'OR', 1.2, 1.5, rec=0.70, min_flush=3.0, cooldown=15,
                    time_start=(9,45), time_end=(15,0), sl=8, target=12)

by_date = defaultdict(list)
for r in best['results']:
    by_date[str(r['date'])].append(r)

running_pnl = 0
print(f"{'Date':<12} {'#T':>3} {'W':>3} {'L':>3} {'PnL':>7} {'Cumul':>8}")
print('-' * 45)
for d in sorted(by_date.keys()):
    trades = by_date[d]
    pnl = sum(t['pnl'] for t in trades)
    running_pnl += pnl
    w = sum(1 for t in trades if t['result'] == 'WIN')
    l = sum(1 for t in trades if t['result'] == 'LOSS')
    print(f"  {d}  {len(trades):>3}  {w:>3}  {l:>3}  {pnl:>+6.1f}  {running_pnl:>+7.1f}")

print(f"\nTotal: {best['total']} signals, {best['days']} days, {best['total']/best['days']:.1f} sig/day")
print(f"WR: {best['wr']:.1f}%, PnL: {best['pnl']:+.1f}, PnL/day: {best['pnl_per_day']:+.1f}, MaxDD: {best['max_dd']:+.1f}, PF: {best['pf']:.2f}")


# ═══════════════════════════════════════════════════════════
# PHASE 5: With Volland SVB filter (block SVB < 0)
# ═══════════════════════════════════════════════════════════
print("\n" + "="*95)
print("PHASE 5: SVB filter (block when SVB < 0)")
print("="*95)
print(f"{'Scenario':<50} {'#Sig':>5} {'S/d':>4} {'WR':>6} {'PnL':>8} {'PnL/d':>7} {'MaxDD':>7} {'PF':>5}")
print('-'*100)

configs = [
    # (name, mode, vm, dm, rec, mf, cd, svb)
    ('CURRENT no SVB (AND 1.2/1.0, 9:45-15, r60 f2 c10)', 'AND', 1.2, 1.0, 0.60, 2.0, 10, False),
    ('CURRENT + SVB', 'AND', 1.2, 1.0, 0.60, 2.0, 10, True),
    ('OR 1.2/1.5 no SVB (9:45-15, r60 f2 c10)', 'OR', 1.2, 1.5, 0.60, 2.0, 10, False),
    ('OR 1.2/1.5 + SVB', 'OR', 1.2, 1.5, 0.60, 2.0, 10, True),
    ('OR 1.2/1.5 + SVB + r70 f3 c15', 'OR', 1.2, 1.5, 0.70, 3.0, 15, True),
    ('OR 1.2/1.5 + SVB + r70 f3 c20', 'OR', 1.2, 1.5, 0.70, 3.0, 20, True),
    ('OR 1.2/1.5 + SVB + r80 f3 c15', 'OR', 1.2, 1.5, 0.80, 3.0, 15, True),
    ('OR 1.2/1.5 + SVB + r60 f3 c20', 'OR', 1.2, 1.5, 0.60, 3.0, 20, True),
    ('OR 1.2/1.5 + SVB + r60 f4 c15', 'OR', 1.2, 1.5, 0.60, 4.0, 15, True),
    ('OR 1.5/2.0 + SVB + r70 f3 c15', 'OR', 1.5, 2.0, 0.70, 3.0, 15, True),
]

for name, mode, vm, dm, rec, mf, cd, svb in configs:
    s = run_scenario(name, mode, vm, dm, rec=rec, min_flush=mf, cooldown=cd,
                     time_start=(9,45), time_end=(15,0), svb_filter=svb)
    spd = s['total'] / s['days'] if s['days'] > 0 else 0
    print(f"{s['name']:<50} {s['total']:>5} {spd:>3.1f} {s['wr']:>5.1f}% {s['pnl']:>+7.1f} {s['pnl_per_day']:>+6.1f} {s['max_dd']:>+6.1f} {s['pf']:>5.2f}")

# Show per-day for best SVB config
print("\n--- Best SVB config per-day ---")
svb_best = run_scenario('SVB_BEST', 'OR', 1.2, 1.5, rec=0.70, min_flush=3.0, cooldown=15,
                        time_start=(9,45), time_end=(15,0), svb_filter=True)
by_date2 = defaultdict(list)
for r in svb_best['results']:
    by_date2[str(r['date'])].append(r)
running_pnl = 0
print(f"{'Date':<12} {'#T':>3} {'W':>3} {'L':>3} {'PnL':>7} {'Cumul':>8}")
print('-' * 45)
for d in sorted(by_date2.keys()):
    trades = by_date2[d]
    pnl = sum(t['pnl'] for t in trades)
    running_pnl += pnl
    w = sum(1 for t in trades if t['result'] == 'WIN')
    l = sum(1 for t in trades if t['result'] == 'LOSS')
    print(f"  {d}  {len(trades):>3}  {w:>3}  {l:>3}  {pnl:>+6.1f}  {running_pnl:>+7.1f}")
print(f"\nTotal: {svb_best['total']} sig, {svb_best['days']}d, {svb_best['total']/svb_best['days']:.1f}/d, "
      f"WR={svb_best['wr']:.1f}%, PnL={svb_best['pnl']:+.1f}, MaxDD={svb_best['max_dd']:+.1f}, PF={svb_best['pf']:.2f}")
