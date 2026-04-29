"""SB2 gate backtest v3: with proper Volland filters (SVB, paradigm, charm)"""
import os, json
from sqlalchemy import create_engine, text
from collections import defaultdict

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway")
engine = create_engine(DATABASE_URL)

# Load ES range bars
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

# Load Volland data: SVB correlation, paradigm, charm — as time series per date
volland_by_date = {}
with engine.begin() as conn:
    vrows = conn.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as ts_et,
               date(ts AT TIME ZONE 'America/New_York') as td,
               payload->'statistics' as stats
        FROM volland_snapshots
        WHERE payload->>'error_event' IS NULL
          AND payload->'statistics' IS NOT NULL
        ORDER BY ts
    """)).fetchall()
    for r in vrows:
        td = r[1]
        stats = json.loads(r[2]) if isinstance(r[2], str) else r[2]
        if not stats:
            continue
        # Parse SVB correlation
        svb_obj = stats.get('spot_vol_beta')
        svb_corr = None
        if isinstance(svb_obj, dict):
            svb_corr = svb_obj.get('correlation')
        elif isinstance(svb_obj, (int, float)):
            svb_corr = svb_obj

        # Parse charm
        charm = stats.get('aggregatedCharm')
        if isinstance(charm, str):
            charm = float(charm.replace(',', '').replace('$', ''))

        paradigm = stats.get('paradigm', '')

        if td not in volland_by_date:
            volland_by_date[td] = []
        volland_by_date[td].append({
            'ts': r[0], 'svb': svb_corr, 'paradigm': paradigm, 'charm': charm
        })

print(f"Volland days: {len(volland_by_date)}")
# Verify SVB data
svb_count = sum(1 for td in volland_by_date for v in volland_by_date[td] if v['svb'] is not None)
print(f"Volland snapshots with SVB: {svb_count}")


def get_volland_at(td, ts):
    """Get nearest Volland snapshot before given timestamp."""
    vsnaps = volland_by_date.get(td, [])
    if not vsnaps:
        return None, None, None
    best = None
    for v in vsnaps:
        vts = v['ts']
        # Make both naive for comparison
        if hasattr(vts, 'tzinfo') and vts.tzinfo is not None:
            vts = vts.replace(tzinfo=None)
        ts_cmp = ts
        if hasattr(ts_cmp, 'tzinfo') and ts_cmp.tzinfo is not None:
            ts_cmp = ts_cmp.replace(tzinfo=None)
        if vts <= ts_cmp:
            best = v
        else:
            break
    if best:
        return best['svb'], best['paradigm'], best['charm']
    return None, None, None


def sim_sb2(bars_list, vol_mult, delta_mult, recovery_pct, gate_mode='OR',
            time_start=(9,45), time_end=(15,0), min_flush_range=2.0, cooldown=10,
            use_svb=False, use_paradigm=False, use_charm=False):
    signals = []
    last_bull_idx = -100
    last_bear_idx = -100

    for i in range(22, len(bars_list)):
        bar_n = bars_list[i]
        bar_n1 = bars_list[i-1]

        ts = bar_n.get('ts_end')
        if ts and hasattr(ts, 'hour'):
            t = (ts.hour, ts.minute)
            if t < time_start or t >= time_end:
                continue
        else:
            continue

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

        # Volland filters
        td = bar_n['trade_date']
        svb, paradigm, charm = get_volland_at(td, ts)

        if use_svb and svb is not None and svb < 0:
            continue

        if use_paradigm and paradigm:
            # Block if paradigm contradicts direction
            p = paradigm.upper()
            if direction == 'bullish' and 'AG' in p and 'GEX' not in p:
                continue  # AG paradigm = bearish, block longs
            if direction == 'bearish' and 'GEX' in p and 'AG' not in p:
                continue  # GEX paradigm = bullish, block shorts

        if use_charm and charm is not None:
            # Block if charm contradicts direction
            if direction == 'bullish' and charm < -100_000_000:
                continue  # strong negative charm = bearish pressure
            if direction == 'bearish' and charm > 100_000_000:
                continue  # strong positive charm = bullish support

        if direction == 'bullish':
            last_bull_idx = idx
        else:
            last_bear_idx = idx

        signals.append({
            'date': td, 'idx': idx, 'direction': direction,
            'es_entry': bar_n['bar_close'],
            'vol_ratio': flush_vol / avg_vol if avg_vol > 0 else 0,
            'delta_ratio': abs(flush_delta) / avg_delta if avg_delta > 0 else 0,
            'svb': svb, 'paradigm': paradigm, 'charm': charm,
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
            fav = (bar['bar_high'] - entry) if is_long else (entry - bar['bar_low'])
            if fav > trail_peak:
                trail_peak = fav

            if trail_peak >= 20:
                lock = max(trail_peak - 10, 0)
                ns = (entry + lock) if is_long else (entry - lock)
                if is_long and ns > trail_stop:
                    trail_stop = ns
                elif not is_long and ns < trail_stop:
                    trail_stop = ns
            elif trail_peak >= 10:
                if is_long and entry > trail_stop:
                    trail_stop = entry
                elif not is_long and entry < trail_stop:
                    trail_stop = entry

            if is_long and bar['bar_low'] <= trail_stop:
                pnl = trail_stop - entry
                break
            elif not is_long and bar['bar_high'] >= trail_stop:
                pnl = entry - trail_stop
                break
            if is_long and bar['bar_high'] >= target_lvl:
                pnl = target
                break
            elif not is_long and bar['bar_low'] <= target_lvl:
                pnl = target
                break

        if pnl is None:
            pnl = 0
        results.append({**sig, 'pnl': round(pnl, 1),
                        'result': 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED'),
                        'max_fav': round(trail_peak, 1)})
    return results


def run(name, mode='OR', vm=1.2, dm=1.5, rec=0.60, mf=2.0, cd=10,
        ts=(9,45), te=(15,0), sl=8, tgt=12, svb=False, para=False, charm=False):
    all_results = []
    for td in bars_by_date:
        bars = bars_by_date[td]
        if len(bars) < 25:
            continue
        sigs = sim_sb2(bars, vm, dm, rec, gate_mode=mode, time_start=ts, time_end=te,
                       min_flush_range=mf, cooldown=cd, use_svb=svb, use_paradigm=para, use_charm=charm)
        if sigs:
            all_results.extend(forward_sim(sigs, bars, sl=sl, target=tgt))

    total = len(all_results)
    if total == 0:
        return {'name': name, 'total': 0, 'wr': 0, 'pnl': 0, 'pnl_per_day': 0,
                'max_dd': 0, 'pf': 0, 'results': [], 'days': 0}
    wins = sum(1 for r in all_results if r['result'] == 'WIN')
    wr = wins / total * 100
    pnl = sum(r['pnl'] for r in all_results)
    peak = running = max_dd = 0
    for r in sorted(all_results, key=lambda x: (str(x['date']), x['idx'])):
        running += r['pnl']
        if running > peak: peak = running
        if running - peak < max_dd: max_dd = running - peak
    gw = sum(r['pnl'] for r in all_results if r['pnl'] > 0)
    gl = abs(sum(r['pnl'] for r in all_results if r['pnl'] < 0))
    pf = gw / gl if gl > 0 else 999
    days = len(set(str(r['date']) for r in all_results)) or 1
    return {'name': name, 'total': total, 'wins': wins, 'wr': wr, 'pnl': pnl,
            'pnl_per_day': pnl/days, 'max_dd': max_dd, 'pf': pf, 'results': all_results, 'days': days}


def pr(s):
    spd = s['total'] / s['days'] if s['days'] > 0 else 0
    print(f"{s['name']:<55} {s['total']:>5} {spd:>4.1f} {s['wr']:>5.1f}% {s['pnl']:>+7.1f} {s['pnl_per_day']:>+6.1f} {s['max_dd']:>+6.1f} {s['pf']:>5.2f}")


hdr = f"{'Scenario':<55} {'#Sig':>5} {'S/d':>4} {'WR':>6} {'PnL':>8} {'PnL/d':>7} {'MaxDD':>7} {'PF':>5}"
sep = '-' * 105

# ═══════════════════════════════════════════════════════════
print("\n" + "="*105)
print("PHASE 1: Raw vs Volland filters (OR 1.2/1.5, 9:45-15:00, rec=60%, flush=2, cd=10)")
print("="*105)
print(hdr); print(sep)

pr(run('RAW (no Volland)', cd=10))
pr(run('+ SVB only (block SVB<0)', cd=10, svb=True))
pr(run('+ Paradigm only (block contra-paradigm)', cd=10, para=True))
pr(run('+ Charm only (block contra-charm >100M)', cd=10, charm=True))
pr(run('+ SVB + Paradigm', cd=10, svb=True, para=True))
pr(run('+ SVB + Charm', cd=10, svb=True, charm=True))
pr(run('+ ALL (SVB + Paradigm + Charm)', cd=10, svb=True, para=True, charm=True))

# ═══════════════════════════════════════════════════════════
print("\n" + "="*105)
print("PHASE 2: Best Volland combo + tuning (cd=20)")
print("="*105)
print(hdr); print(sep)

pr(run('RAW cd=20', cd=20))
pr(run('SVB cd=20', cd=20, svb=True))
pr(run('SVB+Para cd=20', cd=20, svb=True, para=True))
pr(run('ALL cd=20', cd=20, svb=True, para=True, charm=True))
pr(run('ALL cd=20 rec=70%', cd=20, rec=0.70, svb=True, para=True, charm=True))
pr(run('ALL cd=20 flush=3', cd=20, mf=3.0, svb=True, para=True, charm=True))
pr(run('ALL cd=20 rec=70% flush=3', cd=20, rec=0.70, mf=3.0, svb=True, para=True, charm=True))
pr(run('SVB+Para cd=20 rec=70%', cd=20, rec=0.70, svb=True, para=True))
pr(run('SVB+Para cd=20 flush=3', cd=20, mf=3.0, svb=True, para=True))

# ═══════════════════════════════════════════════════════════
print("\n" + "="*105)
print("PHASE 3: Stricter cooldowns (cd=30, cd=40) to get to 3-5 sig/day")
print("="*105)
print(hdr); print(sep)

for cd_val in [20, 25, 30, 40, 50]:
    pr(run(f'SVB+Para cd={cd_val}', cd=cd_val, svb=True, para=True))
    pr(run(f'ALL cd={cd_val}', cd=cd_val, svb=True, para=True, charm=True))

# ═══════════════════════════════════════════════════════════
print("\n" + "="*105)
print("PHASE 4: Higher gate thresholds (reduce noise)")
print("="*105)
print(hdr); print(sep)

for vm, dm in [(1.5, 1.5), (1.5, 2.0), (2.0, 2.0), (2.0, 1.5)]:
    pr(run(f'OR {vm}/{dm} SVB+Para cd=20', vm=vm, dm=dm, cd=20, svb=True, para=True))

# ═══════════════════════════════════════════════════════════
# BEST CONFIG: per-day
print("\n" + "="*105)
print("BEST CONFIGS — per-day breakdown")
print("="*105)

for label, kwargs in [
    ('SVB+Para cd=30', dict(cd=30, svb=True, para=True)),
    ('ALL cd=30', dict(cd=30, svb=True, para=True, charm=True)),
    ('OR 1.5/2.0 SVB+Para cd=20', dict(vm=1.5, dm=2.0, cd=20, svb=True, para=True)),
]:
    s = run(label, **kwargs)
    by_date = defaultdict(list)
    for r in s['results']:
        by_date[str(r['date'])].append(r)

    spd = s['total'] / s['days'] if s['days'] else 0
    print(f"\n--- {label}: {s['total']} sig, {spd:.1f}/d, WR={s['wr']:.1f}%, PnL={s['pnl']:+.1f}, MaxDD={s['max_dd']:+.1f}, PF={s['pf']:.2f} ---")
    running = 0
    for d in sorted(by_date.keys()):
        trades = by_date[d]
        p = sum(t['pnl'] for t in trades)
        running += p
        w = sum(1 for t in trades if t['result'] == 'WIN')
        l = sum(1 for t in trades if t['result'] == 'LOSS')
        print(f"  {d}  {len(trades):>2}t  {w}W/{l}L  {p:>+6.1f}  cum={running:>+7.1f}")
