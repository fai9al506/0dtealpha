import csv
from datetime import datetime, timedelta

# Load 5-min OHLC bars (UTC -> ET)
bars = []
with open('exports/spx_5min_ohlc.csv', 'r') as f:
    for r in csv.DictReader(f):
        ts_utc = datetime.utcfromtimestamp(int(r['time']))
        ts_et = ts_utc - timedelta(hours=4)
        bars.append({'ts': ts_et, 'high': float(r['high']), 'low': float(r['low']), 'close': float(r['close'])})
bars.sort(key=lambda x: x['ts'])
print(f"Loaded {len(bars)} 5-min bars")

# Load ALL trades, filter to SC / March / V11 / A+/A/B
with open('exports/setup_log_full.csv', 'r', encoding='utf-8') as f:
    all_trades = list(csv.DictReader(f))

# Filter: SC, March, outcome exists
sc_trades = [t for t in all_trades
             if t['setup_name'] == 'Skew Charm'
             and t['trade_date'] >= '2026-03-01'
             and t['outcome_result'] in ('WIN', 'LOSS', 'EXPIRED')]

print(f"SC March trades with outcomes: {len(sc_trades)}")

# Now apply V11 filter + grade gate (A+/A/B)
# V11 for SC:
#   Longs: alignment >= +2 (VIX exempt)
#   Shorts: whitelisted, but blocked on GEX-LIS, blocked 14:30-15:00, blocked 15:30+
#   Grade gate: only A+/A/B
from datetime import time as dtime


def passes_v11_sc(t):
    grade = t['grade']
    if grade not in ('A+', 'A', 'B'):
        return False
    direction = t['direction']
    is_long = direction in ('long', 'bullish')
    align = int(t['greek_alignment']) if t['greek_alignment'] else 0

    # Time gates
    ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
    tt = ts.time()
    if dtime(14, 30) <= tt < dtime(15, 0):
        return False
    if tt >= dtime(15, 30):
        return False

    if is_long:
        if align < 2:
            return False
        return True  # SC longs VIX exempt
    else:
        paradigm = t.get('paradigm', '')
        if paradigm == 'GEX-LIS':
            return False
        return True


v11_trades = [t for t in sc_trades if passes_v11_sc(t)]
print(f"SC March V11 A+/A/B: {len(v11_trades)}")

# SC trail params
SC_SL = 14
SC_BE_TRIGGER = 10
SC_TRAIL_ACTIVATION = 10
SC_TRAIL_GAP = 8


def market_close(dt):
    return dt.replace(hour=16, minute=0, second=0, microsecond=0)


def simulate(entry, ts_start, is_long, mode):
    """
    mode: 'option1' = fixed +10 target only (1 contract)
          'option2' = split: T1=+10, T2=trail, pnl = avg
          'option3' = trail only (no fixed target)
    """
    stop = entry - SC_SL if is_long else entry + SC_SL
    trail_stop = stop
    close_time = market_close(ts_start)
    max_fav = 0.0
    t1_hit = False

    sim_bars = [b for b in bars if b['ts'] >= ts_start and b['ts'] <= close_time]

    for b in sim_bars:
        if is_long:
            fav = b['high'] - entry
            adverse = b['low']
        else:
            fav = entry - b['low']
            adverse = b['high']

        if fav > max_fav:
            max_fav = fav

        if max_fav >= 10 and not t1_hit:
            t1_hit = True

        # Trail logic (hybrid: BE at 10, continuous at 10, gap 8)
        new_stop = None
        if max_fav >= SC_TRAIL_ACTIVATION:
            if is_long:
                new_stop = entry + (max_fav - SC_TRAIL_GAP)
            else:
                new_stop = entry - (max_fav - SC_TRAIL_GAP)
        elif max_fav >= SC_BE_TRIGGER:
            new_stop = entry  # breakeven

        if new_stop is not None:
            if is_long and new_stop > trail_stop:
                trail_stop = new_stop
            elif not is_long and new_stop < trail_stop:
                trail_stop = new_stop

        # Option 1: fixed target at +10
        if mode == 'option1':
            if is_long and b['high'] >= entry + 10:
                return 'WIN', 10.0
            if not is_long and b['low'] <= entry - 10:
                return 'WIN', 10.0
            # Check stop
            if is_long and b['low'] <= trail_stop:
                pnl = trail_stop - entry
                return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1)
            if not is_long and b['high'] >= trail_stop:
                pnl = entry - trail_stop
                return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1)

        # Option 2: split target (T1=+10 + T2=trail, avg)
        elif mode == 'option2':
            # Check stop hit first (applies to all remaining contracts)
            if is_long and b['low'] <= trail_stop:
                t2_pnl = trail_stop - entry
                if t1_hit:
                    pnl = round((10.0 + t2_pnl) / 2, 1)
                else:
                    pnl = round(t2_pnl, 1)  # both hit stop
                return ('WIN' if pnl > 0 else 'LOSS'), pnl
            if not is_long and b['high'] >= trail_stop:
                t2_pnl = entry - trail_stop
                if t1_hit:
                    pnl = round((10.0 + t2_pnl) / 2, 1)
                else:
                    pnl = round(t2_pnl, 1)
                return ('WIN' if pnl > 0 else 'LOSS'), pnl

        # Option 3: trail only (no fixed target)
        elif mode == 'option3':
            if is_long and b['low'] <= trail_stop:
                pnl = trail_stop - entry
                return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1)
            if not is_long and b['high'] >= trail_stop:
                pnl = entry - trail_stop
                return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1)

    # EXPIRED at market close
    last_close = sim_bars[-1]['close'] if sim_bars else entry
    if is_long:
        raw_pnl = last_close - entry
    else:
        raw_pnl = entry - last_close

    if mode == 'option2' and t1_hit:
        pnl = round((10.0 + raw_pnl) / 2, 1)
    else:
        pnl = round(raw_pnl, 1)

    outcome = 'WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED')
    return outcome, pnl


# Run all 3 options
for mode, label in [('option1', 'Option 1: Fixed +10 target (1 contract)'),
                     ('option2', 'Option 2: Split T1=+10 / T2=trail (avg)'),
                     ('option3', 'Option 3: Trail only (no fixed target)')]:
    wins, losses, expired = 0, 0, 0
    total_pnl = 0.0
    details = []
    equity = 0.0
    peak = 0.0
    max_dd = 0.0

    for t in v11_trades:
        ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
        spot = float(t['spot'])
        is_long = t['direction'] in ('long', 'bullish')

        outcome, pnl = simulate(spot, ts, is_long, mode)

        if outcome == 'WIN':
            wins += 1
        elif outcome == 'LOSS':
            losses += 1
        else:
            expired += 1
        total_pnl += pnl

        equity += pnl
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

        details.append({
            'id': t['id'], 'date': t['trade_date'], 'dir': t['direction'],
            'grade': t['grade'], 'spot': spot,
            'outcome': outcome, 'pnl': pnl,
            'db_outcome': t['outcome_result'], 'db_pnl': float(t['outcome_pnl']) if t['outcome_pnl'] else 0,
        })

    total = wins + losses + expired
    wr = wins / total * 100 if total > 0 else 0
    ratio = total_pnl / max_dd if max_dd > 0 else float('inf')
    avg = total_pnl / total if total > 0 else 0

    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    print(f"  Trades: {total}  |  W: {wins}  L: {losses}  E: {expired}")
    print(f"  WR: {wr:.0f}%  |  PnL: {total_pnl:+.1f} pts  |  MaxDD: {max_dd:.1f}  |  PnL/DD: {ratio:.2f}")
    print(f"  Avg/trade: {avg:+.1f} pts")

    # Show per-trade comparison with DB
    diff_count = 0
    for d in details:
        if d['outcome'] != d['db_outcome'] or abs(d['pnl'] - d['db_pnl']) > 0.5:
            diff_count += 1
    print(f"  Differs from DB: {diff_count}/{total} trades")

# Also show what the DB currently has (for comparison)
print(f"\n{'='*70}")
print(f"  DB Current (portal): SC March V11 A+/A/B")
print(f"{'='*70}")
db_wins = sum(1 for t in v11_trades if t['outcome_result'] == 'WIN')
db_losses = sum(1 for t in v11_trades if t['outcome_result'] == 'LOSS')
db_expired = sum(1 for t in v11_trades if t['outcome_result'] == 'EXPIRED')
db_pnl = sum(float(t['outcome_pnl']) for t in v11_trades if t['outcome_pnl'])
total = len(v11_trades)
print(f"  Trades: {total}  |  W: {db_wins}  L: {db_losses}  E: {db_expired}")
print(f"  WR: {db_wins/total*100:.0f}%  |  PnL: {db_pnl:+.1f} pts")

# Per-date comparison for all 3 options
print(f"\n{'='*70}")
print(f"  PER-DATE COMPARISON")
print(f"{'='*70}")
print(f"{'Date':<12} {'Trades':>6} | {'Opt1 PnL':>9} {'Opt2 PnL':>9} {'Opt3 PnL':>9} {'DB PnL':>9}")
print("-" * 65)

dates = sorted(set(t['trade_date'] for t in v11_trades))
for d in dates:
    dt = [t for t in v11_trades if t['trade_date'] == d]
    n = len(dt)
    db_d = sum(float(t['outcome_pnl']) for t in dt if t['outcome_pnl'])

    # Re-simulate per date
    pnls = {m: 0.0 for m in ['option1', 'option2', 'option3']}
    for t in dt:
        ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
        spot = float(t['spot'])
        is_long = t['direction'] in ('long', 'bullish')
        for m in pnls:
            _, pnl = simulate(spot, ts, is_long, m)
            pnls[m] += pnl

    print(f"{d:<12} {n:>6} | {pnls['option1']:>+9.1f} {pnls['option2']:>+9.1f} {pnls['option3']:>+9.1f} {db_d:>+9.1f}")
