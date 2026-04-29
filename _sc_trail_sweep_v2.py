"""
SC Trail Optimization v2 - MFE/MAE-based + Fixed DST OHLC simulation
Two approaches:
  1. OHLC sim with proper DST handling (Feb 17 - Mar 23)
  2. DB MFE-based analysis (all 151 trades, captures actual system behavior)
"""
import csv, sys
from datetime import datetime, timedelta, time as dtime, date as dateclass
from collections import defaultdict
from zoneinfo import ZoneInfo

sys.stdout.reconfigure(encoding='utf-8')

# =========================================================
# APPROACH 1: Fixed OHLC simulation with DST
# =========================================================
ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

bars = []
with open('exports/spx_5min_ohlc.csv', 'r') as f:
    for r in csv.DictReader(f):
        ts_utc = datetime.fromtimestamp(int(r['time']), tz=UTC)
        ts_et = ts_utc.astimezone(ET).replace(tzinfo=None)  # naive ET
        bars.append({
            'ts': ts_et,
            'h': float(r['high']),
            'l': float(r['low']),
            'c': float(r['close']),
        })
bars.sort(key=lambda x: x['ts'])
print(f"OHLC bars: {len(bars)} ({bars[0]['ts'].date()} to {bars[-1]['ts'].date()})")

# Load SC trades
with open('exports/sc_trades_full.csv', 'r', encoding='utf-8') as f:
    all_trades = list(csv.DictReader(f))

def passes_v12(t):
    grade = t['grade'].strip()
    if grade not in ('A+', 'A', 'B'): return False
    direction = t['direction'].strip()
    is_long = direction == 'long'
    try: ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
    except: return False
    tt = ts.time()
    if dtime(14,30) <= tt < dtime(15,0): return False
    if tt >= dtime(15,30): return False
    if is_long:
        try: align = int(float(t['greek_alignment'])) if t.get('greek_alignment') else 0
        except: align = 0
        if align < 2: return False
    else:
        if t.get('paradigm','').strip() == 'GEX-LIS': return False
    return True

v12 = [t for t in all_trades if passes_v12(t)]
print(f"V12 SC A+/A/B: {len(v12)} trades")

# Split by OHLC coverage
bar_start = bars[0]['ts'].date()
bar_end = bars[-1]['ts'].date()
v12_ohlc = [t for t in v12
            if bar_start <= datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S').date() <= bar_end]
v12_no_ohlc = [t for t in v12
               if datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S').date() > bar_end]
print(f"  With OHLC: {len(v12_ohlc)}, Without: {len(v12_no_ohlc)}")


def simulate_trade(entry, ts_start, is_long, sl, act, gap):
    be = min(act, 10)
    stop = entry - sl if is_long else entry + sl
    trail_stop = stop
    close_time = ts_start.replace(hour=16, minute=0, second=0)
    max_fav = 0.0

    sim_bars = [b for b in bars if b['ts'] >= ts_start and b['ts'] <= close_time]
    if not sim_bars:
        return 'NO_BARS', 0, 0

    for b in sim_bars:
        fav = (b['h'] - entry) if is_long else (entry - b['l'])
        if fav > max_fav:
            max_fav = fav

        new_stop = None
        if max_fav >= act:
            new_stop = (entry + (max_fav - gap)) if is_long else (entry - (max_fav - gap))
        elif max_fav >= be:
            new_stop = entry
        if new_stop:
            if is_long and new_stop > trail_stop: trail_stop = new_stop
            elif not is_long and new_stop < trail_stop: trail_stop = new_stop

        if is_long and b['l'] <= trail_stop:
            pnl = trail_stop - entry
            return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1), max_fav
        if not is_long and b['h'] >= trail_stop:
            pnl = entry - trail_stop
            return ('WIN' if pnl > 0 else 'LOSS'), round(pnl, 1), max_fav

    last = sim_bars[-1]['c']
    pnl = round((last - entry) if is_long else (entry - last), 1)
    return ('WIN' if pnl > 0 else ('LOSS' if pnl < 0 else 'EXPIRED')), pnl, max_fav


# Validate DST fix
print("\nVALIDATING DST FIX (current params SL=14/ACT=10/GAP=8):")
sim_total = 0
match_count = 0
for t in v12_ohlc:
    ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
    spot = float(t['spot'])
    is_long = t['direction'].strip() == 'long'
    db_out = t['outcome_result']

    sim_out, sim_pnl, sim_mfe = simulate_trade(spot, ts, is_long, 14, 10, 8)
    if sim_out == 'NO_BARS':
        continue
    sim_total += 1
    if sim_out == db_out:
        match_count += 1

print(f"  Outcome match: {match_count}/{sim_total} ({match_count/sim_total*100:.1f}%)")

# Run OHLC sweep
def parse_ohlc_trades(trade_list):
    parsed = []
    for t in trade_list:
        ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
        if ts.date() < bar_start or ts.date() > bar_end:
            continue
        spot = float(t['spot'])
        is_long = t['direction'].strip() == 'long'
        parsed.append({'id': t['id'], 'ts': ts, 'spot': spot, 'is_long': is_long,
                        'grade': t['grade'].strip(), 'date': ts.date()})
    return parsed

parsed_ohlc = parse_ohlc_trades(v12)

SL_RANGE = [10, 12, 14, 16, 18, 20]
ACT_RANGE = [8, 10, 12, 14, 16]
GAP_RANGE = [3, 4, 5, 6, 7, 8, 10]


def run_ohlc_sweep(parsed, label):
    print(f"\n{'='*105}")
    print(f"  OHLC SIMULATION SWEEP: {label} ({len(parsed)} trades)")
    print(f"{'='*105}")

    results = []
    for sl in SL_RANGE:
        for act in ACT_RANGE:
            for gap in GAP_RANGE:
                if gap >= act: continue

                wins, losses, expired = 0, 0, 0
                total_pnl = 0.0
                equity, peak, max_dd = 0.0, 0.0, 0.0
                win_pnls, loss_pnls = [], []
                daily_pnl = defaultdict(float)
                streak, max_losing = 0, 0

                for tr in parsed:
                    outcome, pnl, mfe = simulate_trade(tr['spot'], tr['ts'], tr['is_long'], sl, act, gap)
                    if outcome == 'NO_BARS': continue

                    if outcome == 'WIN':
                        wins += 1; win_pnls.append(pnl); streak = 0
                    elif outcome == 'LOSS':
                        losses += 1; loss_pnls.append(pnl); streak += 1; max_losing = max(max_losing, streak)
                    else:
                        expired += 1
                        if pnl < 0: streak += 1; max_losing = max(max_losing, streak)
                        else: streak = 0

                    total_pnl += pnl
                    daily_pnl[tr['date']] += pnl
                    equity += pnl
                    if equity > peak: peak = equity
                    dd = peak - equity
                    if dd > max_dd: max_dd = dd

                total = wins + losses + expired
                if total == 0: continue
                wr = wins / total * 100
                avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0
                avg_loss = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0
                pf = abs(sum(win_pnls) / sum(loss_pnls)) if loss_pnls and sum(loss_pnls) != 0 else 999
                ratio = total_pnl / max_dd if max_dd > 0 else 999
                losing_days = sum(1 for p in daily_pnl.values() if p < 0)
                total_days = len(daily_pnl)
                green_pct = (total_days - losing_days) / total_days * 100 if total_days else 0

                results.append({
                    'sl': sl, 'act': act, 'gap': gap,
                    'trades': total, 'wins': wins, 'losses': losses, 'expired': expired,
                    'wr': wr, 'pnl': total_pnl, 'max_dd': max_dd, 'ratio': ratio,
                    'avg_win': avg_win, 'avg_loss': avg_loss, 'pf': pf,
                    'max_losing': max_losing, 'green_pct': green_pct,
                })

    # Current baseline
    current = [r for r in results if r['sl'] == 14 and r['act'] == 10 and r['gap'] == 8]
    c_pnl = current[0]['pnl'] if current else 0

    def pr(header, data, limit=15):
        c = current[0] if current else None
        print(f"\n  {header}")
        print(f"  {'SL':>3} {'ACT':>4} {'GAP':>4} | {'PnL':>8} {'WR':>6} {'MaxDD':>7} {'PnL/DD':>7} {'PF':>6} | {'AvgW':>6} {'AvgL':>6} | {'MLS':>3} {'Grn%':>5} | vs Current")
        for r in data[:limit]:
            delta = r['pnl'] - c_pnl
            tag = " <--" if r['sl'] == 14 and r['act'] == 10 and r['gap'] == 8 else ""
            print(f"  {r['sl']:>3} {r['act']:>4} {r['gap']:>4} | {r['pnl']:>+8.1f} {r['wr']:>5.1f}% {r['max_dd']:>7.1f} {r['ratio']:>7.2f} {r['pf']:>6.2f} | {r['avg_win']:>+6.1f} {r['avg_loss']:>+6.1f} | {r['max_losing']:>3} {r['green_pct']:>4.0f}% | {delta:>+7.1f}{tag}")

    if current:
        c = current[0]
        print(f"\n  BASELINE (SL=14, ACT=10, GAP=8):")
        print(f"  PnL: {c['pnl']:+.1f}  WR: {c['wr']:.1f}%  MaxDD: {c['max_dd']:.1f}  PnL/DD: {c['ratio']:.2f}  PF: {c['pf']:.2f}")

    pr("TOP 15 BY PnL:", sorted(results, key=lambda x: -x['pnl']))
    pr("TOP 15 BY PnL/DD:", sorted(results, key=lambda x: -x['ratio']))

    # GAP sensitivity
    print(f"\n  GAP SENSITIVITY (SL=14, ACT=10 fixed):")
    for r in sorted([r for r in results if r['sl'] == 14 and r['act'] == 10], key=lambda x: x['gap']):
        delta = r['pnl'] - c_pnl
        tag = " <--" if r['gap'] == 8 else ""
        print(f"    GAP={r['gap']:>2}: {r['pnl']:>+8.1f}  WR {r['wr']:>5.1f}%  MaxDD {r['max_dd']:>6.1f}  PnL/DD {r['ratio']:>6.2f}  AvgW {r['avg_win']:>+5.1f}  delta {delta:>+7.1f}{tag}")

    # ACT sensitivity
    print(f"\n  ACT SENSITIVITY (SL=14, GAP=5 fixed):")
    for r in sorted([r for r in results if r['sl'] == 14 and r['gap'] == 5], key=lambda x: x['act']):
        print(f"    ACT={r['act']:>2}: {r['pnl']:>+8.1f}  WR {r['wr']:>5.1f}%  MaxDD {r['max_dd']:>6.1f}  {r['wins']}W/{r['losses']}L/{r['expired']}E")

    # SL sensitivity
    print(f"\n  SL SENSITIVITY (ACT=10, GAP=5 fixed):")
    for r in sorted([r for r in results if r['act'] == 10 and r['gap'] == 5], key=lambda x: x['sl']):
        print(f"    SL={r['sl']:>2}: {r['pnl']:>+8.1f}  WR {r['wr']:>5.1f}%  MaxDD {r['max_dd']:>6.1f}  {r['wins']}W/{r['losses']}L/{r['expired']}E")

    return results

ohlc_results = run_ohlc_sweep(parsed_ohlc, "V12 SC (DST-fixed OHLC)")


# =========================================================
# APPROACH 2: DB MFE-based analysis (all trades, actual system behavior)
# =========================================================
print(f"\n\n{'='*105}")
print(f"  APPROACH 2: DB MFE-BASED ANALYSIS (all {len(v12)} V12 trades)")
print(f"  Uses actual DB outcomes. Changes GAP to estimate PnL improvement.")
print(f"  Logic: winners with MFE >= activation have PnL ~ MFE - gap.")
print(f"  Changing gap adjusts each winner's capture proportionally.")
print(f"{'='*105}")

# Parse all V12 trades with DB data
db_trades = []
for t in v12:
    ts = datetime.strptime(t['ts_et'][:19], '%Y-%m-%d %H:%M:%S')
    db_trades.append({
        'id': t['id'], 'ts': ts, 'date': ts.date(),
        'spot': float(t['spot']),
        'is_long': t['direction'].strip() == 'long',
        'grade': t['grade'].strip(),
        'outcome': t['outcome_result'],
        'pnl': float(t['outcome_pnl']) if t['outcome_pnl'] else 0,
        'mfe': float(t['outcome_max_profit']) if t['outcome_max_profit'] else 0,
        'mae': float(t['outcome_max_loss']) if t['outcome_max_loss'] else 0,
    })

# Baseline DB stats
db_wins = [t for t in db_trades if t['outcome'] == 'WIN']
db_losses = [t for t in db_trades if t['outcome'] == 'LOSS']
db_expired = [t for t in db_trades if t['outcome'] == 'EXPIRED']
db_total_pnl = sum(t['pnl'] for t in db_trades)

print(f"\n  DB BASELINE (SL=14, ACT=10, GAP=8):")
print(f"  {len(db_trades)} trades: {len(db_wins)}W/{len(db_losses)}L/{len(db_expired)}E")
print(f"  Total PnL: {db_total_pnl:+.1f}")
if db_wins:
    avg_win = sum(t['pnl'] for t in db_wins) / len(db_wins)
    avg_mfe = sum(t['mfe'] for t in db_wins) / len(db_wins)
    print(f"  Avg Win: {avg_win:+.1f}  Avg MFE(winners): {avg_mfe:.1f}  Capture: {avg_win/avg_mfe*100:.0f}%")
    print(f"  Avg Loss: {sum(t['pnl'] for t in db_losses)/len(db_losses):+.1f}")

# MFE distribution for winners
print(f"\n  MFE DISTRIBUTION (winners only):")
buckets = [(10, 15), (15, 20), (20, 30), (30, 50), (50, 200)]
for lo, hi in buckets:
    b = [t for t in db_wins if lo <= t['mfe'] < hi]
    if b:
        avg_p = sum(t['pnl'] for t in b) / len(b)
        avg_m = sum(t['mfe'] for t in b) / len(b)
        total_p = sum(t['pnl'] for t in b)
        print(f"    MFE {lo:>2}-{hi:<3}: {len(b):>3} wins, avgPnL {avg_p:+.1f}, avgMFE {avg_m:.1f}, cap {avg_p/avg_m*100:.0f}%, totalPnL {total_p:+.1f}")

# Losers: how many touched activation?
print(f"\n  LOSERS MFE (did they ever reach activation?):")
for thresh in [3, 5, 8, 10, 12]:
    n = sum(1 for t in db_losses if t['mfe'] >= thresh)
    print(f"    MFE >= {thresh}: {n}/{len(db_losses)} losers")

# GAP estimation using DB data
# For each winner: actual_pnl = trail_exit - entry. With current gap=8, trail exit ~= MFE - 8.
# With new gap: new_pnl = pnl + (8 - new_gap), CAPPED at MFE (can't capture more than MFE)
# This assumes the same exit point timing. It OVERESTIMATES tight gaps (no premature exit modeling).
# But it UNDERESTIMATES if tight gaps would let you lock in more before reversal.

print(f"\n  GAP SENSITIVITY (DB MFE-based, SL=14/ACT=10 fixed):")
print(f"  {'GAP':>4} | {'PnL':>8} {'delta':>7} | {'AvgW':>6} {'capture':>8} | Note")
for new_gap in [3, 4, 5, 6, 7, 8]:
    gap_delta = 8 - new_gap  # pts gained per winner
    new_total = 0
    new_avg_wins = []
    for t in db_trades:
        if t['outcome'] == 'WIN' and t['mfe'] >= 10:  # trail was active
            new_pnl = min(t['pnl'] + gap_delta, t['mfe'])  # can't exceed MFE
            new_total += new_pnl
            new_avg_wins.append(new_pnl)
        else:
            new_total += t['pnl']  # losers/expired unchanged

    avg_w = sum(new_avg_wins) / len(new_avg_wins) if new_avg_wins else 0
    avg_mfe_w = sum(t['mfe'] for t in db_wins if t['mfe'] >= 10) / max(1, sum(1 for t in db_wins if t['mfe'] >= 10))
    cap = avg_w / avg_mfe_w * 100 if avg_mfe_w > 0 else 0
    tag = " <-- CURRENT" if new_gap == 8 else ""
    print(f"  {new_gap:>4} | {new_total:>+8.1f} {new_total - db_total_pnl:>+7.1f} | {avg_w:>+6.1f} {cap:>7.0f}% | +{gap_delta}/win, {len([w for w in db_wins if w['mfe'] >= 10])} trail-active wins{tag}")

# SL sensitivity via MAE
print(f"\n  SL SENSITIVITY (DB MAE-based, ACT=10/GAP=8 fixed):")
print(f"  Current losers have MAE distribution:")
for thresh in [10, 12, 14, 16, 18, 20]:
    hit = sum(1 for t in db_losses if t['mae'] <= -thresh)
    # Losers with MAE worse than -SL would still be stopped
    # Losers with MAE between old SL and new SL: if SL tighter, stopped earlier (save pts per loss)
    # if SL wider, some losses become wins IF their MFE > activation
    potential_saves = [t for t in db_losses if -thresh < t['mae'] <= -14 and t['mfe'] >= 10]
    print(f"    SL={thresh:>2}: {hit} would be stopped (MAE<=-{thresh}), {len(potential_saves)} potential saves")

# Activation sensitivity via MFE
print(f"\n  ACT SENSITIVITY (DB MFE-based, SL=14/GAP=8 fixed):")
for new_act in [8, 10, 12, 14, 16]:
    # Winners that would still activate trail
    trail_wins = [t for t in db_wins if t['mfe'] >= new_act]
    # Winners that lose trail activation
    lost = [t for t in db_wins if t['mfe'] < new_act and t['mfe'] >= 10]  # currently have trail, would lose it
    # Trades that gain trail activation
    gained = [t for t in db_trades if t['outcome'] != 'WIN' and t['mfe'] >= new_act and t['mfe'] < 10] if new_act < 10 else []

    # Estimate PnL
    est_pnl = 0
    for t in db_trades:
        if t['outcome'] == 'WIN' and t['mfe'] >= new_act:
            est_pnl += t['pnl']  # same capture
        elif t['outcome'] == 'WIN' and t['mfe'] < new_act:
            est_pnl += 0  # loses trail, exits at BE or worse
        else:
            est_pnl += t['pnl']  # losers/expired unchanged

    tag = " <-- CURRENT" if new_act == 10 else ""
    print(f"    ACT={new_act:>2}: ~{est_pnl:>+7.1f} PnL, {len(trail_wins)} trail wins, {len(lost)} lose trail{tag}")

# Per-grade DB analysis
print(f"\n  PER-GRADE ANALYSIS (DB actuals):")
for grade in ['A+', 'A', 'B']:
    g = [t for t in db_trades if t['grade'] == grade]
    if not g: continue
    gw = [t for t in g if t['outcome'] == 'WIN']
    gl = [t for t in g if t['outcome'] == 'LOSS']
    g_pnl = sum(t['pnl'] for t in g)
    g_mfe = sum(t['mfe'] for t in gw) / len(gw) if gw else 0
    g_cap = (sum(t['pnl'] for t in gw) / sum(t['mfe'] for t in gw) * 100) if gw and sum(t['mfe'] for t in gw) > 0 else 0
    print(f"    {grade:>3}: {len(g)}t, {len(gw)}W/{len(gl)}L, WR {len(gw)/len(g)*100:.0f}%, PnL {g_pnl:+.1f}, AvgMFE(W) {g_mfe:.1f}, Cap {g_cap:.0f}%")

    # Gap improvement per grade
    for new_gap in [4, 5]:
        gap_delta = 8 - new_gap
        new_pnl = 0
        for t in g:
            if t['outcome'] == 'WIN' and t['mfe'] >= 10:
                new_pnl += min(t['pnl'] + gap_delta, t['mfe'])
            else:
                new_pnl += t['pnl']
        print(f"          GAP={new_gap}: {new_pnl:+.1f} (delta {new_pnl - g_pnl:+.1f})")

print(f"\n{'='*105}")
print(f"  ANALYSIS COMPLETE")
print(f"{'='*105}")
