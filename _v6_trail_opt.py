"""V6 trail optimization — T1+T3, test wider gaps"""
import psycopg2, pandas as pd, subprocess, json, numpy as np
from datetime import time as dtime
from collections import defaultdict

DB = json.loads(subprocess.run(
    ['railway','variables','--json'], capture_output=True, text=True, shell=True
).stdout)['DATABASE_URL']

conn = psycopg2.connect(DB)
df = pd.read_sql("""
    SELECT bar_idx, trade_date, bar_open o, bar_high h, bar_low l, bar_close c,
           bar_volume vol, bar_delta delta,
           cvd_open, cvd_high, cvd_low, cvd_close,
           ts_start AT TIME ZONE 'America/New_York' as ts_et,
           EXTRACT(EPOCH FROM (ts_end - ts_start)) as dur
    FROM es_range_bars WHERE source='rithmic'
      AND trade_date >= '2026-03-01' AND trade_date < '2026-04-01'
    ORDER BY trade_date, bar_idx
""", conn)
conn.close()

df['t'] = pd.to_datetime(df['ts_et']).dt.time
df = df[(df['t'] >= dtime(9,30)) & (df['t'] < dtime(15,0))].copy().reset_index(drop=True)
df['color'] = (df.c >= df.o).map({True: 'GREEN', False: 'RED'})
df['body'] = abs(df.c - df.o)
df['abs_delta'] = abs(df.delta)
df['is_green'] = (df.c >= df.o).astype(int)
df['tod'] = pd.to_datetime(df['ts_et']).dt.hour + pd.to_datetime(df['ts_et']).dt.minute / 60.0

n_days = len(df.trade_date.unique())

# Collect signal locations (T1 doji all + T3 afternoon)
raw_signals = []
for date, day_df in df.groupby('trade_date'):
    day = day_df.copy().reset_index(drop=True)
    if len(day) < 30:
        continue
    day['greens_5'] = day.is_green.rolling(5, min_periods=3).sum().shift(1)
    day['reds_5'] = 5 - day.greens_5
    day['avg20_d'] = day.abs_delta.rolling(20, min_periods=5).mean()

    cd_bull = 0
    cd_bear = 0
    for i in range(len(day)):
        r = day.iloc[i]
        if cd_bull > 0: cd_bull -= 1
        if cd_bear > 0: cd_bear -= 1
        if r.abs_delta < 100 or pd.isna(r.avg20_d):
            continue

        direction = None
        if r.delta > 0 and r.color == 'RED':
            direction = 'BEAR'
        elif r.delta < 0 and r.color == 'GREEN':
            direction = 'BULL'
        elif r.body <= 1.0 and i > 0:
            prev_clr = day.iloc[i-1].color
            if r.delta > 0 and prev_clr == 'GREEN':
                direction = 'BEAR'
            elif r.delta < 0 and prev_clr == 'RED':
                direction = 'BULL'
        if not direction:
            continue
        if direction == 'BULL' and cd_bull > 0:
            continue
        if direction == 'BEAR' and cd_bear > 0:
            continue

        greens = r.greens_5 if not pd.isna(r.greens_5) else 2.5
        reds = r.reds_5 if not pd.isna(r.reds_5) else 2.5
        trend_ok = False
        if direction == 'BEAR' and greens >= 3:
            trend_ok = True
        elif direction == 'BULL' and reds >= 3:
            trend_ok = True
        if pd.isna(r.greens_5):
            trend_ok = True
        if not trend_ok:
            continue

        is_doji = r.body < 1.0
        is_afternoon = r.tod >= 12.5
        tier = 0
        if is_doji:
            tier = 1
        elif is_afternoon and r.abs_delta >= 200:
            tier = 3
        if tier == 0:
            continue

        # Store bar references for simulation
        raw_signals.append({
            'date': str(date), 'time': str(r.ts_et)[11:19],
            'dir': direction, 'delta': int(r.delta),
            'body': round(r.body, 2), 'tier': tier,
            'entry': r.c, 'day_key': str(date), 'bar_i': i,
            '_day': day
        })
        if direction == 'BULL':
            cd_bull = 5
        else:
            cd_bear = 5

print(f"Signals (T1+T3): {len(raw_signals)}")


def simulate(signals, sl, gap, be_trigger=None, timeout=100, immediate=False, mixed=False):
    results = []
    for s in signals:
        entry = s['entry']
        d = 1 if s['dir'] == 'BULL' else -1
        day = s['_day']
        i = s['bar_i']
        max_profit = 0.0
        mfe = 0.0
        mae = 0.0
        outcome = 'EXPIRED'
        pnl = 0.0

        # Mixed mode: T1=gap12, T3=gap8
        if mixed:
            actual_gap = 12 if s['tier'] == 1 else 8
        else:
            actual_gap = gap

        for j in range(i + 1, min(i + timeout + 1, len(day))):
            bar = day.iloc[j]
            cf = (bar.h - entry) if d == 1 else (entry - bar.l)
            ca = (bar.l - entry) if d == 1 else (entry - bar.h)
            mfe = max(mfe, cf)
            mae = min(mae, ca)

            if immediate or mixed:
                # Immediate trail: trail = max(maxProfit - gap, -SL) always
                trail = max(max_profit - actual_gap, -sl)
            else:
                if max_profit >= actual_gap:
                    trail = max_profit - actual_gap
                else:
                    trail = -sl

            if ca <= trail:
                pnl = trail
                outcome = 'WIN' if pnl > 0 else ('BE' if pnl == 0 else 'LOSS')
                break
            max_profit = max(max_profit, cf)
        else:
            last = day.iloc[min(i + timeout, len(day) - 1)]
            pnl = (last.c - entry) * d
            outcome = 'WIN' if pnl > 0 else 'LOSS'

        results.append({
            'date': s['date'], 'time': s['time'], 'dir': s['dir'],
            'delta': s['delta'], 'body': s['body'], 'tier': s['tier'],
            'pnl': round(pnl, 2), 'outcome': outcome,
            'mfe': round(mfe, 2), 'mae': round(mae, 2)
        })
    return results


configs = [
    ('SL8 gap8', 8, 8, None, False),
    ('SL8 gap10', 8, 10, None, False),
    ('SL8 gap12', 8, 12, None, False),
    ('IMM SL8 gap8', 8, 8, None, True),
    ('IMM SL8 gap10', 8, 10, None, True),
    ('IMM SL8 gap12', 8, 12, None, True),
    ('IMM SL8 gap15', 8, 15, None, True),
    ('IMM SL10 gap10', 10, 10, None, True),
    ('IMM SL10 gap12', 10, 12, None, True),
    ('IMM SL10 gap15', 10, 15, None, True),
    ('IMM SL12 gap12', 12, 12, None, True),
    ('IMM SL12 gap15', 12, 15, None, True),
    ('T1=12 T3=8 IMM', 8, 0, None, 'mixed'),
]

print(f"\n{'Config':<22} {'N':>4} {'WR':>6} {'PnL':>8} {'MaxDD':>7} {'PF':>6} {'Avg':>6} {'Capt%':>6}")
print("-" * 70)

best_pnl = -999
best_name = ""
best_res = None
for name, sl, gap, be, imm in configs:
    is_mixed = (imm == 'mixed')
    is_imm = (imm is True)
    res = simulate(raw_signals, sl, gap, be, immediate=is_imm, mixed=is_mixed)
    n = len(res)
    w = sum(1 for r in res if r['pnl'] > 0)
    tp = sum(r['pnl'] for r in res)
    gw = sum(r['pnl'] for r in res if r['pnl'] > 0)
    gl = sum(r['pnl'] for r in res if r['pnl'] <= 0)
    pf = abs(gw / gl) if gl else 999
    winners = [r for r in res if r['pnl'] > 0]
    avg_capt = np.mean([r['pnl'] / r['mfe'] * 100 for r in winners if r['mfe'] > 0]) if winners else 0
    running = 0
    peak_eq = 0
    maxdd = 0
    for r in sorted(res, key=lambda x: (x['date'], x['time'])):
        running += r['pnl']
        peak_eq = max(peak_eq, running)
        maxdd = min(maxdd, running - peak_eq)
    wr = 100 * w / n
    print(f"{name:<22} {n:>4} {wr:>5.1f}% {tp:>+8.1f} {maxdd:>7.1f} {pf:>6.2f} {tp/n:>+6.1f} {avg_capt:>5.0f}%")
    if tp > best_pnl:
        best_pnl = tp
        best_name = name
        best_res = res

# Best config details
print(f"\n=== BEST: {best_name} ===")
daily = defaultdict(list)
for r in best_res:
    daily[r['date']].append(r)

daily_pnls = []
for d in sorted(daily.keys()):
    ds = daily[d]
    dp = sum(r['pnl'] for r in ds)
    daily_pnls.append(dp)

green = sum(1 for p in daily_pnls if p > 0)
w = sum(1 for r in best_res if r['pnl'] > 0)
tp = sum(r['pnl'] for r in best_res)
print(f"Total: {len(best_res)} sig, {100*w/len(best_res):.1f}% WR, {tp:+.1f} PnL")
print(f"Green days: {green}/{len(daily_pnls)} ({100*green/len(daily_pnls):.0f}%)")
print(f"Avg daily: {np.mean(daily_pnls):+.1f} | Best: {max(daily_pnls):+.1f} | Worst: {min(daily_pnls):+.1f}")

for t, tlbl in [(1, 'T1 Doji'), (3, 'T3 Afternoon')]:
    ts = [r for r in best_res if r['tier'] == t]
    if not ts:
        continue
    w = sum(1 for r in ts if r['pnl'] > 0)
    p = sum(r['pnl'] for r in ts)
    winners = [r for r in ts if r['pnl'] > 0]
    avg_mfe = np.mean([r['mfe'] for r in winners]) if winners else 0
    avg_pnl_w = np.mean([r['pnl'] for r in winners]) if winners else 0
    capt = 100 * avg_pnl_w / avg_mfe if avg_mfe else 0
    print(f"  {tlbl}: {len(ts)} sig, {100*w/len(ts):.1f}% WR, {p:+.1f} PnL, capture {capt:.0f}%")

# Daily breakdown
print(f"\n--- Daily PnL ({best_name}) ---")
for d in sorted(daily.keys()):
    ds = daily[d]
    dp = sum(r['pnl'] for r in ds)
    dw = sum(1 for r in ds if r['pnl'] > 0)
    marker = " ***" if dp >= 10 else " !!" if dp <= -10 else ""
    print(f"  {d}: {len(ds)} sig, {dw}W/{len(ds)-dw}L, {dp:+.1f}{marker}")
