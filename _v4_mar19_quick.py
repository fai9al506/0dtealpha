"""V4 Delta-Price Divergence — Mar 19 backtest (9:30-15:00 ET)"""
import psycopg2, pandas as pd, os, subprocess
from zoneinfo import ZoneInfo
from datetime import time as dtime

# Get DATABASE_URL
result = subprocess.run(['railway','variables','--json'], capture_output=True, text=True)
import json
DATABASE_URL = json.loads(result.stdout)['DATABASE_URL']

conn = psycopg2.connect(DATABASE_URL)
df = pd.read_sql("""
    SELECT bar_idx, bar_open o, bar_high h, bar_low l, bar_close c,
           bar_volume vol, bar_delta delta, bar_buy_volume bv, bar_sell_volume sv,
           cvd_open, cvd_high, cvd_low, cvd_close,
           ts_start AT TIME ZONE 'America/New_York' as ts_et,
           ts_end AT TIME ZONE 'America/New_York' as ts_end_et,
           EXTRACT(EPOCH FROM (ts_end - ts_start)) as dur
    FROM es_range_bars WHERE source='rithmic' AND trade_date='2026-03-19'
    ORDER BY bar_idx
""", conn)
conn.close()

# Market hours 9:30-15:00 ET
df['t'] = pd.to_datetime(df['ts_et']).dt.time
df = df[(df['t'] >= dtime(9,30)) & (df['t'] < dtime(15,0))].copy().reset_index(drop=True)
df['color'] = df.apply(lambda r: 'GREEN' if r.c >= r.o else 'RED', axis=1)
df['body'] = abs(df.c - df.o)
df['body_ratio'] = df['body'] / (df.h - df.l).clip(lower=0.25)
df['peak'] = df.cvd_high - df.cvd_open
df['trough'] = df.cvd_low - df.cvd_open
df['abs_delta'] = abs(df.delta)
df['dur_s'] = df.dur.clip(lower=1)
df['vps'] = df.vol / df.dur_s

# Rolling stats
df['avg20_delta'] = df.abs_delta.rolling(20, min_periods=5).mean()
df['std20_delta'] = df.abs_delta.rolling(20, min_periods=5).std().clip(lower=1)
df['dz'] = (df.abs_delta - df.avg20_delta) / df.std20_delta
df['avg20_vps'] = df.vps.rolling(20, min_periods=5).mean()
df['std20_vps'] = df.vps.rolling(20, min_periods=5).std().clip(lower=0.1)
df['vz'] = (df.vps - df.avg20_vps) / df.std20_vps

# Signal detection
signals = []
cooldown_bull = 0
cooldown_bear = 0

for i in range(len(df)):
    r = df.iloc[i]
    if r.abs_delta < 100:
        if cooldown_bull > 0: cooldown_bull -= 1
        if cooldown_bear > 0: cooldown_bear -= 1
        continue
    if pd.isna(r.avg20_delta):
        continue

    direction = None
    if r.delta > 0 and (r.color == 'RED' or r.body <= 1.0):
        direction = 'BEAR'
    elif r.delta < 0 and (r.color == 'GREEN' or r.body <= 1.0):
        direction = 'BULL'

    if cooldown_bull > 0: cooldown_bull -= 1
    if cooldown_bear > 0: cooldown_bear -= 1

    if not direction:
        continue

    if direction == 'BULL' and cooldown_bull > 0:
        continue
    if direction == 'BEAR' and cooldown_bear > 0:
        continue

    # Grading
    dz_val = abs(r.dz) if not pd.isna(r.dz) else 0
    if dz_val >= 2.0: g1 = 30
    elif dz_val >= 1.5: g1 = 25
    elif dz_val >= 1.0: g1 = 20
    elif dz_val >= 0.5: g1 = 10
    else: g1 = 5

    if direction == 'BEAR':
        peak_abs = r.peak if r.peak > 0 else 0
    else:
        peak_abs = abs(r.trough) if r.trough < 0 else 0
    peak_ratio = peak_abs / max(r.abs_delta, 1)
    if peak_ratio >= 2.0: g2 = 20
    elif peak_ratio >= 1.5: g2 = 15
    elif peak_ratio >= 1.0: g2 = 10
    else: g2 = 5

    vz_val = r.vz if not pd.isna(r.vz) else 0
    if vz_val >= 2.0: g3 = 20
    elif vz_val >= 1.0: g3 = 15
    elif vz_val >= 0: g3 = 10
    else: g3 = 5

    if direction == 'BEAR':
        body_against = max(0, r.o - r.c)
    else:
        body_against = max(0, r.c - r.o)
    div_score = r.abs_delta * (body_against + 1) / 1000
    if div_score >= 3.0: g4 = 30
    elif div_score >= 2.0: g4 = 25
    elif div_score >= 1.0: g4 = 20
    elif div_score >= 0.5: g4 = 15
    else: g4 = 5

    score = g1 + g2 + g3 + g4
    if score >= 80: grade = 'A+'
    elif score >= 65: grade = 'A'
    elif score >= 50: grade = 'B'
    elif score >= 35: grade = 'C'
    else: grade = 'LOG'

    signals.append({
        'idx': int(r.bar_idx), 'time': str(r.ts_et)[11:19], 'dir': direction,
        'o': r.o, 'h': r.h, 'l': r.l, 'c': r.c, 'color': r.color,
        'delta': int(r.delta), 'body': round(r.body, 2), 'body_ratio': round(r.body_ratio, 2),
        'peak': int(r.peak), 'trough': int(r.trough),
        'vol': int(r.vol), 'vps': round(r.vps, 1),
        'dz': round(dz_val, 2), 'score': score, 'grade': grade,
        'entry': r.c, 'df_i': i
    })

    if direction == 'BULL': cooldown_bull = 5
    else: cooldown_bear = 5

# Simulate outcomes — Config F trail (SL=8, gap=8, timeout=100)
for s in signals:
    entry = s['entry']
    d = 1 if s['dir'] == 'BULL' else -1
    start_i = s['df_i']
    max_profit = 0.0
    pnl = 0.0
    outcome = 'EXPIRED'
    mfe = 0.0
    mae = 0.0

    for j in range(start_i + 1, min(start_i + 101, len(df))):
        bar = df.iloc[j]
        if d == 1:
            cur_fav = bar.h - entry
            cur_adv = bar.l - entry
        else:
            cur_fav = entry - bar.l
            cur_adv = entry - bar.h

        mfe = max(mfe, cur_fav)
        mae = min(mae, cur_adv)

        trail_stop = max(max_profit - 8, -8)

        if cur_adv <= trail_stop:
            pnl = trail_stop
            outcome = 'WIN' if pnl > 0 else ('BE' if pnl == 0 else 'LOSS')
            break

        max_profit = max(max_profit, cur_fav)
    else:
        last = df.iloc[min(start_i + 100, len(df)-1)]
        pnl = (last.c - entry) * d
        outcome = 'WIN' if pnl > 0 else 'LOSS'

    s['pnl'] = round(pnl, 2)
    s['outcome'] = outcome
    s['mfe'] = round(mfe, 2)
    s['mae'] = round(mae, 2)

# Print
print(f'\n=== V4 Delta-Price Divergence — Mar 19 (9:30-15:00 ET) ===')
print(f'Total: {len(signals)} signals')
wins = sum(1 for s in signals if s['pnl'] > 0)
total_pnl = sum(s['pnl'] for s in signals)
print(f'WR: {wins}/{len(signals)} = {100*wins/len(signals):.1f}%  |  PnL: {total_pnl:+.1f}')

print(f'\n{"#":>3} {"Time":<10} {"Dir":<5} {"OHLC":<35} {"Clr":<6} {"Delta":>6} {"Body":>5} {"Peak":>6} {"Trgh":>6} {"Vol":>6} {"V/s":>6} {"DZ":>5} {"Grd":<4} {"Scr":>3} {"Out":<5} {"PnL":>7} {"MFE":>6} {"MAE":>7}')
print('-'*155)
for i, s in enumerate(signals):
    ohlc = f'{s["o"]:.2f}/{s["h"]:.2f}/{s["l"]:.2f}/{s["c"]:.2f}'
    print(f'{i+1:>3} {s["time"]:<10} {s["dir"]:<5} {ohlc:<35} {s["color"]:<6} {s["delta"]:>6} {s["body"]:>5} {s["peak"]:>6} {s["trough"]:>6} {s["vol"]:>6} {s["vps"]:>6} {s["dz"]:>5} {s["grade"]:<4} {s["score"]:>3} {s["outcome"]:<5} {s["pnl"]:>+7.1f} {s["mfe"]:>6.1f} {s["mae"]:>7.1f}')

# Summary by grade
print(f'\n--- By Grade ---')
for g in ['A+','A','B','C','LOG']:
    gs = [s for s in signals if s['grade'] == g]
    if not gs: print(f'{g}: 0'); continue
    w = sum(1 for s in gs if s['pnl'] > 0)
    p = sum(s['pnl'] for s in gs)
    print(f'{g}: {len(gs)}sig, {100*w/len(gs):.0f}% WR, {p:+.1f} PnL')

print(f'\n--- By Direction ---')
for d in ['BULL','BEAR']:
    ds = [s for s in signals if s['dir'] == d]
    if not ds: continue
    w = sum(1 for s in ds if s['pnl'] > 0)
    p = sum(s['pnl'] for s in ds)
    print(f'{d}: {len(ds)}sig, {100*w/len(ds):.0f}% WR, {p:+.1f} PnL')
