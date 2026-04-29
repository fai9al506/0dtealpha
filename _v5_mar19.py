"""V5 Delta-Price Divergence — Full March backtest
Fixes: doji bug, trend precondition, grade-based trail gap"""
import psycopg2, pandas as pd, os, subprocess, json
from zoneinfo import ZoneInfo
from datetime import time as dtime, datetime

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
df['peak'] = df.cvd_high - df.cvd_open
df['trough'] = df.cvd_low - df.cvd_open
df['dur_s'] = df.dur.clip(lower=1)
df['vps'] = df.vol / df.dur_s

df['avg20_d'] = df.abs_delta.rolling(20, min_periods=5).mean()
df['std20_d'] = df.abs_delta.rolling(20, min_periods=5).std().clip(lower=1)
df['dz'] = (df.abs_delta - df.avg20_d) / df.std20_d
df['avg20_vps'] = df.vps.rolling(20, min_periods=5).mean()
df['std20_vps'] = df.vps.rolling(20, min_periods=5).std().clip(lower=0.1)
df['vz'] = (df.vps - df.avg20_vps) / df.std20_vps

# Trend: count greens in last 5 bars (shifted by 1 to exclude current)
df['is_green'] = (df.c >= df.o).astype(int)
df['greens_5'] = df.is_green.rolling(5, min_periods=3).sum().shift(1)
df['reds_5'] = 5 - df.greens_5

signals = []
cd_bull = 0
cd_bear = 0

for i in range(len(df)):
    r = df.iloc[i]
    if cd_bull > 0: cd_bull -= 1
    if cd_bear > 0: cd_bear -= 1
    if r.abs_delta < 100 or pd.isna(r.avg20_d):
        continue

    direction = None
    # Core: delta opposes bar color
    if r.delta > 0 and r.color == 'RED':
        direction = 'BEAR'
    elif r.delta < 0 and r.color == 'GREEN':
        direction = 'BULL'
    elif r.body <= 1.0 and i > 0:
        # Doji: only if delta opposes the prior trend direction
        prev_clr = df.iloc[i-1].color
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

    # Trend precondition
    greens = r.greens_5 if not pd.isna(r.greens_5) else 2.5
    reds = r.reds_5 if not pd.isna(r.reds_5) else 2.5
    trend_ok = False
    if direction == 'BEAR' and greens >= 3:
        trend_ok = True
    elif direction == 'BULL' and reds >= 3:
        trend_ok = True
    if pd.isna(r.greens_5):
        trend_ok = True

    # Grading
    dz_val = abs(r.dz) if not pd.isna(r.dz) else 0
    g1 = 30 if dz_val >= 2.0 else 25 if dz_val >= 1.5 else 20 if dz_val >= 1.0 else 10 if dz_val >= 0.5 else 5

    peak_abs = (r.peak if r.peak > 0 else 0) if direction == 'BEAR' else (abs(r.trough) if r.trough < 0 else 0)
    pr = peak_abs / max(r.abs_delta, 1)
    g2 = 20 if pr >= 2.0 else 15 if pr >= 1.5 else 10 if pr >= 1.0 else 5

    vz_val = r.vz if not pd.isna(r.vz) else 0
    g3 = 20 if vz_val >= 2.0 else 15 if vz_val >= 1.0 else 10 if vz_val >= 0 else 5

    body_ag = max(0, r.o - r.c) if direction == 'BEAR' else max(0, r.c - r.o)
    ds = r.abs_delta * (body_ag + 1) / 1000
    g4 = 30 if ds >= 3.0 else 25 if ds >= 2.0 else 20 if ds >= 1.0 else 15 if ds >= 0.5 else 5

    score = g1 + g2 + g3 + g4
    grade = 'A+' if score >= 80 else 'A' if score >= 65 else 'B' if score >= 50 else 'C' if score >= 35 else 'LOG'

    signals.append({
        'idx': int(r.bar_idx), 'time': str(r.ts_et)[11:19], 'dir': direction,
        'o': r.o, 'h': r.h, 'l': r.l, 'c': r.c, 'color': r.color,
        'delta': int(r.delta), 'body': round(r.body, 2),
        'peak': int(r.peak), 'trough': int(r.trough),
        'vol': int(r.vol), 'vps': round(r.vps, 1),
        'dz': round(dz_val, 2), 'score': score, 'grade': grade,
        'trend_ok': trend_ok, 'greens5': greens, 'reds5': reds,
        'entry': r.c, 'df_i': i
    })
    if direction == 'BULL': cd_bull = 5
    else: cd_bear = 5

# Simulate — grade-based trail
for s in signals:
    entry = s['entry']
    d = 1 if s['dir'] == 'BULL' else -1
    gap = 12 if s['grade'] in ('A+', 'A') else 10 if s['grade'] == 'B' else 8
    sl = 8
    start_i = s['df_i']
    max_profit = 0.0; mfe = 0.0; mae = 0.0
    outcome = 'EXPIRED'; pnl = 0.0

    for j in range(start_i + 1, min(start_i + 101, len(df))):
        bar = df.iloc[j]
        cf = (bar.h - entry) if d == 1 else (entry - bar.l)
        ca = (bar.l - entry) if d == 1 else (entry - bar.h)
        mfe = max(mfe, cf); mae = min(mae, ca)
        trail = max(max_profit - gap, -sl)
        if ca <= trail:
            pnl = trail
            outcome = 'WIN' if pnl > 0 else 'LOSS'
            break
        max_profit = max(max_profit, cf)
    else:
        last = df.iloc[min(start_i + 100, len(df) - 1)]
        pnl = (last.c - entry) * d
        outcome = 'WIN' if pnl > 0 else 'LOSS'

    s['pnl'] = round(pnl, 2); s['outcome'] = outcome
    s['mfe'] = round(mfe, 2); s['mae'] = round(mae, 2); s['gap'] = gap

# === PRINT ===
passed = [s for s in signals if s['trend_ok']]
blocked = [s for s in signals if not s['trend_ok']]

print("=" * 120)
print("V5 DELTA-PRICE DIVERGENCE - Mar 19 (9:30-15:00 ET)")
print("Fixes: doji bug, trend precondition (3/5 bars opposite), grade-based trail")
print("=" * 120)
print(f"\nTotal detected: {len(signals)} | Passed trend filter: {len(passed)} | Blocked: {len(blocked)}")

print(f"\n--- BLOCKED by trend filter ({len(blocked)}) ---")
for s in blocked:
    need = '>=3 green' if s['dir'] == 'BEAR' else '>=3 red'
    had = f"greens={s['greens5']:.0f} reds={s['reds5']:.0f}"
    would = 'WIN' if s['pnl'] > 0 else 'LOSS'
    print(f"  {s['time']} {s['dir']:<5} dlt={s['delta']:>+5} {s['grade']:<3} {had} (needed {need}) would={would} {s['pnl']:+.1f}")

print(f"\n--- PASSED signals ({len(passed)}) ---")
hdr = f"{'#':>3} {'Time':<10} {'Dir':<5} {'Clr':<6} {'Dlt':>5} {'Bdy':>4} {'Pk':>5} {'Tr':>6} {'Vol':>6} {'V/s':>5} {'DZ':>4} {'G':<3} {'Sc':>3} {'Gp':>3} {'Out':<5} {'PnL':>7} {'MFE':>5} {'MAE':>6}"
print(hdr)
print("-" * len(hdr))
for i, s in enumerate(passed):
    print(f"{i+1:>3} {s['time']:<10} {s['dir']:<5} {s['color']:<6} {s['delta']:>+5} {s['body']:>4} {s['peak']:>5} {s['trough']:>6} {s['vol']:>6} {s['vps']:>5} {s['dz']:>4} {s['grade']:<3} {s['score']:>3} {s['gap']:>3} {s['outcome']:<5} {s['pnl']:>+7.1f} {s['mfe']:>5.1f} {s['mae']:>6.1f}")

# Summary
wins = sum(1 for s in passed if s['pnl'] > 0)
total_pnl = sum(s['pnl'] for s in passed)
gw = sum(s['pnl'] for s in passed if s['pnl'] > 0)
gl = sum(s['pnl'] for s in passed if s['pnl'] <= 0)
pf = abs(gw / gl) if gl != 0 else 999

print(f"\n--- SUMMARY ---")
print(f"Signals: {len(passed)} | WR: {wins}/{len(passed)} = {100*wins/len(passed):.1f}% | PnL: {total_pnl:+.1f} | PF: {pf:.2f}")

print(f"\n--- By Grade ---")
for g in ['A+', 'A', 'B', 'C', 'LOG']:
    gs = [s for s in passed if s['grade'] == g]
    if not gs: continue
    w = sum(1 for s in gs if s['pnl'] > 0)
    p = sum(s['pnl'] for s in gs)
    print(f"  {g}: {len(gs)} sig, {100*w/len(gs):.0f}% WR, {p:+.1f}")

print(f"\n--- By Direction ---")
for dd in ['BULL', 'BEAR']:
    ds = [s for s in passed if s['dir'] == dd]
    if not ds: continue
    w = sum(1 for s in ds if s['pnl'] > 0)
    p = sum(s['pnl'] for s in ds)
    print(f"  {dd}: {len(ds)} sig, {100*w/len(ds):.0f}% WR, {p:+.1f}")

# V4 vs V5
print(f"\n--- V4 vs V5 ---")
print(f"  V4: 28 signals, 28.6% WR, -11.0 PnL")
print(f"  V5: {len(passed)} signals, {100*wins/len(passed):.1f}% WR, {total_pnl:+.1f} PnL")

# Recall
user_times = ['09:35','09:46','09:50','09:54','10:11','11:05','11:36',
              '12:07','12:35','12:40','12:42','12:52','14:13','14:41']
print(f"\n--- User Recall ---")
caught = 0
for ut in user_times:
    found = False
    for s in passed:
        st = s['time'][:5]
        try:
            t1 = datetime.strptime(ut, '%H:%M')
            t2 = datetime.strptime(st, '%H:%M')
            if abs((t1 - t2).total_seconds()) <= 120:
                found = True
                caught += 1
                print(f"  {ut}: YES -> {s['time']} {s['dir']} dlt={s['delta']:+d} {s['grade']} pnl={s['pnl']:+.1f}")
                break
        except:
            pass
    if not found:
        for s in blocked:
            st = s['time'][:5]
            try:
                t1 = datetime.strptime(ut, '%H:%M')
                t2 = datetime.strptime(st, '%H:%M')
                if abs((t1 - t2).total_seconds()) <= 120:
                    found = True
                    print(f"  {ut}: TREND-BLOCKED -> {s['time']} {s['dir']} g5={s['greens5']:.0f} r5={s['reds5']:.0f}")
                    break
            except:
                pass
        if not found:
            print(f"  {ut}: NOT DETECTED")
print(f"  Recall: {caught}/{len(user_times)} = {100*caught/len(user_times):.0f}%")
