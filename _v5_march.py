"""V5 Delta-Price Divergence — Full March backtest"""
import psycopg2, pandas as pd, subprocess, json
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
df['is_green'] = (df.c >= df.o).astype(int)

# Per-day rolling stats
all_signals = []
for date, day_df in df.groupby('trade_date'):
    day = day_df.copy().reset_index(drop=True)
    if len(day) < 30:
        continue

    day['avg20_d'] = day.abs_delta.rolling(20, min_periods=5).mean()
    day['std20_d'] = day.abs_delta.rolling(20, min_periods=5).std().clip(lower=1)
    day['dz'] = (day.abs_delta - day.avg20_d) / day.std20_d
    day['avg20_vps'] = day.vps.rolling(20, min_periods=5).mean()
    day['std20_vps'] = day.vps.rolling(20, min_periods=5).std().clip(lower=0.1)
    day['vz'] = (day.vps - day.avg20_vps) / day.std20_vps
    day['greens_5'] = day.is_green.rolling(5, min_periods=3).sum().shift(1)
    day['reds_5'] = 5 - day.greens_5

    cd_bull = 0; cd_bear = 0

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

        # Trend precondition
        greens = r.greens_5 if not pd.isna(r.greens_5) else 2.5
        reds = r.reds_5 if not pd.isna(r.reds_5) else 2.5
        trend_ok = False
        if direction == 'BEAR' and greens >= 3: trend_ok = True
        elif direction == 'BULL' and reds >= 3: trend_ok = True
        if pd.isna(r.greens_5): trend_ok = True

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

        # Simulate outcome — grade-based trail
        entry = r.c
        d = 1 if direction == 'BULL' else -1
        gap = 12 if grade in ('A+', 'A') else 10 if grade == 'B' else 8
        sl = 8
        max_profit = 0.0; mfe = 0.0; mae = 0.0
        outcome = 'EXPIRED'; pnl = 0.0

        for j in range(i + 1, min(i + 101, len(day))):
            bar = day.iloc[j]
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
            last = day.iloc[min(i + 100, len(day) - 1)]
            pnl = (last.c - entry) * d
            outcome = 'WIN' if pnl > 0 else 'LOSS'

        all_signals.append({
            'date': str(date), 'time': str(r.ts_et)[11:19], 'dir': direction,
            'delta': int(r.delta), 'body': round(r.body, 2),
            'peak': int(r.peak), 'trough': int(r.trough),
            'vol': int(r.vol), 'vps': round(r.vps, 1),
            'dz': round(dz_val, 2), 'score': score, 'grade': grade,
            'trend_ok': trend_ok, 'gap': gap,
            'outcome': outcome, 'pnl': round(pnl, 2),
            'mfe': round(mfe, 2), 'mae': round(mae, 2)
        })

        if direction == 'BULL': cd_bull = 5
        else: cd_bear = 5

# === RESULTS ===
passed = [s for s in all_signals if s['trend_ok']]
blocked = [s for s in all_signals if not s['trend_ok']]
n_days = len(df.trade_date.unique())

print("=" * 100)
print("V5 DELTA-PRICE DIVERGENCE — FULL MARCH 2026 (9:30-15:00 ET)")
print(f"Trading days: {n_days} | Total bars: {len(df)}")
print("=" * 100)

print(f"\nAll detected: {len(all_signals)} | Passed trend: {len(passed)} | Blocked: {len(blocked)}")
print(f"Signals/day: {len(passed)/n_days:.1f}")

# Cumulative grade analysis — THE KEY TABLE
print(f"\n{'='*80}")
print("CUMULATIVE GRADE ANALYSIS (trade only signals >= this grade)")
print(f"{'='*80}")
print(f"{'Grade Cut':>10} {'Signals':>8} {'Sig/Day':>8} {'WR':>8} {'PnL':>8} {'MaxDD':>8} {'PF':>8}")
print("-" * 60)

for grade_cut, grades_included in [
    ('A+ only', ['A+']),
    ('A+, A', ['A+', 'A']),
    ('A+, A, B', ['A+', 'A', 'B']),
    ('A+ to C', ['A+', 'A', 'B', 'C']),
    ('All', ['A+', 'A', 'B', 'C', 'LOG']),
]:
    subset = [s for s in passed if s['grade'] in grades_included]
    if not subset:
        print(f"{grade_cut:>10} {'0':>8}")
        continue
    w = sum(1 for s in subset if s['pnl'] > 0)
    tp = sum(s['pnl'] for s in subset)
    gw = sum(s['pnl'] for s in subset if s['pnl'] > 0)
    gl = sum(s['pnl'] for s in subset if s['pnl'] <= 0)
    pf = abs(gw / gl) if gl != 0 else 999
    # MaxDD
    running = 0; peak_eq = 0; maxdd = 0
    for s in sorted(subset, key=lambda x: (x['date'], x['time'])):
        running += s['pnl']
        peak_eq = max(peak_eq, running)
        maxdd = min(maxdd, running - peak_eq)
    wr = 100 * w / len(subset)
    print(f"{grade_cut:>10} {len(subset):>8} {len(subset)/n_days:>8.1f} {wr:>7.1f}% {tp:>+8.1f} {maxdd:>8.1f} {pf:>8.2f}")

# Individual grade breakdown
print(f"\n--- Individual Grade ---")
print(f"{'Grade':>6} {'Cnt':>5} {'WR':>7} {'PnL':>8} {'Avg':>7}")
print("-" * 40)
for g in ['A+', 'A', 'B', 'C', 'LOG']:
    gs = [s for s in passed if s['grade'] == g]
    if not gs: continue
    w = sum(1 for s in gs if s['pnl'] > 0)
    p = sum(s['pnl'] for s in gs)
    print(f"{g:>6} {len(gs):>5} {100*w/len(gs):>6.1f}% {p:>+8.1f} {p/len(gs):>+7.1f}")

# By direction
print(f"\n--- By Direction (passed only) ---")
for dd in ['BULL', 'BEAR']:
    ds = [s for s in passed if s['dir'] == dd]
    if not ds: continue
    w = sum(1 for s in ds if s['pnl'] > 0)
    p = sum(s['pnl'] for s in ds)
    print(f"  {dd}: {len(ds)} sig, {100*w/len(ds):.1f}% WR, {p:+.1f}")

# By direction + grade A
print(f"\n--- Grade A+ & A by Direction ---")
for dd in ['BULL', 'BEAR']:
    ds = [s for s in passed if s['dir'] == dd and s['grade'] in ('A+', 'A')]
    if not ds: continue
    w = sum(1 for s in ds if s['pnl'] > 0)
    p = sum(s['pnl'] for s in ds)
    print(f"  {dd}: {len(ds)} sig, {100*w/len(ds):.1f}% WR, {p:+.1f}")

# Per-day breakdown for A+/A
print(f"\n--- A+/A Signals Per Day ---")
a_sigs = [s for s in passed if s['grade'] in ('A+', 'A')]
a_sigs_sorted = sorted(a_sigs, key=lambda x: (x['date'], x['time']))
cur_date = None
day_pnl = 0
for s in a_sigs_sorted:
    if s['date'] != cur_date:
        if cur_date: print(f"  Day total: {day_pnl:+.1f}")
        cur_date = s['date']
        day_pnl = 0
        print(f"\n  {cur_date}:")
    day_pnl += s['pnl']
    print(f"    {s['time']} {s['dir']:<5} dlt={s['delta']:>+5} {s['grade']:<3} gap={s['gap']} -> {s['outcome']:<5} {s['pnl']:>+7.1f} (MFE={s['mfe']:.0f} MAE={s['mae']:.0f})")
if cur_date: print(f"  Day total: {day_pnl:+.1f}")

# Trend filter impact
print(f"\n--- Trend Filter Impact ---")
bw = sum(1 for s in blocked if s['pnl'] > 0)
bp = sum(s['pnl'] for s in blocked)
print(f"  Blocked: {len(blocked)} signals, {100*bw/len(blocked):.1f}% WR, {bp:+.1f} PnL")
print(f"  (negative PnL = filter is helping)")

# Save CSV
sdf = pd.DataFrame(passed)
sdf.to_csv('exports/v5_march_signals.csv', index=False)
print(f"\nSaved: exports/v5_march_signals.csv ({len(passed)} signals)")
