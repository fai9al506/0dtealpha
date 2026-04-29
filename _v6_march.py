"""V6 Delta-Price Divergence — Data-driven filters from winner analysis"""
import psycopg2, pandas as pd, subprocess, json, numpy as np
from datetime import time as dtime

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
df['hour'] = pd.to_datetime(df['ts_et']).dt.hour
df['minute'] = pd.to_datetime(df['ts_et']).dt.minute
df['tod'] = df.hour + df.minute / 60.0

all_signals = []
n_days = len(df.trade_date.unique())

for date, day_df in df.groupby('trade_date'):
    day = day_df.copy().reset_index(drop=True)
    if len(day) < 30:
        continue

    day['avg20_d'] = day.abs_delta.rolling(20, min_periods=5).mean()
    day['std20_d'] = day.abs_delta.rolling(20, min_periods=5).std().clip(lower=1)
    day['dz'] = (day.abs_delta - day.avg20_d) / day.std20_d
    day['greens_5'] = day.is_green.rolling(5, min_periods=3).sum().shift(1)
    day['reds_5'] = 5 - day.greens_5

    cd_bull = 0; cd_bear = 0

    for i in range(len(day)):
        r = day.iloc[i]
        if cd_bull > 0: cd_bull -= 1
        if cd_bear > 0: cd_bear -= 1
        if r.abs_delta < 100 or pd.isna(r.avg20_d):
            continue

        # Direction: delta opposes bar color (fixed doji logic)
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

        if not trend_ok:
            continue

        # V6 QUALITY TIERS (data-driven from winner analysis)
        is_doji = r.body < 1.0
        is_hi_delta = r.abs_delta >= 400
        is_afternoon = r.tod >= 12.5
        is_morning = r.tod < 10.5
        is_midday = 10.5 <= r.tod < 12.5
        dz_val = abs(r.dz) if not pd.isna(r.dz) else 0

        # Tier 1: Doji (body < 1) — 62.5% WR historically
        # Tier 2: BEAR + morning + high delta — 62% WR
        # Tier 3: Afternoon + delta >= 200 — ~52% WR
        # Tier 4: Everything else (noise)
        tier = 4
        if is_doji:
            tier = 1
        elif direction == 'BEAR' and is_morning and is_hi_delta:
            tier = 2
        elif is_afternoon and r.abs_delta >= 200:
            tier = 3

        if tier == 4:
            continue  # Skip noise

        # Trail gap by tier
        gap = {1: 12, 2: 10, 3: 8}[tier]

        # Simulate outcome
        entry = r.c
        d = 1 if direction == 'BULL' else -1
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
            'dz': round(dz_val, 2), 'tier': tier, 'gap': gap,
            'outcome': outcome, 'pnl': round(pnl, 2),
            'mfe': round(mfe, 2), 'mae': round(mae, 2)
        })

        if direction == 'BULL': cd_bull = 5
        else: cd_bear = 5

# === RESULTS ===
sigs = all_signals
print("=" * 100)
print("V6 DELTA-PRICE DIVERGENCE — FULL MARCH 2026 (9:30-15:00 ET)")
print("Data-driven tiers: T1=Doji, T2=Bear+Morning+HiDelta, T3=Afternoon+Delta200+")
print(f"Trading days: {n_days}")
print("=" * 100)

# Overall
wins = sum(1 for s in sigs if s['pnl'] > 0)
total_pnl = sum(s['pnl'] for s in sigs)
gw = sum(s['pnl'] for s in sigs if s['pnl'] > 0)
gl = sum(s['pnl'] for s in sigs if s['pnl'] <= 0)
pf = abs(gw/gl) if gl else 999
print(f"\nTotal: {len(sigs)} signals, {len(sigs)/n_days:.1f}/day | WR: {wins}/{len(sigs)} = {100*wins/len(sigs):.1f}% | PnL: {total_pnl:+.1f} | PF: {pf:.2f}")

# By tier
print(f"\n--- By Tier ---")
print(f"{'Tier':<25} {'N':>4} {'N/day':>6} {'WR':>7} {'PnL':>8} {'MaxDD':>7} {'PF':>6} {'AvgPnL':>7} {'AvgMFE':>7}")
print("-" * 85)
for t, label in [(1,'T1: Doji'),(2,'T2: Bear AM HiDelta'),(3,'T3: Afternoon Delta200+')]:
    ts = [s for s in sigs if s['tier'] == t]
    if not ts: continue
    w = sum(1 for s in ts if s['pnl'] > 0)
    p = sum(s['pnl'] for s in ts)
    gw_t = sum(s['pnl'] for s in ts if s['pnl'] > 0)
    gl_t = sum(s['pnl'] for s in ts if s['pnl'] <= 0)
    pf_t = abs(gw_t/gl_t) if gl_t else 999
    # MaxDD
    running = 0; peak_eq = 0; maxdd = 0
    for s in sorted(ts, key=lambda x: (x['date'], x['time'])):
        running += s['pnl']
        peak_eq = max(peak_eq, running)
        maxdd = min(maxdd, running - peak_eq)
    amfe = np.mean([s['mfe'] for s in ts])
    print(f"{label:<25} {len(ts):>4} {len(ts)/n_days:>6.1f} {100*w/len(ts):>6.1f}% {p:>+8.1f} {maxdd:>7.1f} {pf_t:>6.2f} {p/len(ts):>+7.1f} {amfe:>7.1f}")

# Cumulative tiers
print(f"\n--- Cumulative Tiers ---")
for tiers, label in [([1], 'T1 only'), ([1,2], 'T1+T2'), ([1,2,3], 'T1+T2+T3')]:
    ts = [s for s in sigs if s['tier'] in tiers]
    if not ts: continue
    w = sum(1 for s in ts if s['pnl'] > 0)
    p = sum(s['pnl'] for s in ts)
    gw_t = sum(s['pnl'] for s in ts if s['pnl'] > 0)
    gl_t = sum(s['pnl'] for s in ts if s['pnl'] <= 0)
    pf_t = abs(gw_t/gl_t) if gl_t else 999
    running = 0; peak_eq = 0; maxdd = 0
    for s in sorted(ts, key=lambda x: (x['date'], x['time'])):
        running += s['pnl']
        peak_eq = max(peak_eq, running)
        maxdd = min(maxdd, running - peak_eq)
    print(f"  {label:<12} {len(ts):>4} sig, {len(ts)/n_days:.1f}/day, {100*w/len(ts):.1f}% WR, {p:+.1f} PnL, {maxdd:.1f} MaxDD, {pf_t:.2f} PF")

# By direction within each tier
print(f"\n--- By Direction per Tier ---")
for t, label in [(1,'T1: Doji'),(2,'T2: Bear AM HiDelta'),(3,'T3: Afternoon')]:
    for d in ['BULL','BEAR']:
        ds = [s for s in sigs if s['tier'] == t and s['dir'] == d]
        if not ds: continue
        w = sum(1 for s in ds if s['pnl'] > 0)
        p = sum(s['pnl'] for s in ds)
        print(f"  {label} {d}: {len(ds)} sig, {100*w/len(ds):.1f}% WR, {p:+.1f}")

# Per-day PnL
print(f"\n--- Daily PnL ---")
from collections import defaultdict
daily = defaultdict(list)
for s in sigs:
    daily[s['date']].append(s)

total_days_active = 0
daily_pnls = []
for d in sorted(daily.keys()):
    ds = daily[d]
    dp = sum(s['pnl'] for s in ds)
    dw = sum(1 for s in ds if s['pnl'] > 0)
    daily_pnls.append(dp)
    total_days_active += 1
    marker = " ***" if dp >= 10 else " !!" if dp <= -10 else ""
    print(f"  {d}: {len(ds)} sig, {dw}W/{len(ds)-dw}L, {dp:+.1f}{marker}")

green_days = sum(1 for p in daily_pnls if p > 0)
print(f"\n  Green days: {green_days}/{total_days_active} ({100*green_days/total_days_active:.0f}%)")
print(f"  Avg daily PnL: {np.mean(daily_pnls):+.1f}")
print(f"  Best day: {max(daily_pnls):+.1f} | Worst day: {min(daily_pnls):+.1f}")

# All signals list for best tier combo
print(f"\n--- All T1+T2 Signals (best combo detail) ---")
best = sorted([s for s in sigs if s['tier'] in [1,2]], key=lambda x: (x['date'], x['time']))
print(f"{'Date':<12} {'Time':<10} {'Dir':<5} {'Dlt':>5} {'Bdy':>4} {'Pk':>5} {'Tr':>6} {'T':>2} {'Gp':>3} {'Out':<5} {'PnL':>7} {'MFE':>5} {'MAE':>6}")
print("-" * 90)
for s in best:
    print(f"{s['date']:<12} {s['time']:<10} {s['dir']:<5} {s['delta']:>+5} {s['body']:>4} {s['peak']:>5} {s['trough']:>6} {s['tier']:>2} {s['gap']:>3} {s['outcome']:<5} {s['pnl']:>+7.1f} {s['mfe']:>5.1f} {s['mae']:>6.1f}")

bw = sum(1 for s in best if s['pnl'] > 0)
bp = sum(s['pnl'] for s in best)
print(f"\nT1+T2: {len(best)} sig, {100*bw/len(best):.1f}% WR, {bp:+.1f} PnL")

# Save
sdf = pd.DataFrame(sigs)
sdf.to_csv('exports/v6_march_signals.csv', index=False)
print(f"\nSaved: exports/v6_march_signals.csv")
