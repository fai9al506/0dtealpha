"""V7 FINAL — Best filters + IMM SL8 gap8 trail"""
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
df['peak'] = df.cvd_high - df.cvd_open
df['trough'] = df.cvd_low - df.cvd_open
df['dur_s'] = df.dur.clip(lower=1)
df['vps'] = df.vol / df.dur_s
df['is_green'] = (df.c >= df.o).astype(int)
df['tod'] = pd.to_datetime(df['ts_et']).dt.hour + pd.to_datetime(df['ts_et']).dt.minute / 60.0

n_days = len(df.trade_date.unique())

# Collect signals with all V7 filters
configs = {
    'V6 baseline (T1+T3)': {'peak_cap': 999, 'drop_14': False},
    'V7a: + peak<2.5': {'peak_cap': 2.5, 'drop_14': False},
    'V7b: + drop T3 14:00-14:30': {'peak_cap': 999, 'drop_14': True},
    'V7 FINAL: peak<2.5 + drop T3 14-14:30': {'peak_cap': 2.5, 'drop_14': True},
}

for cfg_name, cfg in configs.items():
    all_signals = []
    for date, day_df in df.groupby('trade_date'):
        day = day_df.copy().reset_index(drop=True)
        if len(day) < 30: continue

        day['greens_5'] = day.is_green.rolling(5, min_periods=3).sum().shift(1)
        day['reds_5'] = 5 - day.greens_5

        cd_bull = 0; cd_bear = 0
        for i in range(len(day)):
            r = day.iloc[i]
            if cd_bull > 0: cd_bull -= 1
            if cd_bear > 0: cd_bear -= 1
            if r.abs_delta < 100: continue

            direction = None
            if r.delta > 0 and r.color == 'RED': direction = 'BEAR'
            elif r.delta < 0 and r.color == 'GREEN': direction = 'BULL'
            elif r.body <= 1.0 and i > 0:
                prev_clr = day.iloc[i-1].color
                if r.delta > 0 and prev_clr == 'GREEN': direction = 'BEAR'
                elif r.delta < 0 and prev_clr == 'RED': direction = 'BULL'
            if not direction: continue
            if direction == 'BULL' and cd_bull > 0: continue
            if direction == 'BEAR' and cd_bear > 0: continue

            greens = r.greens_5 if not pd.isna(r.greens_5) else 2.5
            reds = r.reds_5 if not pd.isna(r.reds_5) else 2.5
            trend_ok = False
            if direction == 'BEAR' and greens >= 3: trend_ok = True
            elif direction == 'BULL' and reds >= 3: trend_ok = True
            if pd.isna(r.greens_5): trend_ok = True
            if not trend_ok: continue

            is_doji = r.body < 1.0
            is_afternoon = r.tod >= 12.5
            tier = 0
            if is_doji: tier = 1
            elif is_afternoon and r.abs_delta >= 200: tier = 3
            if tier == 0: continue

            # V7 filters
            # Peak ratio cap
            if direction == 'BEAR':
                peak_abs = r.peak if r.peak > 0 else 0
            else:
                peak_abs = abs(r.trough) if r.trough < 0 else 0
            peak_ratio = peak_abs / max(r.abs_delta, 1)
            if peak_ratio >= cfg['peak_cap']:
                continue

            # Drop T3 14:00-14:30
            if cfg['drop_14'] and tier == 3 and 14.0 <= r.tod < 14.5:
                continue

            # Simulate — IMM SL8 gap8
            entry = r.c
            d = 1 if direction == 'BULL' else -1
            sl = 8; gap = 8
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
                'delta': int(r.delta), 'body': round(r.body, 2), 'tier': tier,
                'peak_ratio': round(peak_ratio, 2),
                'outcome': outcome, 'pnl': round(pnl, 2),
                'mfe': round(mfe, 2), 'mae': round(mae, 2)
            })
            if direction == 'BULL': cd_bull = 5
            else: cd_bear = 5

    # Report
    sigs = all_signals
    wins = sum(1 for s in sigs if s['pnl'] > 0)
    tp = sum(s['pnl'] for s in sigs)
    gw = sum(s['pnl'] for s in sigs if s['pnl'] > 0)
    gl = sum(s['pnl'] for s in sigs if s['pnl'] <= 0)
    pf = abs(gw/gl) if gl else 999
    running = 0; peak_eq = 0; maxdd = 0
    for s in sorted(sigs, key=lambda x: (x['date'], x['time'])):
        running += s['pnl']
        peak_eq = max(peak_eq, running)
        maxdd = min(maxdd, running - peak_eq)
    wr = 100*wins/len(sigs) if sigs else 0

    is_final = 'FINAL' in cfg_name
    marker = " <<<" if is_final else ""
    print(f"{cfg_name:<45} {len(sigs):>4} {len(sigs)/n_days:>5.1f}/d {wr:>5.1f}% {tp:>+8.1f} {maxdd:>7.1f} {pf:>6.2f}{marker}")

    if is_final:
        final_sigs = sigs

# Detailed report for FINAL
print(f"\n{'='*100}")
print("V7 FINAL — DETAILED REPORT")
print(f"{'='*100}")

sigs = final_sigs
wins = sum(1 for s in sigs if s['pnl'] > 0)
tp = sum(s['pnl'] for s in sigs)
gw = sum(s['pnl'] for s in sigs if s['pnl'] > 0)
gl = sum(s['pnl'] for s in sigs if s['pnl'] <= 0)
pf = abs(gw/gl) if gl else 999

print(f"\nSignals: {len(sigs)} ({len(sigs)/n_days:.1f}/day)")
print(f"WR: {wins}/{len(sigs)} = {100*wins/len(sigs):.1f}%")
print(f"PnL: {tp:+.1f} pts ({tp/n_days:+.1f}/day)")
print(f"Gross Win: {gw:+.1f} | Gross Loss: {gl:+.1f}")
print(f"PF: {pf:.2f}")
print(f"Avg Win: {gw/wins:+.1f} | Avg Loss: {gl/(len(sigs)-wins):+.1f}")

# MaxDD
running = 0; peak_eq = 0; maxdd = 0
for s in sorted(sigs, key=lambda x: (x['date'], x['time'])):
    running += s['pnl']
    peak_eq = max(peak_eq, running)
    maxdd = min(maxdd, running - peak_eq)
print(f"MaxDD: {maxdd:.1f}")

# By tier
print(f"\n--- By Tier ---")
for t, tlbl in [(1,'T1 Doji'),(3,'T3 Afternoon')]:
    ts = [s for s in sigs if s['tier'] == t]
    if not ts: continue
    w = sum(1 for s in ts if s['pnl'] > 0)
    p = sum(s['pnl'] for s in ts)
    print(f"  {tlbl}: {len(ts)} sig, {100*w/len(ts):.1f}% WR, {p:+.1f}")

# By direction
print(f"\n--- By Direction ---")
for d in ['BULL','BEAR']:
    ds = [s for s in sigs if s['dir'] == d]
    if not ds: continue
    w = sum(1 for s in ds if s['pnl'] > 0)
    p = sum(s['pnl'] for s in ds)
    print(f"  {d}: {len(ds)} sig, {100*w/len(ds):.1f}% WR, {p:+.1f}")

# Daily breakdown
print(f"\n--- Daily PnL ---")
daily = defaultdict(list)
for s in sigs:
    daily[s['date']].append(s)

daily_pnls = []
for d in sorted(daily.keys()):
    ds = daily[d]
    dp = sum(s['pnl'] for s in ds)
    daily_pnls.append(dp)
    dw = sum(1 for s in ds if s['pnl'] > 0)
    marker = " ***" if dp >= 10 else " !!" if dp <= -10 else ""
    print(f"  {d}: {len(ds)} sig, {dw}W/{len(ds)-dw}L, {dp:+.1f}{marker}")

green = sum(1 for p in daily_pnls if p > 0)
no_sig = n_days - len(daily_pnls)
print(f"\n  Active days: {len(daily_pnls)}/{n_days}")
print(f"  Green days: {green}/{len(daily_pnls)} ({100*green/len(daily_pnls):.0f}%)")
print(f"  Avg daily: {np.mean(daily_pnls):+.1f}")
print(f"  Best: {max(daily_pnls):+.1f} | Worst: {min(daily_pnls):+.1f}")
print(f"  Median: {np.median(daily_pnls):+.1f}")

# Save
sdf = pd.DataFrame(sigs)
sdf.to_csv('exports/v7_final_signals.csv', index=False)
print(f"\nSaved: exports/v7_final_signals.csv")

# Print spec
print(f"\n{'='*100}")
print("V7 FINAL SPEC")
print(f"{'='*100}")
print("""
SIGNAL DETECTION:
  Core: bar_delta opposes bar_color (or doji with delta opposing prior trend)
  Min |delta|: 100
  Trend precondition: >=3 of last 5 bars in opposite direction
  Cooldown: 5 bars per direction

TIERS:
  T1 (Doji): body < 1.0 pt — any time 9:30-15:00
  T3 (Afternoon): 12:30-15:00, |delta| >= 200, NOT 14:00-14:30

FILTERS:
  Peak ratio < 2.5 (absorbed pressure not extreme)

TRAIL:
  Immediate: stop = max(maxProfit - 8, -8)
  SL = 8 pts
  Timeout = 100 bars

NO GRADING — all qualifying signals trade equally.
""")
