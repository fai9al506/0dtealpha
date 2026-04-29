"""V7 — Deep refinement on top of V6 IMM SL8 gap8 baseline (+264.8 pts)
Goal: squeeze out more PnL, higher WR, lower MaxDD"""
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

# Build enriched signals with ALL features for analysis
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

    # Price trend over last 5 and 10 bars
    day['trend_5'] = day.c - day.c.shift(5)
    day['trend_10'] = day.c - day.c.shift(10)

    # Consecutive same-direction bars before current
    consec = []
    streak = 0
    prev_green = None
    for idx in range(len(day)):
        cur_green = day.iloc[idx].is_green
        if prev_green is not None and cur_green == prev_green:
            streak += 1
        else:
            streak = 1
        consec.append(streak)
        prev_green = cur_green
    day['consec_bars'] = consec

    # Signal count today (for "skip first N" filter)
    sig_count_bull = 0
    sig_count_bear = 0

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
        if direction == 'BEAR' and greens >= 3: trend_ok = True
        elif direction == 'BULL' and reds >= 3: trend_ok = True
        if pd.isna(r.greens_5): trend_ok = True
        if not trend_ok:
            continue

        is_doji = r.body < 1.0
        is_afternoon = r.tod >= 12.5
        tier = 0
        if is_doji: tier = 1
        elif is_afternoon and r.abs_delta >= 200: tier = 3
        if tier == 0:
            continue

        if direction == 'BULL': sig_count_bull += 1
        else: sig_count_bear += 1
        sig_num = sig_count_bull if direction == 'BULL' else sig_count_bear

        # Peak ratio
        if direction == 'BEAR':
            peak_abs = r.peak if r.peak > 0 else 0
        else:
            peak_abs = abs(r.trough) if r.trough < 0 else 0
        peak_ratio = peak_abs / max(r.abs_delta, 1)

        dz_val = abs(r.dz) if not pd.isna(r.dz) else 0
        vz_val = r.vz if not pd.isna(r.vz) else 0
        trend5 = r.trend_5 if not pd.isna(r.trend_5) else 0
        trend10 = r.trend_10 if not pd.isna(r.trend_10) else 0

        # Simulate outcome — IMM SL8 gap8
        entry = r.c
        d_mult = 1 if direction == 'BULL' else -1
        sl = 8; gap = 8
        max_profit = 0.0; mfe = 0.0; mae = 0.0
        outcome = 'EXPIRED'; pnl_val = 0.0

        for j in range(i + 1, min(i + 101, len(day))):
            bar = day.iloc[j]
            cf = (bar.h - entry) if d_mult == 1 else (entry - bar.l)
            ca = (bar.l - entry) if d_mult == 1 else (entry - bar.h)
            mfe = max(mfe, cf); mae = min(mae, ca)
            trail = max(max_profit - gap, -sl)
            if ca <= trail:
                pnl_val = trail
                outcome = 'WIN' if pnl_val > 0 else 'LOSS'
                break
            max_profit = max(max_profit, cf)
        else:
            last = day.iloc[min(i + 100, len(day) - 1)]
            pnl_val = (last.c - entry) * d_mult
            outcome = 'WIN' if pnl_val > 0 else 'LOSS'

        all_signals.append({
            'date': str(date), 'time': str(r.ts_et)[11:19], 'dir': direction,
            'delta': int(r.delta), 'abs_d': int(r.abs_delta),
            'body': round(r.body, 2), 'tier': tier,
            'peak': int(peak_abs), 'peak_ratio': round(peak_ratio, 2),
            'vol': int(r.vol), 'vps': round(r.vps, 1),
            'dz': round(dz_val, 2), 'vz': round(vz_val, 2),
            'greens5': greens, 'reds5': reds,
            'trend5': round(trend5, 1), 'trend10': round(trend10, 1),
            'consec': r.consec_bars, 'sig_num': sig_num,
            'tod': round(r.tod, 2),
            'outcome': outcome, 'pnl': round(pnl_val, 2),
            'mfe': round(mfe, 2), 'mae': round(mae, 2),
            'is_win': 1 if pnl_val > 0 else 0
        })

        if direction == 'BULL': cd_bull = 5
        else: cd_bear = 5

sdf = pd.DataFrame(all_signals)
print(f"Total signals: {len(sdf)} | WR: {sdf.is_win.mean()*100:.1f}% | PnL: {sdf.pnl.sum():+.1f}")

# ============================================================
# DEEP ANALYSIS: what separates winners from losers?
# ============================================================
w = sdf[sdf.is_win == 1]
l = sdf[sdf.is_win == 0]

print(f"\n{'='*80}")
print("FEATURE ANALYSIS — Winners vs Losers")
print(f"{'='*80}")

features = {
    'abs_d': '|Delta|',
    'body': 'Body',
    'peak_ratio': 'Peak Ratio',
    'vps': 'Vol/sec',
    'dz': 'Delta Z',
    'vz': 'Vol Z',
    'greens5': 'Greens/5',
    'trend5': 'Trend 5bar',
    'trend10': 'Trend 10bar',
    'consec': 'Consec Bars',
    'sig_num': 'Signal # Today',
    'tod': 'Time of Day',
    'mfe': 'MFE',
}

print(f"\n{'Feature':<16} {'WIN':>8} {'LOSS':>8} {'Diff':>8} {'Sep?':<6}")
print("-" * 50)
for f, label in features.items():
    wm = w[f].mean()
    lm = l[f].mean()
    diff = wm - lm
    pct = abs(100 * diff / max(abs(lm), 0.01))
    sep = "YES" if pct > 20 else "weak" if pct > 10 else "NO"
    print(f"{label:<16} {wm:>8.1f} {lm:>8.1f} {diff:>+8.1f} {sep:<6}")

# Bucket analysis for each feature
print(f"\n{'='*80}")
print("BUCKET ANALYSIS")
print(f"{'='*80}")

def bucket_analysis(df, col, buckets, label):
    print(f"\n--- {label} ---")
    for lo, hi, blbl in buckets:
        sub = df[(df[col] >= lo) & (df[col] < hi)]
        if len(sub) < 3: continue
        w = sub.is_win.sum()
        p = sub.pnl.sum()
        print(f"  {blbl:<20} {len(sub):>4} sig, {100*w/len(sub):>5.1f}% WR, {p:>+8.1f} PnL")

bucket_analysis(sdf, 'abs_d', [
    (100,200,'100-200'),(200,300,'200-300'),(300,500,'300-500'),
    (500,800,'500-800'),(800,9999,'800+')
], '|Delta| Buckets')

bucket_analysis(sdf, 'peak_ratio', [
    (0,0.5,'<0.5x'),(0.5,1.0,'0.5-1.0x'),(1.0,1.5,'1.0-1.5x'),
    (1.5,2.5,'1.5-2.5x'),(2.5,99,'2.5x+')
], 'Peak Ratio (absorbed pressure)')

bucket_analysis(sdf, 'dz', [
    (0,0.3,'<0.3'),(0.3,0.7,'0.3-0.7'),(0.7,1.2,'0.7-1.2'),
    (1.2,2.0,'1.2-2.0'),(2.0,99,'2.0+')
], 'Delta Z-Score')

bucket_analysis(sdf, 'vps', [
    (0,30,'<30 slow'),(30,60,'30-60'),(60,100,'60-100'),(100,9999,'100+ fast')
], 'Vol Intensity (vol/sec)')

bucket_analysis(sdf, 'consec', [
    (1,2,'1 (no streak)'),(2,4,'2-3 bar streak'),(4,6,'4-5 bar streak'),
    (6,99,'6+ bar streak')
], 'Consecutive Same-Dir Bars Before Signal')

bucket_analysis(sdf, 'sig_num', [
    (1,2,'1st signal'),(2,3,'2nd signal'),(3,4,'3rd signal'),
    (4,99,'4th+ signal')
], 'Signal # Today (per direction)')

bucket_analysis(sdf, 'trend5', [
    (-99,-10,'strong down <-10'),(-10,-3,'down -10 to -3'),(-3,3,'flat -3 to +3'),
    (3,10,'up +3 to +10'),(10,99,'strong up >+10')
], 'Price Trend Last 5 Bars (for ALL signals)')

# Bearish signals: trend5 should be positive (price was going up before reversal)
print(f"\n--- Trend5 for BEAR signals (want positive = price was rising) ---")
bear = sdf[sdf.dir == 'BEAR']
for lo, hi, blbl in [(-99,0,'trend5 < 0 (already falling)'),(0,5,'trend5 0-5'),(5,10,'trend5 5-10'),(10,99,'trend5 > 10 (strong rise)')]:
    sub = bear[(bear.trend5 >= lo) & (bear.trend5 < hi)]
    if len(sub) < 3: continue
    w = sub.is_win.sum()
    p = sub.pnl.sum()
    print(f"  {blbl:<30} {len(sub):>4} sig, {100*w/len(sub):>5.1f}% WR, {p:>+8.1f}")

print(f"\n--- Trend5 for BULL signals (want negative = price was falling) ---")
bull = sdf[sdf.dir == 'BULL']
for lo, hi, blbl in [(-99,-10,'trend5 < -10 (strong fall)'),(-10,-3,'trend5 -10 to -3'),(-3,3,'trend5 flat'),(3,99,'trend5 > 3 (already rising)')]:
    sub = bull[(bull.trend5 >= lo) & (bull.trend5 < hi)]
    if len(sub) < 3: continue
    w = sub.is_win.sum()
    p = sub.pnl.sum()
    print(f"  {blbl:<30} {len(sub):>4} sig, {100*w/len(sub):>5.1f}% WR, {p:>+8.1f}")

# T1 vs T3 separate analysis
print(f"\n{'='*80}")
print("TIER-SPECIFIC REFINEMENTS")
print(f"{'='*80}")

for t, tlbl in [(1,'T1 Doji'),(3,'T3 Afternoon')]:
    ts = sdf[sdf.tier == t]
    tw = ts.is_win.sum()
    print(f"\n--- {tlbl}: {len(ts)} sig, {100*tw/len(ts):.1f}% WR, {ts.pnl.sum():+.1f} ---")

    # Direction
    for d in ['BULL','BEAR']:
        sub = ts[ts.dir == d]
        if len(sub) < 3: continue
        w = sub.is_win.sum()
        print(f"  {d}: {len(sub)} sig, {100*w/len(sub):.1f}% WR, {sub.pnl.sum():+.1f}")

    # Time sub-buckets
    if t == 3:
        for lo, hi, lbl in [(12.5,13.0,'12:30-13:00'),(13.0,13.5,'13:00-13:30'),
                             (13.5,14.0,'13:30-14:00'),(14.0,14.5,'14:00-14:30'),(14.5,15.0,'14:30-15:00')]:
            sub = ts[(ts.tod >= lo) & (ts.tod < hi)]
            if len(sub) < 3: continue
            w = sub.is_win.sum()
            print(f"  {lbl}: {len(sub)} sig, {100*w/len(sub):.1f}% WR, {sub.pnl.sum():+.1f}")

    # Signal number
    for sn in [1,2,3]:
        sub = ts[ts.sig_num == sn]
        if len(sub) < 3: continue
        w = sub.is_win.sum()
        print(f"  Sig #{sn}: {len(sub)} sig, {100*w/len(sub):.1f}% WR, {sub.pnl.sum():+.1f}")
    sub = ts[ts.sig_num >= 4]
    if len(sub) >= 3:
        w = sub.is_win.sum()
        print(f"  Sig #4+: {len(sub)} sig, {100*w/len(sub):.1f}% WR, {sub.pnl.sum():+.1f}")

# ============================================================
# TEST REFINED FILTERS
# ============================================================
print(f"\n{'='*80}")
print("FILTER COMBINATIONS")
print(f"{'='*80}")

filters = [
    ('Baseline (V6)', lambda s: True),
    ('Drop T1 bull', lambda s: not (s.tier==1 and s.dir=='BULL')),
    ('T1: dz>=0.3', lambda s: s.tier==3 or s.dz>=0.3),
    ('T3: body<3', lambda s: s.tier==1 or s.body<3),
    ('T3: delta>=250', lambda s: s.tier==1 or s.abs_d>=250),
    ('T3: delta 200-800', lambda s: s.tier==1 or (s.abs_d>=200 and s.abs_d<800)),
    ('Skip sig#1 per dir', lambda s: s.sig_num >= 2),
    ('Consec >= 2', lambda s: s.consec >= 2),
    ('Peak ratio 0.5-2.5', lambda s: 0.5 <= s.peak_ratio <= 2.5),
    ('BEAR trend5>0 | BULL trend5<0', lambda s: (s.dir=='BEAR' and s.trend5>0) or (s.dir=='BULL' and s.trend5<0)),
    ('BEAR trend5>0 | BULL all', lambda s: s.dir=='BULL' or s.trend5>0),
    # Combos
    ('Drop T1 bull + consec>=2', lambda s: not (s.tier==1 and s.dir=='BULL') and s.consec>=2),
    ('Drop T1 bull + trend filter', lambda s: (not (s.tier==1 and s.dir=='BULL')) and ((s.dir=='BEAR' and s.trend5>0) or (s.dir=='BULL' and s.trend5<0))),
    ('Consec>=2 + trend filter', lambda s: s.consec>=2 and ((s.dir=='BEAR' and s.trend5>0) or (s.dir=='BULL' and s.trend5<0))),
    ('Best combo: consec>=2 + BEAR t5>0|BULL all', lambda s: s.consec>=2 and (s.dir=='BULL' or s.trend5>0)),
    ('Tight: consec>=2 + trend + dz>=0.3', lambda s: s.consec>=2 and s.dz>=0.3 and ((s.dir=='BEAR' and s.trend5>0) or (s.dir=='BULL' and s.trend5<0))),
    ('T3 delta300+ | T1 all', lambda s: s.tier==1 or s.abs_d>=300),
    ('T3 delta300+ | T1 bear', lambda s: (s.tier==1 and s.dir=='BEAR') or (s.tier==3 and s.abs_d>=300)),
]

print(f"\n{'Filter':<45} {'N':>4} {'N/d':>5} {'WR':>6} {'PnL':>8} {'MaxDD':>7} {'PF':>6}")
print("-" * 85)
for name, filt in filters:
    sub = sdf[sdf.apply(filt, axis=1)]
    if len(sub) < 5: continue
    w = sub.is_win.sum()
    p = sub.pnl.sum()
    gw = sub[sub.pnl > 0].pnl.sum()
    gl = sub[sub.pnl <= 0].pnl.sum()
    pf = abs(gw/gl) if gl else 999
    running = 0; peak_eq = 0; maxdd = 0
    for _, s in sub.sort_values(['date','time']).iterrows():
        running += s.pnl
        peak_eq = max(peak_eq, running)
        maxdd = min(maxdd, running - peak_eq)
    print(f"{name:<45} {len(sub):>4} {len(sub)/n_days:>5.1f} {100*w/len(sub):>5.1f}% {p:>+8.1f} {maxdd:>7.1f} {pf:>6.2f}")

# Top 5 — show daily breakdown
print(f"\n{'='*80}")
print("TOP FILTER — Daily Breakdown")
print(f"{'='*80}")

# Apply best combo
best_filt = lambda s: s.consec>=2 and (s.dir=='BULL' or s.trend5>0)
best = sdf[sdf.apply(best_filt, axis=1)].sort_values(['date','time'])
daily = defaultdict(list)
for _, s in best.iterrows():
    daily[s.date].append(s)

daily_pnls = []
for d in sorted(daily.keys()):
    ds = daily[d]
    dp = sum(s.pnl for s in ds)
    daily_pnls.append(dp)
    dw = sum(1 for s in ds if s.pnl > 0)
    marker = " ***" if dp >= 10 else " !!" if dp <= -10 else ""
    print(f"  {d}: {len(ds)} sig, {dw}W/{len(ds)-dw}L, {dp:+.1f}{marker}")

green = sum(1 for p in daily_pnls if p > 0)
print(f"\n  Green days: {green}/{len(daily_pnls)} ({100*green/len(daily_pnls):.0f}%)")
print(f"  Avg daily: {np.mean(daily_pnls):+.1f}")
print(f"  Best: {max(daily_pnls):+.1f} | Worst: {min(daily_pnls):+.1f}")
