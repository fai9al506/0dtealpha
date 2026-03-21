"""
VIX-SPX Divergence Backtest
"""
import os, json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

print("=" * 80)
print("VIX-SPX DIVERGENCE BACKTEST")
print("=" * 80)

# Step 1: Get SPX + VIX at key timestamps
query_chain = text("""
WITH daily_data AS (
    SELECT
        DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
        ts AT TIME ZONE 'America/New_York' as ts_et,
        spot, vix,
        EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') as hr,
        EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') as mn
    FROM chain_snapshots
    WHERE spot IS NOT NULL AND vix IS NOT NULL
      AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 9
      AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') <= 16
),
open_snap AS (
    SELECT DISTINCT ON (trade_date)
        trade_date, spot as open_spot, vix as open_vix
    FROM daily_data WHERE (hr = 9 AND mn >= 33 AND mn <= 45)
    ORDER BY trade_date, ts_et
),
mid_snap AS (
    SELECT DISTINCT ON (trade_date)
        trade_date, spot as mid_spot, vix as mid_vix
    FROM daily_data WHERE (hr = 13 AND mn >= 0 AND mn <= 15)
    ORDER BY trade_date, ts_et
),
close_snap AS (
    SELECT DISTINCT ON (trade_date)
        trade_date, spot as close_spot, vix as close_vix
    FROM daily_data WHERE (hr = 15 AND mn >= 48) OR (hr = 16 AND mn = 0)
    ORDER BY trade_date, ts_et DESC
),
afternoon_high AS (
    SELECT trade_date, MAX(spot) as pm_high, MIN(spot) as pm_low
    FROM daily_data WHERE hr >= 13 GROUP BY trade_date
)
SELECT o.trade_date,
       o.open_spot, o.open_vix,
       m.mid_spot, m.mid_vix,
       c.close_spot, c.close_vix,
       a.pm_high, a.pm_low
FROM open_snap o
JOIN mid_snap m ON o.trade_date = m.trade_date
JOIN close_snap c ON o.trade_date = c.trade_date
JOIN afternoon_high a ON o.trade_date = a.trade_date
ORDER BY o.trade_date
""")

df = pd.read_sql(query_chain, engine)
print(f"\nTotal trading days: {len(df)}")
print(f"Date range: {df['trade_date'].min()} to {df['trade_date'].max()}")

df['spx_chg'] = df['mid_spot'] - df['open_spot']
df['vix_chg'] = df['mid_vix'] - df['open_vix']
df['mid_to_close'] = df['close_spot'] - df['mid_spot']
df['mid_to_high'] = df['pm_high'] - df['mid_spot']
df['mid_to_low'] = df['pm_low'] - df['mid_spot']
df['open_to_close'] = df['close_spot'] - df['open_spot']

# Show ALL days
print("\n" + "=" * 80)
print("ALL TRADING DAYS - VIX vs SPX (open to 13:00) and afternoon outcome")
print("=" * 80)
print(f"\n{'Date':12s} {'OSpx':>7s} {'MSpx':>7s} {'SPXchg':>7s} | {'OVix':>6s} {'MVix':>6s} {'VIXchg':>7s} | {'M->Cls':>7s} {'M->Hi':>7s} {'M->Lo':>7s} | {'Pattern':>12s}")
print("-" * 110)

for _, r in df.iterrows():
    # Classify the pattern
    vix_down = r['vix_chg'] < -0.5
    spx_flat = abs(r['spx_chg']) < 15
    if vix_down and spx_flat:
        pattern = "VIX-COMPRESS"
    elif r['vix_chg'] > 0.5 and spx_flat:
        pattern = "VIX-EXPAND"
    elif r['vix_chg'] < -0.5 and r['spx_chg'] > 10:
        pattern = "NORMAL-BULL"
    elif r['vix_chg'] > 0.5 and r['spx_chg'] < -10:
        pattern = "NORMAL-BEAR"
    else:
        pattern = "mixed"

    print(f"{str(r['trade_date']):12s} {r['open_spot']:7.0f} {r['mid_spot']:7.0f} {r['spx_chg']:+7.1f} | {r['open_vix']:6.2f} {r['mid_vix']:6.2f} {r['vix_chg']:+7.2f} | {r['mid_to_close']:+7.1f} {r['mid_to_high']:+7.1f} {r['mid_to_low']:+7.1f} | {pattern:>12s}")

# Group by pattern
print("\n" + "=" * 80)
print("PATTERN COMPARISON")
print("=" * 80)

def classify(r):
    vix_down = r['vix_chg'] < -0.5
    spx_flat = abs(r['spx_chg']) < 15
    if vix_down and spx_flat:
        return "VIX-COMPRESS"
    elif r['vix_chg'] > 0.5 and spx_flat:
        return "VIX-EXPAND"
    elif r['vix_chg'] < -0.5 and r['spx_chg'] > 10:
        return "NORMAL-BULL"
    elif r['vix_chg'] > 0.5 and r['spx_chg'] < -10:
        return "NORMAL-BEAR"
    else:
        return "mixed"

df['pattern'] = df.apply(classify, axis=1)

print(f"\n{'Pattern':15s} {'N':>4s} {'Avg M->Cls':>11s} {'Avg M->Hi':>11s} {'Avg M->Lo':>11s} {'Up%':>6s} {'Rally>10':>9s}")
print("-" * 75)
for pat in ["VIX-COMPRESS", "NORMAL-BULL", "NORMAL-BEAR", "VIX-EXPAND", "mixed"]:
    sub = df[df['pattern'] == pat]
    if len(sub) > 0:
        avg_cls = sub['mid_to_close'].mean()
        avg_hi = sub['mid_to_high'].mean()
        avg_lo = sub['mid_to_low'].mean()
        up = (sub['mid_to_close'] > 0).mean() * 100
        r10 = (sub['mid_to_close'] > 10).mean() * 100
        print(f"{pat:15s} {len(sub):4d} {avg_cls:+11.1f} {avg_hi:+11.1f} {avg_lo:+11.1f} {up:5.0f}% {r10:8.0f}%")

# Sensitivity on VIX drop threshold (use smaller thresholds)
print("\n" + "=" * 80)
print("SENSITIVITY: VIX DROP THRESHOLD (SPX flat <15 pts)")
print("=" * 80)

for thresh in [0.3, 0.5, 0.75, 1.0, 1.5]:
    mask = (df['vix_chg'] < -thresh) & (df['spx_chg'].abs() < 15)
    sub = df[mask]
    if len(sub) > 0:
        avg = sub['mid_to_close'].mean()
        hi = sub['mid_to_high'].mean()
        up = (sub['mid_to_close'] > 0).mean() * 100
        dates = ", ".join([str(d) for d in sub['trade_date']])
        print(f"VIX drop >{thresh:4.2f}: {len(sub):2d} days | M->Cls: {avg:+6.1f} | MFE: {hi:+6.1f} | Up: {up:3.0f}% | Dates: {dates}")
    else:
        print(f"VIX drop >{thresh:4.2f}:  0 days")

# Correlation analysis
print("\n" + "=" * 80)
print("CORRELATION: VIX change (open->13:00) vs SPX afternoon move")
print("=" * 80)

corr = df['vix_chg'].corr(df['mid_to_close'])
print(f"Correlation(VIX change, mid->close): {corr:.3f}")

corr2 = df['vix_chg'].corr(df['mid_to_high'])
print(f"Correlation(VIX change, mid->high):  {corr2:.3f}")

# If VIX fell more, did SPX rally more in the afternoon?
print("\nRegression-style: For every 1pt VIX drop (open->mid), afternoon SPX move:")
if len(df) > 3:
    from numpy.polynomial import polynomial as P
    coefs = np.polyfit(df['vix_chg'], df['mid_to_close'], 1)
    print(f"  Slope: {coefs[0]:+.1f} pts SPX per 1pt VIX change")
    print(f"  Intercept: {coefs[1]:+.1f}")
    print(f"  Meaning: 1pt VIX drop -> {-coefs[0]:+.1f} pts SPX afternoon move")

# Volland for ALL days at 13:00
print("\n" + "=" * 80)
print("VOLLAND METRICS AT 13:00 ET (ALL DAYS)")
print("=" * 80)

all_dates = [str(d) for d in df['trade_date']]
print(f"\n{'Date':12s} {'Pattern':>13s} {'Paradigm':>13s} {'Charm':>10s} {'SVB':>7s} {'DD':>12s} | {'M->Cls':>7s}")
print("-" * 90)

for dt in all_dates:
    q = text(f"""
    SELECT payload->'statistics'->>'paradigm' as paradigm,
           payload->'statistics'->>'aggregatedCharm' as charm,
           payload->'statistics'->>'spotVolBeta' as svb,
           payload->'statistics'->>'ddHedging' as dd_hedging
    FROM volland_snapshots
    WHERE DATE(ts AT TIME ZONE 'America/New_York') = '{dt}'
      AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 13
      AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') BETWEEN 0 AND 20
    ORDER BY ts LIMIT 1
    """)
    vr = pd.read_sql(q, engine)
    row = df[df['trade_date'].astype(str) == dt].iloc[0]

    if len(vr) > 0:
        v = vr.iloc[0]
        try:
            charm_val = float(str(v['charm']).replace(',',''))
            charm_d = f"{charm_val/1e6:+.0f}M"
        except:
            charm_d = "N/A"
        try:
            dd_val = float(str(v['dd_hedging']).replace('$','').replace(',',''))
            dd_d = f"${dd_val/1e9:+.1f}B"
        except:
            dd_d = "N/A"
        svb_d = str(v.get('svb','N/A'))[:7]
        para = str(v['paradigm'])[:13]
    else:
        charm_d = "N/A"
        dd_d = "N/A"
        svb_d = "N/A"
        para = "N/A"

    print(f"{dt:12s} {row['pattern']:>13s} {para:>13s} {charm_d:>10s} {svb_d:>7s} {dd_d:>12s} | {row['mid_to_close']:+7.1f}")

# Setup outcomes by pattern
print("\n" + "=" * 80)
print("SETUP OUTCOMES BY PATTERN")
print("=" * 80)

query_setups = text("""
SELECT DATE(fired_at AT TIME ZONE 'America/New_York')::text as trade_date,
       setup_name, direction, outcome, pnl_pts
FROM setup_log
WHERE outcome IS NOT NULL AND outcome IN ('WIN','LOSS')
ORDER BY fired_at
""")

try:
    sdf = pd.read_sql(query_setups, engine)
    if len(sdf) > 0:
        pat_map = dict(zip(df['trade_date'].astype(str), df['pattern']))
        sdf['pattern'] = sdf['trade_date'].map(pat_map)
        sdf = sdf.dropna(subset=['pattern'])

        for pat in ["VIX-COMPRESS", "NORMAL-BULL", "NORMAL-BEAR", "VIX-EXPAND", "mixed"]:
            sub = sdf[sdf['pattern'] == pat]
            if len(sub) > 0:
                w = (sub['outcome'] == 'WIN').sum()
                l = (sub['outcome'] == 'LOSS').sum()
                t = w + l
                wr = w/t*100 if t else 0
                pnl = sub['pnl_pts'].sum()
                ndays = len(sub['trade_date'].unique())
                print(f"\n{pat}: {t} trades across {ndays} days | {w}W/{l}L | WR: {wr:.0f}% | PnL: {pnl:+.1f}")

                for direction in ['long', 'short']:
                    ds = sub[sub['direction'] == direction]
                    if len(ds) > 0:
                        dw = (ds['outcome'] == 'WIN').sum()
                        dl = (ds['outcome'] == 'LOSS').sum()
                        dpnl = ds['pnl_pts'].sum()
                        dwr = dw/(dw+dl)*100 if (dw+dl) else 0
                        print(f"  {direction:8s}: {dw+dl:3d} | {dw}W/{dl}L | WR: {dwr:.0f}% | PnL: {dpnl:+.1f}")
except Exception as e:
    print(f"Setup query error: {e}")

# Also check earlier time: open to 11:00
print("\n" + "=" * 80)
print("EARLIER DETECTION: VIX drop open->11:00 (2hrs)")
print("=" * 80)

q11 = text("""
WITH open_snap AS (
    SELECT DISTINCT ON (DATE(ts AT TIME ZONE 'America/New_York'))
        DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
        spot as open_spot, vix as open_vix
    FROM chain_snapshots
    WHERE spot IS NOT NULL AND vix IS NOT NULL
      AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 9
      AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') BETWEEN 33 AND 45
    ORDER BY DATE(ts AT TIME ZONE 'America/New_York'), ts
),
hr11_snap AS (
    SELECT DISTINCT ON (DATE(ts AT TIME ZONE 'America/New_York'))
        DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
        spot as h11_spot, vix as h11_vix
    FROM chain_snapshots
    WHERE spot IS NOT NULL AND vix IS NOT NULL
      AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 11
      AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') BETWEEN 0 AND 15
    ORDER BY DATE(ts AT TIME ZONE 'America/New_York'), ts
),
close_snap AS (
    SELECT DISTINCT ON (DATE(ts AT TIME ZONE 'America/New_York'))
        DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
        spot as close_spot
    FROM chain_snapshots
    WHERE spot IS NOT NULL
      AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 15
      AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 48
    ORDER BY DATE(ts AT TIME ZONE 'America/New_York'), ts DESC
),
pm_range AS (
    SELECT DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
           MAX(spot) as pm_high
    FROM chain_snapshots
    WHERE spot IS NOT NULL
      AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 11
    GROUP BY DATE(ts AT TIME ZONE 'America/New_York')
)
SELECT o.trade_date,
       o.open_spot, o.open_vix,
       h.h11_spot, h.h11_vix,
       c.close_spot,
       p.pm_high,
       (h.h11_spot - o.open_spot) as spx_chg_2hr,
       (h.h11_vix - o.open_vix) as vix_chg_2hr,
       (c.close_spot - h.h11_spot) as h11_to_close,
       (p.pm_high - h.h11_spot) as h11_to_high
FROM open_snap o
JOIN hr11_snap h ON o.trade_date = h.trade_date
JOIN close_snap c ON o.trade_date = c.trade_date
JOIN pm_range p ON o.trade_date = p.trade_date
ORDER BY o.trade_date
""")

df11 = pd.read_sql(q11, engine)
print(f"\nDays with 11:00 data: {len(df11)}")

print(f"\n{'Date':12s} {'SPX 2hr':>8s} {'VIX 2hr':>8s} | {'11->Cls':>8s} {'11->Hi':>8s} | {'Pattern':>13s}")
print("-" * 75)

for _, r in df11.iterrows():
    vd = r['vix_chg_2hr'] < -0.5
    sf = abs(r['spx_chg_2hr']) < 15
    pat = "COMPRESS" if vd and sf else ("NORMAL" if not sf else "other")
    print(f"{str(r['trade_date']):12s} {r['spx_chg_2hr']:+8.1f} {r['vix_chg_2hr']:+8.2f} | {r['h11_to_close']:+8.1f} {r['h11_to_high']:+8.1f} | {pat:>13s}")

print("\n" + "=" * 80)
print("DONE - Note: Only 17 trading days in DB. Pattern needs external VIX data for larger sample.")
print("=" * 80)
