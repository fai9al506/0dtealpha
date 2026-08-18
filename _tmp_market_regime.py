# -*- coding: utf-8 -*-
"""Market regime + fill-whippiness: WHY broker fills diverged from sim in June.
1) VIX + whippiness (give-back) by month from the portal CSV
2) SPX intraday reversal/chop regime by month from chain_snapshots (DB)
"""
import os, pandas as pd, numpy as np, psycopg2
pd.set_option('display.width',200)

CSV=r"C:/Users/Faisal/Downloads/trade_log_2026-06-22.csv"
df=pd.read_csv(CSV,encoding='utf-8-sig'); df.columns=[c.strip() for c in df.columns]
df['Date']=pd.to_datetime(df['Date']); df['Month']=df['Date'].dt.to_period('M').astype(str)
for c in ['P&L','Max Profit','Max Loss','VIX','Duration (min)']:
    df[c]=pd.to_numeric(df[c],errors='coerce')
df=df[df['P&L'].notna()].copy()
# give-back = how much of the peak (MFE) was handed back by exit (sim trail capture loss)
df['giveback']=df['Max Profit']-df['P&L']
# adverse excursion magnitude
df['mae']=df['Max Loss'].abs()

print("="*80); print("VIX + WHIPPINESS BY MONTH (portal sim)"); print("="*80)
g=df.groupby('Month').agg(
    n=('P&L','size'),
    vix_med=('VIX','median'),
    vix_max=('VIX','max'),
    avg_MFE=('Max Profit','mean'),
    avg_MAE=('mae','mean'),
    avg_giveback=('giveback','mean'),     # peak handed back before exit
    med_dur=('Duration (min)','median'),
).round(2)
print(g)

print("\nInterpretation: high avg_giveback = trades ran to a peak then reversed before the")
print("trail locked it (sim). Real MES fills amplify this — stops wick on the reversal bar.")

# DB: SPX intraday regime per day -> aggregate by month
DB=os.environ["DATABASE_URL"]
conn=psycopg2.connect(DB); cur=conn.cursor()
cur.execute("""
  WITH d AS (
    SELECT date(ts AT TIME ZONE 'America/New_York') dt,
           ts AT TIME ZONE 'America/New_York' et, spot
    FROM chain_snapshots
    WHERE spot IS NOT NULL
      AND date(ts AT TIME ZONE 'America/New_York') >= '2026-03-01'
      AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
  ),
  agg AS (
    SELECT dt,
      MAX(spot)-MIN(spot) rng,
      (array_agg(spot ORDER BY et ASC))[1] open,
      (array_agg(spot ORDER BY et DESC))[1] close
    FROM d GROUP BY dt
  )
  SELECT to_char(dt,'YYYY-MM') mon,
         COUNT(*) days,
         ROUND(AVG(rng)::numeric,1) avg_range,
         ROUND(AVG(ABS(close-open))::numeric,1) avg_net_move,
         ROUND(AVG(rng/NULLIF(ABS(close-open),0))::numeric,2) chop_ratio
  FROM agg GROUP BY mon ORDER BY mon
""")
print("\n"+"="*80); print("SPX INTRADAY REGIME BY MONTH (chain_snapshots, DB)"); print("="*80)
print(f"{'month':<9}{'days':>6}{'avg_range':>11}{'avg_net':>10}{'chop_ratio':>12}")
for mon,days,ar,nm,cr in cur.fetchall():
    print(f"{mon:<9}{days:>6}{float(ar):>11}{float(nm):>10}{float(cr):>12}")
print("\nchop_ratio = daily range / |close-open|. HIGH = whippy/reversal (big intraday")
print("swings, little net) -> the exact regime that wicks stops & defeats trailing.")
conn.close()
