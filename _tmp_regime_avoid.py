# -*- coding: utf-8 -*-
"""Can we AVOID low-vol-downgrind days using ONLY prior-day data (no look-ahead)?
Flag day D from: VIX at D-1 close + SPX cumulative move through D-1. Test if flagged days bleed ACROSS months."""
import os, sys, pandas as pd, numpy as np
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"]); conn=eng.connect()
gaps=lf.load_gaps(conn)
rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
spx=pd.read_sql(text("""WITH s AS (SELECT date(ts AT TIME ZONE 'America/New_York') d, ts, spot,
   first_value(spot) OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York') ORDER BY ts) o,
   last_value(spot) OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York') ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) c
   FROM chain_snapshots WHERE spot IS NOT NULL AND ts AT TIME ZONE 'America/New_York'>='2026-03-01')
   SELECT DISTINCT d, o, c FROM s ORDER BY d"""),conn)
conn.close()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base); df['pts']=df['outcome_pnl'].astype(float); df['vix']=pd.to_numeric(df['vix'],errors='coerce'); df['d']=df['et'].dt.date
daily=df.groupby('d').agg(pnl=('pts','sum'),vix=('vix','mean')).reset_index()
daily['$']=daily['pnl']*5
spx['d']=pd.to_datetime(spx['d']).dt.date; spx['move']=spx['c']-spx['o']
m=daily.merge(spx[['d','c','move']],on='d',how='left').sort_values('d').reset_index(drop=True)
# NO-LOOK-AHEAD predictors (shift = prior day):
m['vix_prior']=m['vix'].shift(1)
m['spx_3d_prior']=m['c'].shift(1)-m['c'].shift(4)   # 3-day SPX trend ending YESTERDAY
m['spx_5d_prior']=m['c'].shift(1)-m['c'].shift(6)
m['mo']=pd.to_datetime(m['d']).dt.to_period('M').astype(str)
m=m.dropna(subset=['vix_prior','spx_3d_prior'])
def test(vthr,ttd,col):
    flag=(m['vix_prior']<vthr)&(m[col]<ttd)
    fl=m[flag]; un=m[~flag]
    print(f"  VIX_prior<{vthr} & {col}<{ttd}:  flagged {len(fl)}d ${fl['$'].sum():+.0f} (avg {fl['$'].mean():+.0f})  | unflagged {len(un)}d ${un['$'].sum():+.0f}")
    # era stability: flagged-day $ per month
    if len(fl):
        permo=fl.groupby('mo')['$'].agg(['size','sum'])
        print(f"      flagged by month: "+"  ".join(f"{mo}:{int(r['size'])}d/${r['sum']:+.0f}" for mo,r in permo.iterrows()))
print("=== Flag low-vol-downgrind from PRIOR-DAY data only (no look-ahead) ===")
for v in (18,20):
    for col in ('spx_3d_prior','spx_5d_prior'):
        for t in (0,-20):
            test(v,t,col)
print("\n(if flagged days are negative in ONE month only = look-ahead/overfit; negative ACROSS months = avoidable)")
