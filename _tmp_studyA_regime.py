# -*- coding: utf-8 -*-
"""STUDY A: why still bleeding? Day-by-day SPX regime (trend/vol) vs our V16-SB (open 0/0/1) P&L.
Is the bleed regime-driven (down-trend) & is June in-distribution or an outlier?"""
import os, sys, pandas as pd, numpy as np, yfinance as yf
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
    # SPX daily open/close from chain_snapshots
    spx=pd.read_sql(text("""WITH s AS (SELECT date(ts AT TIME ZONE 'America/New_York') d, ts, spot,
        first_value(spot) OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York') ORDER BY ts) o,
        last_value(spot) OVER (PARTITION BY date(ts AT TIME ZONE 'America/New_York') ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) c
        FROM chain_snapshots WHERE spot IS NOT NULL AND ts AT TIME ZONE 'America/New_York' >= '2026-03-01')
        SELECT DISTINCT d, o open_, c close_ FROM s ORDER BY d"""),conn)
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base); df['long']=df['direction'].isin(['long','bullish']); df['pts']=df['outcome_pnl'].astype(float)
df['vix']=pd.to_numeric(df['vix'],errors='coerce'); df['d']=df['et'].dt.date
# open 0/0/1 needs basket; but for regime we use V16-base P&L per day (the placed-ish book) + open001 via stamped
df['stamped']=pd.to_numeric(df['basket_pct'],errors='coerce')
df['o001']=np.where(df['stamped'].isna(),1.0,np.where(df['stamped'].abs()<0.15,0.0,np.where((df['stamped']>0)==df['long'],1.0,0.0)))
daily=df.groupby('d').agg(base_pts=('pts','sum'), sb_pts=('pts',lambda s:(s*df.loc[s.index,'o001']).sum()),
    vix=('vix','mean'), n=('pts','size')).reset_index()
daily['base_$']=daily['base_pts']*5; daily['sb_$']=daily['sb_pts']*5
spx['d']=pd.to_datetime(spx['d']).dt.date
spx['move']=spx['close_']-spx['open_']
m=daily.merge(spx[['d','open_','close_','move']],on='d',how='left')
m['move5']=m['close_'].diff(5)  # 5-day SPX trend
m=m.dropna(subset=['move'])
def reg(r):
    t='UP-trend' if r['move5']>15 else ('DOWN-trend' if r['move5']<-15 else 'chop')
    return t
m['regime']=m.apply(reg,axis=1)
m['vbucket']=np.where(m['vix']>=20,'VIX>=20','VIX<20')
print("=== Our V16-SB ($, sim) by SPX REGIME (Mar-Jun) ===")
print(f"{'regime':<12}{'days':>5}{'SB_$':>9}{'base_$':>9}{'avgVIX':>8}")
for rg,g in m.groupby('regime'):
    print(f"{rg:<12}{len(g):>5}{g['sb_$'].sum():>+9.0f}{g['base_$'].sum():>+9.0f}{g['vix'].mean():>8.1f}")
print(f"\n=== by VIX ===")
for vb,g in m.groupby('vbucket'):
    print(f"{vb:<12}{len(g):>5}{g['sb_$'].sum():>+9.0f}{g['base_$'].sum():>+9.0f}")
print(f"\n=== by MONTH (is June an outlier?) ===")
m['mo']=pd.to_datetime(m['d']).dt.to_period('M').astype(str)
for mo,g in m.groupby('mo'):
    upd=(g['move']>0).sum()
    print(f"  {mo}: SB_$={g['sb_$'].sum():>+7.0f}  base_$={g['base_$'].sum():>+7.0f}  days={len(g)}  up-days={upd}/{len(g)}  avgVIX={g['vix'].mean():.1f}  SPX net={g['move'].sum():+.0f}")
print(f"\n=== down-trend days: do we bleed? (SB_$ on DOWN-trend days, by month) ===")
dt=m[m['regime']=='DOWN-trend']
for mo,g in dt.groupby('mo'): print(f"  {mo}: down-trend days={len(g)}  SB_$={g['sb_$'].sum():+.0f}")
