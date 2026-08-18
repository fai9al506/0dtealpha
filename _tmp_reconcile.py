# -*- coding: utf-8 -*-
"""Reconcile my V16-base (passes_v16) to portal (Jun13+: 98t/39W/58L/-207.1). Portal: NO dedup, sums outcome_pnl."""
import os, sys, pandas as pd
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE (ts AT TIME ZONE 'America/New_York')::date >= '2026-06-13' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET)
df=pd.DataFrame(base)
df['date']=df['et'].dt.date; df['t']=df['et'].dt.strftime('%H:%M')
df['pnl']=df['outcome_pnl'].astype(float)
def rpt(d,lab):
    n=len(d); w=(d['outcome_result']=='WIN').sum(); l=(d['outcome_result']=='LOSS').sum()
    e=(~d['outcome_result'].isin(['WIN','LOSS'])).sum()
    print(f"  {lab:<34} n={n:<4} W={w} L={l} other={e}  pts={d['pnl'].sum():+.1f}")
print("PORTAL TARGET: n=98 W=39 L=58 pts=-207.1\n")
rpt(df,'RAW (>=Jun13, all results)')
rpt(df[df['outcome_result'].isin(['WIN','LOSS'])],'WIN/LOSS only')
rpt(df[df['date']>=pd.to_datetime('2026-06-14').date()],'>=Jun14')
rpt(df[df['et']>=pd.Timestamp('2026-06-13',tz=lf.ET)] if False else df,'(skip)')
# per-result-type breakdown of "other"
print("\n  result types present:", df['outcome_result'].value_counts().to_dict())
print("  setups present:", df['setup_name'].value_counts().to_dict())
print("\n=== PER-DAY base totals (cross-check vs portal daily) ===")
pd_=df.groupby('date').agg(n=('id','size'),W=('outcome_result',lambda s:(s=='WIN').sum()),pts=('pnl','sum')).round(1)
print(pd_.to_string())
print("\n=== FULL TRADE LIST (cross-check each vs portal) ===")
show=df[['id','date','t','setup_name','direction','grade','paradigm','outcome_result','pnl']].copy()
show['pnl']=show['pnl'].round(1)
print(show.to_string(index=False))
