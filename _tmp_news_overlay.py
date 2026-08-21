# -*- coding: utf-8 -*-
import os, sys, pandas as pd, numpy as np
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"]); conn=eng.connect()
ev=pd.read_sql(text("SELECT DISTINCT impact, count(*) n FROM economic_events GROUP BY impact ORDER BY n DESC"),conn)
print("impact levels:", dict(zip(ev['impact'],ev['n'])))
# high-impact US events per ET day
hi=pd.read_sql(text("""SELECT date(ts AT TIME ZONE 'America/New_York') d, string_agg(DISTINCT title,', ') titles
   FROM economic_events WHERE impact IN ('High','high','3') AND country IN ('US','USD','United States')
   AND ts AT TIME ZONE 'America/New_York' >= '2026-03-01'
   GROUP BY d ORDER BY d"""),conn)
hi['d']=pd.to_datetime(hi['d']).dt.date
# our daily SB P&L (stamped basket = realistic) + base
gaps=lf.load_gaps(conn)
rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
conn.close()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base); df['long']=df['direction'].isin(['long','bullish']); df['pts']=df['outcome_pnl'].astype(float)
df['stamped']=pd.to_numeric(df['basket_pct'],errors='coerce'); df['d']=df['et'].dt.date
df['o001']=np.where(df['stamped'].isna(),1.0,np.where(df['stamped'].abs()<0.15,0.0,np.where((df['stamped']>0)==df['long'],1.0,0.0)))
daily=df.groupby('d').agg(base_=('pts','sum'),sb=('pts',lambda s:(s*df.loc[s.index,'o001']).sum())).reset_index()
daily['base_$']=daily['base_']*5; daily['sb_$']=daily['sb']*5
m=daily.merge(hi,on='d',how='left'); m['news']=m['titles'].notna()
print(f"\nHigh-impact US news days: {m['news'].sum()} / {len(m)} trading days")
print(f"  our base_$ on NEWS days:  {m[m['news']]['base_$'].sum():+.0f}   (avg {m[m['news']]['base_$'].mean():+.0f}/day)")
print(f"  our base_$ on QUIET days: {m[~m['news']]['base_$'].sum():+.0f}   (avg {m[~m['news']]['base_$'].mean():+.0f}/day)")
print("\n=== 8 WORST base days + the news that day ===")
for _,r in m.sort_values('base_$').head(8).iterrows():
    print(f"  {r['d']}  base=${r['base_$']:+.0f}  news: {str(r['titles'])[:90] if pd.notna(r['titles']) else '(none scheduled)'}")
print("\n=== June day-by-day (the bleed month) with news ===")
for _,r in m[pd.to_datetime(m['d']).dt.month==6].sort_values('d').iterrows():
    print(f"  {r['d']}  base=${r['base_$']:+6.0f} sb=${r['sb_$']:+6.0f}  {str(r['titles'])[:80] if pd.notna(r['titles']) else ''}")
