# -*- coding: utf-8 -*-
"""S276d - the real scenario: a headline lands WHILE we hold. Find the sharpest
intraday SPX moves (5-min) and score the trades that were open through them."""
import os, sys, pandas as pd, numpy as np
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York")
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
px=pd.read_sql(text("""SELECT ts AT TIME ZONE 'America/New_York' et, bar_close FROM spx_ohlc_1m ORDER BY ts"""),c)
px['d']=px['et'].dt.date
px['m5']=px.groupby('d')['bar_close'].transform(lambda s: s.pct_change(5)*100)
sharp=px.reindex(px['m5'].abs().sort_values(ascending=False).index).head(25)
gaps=lf.load_gaps(c)
rows=c.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
c.close()
b=[]
for r in rows:
    if not lf.passes_v16(r,gaps): continue
    d=dict(r); d['et']=d['ts'].astimezone(ET).replace(tzinfo=None)
    d['exit']=d['et']+timedelta(minutes=float(d.get('outcome_elapsed_min') or 30))
    d['pts']=float(d['outcome_pnl']); b.append(d)
df=pd.DataFrame(b)
print("="*104)
print("SHARPEST 5-MIN SPX MOVES (the 'headline just hit' moment) and the V16 trades open through them")
print("="*104)
tot_n=tot_p=0; seen=set()
for _,s in sharp.iterrows():
    t=s['et']
    if any(abs((t-x).total_seconds())<1800 for x in seen): continue
    seen.add(t)
    hit=df[(df['et']<=t)&(df['exit']>=t)]
    tot_n+=len(hit); tot_p+=hit['pts'].sum()
    lst=", ".join(f"{r['setup_name'][:9]}/{'L' if str(r['direction']).lower() in ('long','bullish') else 'S'}{r['pts']:+.0f}" for _,r in hit.iterrows())
    print(f"  {t:%Y-%m-%d %H:%M}  SPX {s['m5']:+5.2f}% in 5 min   open trades: {len(hit)}  "
          f"their P&L {hit['pts'].sum():+7.1f} pt   {lst[:70]}")
print(f"\n  TOTAL across these shock minutes: {tot_n} trades, {tot_p:+.1f} pt "
      f"(avg {tot_p/max(tot_n,1):+.2f} pt/trade)")
print(f"  book-wide average for comparison: {df['pts'].mean():+.2f} pt/trade over {len(df)} trades")
