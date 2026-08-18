# -*- coding: utf-8 -*-
"""Does open 0/1/2 (filter+2x conf) amplify GREEN days more than RED days vs open 0/0/1?
Verified V16-base, 3-month, chain pts @1MES, 1h basket."""
import os, sys, pandas as pd, numpy as np, yfinance as yf
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-27' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base); df['long']=df['direction'].isin(['long','bullish']); df['pts']=df['outcome_pnl'].astype(float); df['d']=df['et'].dt.date
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='90d',interval='1h',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1); parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df=df.sort_values('et').reset_index(drop=True)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('75min'))['bp'].values
k=pd.Series(np.where(df['b_now'].isna()|(df['b_now'].abs()<0.15),'neu',np.where((df['b_now']>0)==df['long'],'conf','fight')),index=df.index)
df['m001']=k.map({'fight':0,'neu':0,'conf':1}); df['m012']=k.map({'fight':0,'neu':1,'conf':2})
day=df.groupby('d').apply(lambda g:pd.Series({'o001':(g['pts']*g['m001']).sum(),'o012':(g['pts']*g['m012']).sum()})).reset_index()
day['o001']*=5; day['o012']*=5  # $@1MES
green=day[day['o001']>0]; red=day[day['o001']<0]; flat=day[day['o001']==0]
print(f"3-month days: {len(day)}  (green {len(green)} / red {len(red)} / flat {len(flat)}) — green/red by open 0/0/1 sign\n")
def blk(g,lab):
    a=g['o001'].sum(); b=g['o012'].sum()
    print(f"  {lab:<16} days={len(g):<3} open0/0/1=${a:>+8.0f}  open0/1/2=${b:>+8.0f}  extra=${b-a:>+7.0f}  ratio={b/a if a else float('nan'):.2f}x")
blk(green,'GREEN days'); blk(red,'RED days'); blk(day,'ALL days')
print(f"\n  >> GREEN amplification ${green['o012'].sum()-green['o001'].sum():+.0f}  vs  RED amplification ${red['o012'].sum()-red['o001'].sum():+.0f}")
print(f"  >> on green days 0/1/2 makes {green['o012'].sum()/green['o001'].sum():.2f}x ; on red days it loses {red['o012'].sum()/red['o001'].sum():.2f}x\n")
print("Top 6 GREEN days (open0/0/1 -> open0/1/2):")
for _,r in green.sort_values('o001',ascending=False).head(6).iterrows():
    print(f"  {r['d']}: ${r['o001']:+.0f} -> ${r['o012']:+.0f}  (x{r['o012']/r['o001']:.1f})")
print("Top 6 RED days:")
for _,r in red.sort_values('o001').head(6).iterrows():
    print(f"  {r['d']}: ${r['o001']:+.0f} -> ${r['o012']:+.0f}  (x{r['o012']/r['o001']:.1f})")
