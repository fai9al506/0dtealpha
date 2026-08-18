# -*- coding: utf-8 -*-
"""Drawdown of open 0/1/2 vs 0/0/1 vs baseline over 3 months. Verified base, chain $@1MES, 1h basket."""
import os, sys, pandas as pd, numpy as np, yfinance as yf
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-27' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
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
k=np.where(df['b_now'].isna()|(df['b_now'].abs()<0.15),'neu',np.where((df['b_now']>0)==df['long'],'conf','fight'))
sch={'baseline':np.ones(len(df)),'open 0/0/1':np.where(k=='conf',1.0,0.0),'open 0/1/2':np.where(k=='fight',0.0,np.where(k=='conf',2.0,1.0))}
def dd_detail(col):
    day=(df['pts']*col).groupby(df['d']).sum().sort_index()*5
    cum=day.cumsum(); peak=cum.cummax(); ddseries=cum-peak
    mdd=ddseries.min(); trough=ddseries.idxmin()
    peak_d=cum[:trough].idxmax() if (cum[:trough]>0).any() else cum.index[0]
    # recovery date
    after=cum[trough:]; pk=cum[:trough].max()
    rec=after[after>=pk].index.min() if (after>=pk).any() else None
    return cum.iloc[-1], mdd, peak_d, trough, rec
print(f"3-month (Mar27-Jun24), $@1MES:\n{'scheme':<14}{'total$':>8}{'maxDD$':>8}{'Ret/DD':>8}  worst stretch")
for nm,col in sch.items():
    tot,mdd,pk,tr,rec=dd_detail(col)
    recs=f"recovered {rec}" if rec else "not yet recovered"
    print(f"{nm:<14}{tot:>+8.0f}{mdd:>+8.0f}{tot/abs(mdd) if mdd else 0:>8.1f}  peak {pk} -> trough {tr} ({recs})")
