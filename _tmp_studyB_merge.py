# -*- coding: utf-8 -*-
"""STUDY B: merge OPEN + MOMENTUM basket signals into a stronger gate/sizing.
Goal: (a) drop more worst trades, (b) rescue V-shape trades open drops. Verified base, 3-mo, chain $@1MES, 1h basket."""
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
tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(hours=2); tt=tt.sort_values('q')
df['mom']=df['b_now']-pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('90min')).sort_index()['bp'].reindex(df.index).values
def cl(v): return np.where(pd.isna(v)|(np.abs(v)<0.15),'neu',np.where((v>0)==df['long'],'conf','fight'))
df['ko']=cl(df['b_now']); df['km']=cl(df['mom'])
def metr(col):
    pts=df['pts']*col; tot=pts.sum()*5; ntk=int((col>0).sum()); w=((df['outcome_result']=='WIN')&(col>0)).sum(); wr=100*w/ntk if ntk else 0
    dd=((pts.groupby(df['d']).sum().sort_index().cumsum()).pipe(lambda c:(c.cummax()-c).max()))*5
    day=pts.groupby(df['d']).sum()*5; g=day[day>0].sum(); r=day[day<0].sum()
    return tot,ntk,wr,dd,g,r
def show(lab,col):
    t,n,wr,dd,g,r=metr(col); print(f"{lab:<26}${t:>+6.0f}{n:>6}{wr:>5.0f}%{dd:>8.0f}   green${g:+.0f} / red${r:+.0f}")
o,m=df['ko'],df['km']
print(f"{'scheme':<26}{'$tot':>7}{'trades':>6}{'WR':>6}{'maxDD':>8}   green/red")
# references
show('open 0/0/1', np.where(o=='conf',1.0,0.0))
show('open 0/1/2', np.where(o=='fight',0.0,np.where(o=='conf',2.0,1.0)))
# MERGES:
# M1 AND-confirm gate (both must confirm) -> drops most, highest quality
show('M1 AND-confirm 1x', np.where((o=='conf')&(m=='conf'),1.0,0.0))
# M2 either-fight DROP (drop if EITHER says fight), keep rest 1x -> (a) drop more worst
show('M2 drop-if-either-fight', np.where((o=='fight')|(m=='fight'),0.0,1.0))
# M3 open-gate + MOM RESCUE (take open-conf; also rescue open-fight when mom-conf = V-shape)  (b)
show('M3 open0/0/1 + momRescue', np.where((o=='conf')|((o!='conf')&(m=='conf')),1.0,0.0))
# M4 COMBINED SIZING: both-conf=2, both-fight=0, else 1  (a+b graded)
show('M4 combined 0/1/2-style', np.where((o=='fight')&(m=='fight'),0.0,np.where((o=='conf')&(m=='conf'),2.0,1.0)))
# M5 open0/1/2 + momRescue fighters (open skips fight, but mom-conf rescues them at 1x; conf 2x)
show('M5 open0/1/2+momRescueFight', np.where(o=='conf',2.0,np.where(o=='fight',np.where(m=='conf',1.0,0.0),1.0)))
