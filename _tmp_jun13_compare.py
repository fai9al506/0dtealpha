# -*- coding: utf-8 -*-
"""Jun-13+ : TSRT broker actual (live SB open 0/0/1) vs sim schemes on verified V16-base.
Schemes: V16-base / open 0/0/1 / open 0/1/2 / mom 0/0/1 / mom 0/1/2. chain pts @1MES, dense 5m basket."""
import os, sys, pandas as pd, numpy as np, yfinance as yf, json
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"]); conn=eng.connect()
# 1) TSRT broker actual (real $)
tsrt=pd.read_sql(text("""SELECT day, net, n_trades, n_wins FROM tsrt_daily_stmt
   WHERE day >= '2026-06-13' ORDER BY day"""),conn)
print("=== TSRT BROKER ACTUAL (live = SB open 0/0/1), Jun-13+ ===")
print(tsrt.to_string(index=False))
print(f"  TSRT total net = ${tsrt['net'].sum():+.2f}   trades={int(tsrt['n_trades'].sum())}  wins={int(tsrt['n_wins'].sum())}\n")
# 2) verified base
gaps=lf.load_gaps(conn)
rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
    f"WHERE (ts AT TIME ZONE 'America/New_York')::date >= '2026-06-13' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base); df['long']=df['direction'].isin(['long','bullish']); df['pts']=df['outcome_pnl'].astype(float); df['d']=df['et'].dt.date
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1); parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df=df.sort_values('et').reset_index(drop=True)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(minutes=30); tt=tt.sort_values('q')
df['mom']=df['b_now']-pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min')).sort_index()['bp'].reindex(df.index).values
def kl(ref):
    v=df['b_now'] if ref=='open' else df['mom']
    return pd.Series(np.where(v.isna()|(v.abs()<0.15),'neu',np.where((v>0)==df['long'],'conf','fight')),index=df.index)
df['ko']=kl('open'); df['km']=kl('mom')
GATE={'fight':0,'neu':0,'conf':1}; SZ={'fight':0,'neu':1,'conf':2}
def metr(col):
    pts=df['pts']*col; tot=pts.sum(); ntk=int((col>0).sum())
    w=((df['outcome_result']=='WIN')&(col>0)).sum(); wr=100*w/ntk if ntk else 0
    cum=pts.groupby(df['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    return tot,ntk,wr,dd*5
print(f"=== SIM schemes on verified V16-base, Jun-13+ (n={len(df)}, chain pts @1MES) ===")
print(f"{'scheme':<22}{'pts':>7}{'$@1MES':>8}{'trades':>8}{'WR':>5}{'maxDD$':>8}")
schemes=[('V16-base',pd.Series(1.0,index=df.index)),
         ('open 0/0/1 (=TSRT)',df['ko'].map(GATE)),('open 0/1/2',df['ko'].map(SZ)),
         ('mom 0/0/1',df['km'].map(GATE)),('mom 0/1/2',df['km'].map(SZ))]
for lab,col in schemes:
    tot,ntk,wr,dd=metr(col); print(f"{lab:<22}{tot:>7.1f}{tot*5:>8.0f}{ntk:>6}/{len(df)}{wr:>4.0f}%{dd:>8.0f}")
print("\n=== per-day (pts): base / open001 / open012 / mom001 / mom012 ===")
for d,g in df.groupby('d'):
    r=lambda col:(g['pts']*col.loc[g.index]).sum()
    print(f"  {d}: {g['pts'].sum():+6.1f} / {r(df['ko'].map(GATE)):+6.1f} / {r(df['ko'].map(SZ)):+6.1f} / {r(df['km'].map(GATE)):+6.1f} / {r(df['km'].map(SZ)):+6.1f}  (n={len(g)})")
conn.close()
