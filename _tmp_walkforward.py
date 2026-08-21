# -*- coding: utf-8 -*-
"""Walk-forward: pick best momentum (window,threshold) on APRIL only, then test FROZEN on MAY-JUN (unseen)."""
import os, psycopg2, pandas as pd, numpy as np, yfinance as yf
from datetime import timedelta
import warnings; warnings.filterwarnings('ignore')
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1)
    parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
df=pd.read_sql("""SELECT id, ts AT TIME ZONE 'America/New_York' et, setup_name nm, direction dir,
   outcome_pnl cpnl, outcome_result res FROM setup_log WHERE live_pass=true
   AND ts AT TIME ZONE 'America/New_York'>='2026-03-27' AND outcome_pnl IS NOT NULL ORDER BY ts""",c); c.close()
df['et']=pd.to_datetime(df['et']).dt.tz_localize(None); df['long']=df['dir'].isin(['long','bullish'])
df=df.sort_values('et'); keep=[]; last={}
for _,r in df.iterrows():
    k=(r['nm'],r['long'])
    if k in last and (r['et']-last[k])<timedelta(minutes=15): continue
    last[k]=r['et']; keep.append(r)
df=pd.DataFrame(keep).sort_values('et').reset_index(drop=True)
df['d']=df['et'].dt.date; df['pnl']=df['cpnl'].astype(float)*5; df['mo']=pd.to_datetime(df['et']).dt.to_period('M').astype(str)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
def prior(win):
    tt=df[['et']].copy(); tt['q']=tt['et']-pd.Timedelta(minutes=win); tt=tt.sort_values('q')
    return pd.merge_asof(tt,bk,left_on='q',right_on='bt',direction='backward',tolerance=pd.Timedelta('15min')).sort_index()['bp'].reindex(df.index).values
for w in (15,30,45): df[f'mom{w}']=df['b_now']-prior(w)
def mom_mult(r,w,th):
    m=r[f'mom{w}']
    if pd.isna(m) or abs(m)<th: return 1.0
    return 2.0 if (m>0)==r['long'] else 0.5
def open_mult(r):
    b=r['b_now']
    if pd.isna(b) or abs(b)<0.15: return 1.0
    return 2.0 if (b>0)==r['long'] else 0.5
def metrics(sub,col):
    pnl=sub['pnl']*sub[col]; tot=pnl.sum()
    cum=pnl.groupby(sub['d']).sum().sort_index().cumsum(); dd=(cum.cummax()-cum).max()
    return tot,(tot/dd if dd>0 else np.nan),dd

GRID=[(w,th) for w in (15,30,45) for th in (0.10,0.15,0.20,0.30)]
for w,th in GRID: df[f'm_{w}_{th}']=df.apply(lambda r:mom_mult(r,w,th),axis=1)
df['m_open']=df.apply(open_mult,axis=1); df['m_base']=1.0

train=df[df['mo']=='2026-04']; test=df[df['mo'].isin(['2026-05','2026-06'])]
print(f"TRAIN=April n={len(train)}   TEST=May-Jun n={len(test)}\n")
print("--- APRIL (train) grid, ranked by Ret/DD ---")
rows=[]
for w,th in GRID:
    t,rdd,dd=metrics(train,f'm_{w}_{th}'); rows.append((w,th,t,rdd,dd))
rows.sort(key=lambda x:-x[3])
for w,th,t,rdd,dd in rows[:6]: print(f"  mom{w} th{th}: April$={t:.0f}  Ret/DD={rdd:.1f}  DD={dd:.0f}")
bw,bth=rows[0][0],rows[0][1]
print(f"\n>>> LOCKED by April Ret/DD: mom{bw} th{bth}")
# also locked by April $ (alt)
rows2=sorted(rows,key=lambda x:-x[2]); aw,ath=rows2[0][0],rows2[0][1]
print(f">>> (alt) LOCKED by April $: mom{aw} th{ath}")

print("\n--- TEST May-Jun (FROZEN, unseen) ---")
for lab,col in [('Baseline','m_base'),('Open-semi(study)','m_open'),
                (f'LOCKED mom{bw} th{bth}',f'm_{bw}_{bth}'),(f'alt mom{aw} th{ath}',f'm_{aw}_{ath}')]:
    t,rdd,dd=metrics(test,col); tb,_,_=metrics(test,'m_base')
    print(f"  {lab:<22} test$={t:.0f}  vsBase={t/tb:.2f}x  Ret/DD={rdd:.1f}  DD={dd:.0f}")
# split test by month for stability
print("\n  test by month (locked vs base vs open):")
for mo in ['2026-05','2026-06']:
    g=test[test['mo']==mo]
    b,_,_=metrics(g,'m_base'); o,_,_=metrics(g,'m_open'); l,_,_=metrics(g,f'm_{bw}_{bth}')
    print(f"    {mo}: base={b:.0f}  open-semi={o:.0f}  locked={l:.0f}")
