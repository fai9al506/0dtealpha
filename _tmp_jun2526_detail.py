# -*- coding: utf-8 -*-
import os, sys, json, pandas as pd, numpy as np, yfinance as yf
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"]); conn=eng.connect()
gaps=lf.load_gaps(conn)
rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, mes_sim_outcome_pnl, outcome_result FROM setup_log "
    f"WHERE (ts AT TIME ZONE 'America/New_York')::date IN ('2026-06-25','2026-06-26') AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base); df['long']=df['direction'].isin(['long','bullish']); df['d']=df['et'].dt.date
df['chain']=df['outcome_pnl'].astype(float)*5; df['mes']=pd.to_numeric(df['mes_sim_outcome_pnl'],errors='coerce')*5
# dense basket
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='30d',interval='5m',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1); parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df=df.sort_values('et').reset_index(drop=True)
df['b_now']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
df['cls']=np.where(df['b_now'].isna()|(df['b_now'].abs()<0.15),'neu',np.where((df['b_now']>0)==df['long'],'CONF','fight'))
# real pre-FIFO P&L
ids=tuple(int(x) for x in df['id'])
rt={}
for sid,st in conn.execute(text("SELECT setup_log_id, state FROM real_trade_orders WHERE setup_log_id IN :ids"),{"ids":ids}):
    s=st if isinstance(st,dict) else json.loads(st); il=s.get('direction') in ('long','bullish')
    en=s.get('fill_price'); ex=s.get('close_fill_price_pre_fifo_reconcile') or s.get('close_fill_price')
    q=s.get('quantity') or 1
    rt[sid]=((ex-en) if il else (en-ex))*q*5 if (en and ex) else None
conn.close()
df['real']=df['id'].map(rt)
print("=== Jun-25/26 V16-base trades (basket class shown) ===")
print(f"{'id':>5} {'t':>10} {'setup':<14}{'dir':<6}{'b%':>7}{'class':<6}{'chain$':>8}{'mes$':>8}{'real$':>8}")
for _,r in df.iterrows():
    print(f"{r['id']:>5} {r['et'].strftime('%m-%d %H:%M'):>10} {str(r['setup_name']):<14}{('long' if r['long'] else 'short'):<6}{(r['b_now'] if pd.notna(r['b_now']) else 0):>7.2f}{r['cls']:<6}{r['chain']:>8.0f}{(r['mes'] if pd.notna(r['mes']) else 0):>8.0f}{(r['real'] if pd.notna(r['real']) else 0):>8.0f}")
conf=df[df['cls']=='CONF']
print(f"\nCONFIRMED trades (what 0/0/1 takes, 0/1/2 doubles): {len(conf)}")
print(f"  chain ${conf['chain'].sum():+.0f}   mes-sim ${conf['mes'].sum():+.0f}   real(pre-FIFO) ${conf['real'].sum():+.0f}   (real present: {conf['real'].notna().sum()}/{len(conf)})")
print(f"  neutral={int((df['cls']=='neu').sum())}  fight={int((df['cls']=='fight').sum())}")
