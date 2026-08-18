# -*- coding: utf-8 -*-
"""Test: GEX Long EXCEPTION (take all incl. contradict, 2x) vs basket-gated. Is contradicted-GEX a winner?
Dense yfinance basket classify; chain + mes. HONEST on sample size."""
import os, sys, pandas as pd, numpy as np, yfinance as yf
from sqlalchemy import create_engine, text
import warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, mes_sim_outcome_pnl, outcome_result FROM setup_log "
        f"WHERE setup_name='GEX Long' AND (ts AT TIME ZONE 'America/New_York')>='2026-03-27' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
# GEX Long that passes V16-BASE (before basket) = the candidates
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base)
print(f"GEX Long passing V16-base, Mar27-now: {len(df)} trades total")
if len(df)==0: sys.exit()
df['long']=True; df['chain']=df['outcome_pnl'].astype(float)*5; df['mes']=pd.to_numeric(df['mes_sim_outcome_pnl'],errors='coerce')*5
df['d']=df['et'].dt.date
TK=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
px=yf.download(TK,period='60d',interval='5m',progress=False,auto_adjust=True)['Close'].tz_convert('America/New_York').between_time('09:30','15:59')
px['day']=px.index.date; parts=[]
for d,g in px.groupby('day'):
    o=g[TK].iloc[0]; b=((g[TK]-o)/o*100).mean(axis=1); parts.append(pd.DataFrame({'bt':g.index.tz_localize(None),'bp':b.values}))
bk=pd.concat(parts).sort_values('bt').reset_index(drop=True)
df=df.sort_values('et').reset_index(drop=True)
df['b']=pd.merge_asof(df[['et']],bk,left_on='et',right_on='bt',direction='backward',tolerance=pd.Timedelta('10min'))['bp'].values
# also use stamped basket_pct where present (June live)
df['bp_use']=df['b'].fillna(pd.to_numeric(df['basket_pct'],errors='coerce'))
df['cls']=np.where(df['bp_use'].isna(),'no_data',np.where(df['bp_use'].abs()<0.15,'neutral',np.where(df['bp_use']>0,'CONFIRM','contradict')))
print(f"basket coverage: {df['bp_use'].notna().mean()*100:.0f}%   date range {df['d'].min()} .. {df['d'].max()}\n")
print(f"{'bucket':<12}{'n':>4}{'WR':>6}{'chain$':>9}{'mes$':>9}  (mes n)")
for k in ['CONFIRM','neutral','contradict','no_data']:
    g=df[df['cls']==k]; n=len(g)
    if n==0: print(f"{k:<12}{0:>4}"); continue
    m=g[g['mes'].notna()]
    print(f"{k:<12}{n:>4}{(g['outcome_result']=='WIN').mean()*100:>5.0f}%{g['chain'].sum():>+9.0f}{m['mes'].sum():>+9.0f}   ({len(m)})")
print(f"\nALL GEX Long: n={len(df)} WR={(df['outcome_result']=='WIN').mean()*100:.0f}% chain={df['chain'].sum():+.0f} mes={df[df['mes'].notna()]['mes'].sum():+.0f}")
contra=df[df['cls']=='contradict']
print(f"\n>>> the EXCEPTION question: CONTRADICTED GEX Long = {len(contra)} trades, WR {(contra['outcome_result']=='WIN').mean()*100 if len(contra) else 0:.0f}%, chain {contra['chain'].sum():+.0f}, mes {contra[contra['mes'].notna()]['mes'].sum():+.0f}")

print("\n\n=== GEX Long by TIME-OF-DAY (the 'open always works' test) ===")
df['mins']=df['et'].dt.hour*60+df['et'].dt.minute
def bucket(m): return 'open <10:00' if m<600 else ('10-12' if m<720 else '12+')
df['tb']=df['mins'].apply(bucket)
print(f"{'window':<12}{'n':>4}{'WR':>6}{'chain$':>9}  basket-classes")
for tb in ['open <10:00','10-12','12+']:
    g=df[df['tb']==tb]; n=len(g)
    if n==0: print(f"{tb:<12}{0:>4}"); continue
    cls=g['cls'].value_counts().to_dict()
    print(f"{tb:<12}{n:>4}{(g['outcome_result']=='WIN').mean()*100:>5.0f}%{g['chain'].sum():>+9.0f}  {cls}")
print("\n--- GEX Long first 30 min (<10:00) detail ---")
for _,r in df[df['tb']=='open <10:00'].sort_values('et').iterrows():
    print(f"  {r['et'].strftime('%m-%d %H:%M')} basket={r['bp_use']:+.2f} {r['cls']:<11} chain=${r['chain']:+.0f} {r['outcome_result']}")
