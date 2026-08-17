# -*- coding: utf-8 -*-
import os, sys, pandas as pd, numpy as np
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); rng=np.random.default_rng(11)
EV=['2026-03-02','2026-03-03','2026-03-06','2026-03-09','2026-03-13','2026-03-20','2026-03-23',
    '2026-03-26','2026-03-27','2026-03-30','2026-03-31','2026-04-13','2026-04-14','2026-06-17',
    '2026-06-18','2026-07-06','2026-07-07','2026-07-08','2026-07-23','2026-07-24']
EVD=set(pd.Timestamp(x).date() for x in EV)
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
rows=c.execute(text(f"SELECT {lf.COLS}, outcome_pnl FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
c.close()
b=[]
for r in rows:
    if not lf.passes_v16(r,gaps): continue
    d=dict(r); d['et']=d['ts'].astimezone(ET).replace(tzinfo=None)
    d['long']=str(d.get('direction','')).lower() in ('long','bullish'); b.append(d)
df=pd.DataFrame(b); df['pts']=df['outcome_pnl'].astype(float); df['d']=df['et'].dt.date
df['ev']=df['d'].isin(EVD); df['side']=np.where(df['long'],'LONG','SHORT')
print("="*100); print("4. WAR DAYS BY SIDE  (per-trade points, V16 signals, no cap)"); print("="*100)
for side,s in df.groupby('side'):
    e=s[s['ev']]; o=s[~s['ev']]
    print(f"  {side:5s}  war-day n={len(e):3d} avg {e['pts'].mean():+5.2f} pt  WR {(e['pts']>0).mean()*100:3.0f}%   |   "
          f"other n={len(o):4d} avg {o['pts'].mean():+5.2f} pt  WR {(o['pts']>0).mean()*100:3.0f}%")
print(); print("="*100); print("5. BY SETUP ON WAR DAYS"); print("="*100)
for su,s in df.groupby('setup_name'):
    e=s[s['ev']]
    if len(e)<5: continue
    o=s[~s['ev']]
    print(f"  {su:18s} war n={len(e):3d} avg {e['pts'].mean():+5.2f} pt WR {(e['pts']>0).mean()*100:3.0f}%   |   "
          f"other n={len(o):4d} avg {o['pts'].mean():+5.2f} pt WR {(o['pts']>0).mean()*100:3.0f}%   "
          f"delta {e['pts'].mean()-o['pts'].mean():+5.2f}")
print(); print("="*100); print("6. VIX BUCKETS - per-trade, is high VIX really better?"); print("="*100)
df['vixf']=pd.to_numeric(df['vix'],errors='coerce')
for lo,hi in [(0,16),(16,18),(18,20),(20,22),(22,26),(26,99)]:
    s=df[(df['vixf']>=lo)&(df['vixf']<hi)]
    if len(s)==0: continue
    print(f"  VIX {lo:2d}-{hi:2d}  n={len(s):4d}  avg {s['pts'].mean():+5.2f} pt  WR {(s['pts']>0).mean()*100:3.0f}%  total {s['pts'].sum():+8.0f} pt")
print(); print("="*100); print("7. WORST SINGLE TRADES ON WAR DAYS (was anything a gap-through-stop?)"); print("="*100)
for _,r in df[df['ev']].nsmallest(8,'pts').iterrows():
    print(f"  {r['et']:%Y-%m-%d %H:%M}  {r['setup_name']:16s} {r['side']:5s}  {r['pts']:+7.2f} pt  vix {r['vixf']:.1f}")
print(); print("  worst NON-war trades for scale:")
for _,r in df[~df['ev']].nsmallest(5,'pts').iterrows():
    print(f"  {r['et']:%Y-%m-%d %H:%M}  {r['setup_name']:16s} {r['side']:5s}  {r['pts']:+7.2f} pt  vix {r['vixf']:.1f}")
