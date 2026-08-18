# -*- coding: utf-8 -*-
"""S302 - AUDIT the relief-rally rule before anyone trades it.
The user's worry: "when the market goes down sometimes it goes MORE down, and we would
be skipping good shorts." That is the right question. Eight checks."""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
DAILY=-300.0; N_SESS=117
CAL={'2026-03':22,'2026-04':21,'2026-05':20,'2026-06':21,'2026-07':22,'2026-08':11}
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
px=pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d, bar_open, bar_close
    from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""),c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
px['d']=pd.to_datetime(px['d']).dt.date
g=px.groupby('d')
day=pd.DataFrame({'open':g['bar_open'].first(),'close':g['bar_close'].last()}).reset_index()
day['ret']=(day['close']-day['open'])/day['open']*100
day['prev']=day['ret'].shift(1)
PR=dict(zip(day['d'],day['prev']))
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')

print("="*100); print("CHECK 1 — LOOK-AHEAD. Is 'prev' really yesterday, known before the open?"); print("="*100)
chk=day.dropna(subset=['prev']).head(4)
for i,r in chk.iterrows():
    real_prev=day.iloc[i-1]['ret'] if i>0 else None
    ok = abs(r['prev']-real_prev)<1e-9 if real_prev is not None else False
    print(f"  {r['d']}  prev={r['prev']:+.3f}  actual previous session ({day.iloc[i-1]['d']})="
          f"{real_prev:+.3f}  {'OK' if ok else 'MISMATCH'}")
print("  -> 'prev' is the PREVIOUS session's open-to-close, fully known at yesterday's 16:00.")

print()
print("="*100); print("CHECK 2 — THE USER'S WORRY: after a down day, how often does it keep falling?"); print("="*100)
dd=day.dropna(subset=['prev'])
for thr in [-0.3,-0.5,-0.8,-1.0]:
    a=dd[dd['prev']<thr]
    down=a[a['ret']<0]; up=a[a['ret']>=0]
    print(f"  after a day < {thr:+.1f}%  (n={len(a):2d}):  kept falling {len(down):2d} ({len(down)/len(a)*100:3.0f}%)"
          f"  avg {down['ret'].mean() if len(down) else 0:+.2f}%   |   bounced {len(up):2d} ({len(up)/len(a)*100:3.0f}%)"
          f"  avg {up['ret'].mean() if len(up) else 0:+.2f}%")
print("  -> the continuation days DO exist. The question is whether our shorts make money on them.")

print()
print("="*100); print("CHECK 3 — ON THOSE CONTINUATION DAYS, DO OUR SHORTS ACTUALLY WIN?"); print("="*100)
book=[r for r in rows if lf.passes_v20(r,gaps)]
df=pd.DataFrame([{'d':r['et'].date(),'long':r['is_long'],'pts':float(r['outcome_pnl']),
                  'prev':PR.get(r['et'].date()),'today':dict(zip(day['d'],day['ret'])).get(r['et'].date()),
                  'mo':r['et'].strftime('%Y-%m'),'vix':pd.to_numeric(r.get('vix'),errors='coerce')}
                 for r in book]).dropna(subset=['prev','today'])
sh=df[~df['long']]
for thr in [-0.5,-0.8]:
    a=sh[sh['prev']<thr]
    cont=a[a['today']<0]; bounce=a[a['today']>=0]
    print(f"  prev < {thr:+.1f}%:  shorts on CONTINUATION days n={len(cont):3d} {cont['pts'].mean() if len(cont) else 0:+6.2f} pt"
          f"   |  shorts on BOUNCE days n={len(bounce):3d} {bounce['pts'].mean() if len(bounce) else 0:+6.2f} pt")
print("  -> we WOULD give up the continuation-day shorts. Is the trade worth it? (check 4)")

print()
print("="*100); print("CHECK 4 — WHAT THE RULE COSTS vs SAVES, split explicitly"); print("="*100)
for thr in [-0.5,-0.8]:
    a=sh[sh['prev']<thr]
    cont=a[a['today']<0]; bounce=a[a['today']>=0]
    print(f"  prev < {thr:+.1f}%:  skipping gives up {cont['pts'].sum():+7.1f} pts of good shorts"
          f"   and avoids {bounce['pts'].sum():+7.1f} pts of bad ones   NET {a['pts'].sum()*-1:+7.1f} pts saved")

print()
print("="*100); print("CHECK 5 — MARCH. The user's specific fear."); print("="*100)
for m in sorted(CAL):
    s=sh[sh['mo']==m]; a=s[s['prev']<-0.8]
    if len(a)==0:
        print(f"  {m}: no shorts after a -0.8% day"); continue
    print(f"  {m}: shorts after a -0.8% day n={len(a):3d}  {a['pts'].mean():+6.2f} pt  "
          f"tot {a['pts'].sum():+7.1f}  WR {(a['pts']>0).mean()*100:3.0f}%  <- rule would skip these")

print()
print("="*100); print("CHECK 6 — IS THE WHOLE GAIN JUST JUNE?"); print("="*100)
print("  (points the rule would skip, by month - positive = we lose by skipping)")
tot=0
for m in sorted(CAL):
    a=sh[(sh['mo']==m)&(sh['prev']<-0.8)]
    tot+=a['pts'].sum()
    print(f"    {m}  skipped {len(a):3d} shorts worth {a['pts'].sum():+7.1f} pts")
print(f"    TOTAL skipped: {tot:+.1f} pts  -> skipping is worth {-tot:+.1f} pts")

print()
print("="*100); print("CHECK 7 — THRESHOLD STABILITY. Knife-edge or a broad plateau?"); print("="*100)
print(f"  {'threshold':12s}{'shorts skipped':>16s}{'pts given up':>14s}{'pts avoided':>13s}{'net saved':>11s}")
for thr in [-0.2,-0.3,-0.4,-0.5,-0.6,-0.7,-0.8,-0.9,-1.0,-1.2,-1.5]:
    a=sh[sh['prev']<thr]
    if len(a)<3: continue
    cont=a[a['today']<0]; bounce=a[a['today']>=0]
    print(f"  {thr:+.1f}%{len(a):>16d}{cont['pts'].sum():>+14.1f}{bounce['pts'].sum():>+13.1f}{-a['pts'].sum():>+11.1f}")

print()
print("="*100); print("CHECK 8 — IS IT REALLY 'YESTERDAY DOWN', OR JUST HIGH VIX IN DISGUISE?"); print("="*100)
for lo,hi,t in [(0,18,'VIX < 18'),(18,22,'VIX 18-22'),(22,99,'VIX 22+')]:
    s=sh[(sh['vix']>=lo)&(sh['vix']<hi)]
    a=s[s['prev']<-0.8]; b=s[s['prev']>=-0.8]
    if len(a)<3: 
        print(f"  {t:12s} after down day n={len(a):3d} (too few)   |  otherwise n={len(b):3d} {b['pts'].mean():+6.2f} pt")
        continue
    print(f"  {t:12s} after down day n={len(a):3d} {a['pts'].mean():+6.2f} pt   |  "
          f"otherwise n={len(b):3d} {b['pts'].mean():+6.2f} pt   gap {a['pts'].mean()-b['pts'].mean():+.2f}")
print("  -> if the gap holds inside every VIX band, it is the down-day effect, not the vol regime.")
