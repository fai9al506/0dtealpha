# -*- coding: utf-8 -*-
"""S300 - the user's hypothesis: after a BAD (down) day the market takes a relief rally
and trends up, so all Skew Charm SHORTS get hit. Test it on 6 months.

Prior-day return is known BEFORE the session opens, so any rule built on it is tradeable.
"""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
ET=ZoneInfo("America/New_York"); HAIR,FEE,DPP,DEAD=0.6,1.92,5.0,0.15
E=create_engine(os.environ['DATABASE_URL']); c=E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps=lf.load_gaps(c)
px=pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d,
    ts at time zone 'America/New_York' et, bar_open, bar_close
    from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""),c)
rows=c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01' AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
px['d']=pd.to_datetime(px['d']).dt.date
g=px.groupby('d')
day=pd.DataFrame({'open':g['bar_open'].first(),'close':g['bar_close'].last()}).reset_index()
day['ret']=(day['close']-day['open'])/day['open']*100
day['prev_ret']=day['ret'].shift(1)
day['prev2_ret']=day['ret'].shift(2)
PR=dict(zip(day['d'],day['prev_ret'])); PR2=dict(zip(day['d'],day['prev2_ret']))
TODAY=dict(zip(day['d'],day['ret']))
rows=[dict(r) for r in rows]
for r in rows:
    r['et']=r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long']=str(r.get('direction','')).lower() in ('long','bullish')
    r['prev']=PR.get(r['et'].date()); r['prev2']=PR2.get(r['et'].date())

book=[dict(r) for r in rows if lf.passes_v20(r,gaps)]
df=pd.DataFrame([{'d':r['et'].date(),'setup':r['setup_name'],'long':r['is_long'],
                  'pts':float(r['outcome_pnl']),'prev':r['prev'],'prev2':r['prev2'],
                  'today':TODAY.get(r['et'].date())} for r in book]).dropna(subset=['prev'])

def blk(t,s):
    if len(s)<5: print(f"    {t:26s} n={len(s):3d}  (too few)"); return
    se=s['pts'].std(ddof=1)/np.sqrt(len(s))
    print(f"    {t:26s} n={len(s):3d}  {s['pts'].mean():+6.2f} pt  WR {(s['pts']>0).mean()*100:3.0f}%  "
          f"tot {s['pts'].sum():+7.0f}  t={s['pts'].mean()/se:+5.2f}")

print("="*100)
print("1. DOES YESTERDAY'S MOVE PREDICT TODAY'S SHORTS?  (V20 book, all setups)")
print("="*100)
for side,lab in [(False,'SHORTS'),(True,'LONGS')]:
    s=df[df['long']==side]
    print(f"\n  {lab} by YESTERDAY's SPX move:")
    for lo,hi,t in [(-99,-1.0,'yesterday < -1.0%'),(-1.0,-0.5,'yesterday -1.0 to -0.5%'),
                    (-0.5,0,'yesterday -0.5 to 0%'),(0,0.5,'yesterday 0 to +0.5%'),
                    (0.5,1.0,'yesterday +0.5 to +1%'),(1.0,99,'yesterday > +1.0%')]:
        blk(t, s[(s['prev']>=lo)&(s['prev']<hi)])

print()
print("="*100)
print("2. THE SPECIFIC CLAIM: after a DOWN day, the relief rally kills our shorts")
print("="*100)
sh=df[~df['long']]
for thr in [-0.3,-0.5,-0.8,-1.0]:
    a=sh[sh['prev']<thr]; b=sh[sh['prev']>=thr]
    print(f"  yesterday < {thr:+.1f}% : shorts n={len(a):3d} {a['pts'].mean():+6.2f} pt WR {(a['pts']>0).mean()*100:3.0f}%"
          f"   |  otherwise n={len(b):3d} {b['pts'].mean():+6.2f} pt WR {(b['pts']>0).mean()*100:3.0f}%")
print()
print("  and does the day AFTER a down day actually rally?")
dd=day.dropna(subset=['prev_ret'])
for thr in [-0.3,-0.5,-0.8,-1.0]:
    a=dd[dd['prev_ret']<thr]
    print(f"    after a day < {thr:+.1f}%: next day averaged {a['ret'].mean():+.2f}%  "
          f"({(a['ret']>0).mean()*100:.0f}% up, n={len(a)})")
print()
print("="*100)
print("3. TWO DOWN DAYS IN A ROW -> the bounce is bigger?")
print("="*100)
two=df[(df['prev']<-0.3)&(df['prev2']<-0.3)]
print(f"  after TWO down days: shorts n={len(two[~two['long']])} "
      f"{two[~two['long']]['pts'].mean() if len(two[~two['long']]) else 0:+.2f} pt   "
      f"longs n={len(two[two['long']])} {two[two['long']]['pts'].mean() if len(two[two['long']]) else 0:+.2f} pt")
dd2=day.dropna(subset=['prev_ret','prev2_ret'])
a=dd2[(dd2['prev_ret']<-0.3)&(dd2['prev2_ret']<-0.3)]
print(f"  and the day itself averaged {a['ret'].mean():+.2f}% ({(a['ret']>0).mean()*100:.0f}% up, n={len(a)})")
print()
print("="*100)
print("4. WHAT ACTUALLY PREDICTS A BAD DAY FOR THE BOOK? (today's realised trend)")
print("="*100)
dayp=df.groupby('d').agg(pts=('pts','sum'),n=('pts','size')).reset_index()
dayp['today']=dayp['d'].map(TODAY); dayp['prev']=dayp['d'].map(PR)
print(f"  {'today SPX move':26s}{'days':>6s}{'book pts':>10s}{'avg/day':>9s}")
for lo,hi,t in [(-99,-1.0,'today < -1.0%'),(-1.0,-0.5,'today -1.0 to -0.5%'),(-0.5,0,'today -0.5 to 0%'),
                (0,0.5,'today 0 to +0.5%'),(0.5,1.0,'today +0.5 to +1%'),(1.0,99,'today > +1.0%')]:
    s=dayp[(dayp['today']>=lo)&(dayp['today']<hi)]
    if len(s): print(f"  {t:26s}{len(s):>6d}{s['pts'].sum():>+10,.0f}{s['pts'].mean():>+9.1f}")
print("\n  (this is hindsight - it says WHAT hurts us, not what we can predict)")
