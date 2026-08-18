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



book=[r for r in rows if lf.passes_v20(r,gaps)]
df=pd.DataFrame([{'d':r['et'].date(),'long':r['is_long'],'pts':float(r['outcome_pnl']),
                  'prev':PR.get(r['et'].date()),'mo':r['et'].strftime('%Y-%m'),
                  'vix':pd.to_numeric(r.get('vix'),errors='coerce')} for r in book]).dropna(subset=['prev','vix'])
sh=df[~df['long']]
print('='*96)
print('THE DECISIVE CHECK - what are the SKIPPED trades actually worth, on their own?')
print('='*96)
for thr,vmax in [(-0.8,22),(-0.8,24),(-0.5,22),(-0.5,24)]:
    a=sh[(sh['prev']<thr)&(sh['vix']<vmax)]
    dollars=(a['pts']-0.6)*5-1.92
    print(f'  prev<{thr} & VIX<{vmax}:  skips {len(a):3d} shorts   {a["pts"].sum():+7.1f} pts   '
          f'= ${dollars.sum():+7.0f} at 1 MES   WR {(a["pts"]>0).mean()*100 if len(a) else 0:3.0f}%')
    if len(a):
        print('       by month: ' + '  '.join(f'{m}:{g["pts"].sum():+.0f}' for m,g in a.groupby('mo')))
print()
print('  If the book gain is much BIGGER than the money these trades lose,')
print('  the gain is cap reshuffling - not the rule.')
