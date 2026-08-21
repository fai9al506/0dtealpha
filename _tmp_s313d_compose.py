# -*- coding: utf-8 -*-
"""S313 stage 4 - WHAT exactly gets doubled? If the winning longs are setups we do
not actually trade live, the finding is not actionable. Also confirm the env gates
Railway really has, because live_filter reads them (checklist item 10)."""
import os, sys
import numpy as np, pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
VIXMAX = 24.0
print("ENV AS SEEN BY THIS RUN (must match Railway):")
for k in ('GEX_LONG_V3_REAL_TRADE_ENABLED', 'VPB_REAL_TRADE_ENABLED', 'VIX_DIV_REAL_TRADE_ENABLED',
          'ES_ABS_REAL_TRADE_ENABLED', 'BASKET_SIZING_MODE', 'REAL_TRADE_NO_FRIDAY',
          'DAY_BREAKER_ENABLED'):
    print("   %-34s = %s" % (k, os.getenv(k)))

E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
px = pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d,
    bar_open, bar_close from spx_ohlc_1m
    where (ts at time zone 'America/New_York')>='2026-02-19' order by ts"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()

px['d'] = pd.to_datetime(px['d']).dt.date
g = px.groupby('d')
day = pd.DataFrame({'open': g['bar_open'].first(), 'close': g['bar_close'].last()}).reset_index()
day['oc'] = (day['close'] - day['open']) / day['open'] * 100
prev = {}
ds = list(day['d'])
for i in range(1, len(ds)):
    prev[ds[i]] = day.iloc[i - 1]['oc']

L = []
for r in rows:
    rr = dict(r)
    if not lf.passes_v20(rr, gaps): continue
    v = rr.get('vix')
    if v is None or float(v) >= VIXMAX: continue
    d = rr['ts'].astimezone(ET).date()
    p = prev.get(d)
    if p is None or pd.isna(p): continue
    L.append({'d': d, 'setup': rr['setup_name'],
              'long': str(rr.get('direction', '')).lower() in ('long', 'bullish'),
              'pnl': float(rr['outcome_pnl']), 'oc': p})
L = pd.DataFrame(L)

for thr in (-0.8, -0.5):
    sub = L[(L['oc'] < thr)]
    print()
    print("=" * 104)
    print("TRIGGER previous open-to-close < %.1f%%   -   %d days, %d signals" % (thr, sub['d'].nunique(), len(sub)))
    print("=" * 104)
    for side, lbl in [(True, 'LONGS  (these get doubled)'), (False, 'SHORTS (these get blocked)')]:
        s = sub[sub['long'] == side]
        print("  %s   n=%d   %+.2f pt/trade   total %+.1f pt" % (lbl, len(s), s['pnl'].mean() if len(s) else 0, s['pnl'].sum()))
        if len(s) == 0: continue
        gg = s.groupby('setup')['pnl'].agg(['size', 'mean', 'sum'])
        gg = gg.sort_values('sum', ascending=False)
        for nm, row in gg.iterrows():
            print("      %-22s n=%3d  %+7.2f pt/trade  total %+8.1f" % (nm, int(row['size']), row['mean'], row['sum']))
