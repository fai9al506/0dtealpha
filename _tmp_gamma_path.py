# -*- coding: utf-8 -*-
"""Test the user's refined gamma logic: for a LONG, gamma in the PATH ABOVE spot
matters (+G above = resistance/cap = BAD; -G above = accelerator = GOOD). Per Jun3/4 trade."""
import os, json
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
for DAY in ("2026-06-03","2026-06-04"):
    gr=C.execute(text("""SELECT (ts_utc AT TIME ZONE 'America/New_York') et, strike, value, current_price
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option='TODAY'
        AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d AND strike BETWEEN 7400 AND 7750 ORDER BY ts_utc"""),{"d":DAY}).fetchall()
    snaps=defaultdict(dict); spots={}
    for et,k,v,cp in gr:
        key=et.replace(second=0,microsecond=0); snaps[key][float(k)]=float(v)/1e6
        if cp: spots[key]=float(cp)
    times=sorted(snaps)
    rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
      FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
      WHERE (sl.ts AT TIME ZONE 'America/New_York')::date=DATE :d ORDER BY sl.ts"""),{"d":DAY}).fetchall()
    print(f"\n===== {DAY} =====")
    print(f"{'time':<6}{'dir':<4}{'pnl$':>6}{'spot':>6}  {'gAbove(0..+25)':>15}{'gBelow(-25..0)':>15}  path-read")
    for et,setup,direction,st,spot in rows:
        if not isinstance(st,dict):
            try: st=json.loads(st)
            except: st={}
        en=st.get('fill_price'); ex=st.get('close_fill_price')
        if en is None or ex is None: continue
        sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
        pts=(en-ex) if sh else (ex-en); usd=pts*5; etn=et.replace(tzinfo=None)
        prior=[t for t in times if t<=etn]
        if not prior: continue
        t=prior[-1]; s0=spots.get(t) or float(spot)
        gA=sum(v for k,v in snaps[t].items() if s0< k<=s0+25)   # path above
        gB=sum(v for k,v in snaps[t].items() if s0-25<=k< s0)   # below
        # path read: long wants -G above (accelerator). +G above = capped.
        if L: read = "OK: -G accelerator above" if gA<0 else "BAD: +G resistance above (cap)"
        else: read = "OK: -G accelerator below" if gB<0 else "BAD: +G support below"
        print(f"{et.strftime('%H:%M'):<6}{('L' if L else 'S'):<4}{usd:>+6.0f}{s0:>6.0f}  {gA:>+15.0f}{gB:>+15.0f}  {read}")
