# -*- coding: utf-8 -*-
"""Structural gamma (user's read): for a LONG find the NEAREST +G wall above spot.
If a +G wall is close above -> CAPPED (bad). If the path up is -G/open -> CLEAR (good).
Mirror for shorts (below). Test on Jun 3/4."""
import os, json
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
WALL=7.0; NEAR=15  # +G wall threshold $M ; "near" distance pts
def path_read(snap, spot, L):
    # scan in trade direction for nearest significant +G wall, and -G accel room
    if L: cand=sorted([(k,v) for k,v in snap.items() if spot<k<=spot+40])  # ascending
    else: cand=sorted([(k,v) for k,v in snap.items() if spot-40<=k<spot], reverse=True)
    wall=None
    for k,v in cand:
        if v>=WALL: wall=abs(k-spot); break
        # if we pass a -G accelerator before any wall, that's open road
    nearest_neg = any(v<=-WALL for k,v in cand[:4])  # -G accelerator in the immediate path
    if wall is not None and wall<=NEAR: return "CAPPED", wall
    if nearest_neg: return "CLEAR(-G accel)", wall
    return "neutral", wall

for DAY in ("2026-06-03","2026-06-04"):
    gr=C.execute(text("""SELECT (ts_utc AT TIME ZONE 'America/New_York') et, strike, value, current_price
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option='TODAY'
        AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d AND strike BETWEEN 7400 AND 7750 ORDER BY ts_utc"""),{"d":DAY}).fetchall()
    snaps=defaultdict(dict); spots={}
    for et,k,v,cp in gr:
        key=et.replace(second=0,microsecond=0); snaps[key][float(k)]=float(v)/1e6
        if cp: spots[key]=float(cp)
    times=sorted(snaps)
    rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.direction, rto.state, sl.spot, sl.setup_name
      FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
      WHERE (sl.ts AT TIME ZONE 'America/New_York')::date=DATE :d ORDER BY sl.ts"""),{"d":DAY}).fetchall()
    print(f"\n===== {DAY} =====  ({'longs lost' if DAY=='2026-06-03' else 'longs won'})")
    print(f"{'time':<6}{'dir':<4}{'pnl$':>6}{'spot':>6}  {'path-read':<22}{'nearest +G wall'}")
    nW=nL=0; cap_pnl=clr_pnl=0
    for et,direction,st,spot,setup in rows:
        if not isinstance(st,dict):
            try: st=json.loads(st)
            except: st={}
        en=st.get('fill_price'); ex=st.get('close_fill_price')
        if en is None or ex is None: continue
        sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
        pts=(en-ex) if sh else (ex-en); usd=pts*5; etn=et.replace(tzinfo=None)
        prior=[t for t in times if t<=etn]
        if not prior: continue
        s0=spots.get(prior[-1]) or float(spot)
        read,wall=path_read(snaps[prior[-1]], s0, L)
        if read=="CAPPED": cap_pnl+=usd
        elif read.startswith("CLEAR"): clr_pnl+=usd
        print(f"{et.strftime('%H:%M'):<6}{('L' if L else 'S'):<4}{usd:>+6.0f}{s0:>6.0f}  {read:<22}{('%.0fpt up'%wall) if wall is not None else 'none<=40pt'}")
    print(f"   --> CAPPED trades P&L ${cap_pnl:+.0f} | CLEAR(-G accel) trades P&L ${clr_pnl:+.0f}")
