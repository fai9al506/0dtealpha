# -*- coding: utf-8 -*-
"""Jun 3 (big loss) vs Jun 4 (big win) for the sizing — WHY?
Per-trade detail + intraday SPX tape + BAR-BY-BAR intraday gamma (as-of each time, +-30/+-60)."""
import os, json
from datetime import timedelta
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")

# semis (15m table)
basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def semi_at(day,t):
    co=t-timedelta(minutes=20); a=bd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None

for DAY in ("2026-06-03","2026-06-04"):
    print("\n"+"="*70); print(f"  {DAY}"); print("="*70)
    # intraday SPX tape
    sp=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York')::text et, spot FROM chain_snapshots
       WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND spot IS NOT NULL
         AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN TIME '09:30' AND TIME '16:00' ORDER BY ts"""),{"d":DAY}).fetchall()
    if sp:
        ys=[float(r[1]) for r in sp]
        print(f"  SPX: open {ys[0]:.0f} high {max(ys):.0f} low {min(ys):.0f} close {ys[-1]:.0f}  (range {max(ys)-min(ys):.0f}p, net {ys[-1]-ys[0]:+.0f}p)")
    # bar-by-bar intraday gamma (all snapshots that day, net near spot at each snapshot's own spot)
    gr=C.execute(text("""SELECT (ts_utc AT TIME ZONE 'America/New_York') et, strike, value, current_price
       FROM volland_exposure_points WHERE greek='gamma' AND expiration_option='TODAY'
         AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d AND strike BETWEEN 6800 AND 7800 ORDER BY ts_utc"""),{"d":DAY}).fetchall()
    snaps=defaultdict(list); spots={}
    for et,k,v,cp in gr:
        key=et.replace(second=0,microsecond=0); snaps[key].append((float(k),float(v)))
        if cp: spots[key]=float(cp)
    print("  intraday gamma (net near spot):  time   spot   gamma±30M  gamma±60M")
    times=sorted(snaps)
    for i in range(0,len(times),max(1,len(times)//8)):
        t=times[i]; sp0=spots.get(t)
        if not sp0:
            # use nearest chain spot
            sp0=ys[0] if sp else 7000
        g30=sum(v for k,v in snaps[t] if abs(k-sp0)<=30)/1e6
        g60=sum(v for k,v in snaps[t] if abs(k-sp0)<=60)/1e6
        print(f"        {t.strftime('%H:%M'):>16}  {sp0:.0f}   {g30:+8.0f}   {g60:+8.0f}")
    # trades
    rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
       FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
       WHERE (sl.ts AT TIME ZONE 'America/New_York')::date=DATE :d ORDER BY sl.ts"""),{"d":DAY}).fetchall()
    print("  TRADES:  time   setup           dir    entry   exit    pnl$   semi%   intraday-gamma(near)")
    for et,setup,direction,st,spot in rows:
        if not isinstance(st,dict):
            try: st=json.loads(st)
            except: st={}
        en=st.get('fill_price'); ex=st.get('close_fill_price')
        if en is None or ex is None: continue
        sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
        pts=(en-ex) if sh else (ex-en); usd=pts*5
        etn=et.replace(tzinfo=None); sbv=semi_at(DAY,etn)
        # nearest gamma snapshot <= entry
        prior=[t for t in times if t<=etn]
        g="-"
        if prior:
            t=prior[-1]; sp0=spots.get(t) or float(spot)
            g=f"{sum(v for k,v in snaps[t] if abs(k-sp0)<=60)/1e6:+.0f}M"
        print(f"        {et.strftime('%H:%M'):>10}  {setup[:14]:<14}  {'L' if L else 'S':<5}  {en:.1f} {ex:.1f}  {usd:+6.0f}  {('%+.1f'%sbv) if sbv is not None else '-':>6}   {g}")
