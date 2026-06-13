# -*- coding: utf-8 -*-
"""NEW vanna-magnet SETUP (today's 7400 idea): when a strong +vanna magnet sits ABOVE
spot in normal vol, price drifts UP into it. Test: long at ~09:50, target = magnet
strike, stop = entry-STOP. Walk the SPX intraday path. Split by VIX regime."""
import os
from datetime import time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
MAGMIN=100.0   # min +vanna $M to count as a magnet
DMIN, DMAX = 8, 80   # magnet must be this many pts above spot
STOP=15.0
# all days with vanna ALL data
days=[r[0].isoformat() for r in C.execute(text("""SELECT DISTINCT (ts_utc AT TIME ZONE 'America/New_York')::date d
  FROM volland_exposure_points WHERE greek='vanna' AND expiration_option='ALL'
  AND ts_utc > '2026-02-01' ORDER BY d""")).fetchall()]
def spath(day):
    return [(r[0].time(), float(r[1])) for r in C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, spot
      FROM chain_snapshots WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND spot IS NOT NULL
      AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN TIME '09:30' AND TIME '16:00' ORDER BY ts"""),{"d":day}).fetchall()]
def magnet(day,spot):
    mt=C.execute(text("""SELECT MAX(ts_utc) FROM volland_exposure_points WHERE greek='vanna' AND expiration_option='ALL'
      AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d AND (ts_utc AT TIME ZONE 'America/New_York')::time<=TIME '09:55'"""),{"d":day}).scalar()
    if not mt: return None
    rows=C.execute(text("""SELECT strike, value/1e6 v FROM volland_exposure_points WHERE greek='vanna' AND expiration_option='ALL'
      AND ts_utc=:t AND strike BETWEEN :lo AND :hi AND value/1e6 > :mm ORDER BY value DESC LIMIT 1"""),
      {"t":mt,"lo":spot+DMIN,"hi":spot+DMAX,"mm":MAGMIN}).fetchall()
    return float(rows[0][0]) if rows else None
def vixof(day):
    return C.execute(text("SELECT vix FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND vix IS NOT NULL ORDER BY ts LIMIT 1"),{"d":day}).scalar()
res=[]
for d in days:
    sp=spath(d)
    if len(sp)<10: continue
    # entry ~09:50
    ent=[(t,s) for t,s in sp if t>=dtime(9,50)]
    if not ent: continue
    et,entry=ent[0]
    mag=magnet(d,entry)
    if mag is None: continue   # no qualifying +vanna magnet above
    target=mag; stop=entry-STOP
    out=None
    for t,s in sp:
        if t<et: continue
        if s>=target: out=("WIN",target-entry); break
        if s<=stop: out=("LOSS",-STOP); break
    if out is None: out=("EOD", sp[-1][1]-entry)
    vx=vixof(d)
    res.append({"day":d,"entry":entry,"mag":mag,"dist":mag-entry,"res":out[0],"pnl":out[1],"vix":float(vx) if vx else None})
def stats(x,lab):
    if not x: print(f"  {lab}: n=0"); return
    w=sum(1 for t in x if t['pnl']>0); pnl=sum(t['pnl'] for t in x)
    print(f"  {lab}: n={len(x)} WR={100*w/len(x):.0f}% totalPts={pnl:+.0f} avg={pnl/len(x):+.1f}pt (~${pnl*5*1:.0f} @1MES)")
print(f"=== VANNA-MAGNET LONG setup: {len(res)} days had a +vanna magnet {DMIN}-{DMAX}pt above open (>{MAGMIN:.0f}M) ===")
print(f"  (target=magnet strike, stop=-{STOP:.0f}, entry ~09:50)")
stats(res,"ALL days")
stats([t for t in res if t['vix'] is not None and t['vix']<20],"VIX<20 (normal)")
stats([t for t in res if t['vix'] is not None and t['vix']>=20],"VIX>=20 (stress)")
# distribution of outcomes
from collections import Counter
print("  outcomes:",dict(Counter(t['res'] for t in res)))
print("  sample recent:", [(t['day'][5:],t['res'],round(t['pnl'])) for t in res[-8:]])
