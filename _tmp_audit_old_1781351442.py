# -*- coding: utf-8 -*-
"""LIVE-REALISM AUDIT of the 2-factor sizing on REAL TSRT trades post-V16.
Every signal forced no-look-ahead; broker fills (not portal); baseline cross-checked
vs tsrt_daily_stmt. Prints an audit checklist.
"""
import os, json
from datetime import timedelta, time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
LAG=20  # min — no-look-ahead buffer for 15-min semi bars

# semi (15m table)
basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def semi_at(day,t):
    co=t-timedelta(minutes=LAG); a=bd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None

# gamma as-of 09:40 and 12:30 (no-look-ahead; pick latest cutoff <= entry-LAG)
def gmap(cut):
    q=text("""SELECT DISTINCT ON (d,strike) (ts_utc AT TIME ZONE 'America/New_York')::date d, strike, value FROM (
      SELECT ts_utc,strike,value,(ts_utc AT TIME ZONE 'America/New_York')::time tt FROM volland_exposure_points
      WHERE greek='gamma' AND expiration_option='TODAY'
        AND (ts_utc AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-09'
        AND strike BETWEEN 6800 AND 7800) q WHERE tt<=TIME :c ORDER BY d,strike,ts_utc DESC""")
    m=defaultdict(dict)
    for d,s,v in C.execute(q,{"c":cut}).fetchall(): m[d.isoformat()][float(s)]=float(v)
    return m
g940=gmap("09:40"); g1230=gmap("12:30")
def gnet(day,spot,entry_t):
    # no-look-ahead: only use a snapshot whose cutoff <= entry-LAG
    et=(entry_t-timedelta(minutes=LAG)).time()
    m=None
    if et>=dtime(12,30): m=g1230.get(day) or g940.get(day)
    elif et>=dtime(9,40): m=g940.get(day)
    else: return None   # pre-09:40+lag: no gamma data available yet -> neutral
    if not m: return None
    return sum(v for k,v in m.items() if abs(k-spot)<=60)

rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
  FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE (sl.ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE '2026-05-18' AND DATE '2026-06-09'
  ORDER BY sl.ts ASC""")).fetchall()
T=[]; n_no_semi=0; n_no_gamma=0; n_pre=0
for et,setup,direction,st,spot in rows:
    if not isinstance(st,dict):
        try: st=json.loads(st)
        except: st={}
    en=st.get('fill_price'); ex=st.get('close_fill_price')
    if en is None or ex is None or spot is None: continue
    sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
    pts=(en-ex) if sh else (ex-en); usd=pts*5
    day=et.date().isoformat(); etn=et.replace(tzinfo=None)
    sb=semi_at(day,etn); g=gnet(day,float(spot),etn)
    if sb is None: n_no_semi+=1
    if g is None: n_no_gamma+=1
    if etn.time()<dtime(9,50): n_pre+=1
    sm=1.0
    if sb is not None:
        if (L and sb>0) or (not L and sb<0): sm=2.0
        elif (L and sb<0) or (not L and sb>0): sm=0.5
    gm=1.0
    if L and g is not None: gm=1.25 if g<0 else 0.75
    T.append({"day":day,"pnl":usd,"sm":sm,"sz":sm*gm,"L":L})

base=sum(t['pnl'] for t in T); semi=sum(t['pnl']*t['sm'] for t in T); two=sum(t['pnl']*t['sz'] for t in T)
aw=sum(t['sz'] for t in T)/len(T); w=sum(1 for t in T if t['pnl']>0)

# Gate-2 cross-check: baseline vs tsrt_daily_stmt
stmt=C.execute(text("""SELECT COALESCE(SUM(gross),0), COALESCE(SUM(net),0) FROM tsrt_daily_stmt
  WHERE day BETWEEN '2026-05-18' AND '2026-06-09'""")).fetchone()
stmt_days=[r[0].isoformat() for r in C.execute(text("SELECT DISTINCT day FROM tsrt_daily_stmt WHERE day BETWEEN '2026-05-18' AND '2026-06-09' ORDER BY day")).fetchall()]

print("="*64)
print("LIVE-REALISM AUDIT — 2-factor sizing, REAL TSRT post-V16 (May18-Jun9)")
print("="*64)
print(f"\nTrades: {len(T)} | WR {100*w/len(T):.0f}% | avg size {aw:.2f} contracts")
print(f"\n  Baseline 1x:      ${base:+.0f}")
print(f"  Semi-only (lag{LAG}): ${semi:+.0f}  ({semi/base:.2f}x)")
print(f"  2-FACTOR (lag{LAG}):  ${two:+.0f}  ({two/base:.2f}x)")
print("\n--- AUDIT CHECKLIST ---")
print(f"[1] Semi signal: LAGGED {LAG}min (no look-ahead). trades w/o semi data (neutral 1x): {n_no_semi}")
print(f"[2] Gamma signal: as-of 09:40/12:30 cutoff <= entry-{LAG}min (no look-ahead). trades w/o gamma (neutral): {n_no_gamma}")
print(f"[3] P&L source: REAL broker fills (fill_price/close_fill_price), NOT portal sim")
print(f"[4] Early trades (<09:50): {n_pre} get neutral signals (data not yet available) -> conservative")
print(f"[5] Gate-2 cross-check baseline vs tsrt_daily_stmt ({len(stmt_days)} stmt days {stmt_days[:3]}...{stmt_days[-2:] if len(stmt_days)>2 else ''}):")
print(f"     my per-lid baseline (full window) = ${base:+.0f}")
print(f"     tsrt_daily_stmt gross (stmt days only) = ${float(stmt[0]):+.0f}, net = ${float(stmt[1]):+.0f}")
print(f"     NOTE: my window includes days not yet in stmt; compare overlap below")
# overlap comparison
ov_base=sum(t['pnl'] for t in T if t['day'] in stmt_days)
ovs=C.execute(text("""SELECT COALESCE(SUM(gross),0) FROM tsrt_daily_stmt WHERE day=ANY(:d)"""),{"d":stmt_days}).scalar()
print(f"     OVERLAP days only: my per-lid ${ov_base:+.0f} vs stmt gross ${float(ovs):+.0f}  (diff ${ov_base-float(ovs):+.0f})")
