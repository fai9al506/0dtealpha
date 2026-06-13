"""FIRST-PASS level-to-level backtest of Dark Matter's method (post-V16).
Rebuild his multi-expiry vanna level map each morning, then trade level-to-level:
  - FADE SHORT when price rallies into a strong RESISTANCE wall (+vanna above) and rejects
  - BUY LONG  when price dips into a strong SUPPORT floor (-vanna below) and holds
  - VIX polarity: in EXTREME regime, floor-LONGS are scout/skipped (floors can cascade);
    wall-FADES emphasized (his polarity-inversion rule).
Fixed rules (stated, minimal): proximity 5pt, rejection/hold confirm 3pt on next bar,
SL 8pt beyond level, target = next level toward spot (min 8pt), one position, exit 15:55.
NOTE: levels frozen at 09:40 each day (he updates intraday -> this UNDERSTATES him).
P&L in ES/SPX points (x5=$@1MES). HONEST first pass — overfit risk flagged.
"""
import os
from collections import defaultdict
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
engine=create_engine(os.environ['DATABASE_URL'])
ERA="2026-05-18"

conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
if True:
    days=[r[0].isoformat() for r in conn.execute(text("""
        SELECT DISTINCT (ts AT TIME ZONE 'America/New_York')::date d FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE :era ORDER BY 1"""),{"era":ERA}).fetchall()]

    def price_series(day):
        return [(r[0], float(r[1])) for r in conn.execute(text("""
            SELECT (ts AT TIME ZONE 'America/New_York') et, spot FROM chain_snapshots
            WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND spot IS NOT NULL
              AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN TIME '09:30' AND TIME '15:55'
            ORDER BY ts"""),{"d":day}).fetchall()]

    def level_map2(day, before_et):
        rows=conn.execute(text("""
            SELECT DISTINCT ON (expiration_option, strike) expiration_option, strike, value
            FROM volland_exposure_points
            WHERE ts_utc::date=DATE :d AND greek='vanna'
              AND expiration_option IN ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
              AND (ts_utc AT TIME ZONE 'America/New_York') <= :bt
            ORDER BY expiration_option, strike, ts_utc DESC
        """),{"d":day,"bt":before_et}).fetchall()
        agg=defaultdict(float)
        for exp,strike,val in rows: agg[float(strike)]+=float(val)
        return agg  # strike -> summed vanna

    def vix_at(day):
        return conn.execute(text("""SELECT vix FROM setup_log
            WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND vix IS NOT NULL
            ORDER BY ts LIMIT 1"""),{"d":day}).scalar()

PROX=5; CONF=3; SL=8; TGT_MIN=8
trades=[]
for day in days:
    ps=price_series(day)
    if len(ps)<20: continue
    cutoff=datetime.fromisoformat(day).replace(hour=9,minute=40)
    agg=level_map2(day, cutoff)
    if not agg: continue
    spot0=ps[0][1]
    vix=vix_at(day); vix=float(vix) if vix else 18
    extreme = vix>=20
    # resistance = strikes above spot0 with +vanna; support = below with -vanna; strong = |v|>8e7
    res=sorted([(k,v) for k,v in agg.items() if k>spot0 and v>8e7], key=lambda x:x[0])
    sup=sorted([(k,v) for k,v in agg.items() if k<spot0 and v<-8e7], key=lambda x:-x[0])
    res_lv=[k for k,_ in res]; sup_lv=[k for k,_ in sup]
    pos=None  # (dir, entry, stop, target)
    for i in range(2,len(ps)):
        t,p=ps[i]; pp=ps[i-1][1]
        if pos:
            d,en,st,tg=pos
            hit_stop = p<=st if d=="L" else p>=st
            hit_tgt  = p>=tg if d=="L" else p<=tg
            if hit_stop or hit_tgt or i==len(ps)-1:
                px = (st if hit_stop else (tg if hit_tgt else p))
                pts=(px-en) if d=="L" else (en-px)
                trades.append({"day":day,"dir":d,"pts":pts,"ext":extreme}); pos=None
            continue
        # look for fade at resistance (short)
        for k in res_lv:
            if abs(pp-k)<=PROX and p<=pp-CONF and p<k:   # touched wall, rejected down
                lower=[s for s in (sup_lv+res_lv) if s<p]
                tg=max(lower) if lower else p-TGT_MIN
                tg=min(tg, p-TGT_MIN)
                pos=("S",p,k+SL,tg); break
        if pos: continue
        if not extreme:  # floor-longs only in NORMAL (his scout/skip in EXTREME)
            for k in sup_lv:
                if abs(pp-k)<=PROX and p>=pp+CONF and p>k:
                    upper=[s for s in (sup_lv+res_lv) if s>p]
                    tg=min(upper) if upper else p+TGT_MIN
                    tg=max(tg, p+TGT_MIN)
                    pos=("L",p,k-SL,tg); break

def stt(ts):
    if not ts: return "n=0"
    w=sum(1 for t in ts if t['pts']>0)
    return f"n={len(ts):>3} WR={100*w/len(ts):3.0f}% pts={sum(t['pts'] for t in ts):+7.1f} (${sum(t['pts'] for t in ts)*5:+6.0f})"
print(f"Level-to-level backtest, post-V16 ({len(days)} days). Levels frozen 09:40 (understates his intraday updates).\n")
print("ALL:    ", stt(trades))
print("SHORTS (wall fades):", stt([t for t in trades if t['dir']=='S']))
print("LONGS  (floor buys):", stt([t for t in trades if t['dir']=='L']))
print("EXTREME-day trades: ", stt([t for t in trades if t['ext']]))
print("NORMAL-day trades:  ", stt([t for t in trades if not t['ext']]))
print("\nBy day:")
byday=defaultdict(list)
for t in trades: byday[t['day']].append(t)
for d in sorted(byday):
    tt=byday[d]; ex=" EXTREME" if tt[0]['ext'] else ""
    print(f"  {d}: {stt(tt)}{ex}")
