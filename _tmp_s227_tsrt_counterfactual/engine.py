import os
"""TSRT counterfactual engine: SPX 1-min OHLC walk replicating real_trader trail logic."""
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, time as dtime
from collections import defaultdict

U=os.environ["DATABASE_URL"]
ENG=create_engine(U)
def conn(): return ENG.connect().execution_options(isolation_level="AUTOCOMMIT")

# real_trader.py globals (lines 176-188)
BE_TRIGGER=10.0; ACT=10.0; GAP=5.0; BE_BUF=0.25
TRAIL_OVERRIDE={"DD Exhaustion":{"be_trigger":None,"activation":10.0,"gap":10.0}}
DOLLAR_PER_PT=5.0   # MES
COMM_PER_CONTRACT=1.0  # $1/RT verified

def load_bars(dates):
    """{date: [(et_datetime, o,h,l,c)]} from clean spx_ohlc_1m."""
    out=defaultdict(list)
    with conn() as c:
        rows=c.execute(text("""
          SELECT (ts AT TIME ZONE 'America/New_York') et, bar_open,bar_high,bar_low,bar_close
          FROM spx_ohlc_1m
          WHERE (ts AT TIME ZONE 'America/New_York')::date = ANY(:ds)
          ORDER BY ts"""), {"ds":list(dates)}).fetchall()
    for et,o,h,l,cl in rows:
        out[et.date()].append((et,float(o),float(h),float(l),float(cl)))
    return out

def walk(bars, entry_et, entry_px, is_long, stop_pts, setup_name,
         target_px=None, eod=dtime(16,0)):
    """Replicate real_trader trail. Adverse-first within bar. Returns (pts, result, exit_et)."""
    ov=TRAIL_OVERRIDE.get(setup_name,{})
    be_trig = ov["be_trigger"] if "be_trigger" in ov else BE_TRIGGER
    act = ov.get("activation", ACT); gap = ov.get("gap", GAP)
    sgn = 1.0 if is_long else -1.0
    stop = entry_px - sgn*stop_pts
    maxfav = 0.0
    seq=[b for b in bars if b[0] > entry_et and b[0].time() <= eod]
    if not seq: return (0.0,"NO_DATA",entry_et)
    for et,o,h,l,cl in seq:
        adverse = l if is_long else h
        favor   = h if is_long else l
        # adverse first (conservative: stop fills before favorable extreme)
        if (is_long and adverse <= stop) or ((not is_long) and adverse >= stop):
            return ((stop-entry_px)*sgn, "STOP", et)
        if target_px is not None:
            if (is_long and favor >= target_px) or ((not is_long) and favor <= target_px):
                return ((target_px-entry_px)*sgn, "TARGET", et)
        f=(favor-entry_px)*sgn
        if f>maxfav: maxfav=f
        # trail update (ratchet only)
        if maxfav >= act:
            ns = entry_px + sgn*(maxfav-gap)
            if (is_long and ns>stop) or ((not is_long) and ns<stop): stop=ns
        elif be_trig is not None and maxfav >= be_trig:
            ns = entry_px + sgn*BE_BUF
            if (is_long and ns>stop) or ((not is_long) and ns<stop): stop=ns
    last=seq[-1]
    return ((last[4]-entry_px)*sgn, "EOD", last[0])

STOP_PTS={"Skew Charm":14.0,"GEX Long":14.0,"DD Exhaustion":12.0,
          "Vanna Pivot Bounce":8.0,"ES Absorption":8.0}
def stop_for(setup, is_long, row_trail_sl, spot, stop_lvl):
    if setup=="AG Short":
        return abs(spot-stop_lvl) if stop_lvl else (row_trail_sl or 14.0)
    if setup=="VIX Divergence": return 8.0 if is_long else 12.0
    return STOP_PTS.get(setup, row_trail_sl or 14.0)
