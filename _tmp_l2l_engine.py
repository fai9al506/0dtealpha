"""Faithful level-to-level backtest engine (Dark Matter method).
Reusable module: builds 15-min bars + multi-expiry vanna level map (no lookahead)
+ sticky VIX regime, walks bars, fades resistance walls / buys support floors on
rejection-hold candles, trades level-to-level. Used by _tmp_l2l_run.py.
"""
import os
from collections import defaultdict
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
_eng=create_engine(os.environ['DATABASE_URL'])
CONN=_eng.connect().execution_options(isolation_level="AUTOCOMMIT")

def days_between(a,b):
    return [r[0].isoformat() for r in CONN.execute(text("""
        SELECT DISTINCT (ts AT TIME ZONE 'America/New_York')::date d FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN DATE :a AND DATE :b
        ORDER BY 1"""),{"a":a,"b":b}).fetchall()]

_CACHE={}
def bars15(day):
    if ("b",day) in _CACHE: return _CACHE[("b",day)]
    r=_bars15(day); _CACHE[("b",day)]=r; return r
def _bars15(day):
    """15-min OHLC from chain_snapshots spot (09:30-15:55)."""
    rows=CONN.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') et, spot FROM chain_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND spot IS NOT NULL
          AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN TIME '09:30' AND TIME '16:00'
        ORDER BY ts"""),{"d":day}).fetchall()
    buckets=defaultdict(list)
    for et,spot in rows:
        b=et.replace(minute=(et.minute//15)*15,second=0,microsecond=0)
        buckets[b].append(float(spot))
    out=[]
    for b in sorted(buckets):
        v=buckets[b]; out.append({"t":b,"o":v[0],"h":max(v),"l":min(v),"c":v[-1]})
    return out

def level_map(day, before_et):
    key=("l",day,before_et.hour,before_et.minute)
    if key in _CACHE: return _CACHE[key]
    r=_level_map(day,before_et); _CACHE[key]=r; return r
def _level_map(day, before_et):
    """multi-expiry vanna summed per strike, latest value <= before_et (no lookahead).
    Index-friendly: bound ts_utc to a UTC range around the day (avoids per-row ::date cast)."""
    from datetime import date as _date, timedelta as _td
    y,m,d=map(int,day.split("-")); d0=_date(y,m,d); d1=d0+_td(days=1)
    rows=CONN.execute(text("""
        SELECT DISTINCT ON (expiration_option, strike) strike, value
        FROM volland_exposure_points
        WHERE ts_utc >= :d0 AND ts_utc < :d1 AND greek='vanna'
          AND expiration_option IN ('TODAY','THIS_WEEK','THIRTY_NEXT_DAYS')
          AND (ts_utc AT TIME ZONE 'America/New_York') <= :bt
        ORDER BY expiration_option, strike, ts_utc DESC
    """),{"d0":d0.isoformat(),"d1":d1.isoformat(),"bt":before_et}).fetchall()
    agg=defaultdict(float)
    for strike,val in rows: agg[float(strike)]+=float(val)
    return agg

def vix_series(day):
    if ("v",day) in _CACHE: return _CACHE[("v",day)]
    r=_vix_series(day); _CACHE[("v",day)]=r; return r
def _vix_series(day):
    rows=CONN.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, vix FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND vix IS NOT NULL ORDER BY ts"""),{"d":day}).fetchall()
    return [(et,float(v)) for et,v in rows]

# persistent sticky regime across days
_REGIME={"state":"NORMAL"}
def regime_reset(): _REGIME["state"]="NORMAL"
def regime_update(vix):
    if vix>=20: _REGIME["state"]="EXTREME"
    elif vix<18: _REGIME["state"]="NORMAL"
    return _REGIME["state"]

def run_day(day, P, carry_regime=True):
    """P=params dict. Returns list of trades for the day."""
    bars=bars15(day)
    if len(bars)<6: return []
    spot0=bars[0]["o"]
    lmap=level_map(day, bars[0]["t"]+timedelta(minutes=10))   # ~09:40 snapshot
    noon=level_map(day, datetime.fromisoformat(day).replace(hour=12,minute=30))
    vix=vix_series(day)
    def vix_at(t):
        prior=[v for (et,v) in vix if et<=t]
        return prior[-1] if prior else (vix[0][1] if vix else 18)
    def levels_at(t, px):
        """Dominant corridor ceiling (strongest +vanna just above px) and floor
        (strongest -vanna just below px) within a band — faithful to 'fade the
        ceiling, buy the floor', not every intermediate node."""
        m = noon if (noon and t.hour>=13) else lmap
        band=P.get("band",70)
        up=[(k,v) for k,v in m.items() if px < k <= px+band and v>P["minv"]]
        dn=[(k,v) for k,v in m.items() if px-band <= k < px and v< -P["minv"]]
        ceil=[max(up,key=lambda x:x[1])] if up else []
        floor=[min(dn,key=lambda x:x[1])] if dn else []
        return ceil, floor
    def full_levels(t):
        m = noon if (noon and t.hour>=13) else lmap
        return sorted(k for k,v in m.items() if abs(v)>P["minv"])
    def magnet(t, px):
        """Dominant vanna node (max |vanna|) within band of px — the magnet."""
        m = noon if (noon and t.hour>=13) else lmap
        band=P.get("band",70)
        cand=[(k,v) for k,v in m.items() if abs(k-px)<=band and abs(v)>P["minv"]]
        if not cand: return None
        return max(cand,key=lambda x:abs(x[1]))[0]
    trades=[]; pos=None
    for bar in bars:
        t=bar["t"]; c=bar["c"]
        st=regime_update(vix_at(t))
        res,sup=levels_at(t, bar["o"])
        levs=full_levels(t)
        if pos:
            d,en,stop,tgt=pos
            hs=bar["l"]<=stop if d=="L" else bar["h"]>=stop
            ht=bar["h"]>=tgt if d=="L" else bar["l"]<=tgt
            if hs or ht or bar is bars[-1]:
                px= stop if hs else (tgt if ht else c)
                pts=(px-en) if d=="L" else (en-px)
                trades.append({"day":day,"dir":d,"pts":pts,"reg":st,"en":en,"px":px}); pos=None
            continue
        def tgt_below(price):
            below=[x for x in levs if x<price-1]
            return min(max(below) if below else price-P["mintgt"], price-P["mintgt"])
        def tgt_above(price):
            above=[x for x in levs if x>price+1]
            return max(min(above) if above else price+P["mintgt"], price+P["mintgt"])
        if P.get("mode")=="magnet" and st!="EXTREME":
            # ---- MAGNET-FOLLOW (NORMAL): trade TOWARD dominant vanna node, target=node ----
            mk=magnet(t, c)
            if mk is not None:
                if mk> c+P["mintgt"] and c>bar["o"]:        # magnet above + up momentum -> LONG to it
                    pos=("L",c,c-P["stop"],mk)
                elif mk< c-P["mintgt"] and c<bar["o"]:      # magnet below + down momentum -> SHORT to it
                    pos=("S",c,c+P["stop"],mk)
            continue
        # ---- FADE SHORT at resistance wall (rally into wall, 15-min reject) — both regimes ----
        for k,v in res:
            if bar["h"]>=k-P["touch"] and c<=k-P["confirm"] and c<k:
                pos=("S",c,k+P["stop"],tgt_below(c)); break
        if pos: continue
        if st=="EXTREME":
            # ---- BREAKDOWN SHORT: price closes below a support floor -> cascade continuation ----
            for k,v in sup:
                if bar["o"]>=k and c<k-P["confirm"]:
                    pos=("S",c,k+P["stop"],tgt_below(c)); break
        else:
            # ---- NORMAL: BUY LONG at support floor (dip, 15-min hold) ----
            for k,v in sup:
                if bar["l"]<=k+P["touch"] and c>=k+P["confirm"] and c>k:
                    pos=("L",c,k-P["stop"],tgt_above(c)); break
    return trades

def run_range(start,end,P):
    regime_reset()
    allt=[]
    for d in days_between(start,end):
        allt+=run_day(d,P)
    return allt

def summarize(trades):
    if not trades: return dict(n=0,wr=0,pts=0,usd=0,mdd=0,avg=0)
    pts=[t["pts"] for t in trades]; tot=sum(pts); w=sum(1 for p in pts if p>0)
    cum=0;peak=0;mdd=0
    for p in pts:
        cum+=p; peak=max(peak,cum); mdd=min(mdd,cum-peak)
    return dict(n=len(pts),wr=round(100*w/len(pts)),pts=round(tot,1),usd=round(tot*5),
                mdd=round(mdd*5),avg=round(tot/len(pts),2))
