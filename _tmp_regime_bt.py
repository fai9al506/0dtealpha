"""Regime backtest: do placed LONGS lose specifically on confirmed down-trend /
risk-off intraday regimes? Build per-trade features from data available in
REAL TIME at entry (no lookahead): spot vs session open, spot vs session
high-so-far, VIX level, VIX change since open, overvix, paradigm.

Broker P&L per lid from real_trade_orders.state fills (post-FIFO-reconcile).
Era: post-V16 (2026-05-18 .. today).
"""
import os, json
from collections import defaultdict
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])

ERA = "2026-05-18"

with engine.connect() as conn:
    # 1) session open spot + intraday spot path per day, from setup_log (every signal logs spot)
    spath = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') as et, spot, vix
        FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date >= DATE :era
          AND spot IS NOT NULL
          AND (ts AT TIME ZONE 'America/New_York')::time >= TIME '09:30'
        ORDER BY ts ASC
    """), {"era": ERA}).fetchall()

    # build per-day ordered list
    day_path = defaultdict(list)  # day -> list of (et_dt, spot, vix)
    for et, spot, vix in spath:
        day_path[et.date().isoformat()].append((et, float(spot), float(vix) if vix is not None else None))

    def regime_at(day, ts_et):
        """Return features using only data at/<= ts_et that day."""
        path = day_path.get(day, [])
        prior = [p for p in path if p[0] <= ts_et]
        if not prior:
            return None
        open_spot = path[0][1]
        open_vix = next((v for (_, _, v) in path if v is not None), None)
        hi = max(p[1] for p in prior)
        lo = min(p[1] for p in prior)
        cur_spot = prior[-1][1]
        cur_vix = next((v for (_, _, v) in reversed(prior) if v is not None), None)
        return {
            "open_spot": open_spot,
            "from_open": cur_spot - open_spot,          # negative = down day
            "from_hi": cur_spot - hi,                   # how far below session high (<=0)
            "from_lo": cur_spot - lo,
            "vix": cur_vix,
            "vix_chg": (cur_vix - open_vix) if (cur_vix is not None and open_vix is not None) else None,
        }

    # 2) all placed lids since era with state
    rows = conn.execute(text("""
        SELECT sl.id, (sl.ts AT TIME ZONE 'America/New_York') as et,
               sl.setup_name, sl.direction, sl.grade, sl.paradigm,
               sl.spot, sl.vix, sl.overvix, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date >= DATE :era
        ORDER BY sl.ts ASC
    """), {"era": ERA}).fetchall()

trades = []
for r in rows:
    sid, et, setup, direction, grade, paradigm, spot, vix, ovx, st = r
    if not isinstance(st, dict):
        try: st = json.loads(st)
        except Exception: st = {}
    entry = st.get('fill_price'); exitp = st.get('close_fill_price')
    if entry is None or exitp is None:
        continue
    is_short = ('bear' in (direction or '')) or (direction == 'short') or (setup == 'AG Short')
    pts = (entry - exitp) if is_short else (exitp - entry)
    usd = pts * 5.0
    day = et.date().isoformat()
    reg = regime_at(day, et)
    trades.append({
        "lid": sid, "et": et, "day": day, "setup": setup,
        "is_short": is_short, "grade": grade, "paradigm": paradigm,
        "pts": pts, "usd": usd, "reg": reg,
    })

longs = [t for t in trades if not t['is_short']]
shorts = [t for t in trades if t['is_short']]
print(f"Era {ERA}..today | placed trades w/ fills: {len(trades)} ({len(longs)} long, {len(shorts)} short)\n")

def stats(ts):
    n = len(ts);
    if n == 0: return "n=0"
    w = sum(1 for t in ts if t['pts'] > 0)
    usd = sum(t['usd'] for t in ts)
    return f"n={n:>3} WR={100*w/n:4.0f}% net=${usd:+8.0f} avg=${usd/n:+6.1f}"

print("ALL LONGS:  ", stats(longs))
print("ALL SHORTS: ", stats(shorts))
print()

# Regime split for LONGS: define "down regime" candidates
def has(t): return t['reg'] is not None and t['reg']['from_open'] is not None
L = [t for t in longs if has(t)]

for thr in [-15, -20, -30, -40]:
    down = [t for t in L if t['reg']['from_open'] <= thr]
    up = [t for t in L if t['reg']['from_open'] > thr]
    print(f"LONGS  from_open<= {thr:>4}: {stats(down)}   |  from_open> {thr:>4}: {stats(up)}")
print()
for thr in [-15, -25, -40]:
    down = [t for t in L if t['reg']['from_hi'] is not None and t['reg']['from_hi'] <= thr]
    print(f"LONGS  from_session_high<= {thr:>4} (deep below high): {stats(down)}")
print()
# VIX rising regime
LV = [t for t in L if t['reg'] and t['reg']['vix_chg'] is not None]
for thr in [0.5, 1.0, 1.5]:
    rise = [t for t in LV if t['reg']['vix_chg'] >= thr]
    calm = [t for t in LV if t['reg']['vix_chg'] < thr]
    print(f"LONGS  vix_chg>= +{thr}: {stats(rise)}   |  vix_chg< +{thr}: {stats(calm)}")
print()
# Combined: down>=20 AND vix rising>=1
combo = [t for t in L if t['reg']['from_open'] <= -20 and (t['reg']['vix_chg'] or 0) >= 1.0]
noncombo = [t for t in L if not (t['reg']['from_open'] <= -20 and (t['reg']['vix_chg'] or 0) >= 1.0)]
print("LONGS in DOWN+VIXrising (from_open<=-20 & vix_chg>=+1):", stats(combo))
print("LONGS otherwise:                                       ", stats(noncombo))
