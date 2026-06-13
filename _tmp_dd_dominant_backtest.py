"""Backtest the 'dominant negative DD strike = support' hypothesis.

Signal at trade entry:
  - From deltaDecay TODAY/SPX points at the nearest volland snapshot to entry ts
  - dominant_neg = strike with most-negative deltaDecay value
  - ratio = |dominant_neg_val| / |2nd_most_negative_val|
  - near = |dominant_neg_strike - spot| <= NEAR_WIN
  - spot_above = spot > dominant_neg_strike
  - SUPPORT_REGIME = near AND spot_above AND ratio >= RATIO_MIN

Question: in SUPPORT_REGIME, do LONGS win more and SHORTS lose more than baseline?

Validation Protocol: real DB outcomes only; report n + WR + pts; split by era.
"""
import os, sys
import psycopg2
from datetime import date
from collections import defaultdict
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC"); ET = ZoneInfo("America/New_York")
NEAR_WIN = 30.0       # dominant neg strike must be within this many pts of spot
RATIO_MIN = 2.0       # dominant neg must be >= this x the 2nd most negative
START = date(2026, 4, 1)   # post-V16-ish era; adjust below
END   = date(2026, 6, 1)

c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

# Pull all deltaDecay snapshots in range, grouped by ts_utc -> list[(strike,val,spot)]
cur.execute("""
    SELECT ts_utc, strike::numeric, value::numeric, current_price::numeric
    FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc::date BETWEEN %s AND %s
    ORDER BY ts_utc
""", (START, END))
snaps = defaultdict(list)
spot_at = {}
for ts, strike, val, cp in cur.fetchall():
    snaps[ts].append((float(strike), float(val)))
    if cp is not None:
        spot_at[ts] = float(cp)
snap_ts_sorted = sorted(snaps.keys())
print(f"Loaded {len(snap_ts_sorted)} deltaDecay snapshots {START}..{END}")

def to_aware(t):
    return t if t.tzinfo else t.replace(tzinfo=UTC)

import bisect
def nearest_snap(ts):
    ts = to_aware(ts)
    aware = [to_aware(t) for t in snap_ts_sorted]
    i = bisect.bisect_left(aware, ts)
    cands = []
    if i < len(aware): cands.append(snap_ts_sorted[i])
    if i > 0: cands.append(snap_ts_sorted[i-1])
    if not cands: return None
    best = min(cands, key=lambda t: abs((to_aware(t)-ts).total_seconds()))
    if abs((to_aware(best)-ts).total_seconds()) > 240:  # within 4 min
        return None
    return best

def dd_features(ts, spot):
    sn = nearest_snap(ts)
    if sn is None: return None
    pts = snaps[sn]
    snap_spot = spot_at.get(sn, spot)
    if spot is None: spot = snap_spot
    if spot is None: return None
    negs = sorted([p for p in pts if p[1] < 0], key=lambda p: p[1])  # most negative first
    if not negs: return None
    dom_strike, dom_val = negs[0]
    second = abs(negs[1][1]) if len(negs) >= 2 else 1.0
    ratio = abs(dom_val) / max(second, 1.0)
    near = abs(dom_strike - spot) <= NEAR_WIN
    spot_above = spot > dom_strike
    support_regime = near and spot_above and ratio >= RATIO_MIN
    return dict(dom_strike=dom_strike, dom_val=dom_val, ratio=ratio,
                near=near, spot_above=spot_above, support=support_regime,
                dist=spot-dom_strike)

# Pull trades
cur.execute("""
    SELECT id, ts, setup_name, direction, grade, paradigm, spot, outcome_result, outcome_pnl
    FROM setup_log
    WHERE ts::date BETWEEN %s AND %s
      AND outcome_result IN ('WIN','LOSS','EXPIRED')
      AND outcome_pnl IS NOT NULL
    ORDER BY ts
""", (START, END))
trades = cur.fetchall()
print(f"Loaded {len(trades)} resolved trades\n")

LONG_DIRS = {"long", "bullish"}
SHORT_DIRS = {"short", "bearish"}

def is_long(d): return (d or "").lower() in LONG_DIRS
def is_short(d): return (d or "").lower() in SHORT_DIRS

# Buckets
def newstat(): return {"n":0, "w":0, "pts":0.0}
def add(st, res, pnl):
    st["n"] += 1
    if res == "WIN": st["w"] += 1
    st["pts"] += float(pnl)
def line(name, st):
    if st["n"]==0: return f"  {name:<34} n=0"
    return f"  {name:<34} n={st['n']:<3} WR={100*st['w']/st['n']:>4.0f}%  pts={st['pts']:>+7.1f}  avg={st['pts']/st['n']:>+5.2f}"

# Distance bands of spot ABOVE the dominant negative strike (ratio>=RATIO_MIN, within NEAR_WIN)
# band by dist = spot - dom_strike
def band_of(f):
    if not (f["near"] and f["ratio"] >= RATIO_MIN):
        return None  # not a dominant-neg-below regime
    d = f["dist"]
    if d <= 0:   return "below_strike (spot under)"
    if d <= 3:   return "0-3pt above (PINNED)"
    if d <= 8:   return "3-8pt above (riding)"
    if d <= 15:  return "8-15pt above (floor)"
    return "15-30pt above (far)"

BANDS = ["below_strike (spot under)","0-3pt above (PINNED)","3-8pt above (riding)",
         "8-15pt above (floor)","15-30pt above (far)"]
long_band = {b: newstat() for b in BANDS}
short_band = {b: newstat() for b in BANDS}
long_all = newstat(); short_all = newstat()
long_noregime = newstat(); short_noregime = newstat()
gex_band = {b: newstat() for b in BANDS}

no_feat = 0
for (sid, ts, setup, dir_, grade, para, spot, res, pnl) in trades:
    f = dd_features(ts, float(spot) if spot is not None else None)
    if f is None:
        no_feat += 1; continue
    b = band_of(f)
    if is_long(dir_):
        add(long_all, res, pnl)
        if b: add(long_band[b], res, pnl)
        else: add(long_noregime, res, pnl)
        if b and setup == "GEX Long": add(gex_band[b], res, pnl)
    elif is_short(dir_):
        add(short_all, res, pnl)
        if b: add(short_band[b], res, pnl)
        else: add(short_noregime, res, pnl)

print(f"(skipped {no_feat} trades with no DD snapshot within 4min)\n")
print(f"=== RATIO_MIN={RATIO_MIN}  NEAR_WIN={NEAR_WIN}pt  era {START}..{END} ===\n")
print("LONGS by distance above dominant-negative-DD strike:")
print(line("LONG | all trades", long_all))
print(line("LONG | no dominant-neg regime", long_noregime))
for b in BANDS: print(line("LONG | "+b, long_band[b]))
print("\nSHORTS by distance above dominant-negative-DD strike:")
print(line("SHORT| all trades", short_all))
print(line("SHORT| no dominant-neg regime", short_noregime))
for b in BANDS: print(line("SHORT| "+b, short_band[b]))
print("\nGEX LONG only by band:")
for b in BANDS: print(line("GEX Long | "+b, gex_band[b]))

# --- Magnitude cut: spot ABOVE a dominant neg strike within 15pt, by |dom_val| ---
print("\n=== FLOOR-STRENGTH cut: spot 0-15pt ABOVE dominant neg strike (ratio>=2, within 15pt) ===")
mag_bands = ["|val| 0.5-1B","|val| 1-2B","|val| 2-3B","|val| >=3B"]
def mag_of(v):
    a = abs(v)/1e9
    if a < 1: return "|val| 0.5-1B" if a>=0.5 else None
    if a < 2: return "|val| 1-2B"
    if a < 3: return "|val| 2-3B"
    return "|val| >=3B"
long_mag = {m: newstat() for m in mag_bands}
short_mag = {m: newstat() for m in mag_bands}
for (sid, ts, setup, dir_, grade, para, spot, res, pnl) in trades:
    f = dd_features(ts, float(spot) if spot is not None else None)
    if f is None: continue
    if not (f["near"] and f["ratio"]>=2.0 and 0 < f["dist"] <= 15 and abs(f["dom_strike"]-(float(spot) if spot else f["dom_strike"]))<=15):
        continue
    m = mag_of(f["dom_val"])
    if not m: continue
    if is_long(dir_): add(long_mag[m], res, pnl)
    elif is_short(dir_): add(short_mag[m], res, pnl)
print("LONGS (spot above floor) by floor strength:")
for m in mag_bands: print(line("LONG | "+m, long_mag[m]))
print("SHORTS (spot above floor) by floor strength:")
for m in mag_bands: print(line("SHORT| "+m, short_mag[m]))

cur.close(); c.close()
