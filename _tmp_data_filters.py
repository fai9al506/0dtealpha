"""Test data-based filters on SC shorts.
Idea: use ACTUAL market/Volland data at fire time to detect bullish regime,
not just intraday SPX trend.
"""
import psycopg2
from datetime import time as dtime

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

# Pull SC shorts with full data: vanna_all, gex magnet, paradigm, charm context
cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
         DATE(ts AT TIME ZONE 'America/New_York') as d,
         grade, paradigm, spot, vix, greek_alignment,
         v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side,
         vanna_all, vanna_weekly, vanna_monthly, spot_vol_beta,
         outcome_result, outcome_pnl
  FROM setup_log
  WHERE setup_name='Skew Charm' AND direction='short'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL
  ORDER BY ts
""")
rows = cur.fetchall()
print(f"Total SC shorts: {len(rows)}\n")

# Compute the VPB-Bull regime classifier at each fire time
def classify_regime_at_fire(ts, spot):
    """Apply VPB-Bull's 4-zone classifier at this trade's fire time."""
    cur.execute("""
      SELECT strike::float, value::float FROM volland_exposure_points
      WHERE greek='vanna' AND expiration_option='THIS_WEEK'
        AND ts_utc = (SELECT MAX(ts_utc) FROM volland_exposure_points
                      WHERE greek='vanna' AND ts_utc <= %s AND expiration_option='THIS_WEEK')
        AND ABS(strike::float - %s) <= 50
    """, (ts, spot))
    levels = [(float(r[0]), float(r[1])) for r in cur.fetchall()]
    near = [(s, v) for s, v in levels if abs(v) >= 10e6]
    if not near: return "mixed"
    pos_above = [v for s, v in near if v > 0 and s > spot]
    pos_below = [v for s, v in near if v > 0 and s < spot]
    neg_above = [abs(v) for s, v in near if v < 0 and s > spot]
    neg_below = [abs(v) for s, v in near if v < 0 and s < spot]
    bull = (max(pos_above) if pos_above else 0) + (max(neg_below) if neg_below else 0)
    bear = (max(pos_below) if pos_below else 0) + (max(neg_above) if neg_above else 0)
    if abs(bull - bear) < 15e6: return "mixed"
    return "bullish" if bull > bear else "bearish"

# Enrich with vanna regime
print("Computing vanna regime per trade (this takes ~1 min)...")
enriched = []
for r in rows:
    lid, ts, d, grade, par, spot, vix, align, gex, dd, cliff, peak, va, vw, vm, svb, res, pnl = r
    spot_f = float(spot)
    regime = classify_regime_at_fire(ts, spot_f)
    enriched.append({
        "id": lid, "ts": ts, "d": d, "spot": spot_f,
        "grade": grade, "par": par, "vix": float(vix) if vix else None,
        "align": align, "gex": gex, "dd": dd, "cliff": cliff, "peak": peak,
        "vanna_all": float(va) if va is not None else None,
        "vanna_weekly": float(vw) if vw is not None else None,
        "regime": regime,
        "res": res, "pnl": float(pnl) if pnl else 0,
    })
print(f"Done. {len(enriched)} trades enriched.\n")

# V13 filter
def v13_pass(t):
    if t["grade"] in ("C", "LOG"): return False
    if dtime(14, 30) <= t["ts"].time() < dtime(15, 0): return False
    if t["ts"].time() >= dtime(15, 30): return False
    if t["gex"] is not None and float(t["gex"]) >= 75: return False
    if t["dd"] is not None and float(t["dd"]) >= 3_000_000_000: return False
    if t["par"] == "GEX-LIS": return False
    if t["cliff"] == 'A' and t["peak"] == 'B': return False
    return True

def stats(group, label):
    n = len(group)
    if n == 0: return f"{label:<55}0t"
    w = sum(1 for t in group if t["res"]=="WIN")
    l = sum(1 for t in group if t["res"]=="LOSS")
    e = sum(1 for t in group if t["res"]=="EXPIRED")
    pnl = sum(t["pnl"] for t in group)
    wr = w/(w+l)*100 if w+l else 0
    eq=0;pk=0;mdd=0
    for t in sorted(group, key=lambda x: x["ts"]):
        eq+=t["pnl"]; pk=max(pk,eq); mdd=max(mdd, pk-eq)
    return f"{label:<55}{n:>4}t W={w:<3} L={l:<3} E={e:<2} WR={wr:>5.1f}% PnL={pnl:+8.1f}pt ${pnl*5:+>5.0f} MaxDD={mdd:>5.1f}"

# Baseline: V13
v13 = [t for t in enriched if v13_pass(t)]
print(stats(enriched, "ALL no filter"))
print(stats(v13, "V13 baseline"))

# === DATA RULE 1: Block SC short when vanna regime is BULLISH ===
print("\n=== Data Rule 1: V13 + block when vanna_regime=bullish ===")
r1 = [t for t in v13 if t["regime"] != "bullish"]
print(stats(r1, "V13 + regime != bullish"))
blocked_r1 = [t for t in v13 if t["regime"] == "bullish"]
print(stats(blocked_r1, "Blocked (regime=bullish)"))

# === DATA RULE 2: Block when vanna_all > 0 (dealers net bullish position) ===
# Note: vanna_all sign interpretation might be inverse — let me test both directions
print("\n=== Data Rule 2: V13 + vanna_all conditions ===")
for cond_label, cond_fn in [
    ("vanna_all > 0 (block)", lambda t: t["vanna_all"] is None or t["vanna_all"] <= 0),
    ("vanna_all < 0 (block)", lambda t: t["vanna_all"] is None or t["vanna_all"] >= 0),
    ("vanna_all > 1B (block)", lambda t: t["vanna_all"] is None or t["vanna_all"] <= 1e9),
    ("vanna_all < -1B (block)", lambda t: t["vanna_all"] is None or t["vanna_all"] >= -1e9),
]:
    kept = [t for t in v13 if cond_fn(t)]
    blkd = [t for t in v13 if not cond_fn(t)]
    print(stats(kept, f"V13 + {cond_label}: KEPT"))
    print(stats(blkd, f"  Blocked: {cond_label}"))

# === DATA RULE 3: Time-of-day + paradigm ===
# Hypothesis: BOFA-PURE shorts in afternoon = "we've shifted to bullish overshoot"
print("\n=== Data Rule 3: Block BOFA-PURE shorts after 12:00 ===")
def r3(t):
    return not (t["par"] == "BOFA-PURE" and t["ts"].time() >= dtime(12, 0))
r3_kept = [t for t in v13 if r3(t)]
r3_blkd = [t for t in v13 if not r3(t)]
print(stats(r3_kept, "V13 + BOFA-PURE after 12:00 = block"))
print(stats(r3_blkd, "Blocked: BOFA-PURE after 12:00"))

# === DATA RULE 4: VIX direction (if dropping during day, bullish; rising = bearish) ===
# Need VIX trend. Use VIX level at fire vs VIX at day open.
print("\n=== Data Rule 4: VIX behavior ===")
# Get VIX at start of day
def get_day_open_vix(d):
    cur.execute("""
      SELECT vix FROM chain_snapshots
      WHERE DATE(ts AT TIME ZONE 'America/New_York') = %s AND vix IS NOT NULL
      ORDER BY ts LIMIT 1
    """, (d,))
    r = cur.fetchone()
    return float(r[0]) if r else None

vix_open_cache = {}
for t in enriched:
    if t["d"] not in vix_open_cache:
        vix_open_cache[t["d"]] = get_day_open_vix(t["d"])
    t["vix_open"] = vix_open_cache[t["d"]]
    if t["vix"] and t["vix_open"]:
        t["vix_delta"] = t["vix"] - t["vix_open"]
    else:
        t["vix_delta"] = None

# Block when VIX dropping >= X (vol compression rally → bullish)
for vd in [-0.5, -1.0, -1.5, -2.0]:
    def vc(t, vd=vd):
        if t["vix_delta"] is None: return True
        return t["vix_delta"] >= vd  # block if VIX dropped more than vd
    kept = [t for t in v13 if vc(t)]
    blkd = [t for t in v13 if not vc(t)]
    print(f"  Block VIX_delta < {vd}: kept={len(kept)}t  pnl=${sum(t['pnl'] for t in kept)*5:+.0f}  blkd={len(blkd)}t pnl=${sum(t['pnl'] for t in blkd)*5:+.0f}")

# === DATA RULE 5: Composite — bullish regime + VIX dropping ===
print("\n=== Data Rule 5: Composite — block if (regime=bullish AND VIX dropping >0.5) ===")
def r5(t):
    if t["regime"] != "bullish": return True
    if t["vix_delta"] is None: return True
    return t["vix_delta"] >= -0.5  # block only if BOTH
r5_kept = [t for t in v13 if r5(t)]
r5_blkd = [t for t in v13 if not r5(t)]
print(stats(r5_kept, "V13 + (NOT (regime=bull AND VIX dropping))"))
print(stats(r5_blkd, "Blocked: regime=bull AND VIX dropping"))

# === DATA RULE 6: vanna_weekly distinguish ===
print("\n=== Data Rule 6: vanna_weekly conditions ===")
for cond_label, cond_fn in [
    ("vanna_weekly > 1B (block)", lambda t: t["vanna_weekly"] is None or t["vanna_weekly"] <= 1e9),
    ("vanna_weekly < -1B (block)", lambda t: t["vanna_weekly"] is None or t["vanna_weekly"] >= -1e9),
    ("vanna_weekly > 500M (block)", lambda t: t["vanna_weekly"] is None or t["vanna_weekly"] <= 500e6),
]:
    kept = [t for t in v13 if cond_fn(t)]
    blkd = [t for t in v13 if not cond_fn(t)]
    print(stats(kept, f"V13 + {cond_label}"))

# === Apr 24-27 specific ===
print("\n=== Apr 24-27 specific ===")
from datetime import date
recent = [t for t in v13 if t["d"] >= date(2026, 4, 24)]
print(stats(recent, "V13 baseline Apr 24-27"))
print(stats([t for t in recent if t["regime"] != "bullish"], "Rule 1 (regime != bullish)"))
print(stats([t for t in recent if r3(t)], "Rule 3 (BOFA-PURE after 12 block)"))
print(stats([t for t in recent if r5(t)], "Rule 5 (composite)"))
