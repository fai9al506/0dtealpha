"""Daily V13 vs V14 comparison Mar 1 - Apr 29."""
import psycopg2
from datetime import time as dtime, date
from collections import defaultdict

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

# Get daily regime
cur.execute("""
  WITH daily AS (
    SELECT DATE(ts AT TIME ZONE 'America/New_York') d,
           (array_agg(spot ORDER BY ts ASC) FILTER (WHERE spot IS NOT NULL))[1] op,
           (array_agg(spot ORDER BY ts DESC) FILTER (WHERE spot IS NOT NULL))[1] cl
    FROM chain_snapshots WHERE DATE(ts AT TIME ZONE 'America/New_York') >= '2026-03-01' GROUP BY 1
  )
  SELECT d, op, cl, cl - op net,
         CASE WHEN cl - op > 15 THEN 'BULL' WHEN cl - op < -15 THEN 'BEAR' ELSE 'CHOP' END
  FROM daily ORDER BY d
""")
regimes = {r[0]: (r[3], r[4]) for r in cur.fetchall()}

# Get all SC trades with full V13 context
cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t, DATE(ts AT TIME ZONE 'America/New_York') as d,
         direction, grade, paradigm, spot, vix, greek_alignment,
         v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side,
         outcome_result, outcome_pnl
  FROM setup_log WHERE setup_name='Skew Charm'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL ORDER BY ts
""")
trades = cur.fetchall()

BAD = ("GEX-LIS", "AG-LIS", "AG-PURE", "SIDIAL-EXTREME", "BOFA-MESSY")

def v13(t):
    lid, ts, d, dr, grade, par, spot, vix, align, gex, dd, cliff, peak, res, pnl = t
    is_long = dr == "long"
    if not is_long and grade in ("C", "LOG"): return False
    t_only = ts.time()
    if dtime(14, 30) <= t_only < dtime(15, 0): return False
    if t_only >= dtime(15, 30): return False
    if is_long:
        if par == "SIDIAL-EXTREME": return False
        if align is None or align < 2: return False
        if cliff == 'A' and peak == 'B': return False
        return True
    else:
        if gex is not None and float(gex) >= 75: return False
        if dd is not None and float(dd) >= 3_000_000_000: return False
        if par == "GEX-LIS": return False
        if cliff == 'A' and peak == 'B': return False
        return True

def v14(t):
    lid, ts, d, dr, grade, par, spot, vix, align, gex, dd, cliff, peak, res, pnl = t
    is_long = dr == "long"
    if not is_long and grade in ("C", "LOG"): return False
    t_only = ts.time()
    if dtime(14, 30) <= t_only < dtime(15, 0): return False
    if t_only >= dtime(15, 30): return False
    if is_long:
        if par == "SIDIAL-EXTREME": return False
        if cliff == 'A' and peak == 'B': return False
        # V14 NEW: only block align=3 in bad paradigms (replaces align>=2)
        if align == 3 and par in BAD: return False
        return True
    else:
        if gex is not None and float(gex) >= 75: return False
        if dd is not None and float(dd) >= 3_000_000_000: return False
        if par == "GEX-LIS": return False
        if cliff == 'A' and peak == 'B': return False
        return True

# Aggregate per day, breakout by direction
daily = defaultdict(lambda: {"v13_long": 0.0, "v14_long": 0.0, "v13_short": 0.0, "v14_short": 0.0,
                              "v13_long_n": 0, "v14_long_n": 0, "v13_short_n": 0, "v14_short_n": 0})

for t in trades:
    d = t[2]; dr = t[3]; pnl = float(t[14]) if t[14] else 0
    is_long = dr == "long"
    side_v13 = "long" if is_long else "short"
    side_v14 = "long" if is_long else "short"
    if v13(t):
        daily[d][f"v13_{side_v13}"] += pnl
        daily[d][f"v13_{side_v13}_n"] += 1
    if v14(t):
        daily[d][f"v14_{side_v14}"] += pnl
        daily[d][f"v14_{side_v14}_n"] += 1

# Print table
print("="*112)
print(f"{'Date':<12}{'Reg':<6}{'Net':>7}  {'V13 LONG':>14}{'V14 LONG':>14}{'V13 SHRT':>14}{'V14 SHRT':>14}{'V13 TOT':>11}{'V14 TOT':>11}{'DELTA':>9}")
print("="*112)

cum_v13 = 0; cum_v14 = 0
for d in sorted(daily.keys()):
    s = daily[d]
    reg_info = regimes.get(d, (0, "?"))
    net = reg_info[0] or 0
    reg = reg_info[1]

    v13_long = s["v13_long"] * 5
    v14_long = s["v14_long"] * 5
    v13_short = s["v13_short"] * 5
    v14_short = s["v14_short"] * 5
    v13_total = v13_long + v13_short
    v14_total = v14_long + v14_short
    delta = v14_total - v13_total
    cum_v13 += v13_total
    cum_v14 += v14_total

    flag = ""
    if delta >= 100: flag = " <-- BIG"
    elif delta <= -50: flag = " <-- HURT"
    elif reg == "BULL": flag = " (bull)"

    def fmt(v, n):
        if n == 0: return "    -"
        return f"${v:+5.0f}({n}t)"

    print(f"{str(d):<12}{reg:<6}{net:>+6.0f}  {fmt(v13_long, s['v13_long_n']):>14}{fmt(v14_long, s['v14_long_n']):>14}{fmt(v13_short, s['v13_short_n']):>14}{fmt(v14_short, s['v14_short_n']):>14}${v13_total:>+8.0f} ${v14_total:>+8.0f} ${delta:>+6.0f}{flag}")

print("="*112)
print(f"\nCUMULATIVE V13: ${cum_v13:+.0f}")
print(f"CUMULATIVE V14: ${cum_v14:+.0f}")
print(f"V14 vs V13 delta: ${cum_v14 - cum_v13:+.0f}")

# Day distribution
print(f"\n=== DELTA DISTRIBUTION ===")
deltas = []
for d in sorted(daily.keys()):
    s = daily[d]
    delta = (s["v14_long"] + s["v14_short"] - s["v13_long"] - s["v13_short"]) * 5
    deltas.append((d, delta))

pos_days = [d for d in deltas if d[1] > 0]
neg_days = [d for d in deltas if d[1] < 0]
zero_days = [d for d in deltas if d[1] == 0]
print(f"Days V14 better: {len(pos_days)}  (avg ${sum(d[1] for d in pos_days)/len(pos_days):+.0f})")
print(f"Days V14 worse:  {len(neg_days)}  (avg ${sum(d[1] for d in neg_days)/len(neg_days):+.0f})")
print(f"Days tied:       {len(zero_days)}")

# Top contributors
print(f"\n=== TOP 5 V14 ADVANTAGE DAYS ===")
top = sorted(deltas, key=lambda x: -x[1])[:5]
for d, dt in top:
    reg = regimes.get(d, (0, "?"))[1]
    print(f"  {d} [{reg}]: V14 +${dt:+.0f} better")

print(f"\n=== TOP 5 V14 DISADVANTAGE DAYS ===")
bot = sorted(deltas, key=lambda x: x[1])[:5]
for d, dt in bot:
    reg = regimes.get(d, (0, "?"))[1]
    print(f"  {d} [{reg}]: V14 ${dt:+.0f} worse")
