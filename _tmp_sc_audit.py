"""SC + V13 performance audit — projection vs actual."""
import psycopg2
from datetime import time as dtime, date, datetime
from collections import defaultdict

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

# Era boundaries (commit dates)
ERAS = [
    ("V11 (Mar 24-28)", date(2026, 3, 24), date(2026, 3, 29)),
    ("V12 (Mar 29-Apr 7)", date(2026, 3, 29), date(2026, 4, 8)),
    ("V12-fix bugs (Apr 8-16)", date(2026, 4, 8), date(2026, 4, 17)),
    ("V13 era (Apr 17+)", date(2026, 4, 17), date(2026, 4, 29)),
]

def v13_pass(t):
    """Replicate V13 filter for SC short."""
    lid, ts, d, grade, par, spot, vix, align, gex, dd, cliff, peak, res, pnl, dr = t
    is_long = dr == "long"
    if grade in ("C", "LOG") and not is_long: return False
    if is_long and grade in ("C", "LOG"):
        # SC longs don't have grade gate per code
        pass
    t_only = ts.time()
    if dtime(14, 30) <= t_only < dtime(15, 0): return False
    if t_only >= dtime(15, 30): return False
    if not is_long:
        if gex is not None and float(gex) >= 75: return False
        if dd is not None and float(dd) >= 3_000_000_000: return False
        if par == "GEX-LIS": return False
        if cliff == 'A' and peak == 'B': return False
    else:
        if par == "SIDIAL-EXTREME": return False
        if align is not None and align < 2: return False
        if cliff == 'A' and peak == 'B': return False
    return True

# Pull all SC trades
cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t,
         DATE(ts AT TIME ZONE 'America/New_York') as d,
         grade, paradigm, spot, vix, greek_alignment,
         v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side,
         outcome_result, outcome_pnl, direction
  FROM setup_log WHERE setup_name='Skew Charm'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL ORDER BY ts
""")
all_sc = cur.fetchall()

print(f"Total SC trades since Mar 1: {len(all_sc)}\n")

# === SC by era ===
print("=== SC BY ERA (portal P&L) ===")
print(f"{'Era':<30}{'Trades':>8}{'Wins':>6}{'Losses':>8}{'WR':>7}{'PnL':>10}{'$':>9}{'Avg':>9}")
print("-"*90)
for label, start, end in ERAS:
    sub = [t for t in all_sc if start <= t[2] < end]
    n = len(sub); w = sum(1 for t in sub if t[12] == "WIN"); l = sum(1 for t in sub if t[12] == "LOSS")
    pnl = sum(float(t[13]) if t[13] else 0 for t in sub)
    wr = w/(w+l)*100 if w+l else 0
    avg = pnl/n if n else 0
    days = (end - start).days
    print(f"  {label:<30}{n:>8}{w:>6}{l:>8}{wr:>6.1f}%{pnl:>8.1f}pt{pnl*5:>+8.0f}{avg:>+6.2f}pt/t")

# === Daily P&L distribution V13 era ===
print("\n=== DAILY P&L V13 era (Apr 17+) — was Apr 24 really an outlier? ===")
v13_era = [t for t in all_sc if t[2] >= date(2026, 4, 17)]
by_day = defaultdict(list)
for t in v13_era:
    by_day[t[2]].append(t)

for d in sorted(by_day.keys()):
    sub = by_day[d]
    by_dir = defaultdict(list)
    for t in sub:
        by_dir[t[14]].append(t)
    pnl_total = sum(float(t[13]) if t[13] else 0 for t in sub)
    n = len(sub)
    flag = " <-- BIG LOSS" if pnl_total < -30 else (" <-- big win" if pnl_total > 50 else "")
    short_pnl = sum(float(t[13]) if t[13] else 0 for t in by_dir.get('short',[]))
    long_pnl = sum(float(t[13]) if t[13] else 0 for t in by_dir.get('long',[]))
    print(f"  {d}  total={pnl_total:+7.1f}pt ({n}t)  Short={short_pnl:+6.1f}pt ({len(by_dir.get('short',[]))}t)  Long={long_pnl:+6.1f}pt ({len(by_dir.get('long',[]))}t){flag}")

# === V13 era WITH V13 filter applied ===
print("\n=== V13 era — what actually passes V13 filter ===")
v13_era_passed = [t for t in v13_era if v13_pass(t)]
v13_era_blocked = [t for t in v13_era if not v13_pass(t)]
def stats(group, label):
    n=len(group); w=sum(1 for t in group if t[12]=="WIN"); l=sum(1 for t in group if t[12]=="LOSS")
    pnl=sum(float(t[13]) if t[13] else 0 for t in group); wr=w/(w+l)*100 if w+l else 0
    return f"{label:<35}{n:>4}t W={w} L={l} WR={wr:.1f}% PnL={pnl:+6.1f}pt ${pnl*5:+>5.0f}"

print(stats(v13_era, "All V13 era SC"))
print(stats(v13_era_passed, "  passes V13 (placed)"))
print(stats(v13_era_blocked, "  blocked by V13"))

# === Where did the degradation come from? Check chop vs bullish ===
print("\n=== V13 era SC by daily regime ===")
cur.execute("""
  WITH daily AS (
    SELECT DATE(ts AT TIME ZONE 'America/New_York') d,
           (array_agg(spot ORDER BY ts ASC) FILTER (WHERE spot IS NOT NULL))[1] open_,
           (array_agg(spot ORDER BY ts DESC) FILTER (WHERE spot IS NOT NULL))[1] close_
    FROM chain_snapshots WHERE DATE(ts AT TIME ZONE 'America/New_York') >= '2026-04-17' GROUP BY 1
  )
  SELECT d, CASE WHEN (close_ - open_) > 15 THEN 'BULLISH' WHEN (close_ - open_) < -15 THEN 'BEARISH' ELSE 'CHOP' END regime
  FROM daily ORDER BY d
""")
day_regime = {r[0]: r[1] for r in cur.fetchall()}

regime_buckets = defaultdict(lambda: defaultdict(list))
for t in v13_era_passed:
    r = day_regime.get(t[2], "?")
    regime_buckets[r][t[14]].append(t)

for r in ("BULLISH", "BEARISH", "CHOP"):
    if r not in regime_buckets: continue
    for dr in ("short", "long"):
        sub = regime_buckets[r].get(dr, [])
        if not sub: continue
        n=len(sub); w=sum(1 for t in sub if t[12]=="WIN"); l=sum(1 for t in sub if t[12]=="LOSS")
        pnl=sum(float(t[13]) if t[13] else 0 for t in sub); wr=w/(w+l)*100 if w+l else 0
        print(f"  V13 era {r:<8} {dr:<6} {n:>3}t  W={w} L={l}  WR={wr:.1f}%  PnL={pnl:+5.1f}pt  ${pnl*5:+.0f}")

# === Per-trading day pnl & monthly extrapolation ===
print("\n=== V13-era projection ===")
v13_pnl = sum(float(t[13]) if t[13] else 0 for t in v13_era_passed)
trading_days = len(set(t[2] for t in v13_era_passed))
all_v13_days = len(set(t[2] for t in v13_era))
days_in_period = (max(t[2] for t in v13_era) - min(t[2] for t in v13_era)).days + 1
print(f"V13 era: Apr 17 - Apr 28 ({all_v13_days} trading days)")
print(f"SC PnL (V13 passed): {v13_pnl:.1f}pt = ${v13_pnl*5:+.0f}")
print(f"$/day = ${v13_pnl*5/all_v13_days:+.0f}")
print(f"Monthly projection (~21 trading days) = ${v13_pnl*5/all_v13_days*21:+.0f}")

# Compare to original V13 projection ($1,800/mo per memory)
print(f"\nOriginal V13 projection (per memory): +$1,800/mo")
print(f"Actual V13 era pace:                 +${v13_pnl*5/all_v13_days*21:+.0f}/mo")

# Without Apr 24 (the outlier)
v13_no_apr24 = [t for t in v13_era_passed if t[2] != date(2026, 4, 24)]
v13_no_apr24_pnl = sum(float(t[13]) if t[13] else 0 for t in v13_no_apr24)
print(f"Without Apr 24:                      +${v13_no_apr24_pnl*5/all_v13_days*21*all_v13_days/(all_v13_days-1):+.0f}/mo")

# Real broker (TSRT) era - actual placements only
print("\n=== Real broker P&L V13 era (broker-truth fills) ===")
cur.execute("""
  SELECT DATE(r.created_at AT TIME ZONE 'America/New_York') d,
         r.state->>'setup_name', r.state->>'direction',
         (CASE WHEN r.state->>'direction'='long'
               THEN (r.state->>'stop_fill_price')::float - (r.state->>'fill_price')::float
               ELSE (r.state->>'fill_price')::float - (r.state->>'stop_fill_price')::float END) * 5 as usd
  FROM real_trade_orders r
  WHERE r.created_at >= '2026-04-17'
    AND r.state->>'fill_price' IS NOT NULL
    AND r.state->>'stop_fill_price' IS NOT NULL
    AND r.state->>'setup_name'='Skew Charm'
""")
real_sc = cur.fetchall()
real_total = sum(r[3] for r in real_sc if r[3] is not None)
real_days = len(set(r[0] for r in real_sc))
print(f"SC real placements: {len(real_sc)}t over {real_days} days")
print(f"Real P&L: ${real_total:+.0f}")
print(f"$/day (trading days): ${real_total/all_v13_days:+.0f}")
print(f"Monthly projection: ${real_total/all_v13_days*21:+.0f}")
