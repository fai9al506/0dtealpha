"""Explore: dominant negative DD strike hypothesis (2026-06-01).
Step 1: confirm today's facts before any analysis (Validation Protocol Gate 1).
"""
import os, json
import psycopg2
from datetime import datetime, date
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
TODAY = date(2026, 6, 1)

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

print("=" * 70)
print("PART 1: deltaDecay per-strike snapshot near 9:48 ET (13:48 UTC)")
print("=" * 70)

# Find the volland snapshot timestamp closest to 13:48 UTC today
cur.execute("""
    SELECT DISTINCT ts_utc
    FROM volland_exposure_points
    WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
      AND ts_utc::date = %s
    ORDER BY ts_utc
""", (TODAY,))
all_ts = [r[0] for r in cur.fetchall()]
print(f"Total deltaDecay snapshots today: {len(all_ts)}")
if all_ts:
    first = all_ts[0]; last = all_ts[-1]
    def et(t): return t.replace(tzinfo=UTC).astimezone(ET).strftime('%H:%M:%S')
    print(f"  first {et(first)} ET   last {et(last)} ET")

# target 13:48 UTC
def to_aware(t):
    return t if t.tzinfo else t.replace(tzinfo=UTC)
target = datetime(2026, 6, 1, 13, 48, tzinfo=UTC)
if all_ts:
    closest = min(all_ts, key=lambda t: abs((to_aware(t) - target).total_seconds()))
    print(f"\nClosest snapshot to 9:48 ET = {closest} UTC "
          f"({to_aware(closest).astimezone(ET).strftime('%H:%M:%S ET')})")

    cur.execute("""
        SELECT strike::numeric, value::numeric, current_price::numeric
        FROM volland_exposure_points
        WHERE greek='deltaDecay' AND expiration_option='TODAY' AND ticker='SPX'
          AND ts_utc=%s
        ORDER BY strike
    """, (closest,))
    rows = cur.fetchall()
    spot = rows[0][2] if rows and rows[0][2] is not None else None
    print(f"Spot at snapshot: {spot}")
    print(f"\n  strike      deltaDecay($)    rel_to_spot")
    # sort by abs value to find dominant
    by_abs = sorted(rows, key=lambda r: abs(float(r[1])), reverse=True)
    print("\n  --- TOP 8 by |deltaDecay| ---")
    for s, v, cp in by_abs[:8]:
        rel = float(s) - float(spot) if spot else None
        print(f"   {float(s):>7.0f}   {float(v):>16,.0f}   {rel:+.0f}" if rel is not None
              else f"   {float(s):>7.0f}   {float(v):>16,.0f}")

    # negative strikes specifically
    negs = sorted([r for r in rows if float(r[1]) < 0], key=lambda r: float(r[1]))
    print("\n  --- TOP 6 NEGATIVE deltaDecay (most negative first) ---")
    for s, v, cp in negs[:6]:
        rel = float(s) - float(spot) if spot else None
        print(f"   {float(s):>7.0f}   {float(v):>16,.0f}   rel={rel:+.0f}")
    if len(negs) >= 2:
        ratio = abs(float(negs[0][1])) / max(abs(float(negs[1][1])), 1)
        print(f"\n  Dominant negative strike {float(negs[0][0]):.0f} = {float(negs[0][1]):,.0f}")
        print(f"  Ratio to 2nd most negative: {ratio:.2f}x")

print("\n" + "=" * 70)
print("PART 2: today's setup_log trades + outcomes")
print("=" * 70)
cur.execute("""
    SELECT id, ts, setup_name, direction, grade, paradigm,
           spot, target, outcome_result, outcome_pnl,
           mes_sim_outcome_pnl, v13_dd_near, real_trade_skip_reason
    FROM setup_log
    WHERE ts::date = %s
    ORDER BY ts
""", (TODAY,))
rows = cur.fetchall()
print(f"Total setup_log rows today: {len(rows)}")
print(f"\n  id   time_ET  setup            dir   grade paradigm        spot    res     pnl    mes   dd_near    skip")
for (sid, ts, setup, dir_, grade, para, spot_e, tgt, res, pnl, mpnl, ddn, skip) in rows:
    tet = to_aware(ts).astimezone(ET).strftime('%H:%M') if ts else "?"
    ddn_b = f"{float(ddn)/1e9:+.1f}B" if ddn is not None else "n/a"
    print(f"  {sid:<5}{tet:<7} {str(setup)[:15]:<15} {str(dir_):<5} {str(grade):<5} "
          f"{str(para)[:14]:<14} {str(spot_e):<7} {str(res):<6} {str(pnl):<6} {str(mpnl):<5} "
          f"{ddn_b:<8} {str(skip or '')[:18]}")

print("\n" + "=" * 70)
print("PART 3: today's real_trade_orders (TSRT)")
print("=" * 70)
cur.execute("""
    SELECT setup_log_id, state FROM real_trade_orders
    WHERE created_at::date = %s ORDER BY setup_log_id
""", (TODAY,))
rows = cur.fetchall()
print(f"Total TSRT orders today: {len(rows)}")
for sid, state in rows:
    if isinstance(state, str): state = json.loads(state)
    print(f"  lid={sid} {state.get('setup_name')} {state.get('direction')} "
          f"fill={state.get('fill_price')} close={state.get('close_fill_price')} "
          f"status={state.get('status')} reason={state.get('close_reason','')}")

cur.close(); c.close()
