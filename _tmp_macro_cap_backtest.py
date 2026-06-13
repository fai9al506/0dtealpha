"""Backtest: macro-day reduced daily loss cap ($150 vs actual $300).

Replay each macro day's real TSRT trades chronologically. The breaker blocks
NEW entries once REALIZED P&L <= -cap (already-open trades run to their real
exit, same as the live breaker). Exit time = entry + outcome_elapsed_min.
Cross-check: cap=300 replay should reproduce the actual day (Jun 5 sanity).
"""
import os
from datetime import timedelta
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

MACRO_DAYS = ["2026-04-01", "2026-04-10", "2026-05-06", "2026-05-08",
              "2026-05-12", "2026-06-03", "2026-06-05"]
TAG = {"2026-04-01": "ADP", "2026-04-10": "CPI", "2026-05-06": "ADP",
       "2026-05-08": "NFP", "2026-05-12": "CPI", "2026-06-03": "ADP",
       "2026-06-05": "NFP"}

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d,
               (sl.ts AT TIME ZONE 'America/New_York') AS et,
               sl.outcome_elapsed_min, sl.direction, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = ANY(:days)
        ORDER BY sl.ts
    """), {"days": MACRO_DAYS}).fetchall()

days = {}
for d, et, elapsed, direction, st in rows:
    st = st or {}
    fill = st.get("fill_price")
    exit_p = st.get("stop_fill_price") or st.get("close_fill_price")
    if fill is None or exit_p is None:
        continue
    is_long = (direction or "").lower() in ("long", "bullish")
    usd = ((float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p))) \
          * 5.0 * int(st.get("quantity") or 1)
    close_et = et + timedelta(minutes=float(elapsed)) if elapsed is not None else et
    days.setdefault(str(d), []).append({"entry": et, "close": close_et, "usd": usd})


def replay(trades, cap):
    """Block new entries once realized P&L <= -cap. Return (pnl, n_taken, n_blocked, trip_time)."""
    taken, blocked, trip = [], 0, None
    for t in sorted(trades, key=lambda x: x["entry"]):
        realized = sum(x["usd"] for x in taken if x["close"] <= t["entry"])
        if realized <= -cap:
            blocked += 1
            if trip is None:
                trip = t["entry"]
            continue
        taken.append(t)
    return sum(x["usd"] for x in taken), len(taken), blocked, trip


print(f"{'day':<12}{'tag':<5}{'actual':>9}{'cap150':>9}{'saved':>8}{'taken/blocked':>15}  trip-time")
tot_a, tot_c = 0.0, 0.0
for d in MACRO_DAYS:
    trades = days.get(d, [])
    if not trades:
        print(f"{d:<12}{TAG[d]:<5}  (no trades)")
        continue
    actual = sum(t["usd"] for t in trades)
    # sanity: replay with cap=300 should ~match actual (it was the live cap)
    chk, _, chk_blocked, _ = replay(trades, 300.0)
    pnl150, n_taken, n_blocked, trip = replay(trades, 150.0)
    note = "" if abs(chk - actual) < 1 else f"  [cap300 replay={chk:+.0f}, blocked {chk_blocked} — differs from actual!]"
    trip_s = trip.strftime("%H:%M") if trip else "-"
    print(f"{d:<12}{TAG[d]:<5}{actual:>+9.0f}{pnl150:>+9.0f}{pnl150-actual:>+8.0f}"
          f"{f'{n_taken}/{n_blocked}':>15}  {trip_s}{note}")
    tot_a += actual
    tot_c += pnl150
print(f"\nTOTAL: actual ${tot_a:+.0f}  ->  with $150 macro cap ${tot_c:+.0f}  (saved ${tot_c - tot_a:+.0f})")
