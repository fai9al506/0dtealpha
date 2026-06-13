"""Option 2 backtest: block Skew Charm + DD Exhaustion LONGS on macro days."""
import os
from collections import defaultdict
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

MACRO_DAYS = ["2026-04-01", "2026-04-10", "2026-05-06", "2026-05-08",
              "2026-05-12", "2026-06-03", "2026-06-05"]
TAG = {"2026-04-01": "ADP", "2026-04-10": "CPI", "2026-05-06": "ADP",
       "2026-05-08": "NFP", "2026-05-12": "CPI", "2026-06-03": "ADP",
       "2026-06-05": "NFP"}
BLOCK = {"Skew Charm", "DD Exhaustion"}

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d,
               sl.setup_name, sl.direction, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = ANY(:days)
        ORDER BY sl.ts
    """), {"days": MACRO_DAYS}).fetchall()

day_actual = defaultdict(float)
day_blockrule = defaultdict(float)
day_removed = defaultdict(lambda: [0.0, 0])
for d, name, direction, st in rows:
    st = st or {}
    fill = st.get("fill_price")
    exit_p = st.get("stop_fill_price") or st.get("close_fill_price")
    if fill is None or exit_p is None:
        continue
    is_long = (direction or "").lower() in ("long", "bullish")
    usd = ((float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p))) \
          * 5.0 * int(st.get("quantity") or 1)
    d = str(d)
    day_actual[d] += usd
    if name in BLOCK and is_long:
        day_removed[d][0] += usd
        day_removed[d][1] += 1
    else:
        day_blockrule[d] += usd

print(f"{'day':<12}{'tag':<5}{'actual':>9}{'SC/DD-L blocked':>17}{'saved':>8}{'removed':>12}")
tot_a = tot_b = 0.0
for d in MACRO_DAYS:
    a, b = day_actual[d], day_blockrule[d]
    rm_usd, rm_n = day_removed[d]
    print(f"{d:<12}{TAG[d]:<5}{a:>+9.0f}{b:>+17.0f}{b-a:>+8.0f}{f'{rm_n}t ({rm_usd:+.0f})':>12}")
    tot_a += a
    tot_b += b
print(f"\nTOTAL: actual ${tot_a:+.0f}  ->  block rule ${tot_b:+.0f}  (saved ${tot_b-tot_a:+.0f})")
print("NOTE: static removal — ignores 2nd-order effects (daily-loss breaker would trip"
      " later/never without these losses; underwater-stack guard interactions).")
