"""Per-direction P&L on each macro-news real-traded day (tests longs-only block)."""
import os
from collections import defaultdict
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

MACRO_DAYS = {  # from economic_events: true NFP, ADP, CPI (8:15/8:30 releases)
    "2026-04-01": "ADP", "2026-04-10": "CPI", "2026-05-06": "ADP",
    "2026-05-08": "NFP", "2026-05-12": "CPI", "2026-06-03": "ADP",
    "2026-06-05": "NFP",
}

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d,
               sl.setup_name, sl.direction, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = ANY(:days)
        ORDER BY sl.ts
    """), {"days": list(MACRO_DAYS.keys())}).fetchall()

agg = defaultdict(lambda: defaultdict(float))
cnt = defaultdict(lambda: defaultdict(int))
for d, name, direction, st in rows:
    st = st or {}
    fill, exit_p = st.get("fill_price"), (st.get("stop_fill_price") or st.get("close_fill_price"))
    if fill is None or exit_p is None:
        continue
    is_long = (direction or "").lower() in ("long", "bullish")
    usd = ((float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p))) * 5.0 * int(st.get("quantity") or 1)
    k = "LONG" if is_long else "SHORT"
    agg[str(d)][k] += usd
    cnt[str(d)][k] += 1

tot = defaultdict(float)
print(f"{'day':<12} {'tag':<4} {'LONG':>14} {'SHORT':>14}")
for d in sorted(MACRO_DAYS):
    tag = MACRO_DAYS[d]
    L, S = agg[d].get("LONG", 0.0), agg[d].get("SHORT", 0.0)
    nl, ns = cnt[d].get("LONG", 0), cnt[d].get("SHORT", 0)
    print(f"{d:<12} {tag:<4} {f'${L:+.0f} ({nl}t)':>14} {f'${S:+.0f} ({ns}t)':>14}")
    tot["LONG"] += L
    tot["SHORT"] += S
print(f"\nTOTAL macro days: LONG ${tot['LONG']:+.0f}  SHORT ${tot['SHORT']:+.0f}")
