"""Jun 3: full state JSONB dump for the None-close lids + day total."""
import os, json
import psycopg2
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""
    SELECT r.setup_log_id, r.state
    FROM real_trade_orders r JOIN setup_log l ON l.id = r.setup_log_id
    WHERE l.ts >= '2026-06-03 13:00+00' AND l.ts < '2026-06-04 01:00+00'
    ORDER BY l.ts
""")
for lid, state in cur.fetchall():
    st = state if isinstance(state, dict) else json.loads(state or "{}")
    keys = ["direction","qty","fill_price","close_fill_price","close_reason","realized_pnl",
            "close_fill_price_pre_fifo_reconcile","fifo_close_oid","status","entry_time","close_time"]
    info = {k: st.get(k) for k in keys if st.get(k) is not None}
    print(f"lid {lid}: {info}")
c.close()
