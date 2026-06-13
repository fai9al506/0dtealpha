import os, psycopg, json
from datetime import datetime
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
today = datetime.now(ET).date()
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()
cur.execute("""
  SELECT s.id, s.ts, s.setup_name, s.direction, s.grade, s.paradigm, r.state
  FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id = s.id
  WHERE s.ts::date = %s ORDER BY s.ts
""", (today,))
rows = cur.fetchall()
print(f"=== REAL TSRT trades today (JOIN real_trade_orders): {len(rows)} ===")
# show available keys once
if rows:
    st0 = rows[0][6] if isinstance(rows[0][6], dict) else json.loads(rows[0][6])
    print("state keys:", sorted(st0.keys()))
print()
for r in rows:
    lid, ts, name, dirn, grade, para, state = r
    t = ts.astimezone(ET).strftime("%H:%M")
    st = state if isinstance(state, dict) else json.loads(state)
    acct = st.get("account")
    entry = st.get("entry_fill_price") or st.get("entry_price")
    close = st.get("close_fill_price") or st.get("stop_fill_price")
    pnl_pts = st.get("realized_pnl") or st.get("pnl_pts")
    pnl_usd = st.get("realized_pnl_dollars") or st.get("pnl_dollars") or st.get("broker_pnl")
    cr = st.get("close_reason")
    qty = st.get("qty") or st.get("quantity")
    print(f"  lid {lid} {t} {name:<16} {str(dirn):<6} {str(grade):<3} acct={acct} q={qty} entry={entry} close={close} ptsPnL={pnl_pts} $={pnl_usd} reason={cr}")
