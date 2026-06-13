import os, psycopg
from datetime import datetime
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
today = datetime.now(ET).date()
print("Trade date (ET):", today, "| now:", datetime.now(ET).strftime("%H:%M %A"))
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()

# tsrt_daily_stmt recent (broker-truth day $)
print("\n=== tsrt_daily_stmt (broker-truth day $, last 15 rows) ===")
try:
    cur.execute("SELECT day, gross, comm, net, n_trades, n_wins FROM tsrt_daily_stmt ORDER BY day DESC LIMIT 15")
    for r in cur.fetchall():
        print(f"  {r[0]}  gross={r[1]}  comm={r[2]}  net={r[3]}  n={r[4]} wins={r[5]}")
except Exception as e:
    print("  ERR", e)

# today's real trades from setup_log
print("\n=== setup_log TODAY (real-traded only: real_trade_skip_reason IS NULL) ===")
cur.execute("""
  SELECT id, ts, setup_name, direction, grade, paradigm, outcome_result, outcome_pnl, real_trade_skip_reason
  FROM setup_log WHERE ts::date = %s ORDER BY ts
""", (today,))
rows = cur.fetchall()
placed = [r for r in rows if r[8] is None]
blocked = [r for r in rows if r[8] is not None]
print(f"  total signals today: {len(rows)} | placed: {len(placed)} | blocked: {len(blocked)}")
for r in placed:
    t = r[1].astimezone(ET).strftime("%H:%M")
    print(f"  PLACED lid {r[0]} {t} {r[2]:<14} {str(r[3]):<6} {str(r[4]):<3} {str(r[5]):<14} -> {r[6]} pnl={r[7]}")
print("  --- blocked reasons (count) ---")
from collections import Counter
c = Counter(r[8] for r in blocked)
for k,v in c.most_common():
    print(f"    {k}: {v}")
