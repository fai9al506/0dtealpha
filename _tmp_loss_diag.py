"""Loss diagnosis Jun 2026 — why -$1300 over last 6 trading days.

Source of truth: tsrt_daily_stmt (broker FIFO) for day-$, plus per-trade
detail from its `trades` JSONB to break down by setup/direction/WR.
Compares winning era (May 19 - Jun 04) vs loss window (Jun 05 - Jun 12).
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# 1) What columns does tsrt_daily_stmt have?
cur.execute("""SELECT column_name FROM information_schema.columns
               WHERE table_name='tsrt_daily_stmt' ORDER BY ordinal_position""")
cols = [r[0] for r in cur.fetchall()]
print("=== tsrt_daily_stmt columns ===")
print(cols)

cur.execute("""SELECT day, gross, comm, net, n_trades, n_wins
               FROM tsrt_daily_stmt
               WHERE day >= '2026-05-19' ORDER BY day""")
print("\n=== DAILY (tsrt_daily_stmt) ===")
print(f"{'day':12} {'net':>9} {'gross':>9} {'n':>4} {'W':>4} {'WR':>5}")
era_win, era_loss = [], []
for day, gross, comm, net, n, w in cur.fetchall():
    wr = (w/n*100) if n else 0
    ds = str(day)
    tag = ''
    if ds <= '2026-06-04': era_win.append((ds, float(net or 0)))
    else: era_loss.append((ds, float(net or 0)))
    print(f"{ds:12} {float(net or 0):>9.2f} {float(gross or 0):>9.2f} {n:>4} {w:>4} {wr:>4.0f}%")

print(f"\nWinning era (May19-Jun04): {sum(x[1] for x in era_win):+.2f} over {len(era_win)} days")
print(f"Loss window  (Jun05-Jun12): {sum(x[1] for x in era_loss):+.2f} over {len(era_loss)} days")

# 2) Per-trade detail from trades JSONB. Inspect shape first.
cur.execute("""SELECT day, trades FROM tsrt_daily_stmt
               WHERE day='2026-06-09' LIMIT 1""")
r = cur.fetchone()
print("\n=== sample trades JSONB (Jun 09) ===")
if r and r[1]:
    t = r[1] if isinstance(r[1], list) else json.loads(r[1])
    print(f"n={len(t)}; first item keys:", list(t[0].keys()) if t else None)
    print(json.dumps(t[0], indent=1, default=str) if t else None)

conn.close()
