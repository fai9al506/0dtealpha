import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"])
c.autocommit = True
cur = c.cursor()

def q(sql, args=None):
    cur.execute(sql, args or ())
    return cur.fetchall()

print("=== setup_log columns of interest ===")
cols = [r[0] for r in q("""
  SELECT column_name FROM information_schema.columns
  WHERE table_name='setup_log' ORDER BY ordinal_position
""")]
interest = [x for x in cols if any(k in x for k in
  ('skip','live_pass','live_filter','basket','outcome','mes_sim','setup_name','ts','grade'))]
print(interest)

print("\n=== real_trade_skip_reason breakdown, June 2026 ===")
for r in q("""
  SELECT real_trade_skip_reason, COUNT(*)
  FROM setup_log
  WHERE ts >= '2026-06-01' AND real_trade_skip_reason IS NOT NULL
  GROUP BY 1 ORDER BY 2 DESC
"""):
    print(f"  {r[0]:<32} {r[1]}")

print("\n=== ANY basket_gate_block skips EVER (all time)? ===")
for r in q("""
  SELECT COUNT(*), MIN(ts)::date, MAX(ts)::date
  FROM setup_log WHERE real_trade_skip_reason='basket_gate_block'
"""):
    print(f"  count={r[0]}  first={r[1]}  last={r[2]}")

print("\n=== outcomes of basket_gate_block skips (did it skip winners or losers?) ===")
rows = q("""
  SELECT setup_name, ts::date,
         COALESCE(mes_sim_outcome_pnl, outcome_pnl) AS pnl
  FROM setup_log
  WHERE real_trade_skip_reason='basket_gate_block'
  ORDER BY ts
""")
if not rows:
    print("  (none logged)")
else:
    tot = sum(float(r[2]) for r in rows if r[2] is not None)
    wins = sum(1 for r in rows if r[2] is not None and float(r[2])>0)
    print(f"  n={len(rows)}  net_pts_skipped={tot:+.1f}  winners_skipped={wins}")
    for r in rows:
        print(f"    {r[1]}  {r[0]:<14} {('%+.1f'%float(r[2])) if r[2] is not None else 'n/a'}")

print("\n=== semi_basket capture: how many days of real data? ===")
for r in q("""
  SELECT MIN(et)::date, MAX(et)::date, COUNT(DISTINCT et::date), COUNT(*)
  FROM semi_basket
"""):
    print(f"  first={r[0]}  last={r[1]}  distinct_days={r[2]}  rows={r[3]}")
c.close()
