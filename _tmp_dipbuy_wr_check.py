import os, psycopg2, json

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()
cur.execute("""SELECT column_name FROM information_schema.columns
               WHERE table_name='setup_log' ORDER BY ordinal_position""")
print("columns:", [r[0] for r in cur.fetchall()])

cur.execute("""
    SELECT id, ts AT TIME ZONE 'America/New_York', direction, spot, grade,
           outcome_result, outcome_pnl, abs_details
    FROM setup_log WHERE setup_name = 'Dip-Buy' ORDER BY ts
""")
rows = cur.fetchall()
print(f"{len(rows)} Dip-Buy rows")
wins = losses = open_ = other = 0
total = 0.0
for (lid, ts, dirn, entry, grade, res, pnl, det) in rows:
    if isinstance(det, str):
        try: det = json.loads(det)
        except Exception: det = {}
    det = det or {}
    print(f"lid={lid} {ts} {dirn} entry={entry} grade={grade} result={res} pnl={pnl} "
          f"prior_close_ok={det.get('prior_close_ok')} vx_diverge_ok={det.get('vx_diverge_ok')}")
    if res == "WIN": wins += 1
    elif res == "LOSS": losses += 1
    elif res is None: open_ += 1
    else: other += 1
    if pnl is not None: total += float(pnl)
print(f"\nW={wins} L={losses} other={other} open={open_}  total_pnl={total:+.1f} pts")
