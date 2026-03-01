import psycopg2, os, json, sys

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get today's GEX Long trades
cur.execute("""
SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
       setup_name, direction, grade, score,
       spot, target, outcome_target_level, outcome_stop_level,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       outcome_first_event, outcome_elapsed_min, lis, paradigm
FROM setup_log
WHERE ts >= '2026-02-26 14:00:00 UTC'
  AND setup_name = 'GEX Long'
ORDER BY ts
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
sys.stdout.write(f'GEX Long trades today: {len(rows)}\n')
for r in rows:
    d = dict(zip(cols, r))
    sys.stdout.write(json.dumps({k: str(v) for k,v in d.items()}, indent=2) + '\n---\n')

# ALL trades today for context
cur.execute("""
SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
       setup_name, direction, grade, outcome_result, outcome_pnl,
       outcome_max_profit, outcome_max_loss, spot, target,
       outcome_target_level, outcome_stop_level
FROM setup_log
WHERE ts >= '2026-02-26 14:00:00 UTC'
ORDER BY ts
""")
rows2 = cur.fetchall()
cols2 = [d[0] for d in cur.description]
sys.stdout.write(f'\n=== ALL trades today: {len(rows2)} ===\n')
for r in rows2:
    d = dict(zip(cols2, r))
    sys.stdout.write(f'{d["id"]:>4} {str(d["ts_et"]):>20} {d["setup_name"]:>20} {d["direction"]:>6} {str(d["grade"]):>4} | {str(d["outcome_result"]):>10} PnL={str(d["outcome_pnl"]):>8} maxP={str(d["outcome_max_profit"]):>8} maxL={str(d["outcome_max_loss"]):>8} | spot={d["spot"]} tgt={d["target"]} o_tgt={d["outcome_target_level"]} o_stop={d["outcome_stop_level"]}\n')

# GEX Long historical stats
cur.execute("""
SELECT outcome_result, count(*), round(avg(outcome_pnl)::numeric, 1),
       round(sum(outcome_pnl)::numeric, 1),
       round(avg(outcome_max_profit)::numeric, 1)
FROM setup_log
WHERE setup_name = 'GEX Long' AND outcome_result IS NOT NULL
GROUP BY outcome_result
""")
sys.stdout.write(f'\n=== GEX Long historical breakdown ===\n')
for r in cur.fetchall():
    sys.stdout.write(f'  {r[0]:>10}: {r[1]} trades, avg_pnl={r[2]}, sum={r[3]}, avg_maxP={r[4]}\n')

# GEX Long recent 20 with target/stop distances
cur.execute("""
SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
       spot, target,
       outcome_target_level, outcome_stop_level,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       paradigm, grade
FROM setup_log
WHERE setup_name = 'GEX Long' AND outcome_result IS NOT NULL
ORDER BY ts DESC LIMIT 25
""")
cols3 = [d[0] for d in cur.description]
sys.stdout.write(f'\n=== GEX Long recent 25 trades ===\n')
for r in cur.fetchall():
    d = dict(zip(cols3, r))
    tgt_dist = round(float(d["outcome_target_level"]) - float(d["spot"]), 1) if d["outcome_target_level"] else None
    stop_dist = round(float(d["spot"]) - float(d["outcome_stop_level"]), 1) if d["outcome_stop_level"] else None
    sys.stdout.write(f'{d["id"]:>4} {str(d["ts_et"]):>20} {d["paradigm"]:>12} {d["grade"]:>3} spot={d["spot"]:>8} tgt={d["outcome_target_level"]}(+{tgt_dist}) stop={d["outcome_stop_level"]}(-{stop_dist}) | {d["outcome_result"]:>10} pnl={str(d["outcome_pnl"]):>6} maxP={str(d["outcome_max_profit"]):>6} maxL={str(d["outcome_max_loss"]):>6}\n')

# GEX Long: what's the current trail logic?
cur.execute("""
SELECT id, outcome_result, outcome_pnl, outcome_max_profit, outcome_first_event
FROM setup_log
WHERE setup_name = 'GEX Long' AND outcome_result IS NOT NULL
  AND outcome_max_profit > 12
ORDER BY ts DESC LIMIT 15
""")
sys.stdout.write(f'\n=== GEX Long trades with maxP > 12 (trail should have kicked in) ===\n')
for r in cur.fetchall():
    sys.stdout.write(f'  id={r[0]} result={r[1]} pnl={r[2]} maxP={r[3]} first_event={r[4]}\n')

sys.stdout.flush()
conn.close()
