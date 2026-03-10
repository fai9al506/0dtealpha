import psycopg2, os
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("""
SELECT id, ts, setup_name, direction, grade, score,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       outcome_first_event, outcome_elapsed_min, outcome_stop_level,
       spot, target
FROM setup_log
WHERE setup_name = 'Skew Charm'
ORDER BY ts
""")
cols = [d[0] for d in cur.description]
rows = cur.fetchall()
print("Found %d Skew Charm trades" % len(rows))
for r in rows:
    d = dict(zip(cols, r))
    print("---")
    print("ID=%s ts=%s dir=%s grade=%s score=%s" % (d['id'], d['ts'], d['direction'], d['grade'], d['score']))
    print("  spot=%s target=%s" % (d['spot'], d['target']))
    print("  result=%s pnl=%s max_profit=%s max_loss=%s" % (d['outcome_result'], d['outcome_pnl'], d['outcome_max_profit'], d['outcome_max_loss']))
    print("  first_event=%s elapsed=%s stop_level=%s" % (d['outcome_first_event'], d['outcome_elapsed_min'], d['outcome_stop_level']))
conn.close()
