import os, psycopg2, json
c = psycopg2.connect(os.environ['DATABASE_URL'])
r = c.cursor()
r.execute("SELECT id, ts, setup_name, direction, spot, abs_es_price, grade, score, notified, outcome_result, outcome_pnl, outcome_target_level, outcome_stop_level FROM setup_log WHERE setup_name='ES Absorption' AND ts::date='2026-02-26' ORDER BY id")
for row in r.fetchall():
    print(f"ID={row[0]} {row[1]} {row[3]} ES={row[5]} grade={row[6]} score={row[7]} notify={row[8]}")
    print(f"  {row[9]} {row[10]} tgt={row[11]} stp={row[12]}")
c.close()
