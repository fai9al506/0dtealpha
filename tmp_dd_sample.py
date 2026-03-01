import os, psycopg2
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute("""
    SELECT id, comments, paradigm,
           ts AT TIME ZONE 'America/New_York' as ts_et,
           direction, score, outcome_result, outcome_pnl,
           spot, support_score, upside_score, floor_cluster_score,
           target_cluster_score, rr_score, vix
    FROM setup_log
    WHERE setup_name = 'DD Exhaustion'
      AND outcome_result IS NOT NULL
    ORDER BY ts
    LIMIT 3
""")
for r in cur.fetchall():
    print(f"id={r[0]} ts={r[3]} dir={r[4]} score={r[5]} result={r[6]} pnl={r[7]}")
    print(f"  spot={r[8]} dd_shift_score={r[9]} charm_score={r[10]} time_score={r[11]} para_score={r[12]} dir_score={r[13]} vix={r[14]}")
    print(f"  paradigm={r[2]}")
    cmt = r[1] or ""
    print(f"  comments={cmt[:300]}")
    print()

# Also count total
cur.execute("SELECT COUNT(*) FROM setup_log WHERE setup_name = 'DD Exhaustion' AND outcome_result IS NOT NULL")
print(f"Total DD trades with outcome: {cur.fetchone()[0]}")

# Check what's in the comments (DD shift / charm values)
cur.execute("""
    SELECT id, comments FROM setup_log
    WHERE setup_name = 'DD Exhaustion' AND outcome_result IS NOT NULL
    ORDER BY ts LIMIT 1
""")
r = cur.fetchone()
print(f"\nFull comments of trade {r[0]}:")
print(r[1])
conn.close()
