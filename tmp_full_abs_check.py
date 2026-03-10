"""Check ALL ES Absorption trades — what data is available."""
import psycopg2, os, json, sys
sys.stdout.reconfigure(encoding='utf-8')

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("""
SELECT id, ts::date, direction, outcome_result, outcome_pnl,
       abs_details::text, outcome_max_profit, outcome_max_loss
FROM setup_log
WHERE setup_name = 'ES Absorption'
ORDER BY id
""")

total = 0
has_swing = 0
no_swing = 0
dates = set()
for r in cur.fetchall():
    total += 1
    abs_d = json.loads(r[5]) if r[5] else {}
    best = abs_d.get('best_swing', {})
    dates.add(str(r[1]))
    if best and best.get('ref_swing'):
        has_swing += 1
    else:
        no_swing += 1
        # Show what keys exist in abs_details for pre-rewrite
        if total <= 5 or r[0] in [229, 292]:
            print(f"  #{r[0]} {r[1]} keys={list(abs_d.keys())[:10]}")

print(f"\nTotal ES Absorption trades: {total}")
print(f"With swing pair data: {has_swing} (Feb 27+)")
print(f"Without swing pair (pre-rewrite): {no_swing}")
print(f"Dates: {sorted(dates)}")

conn.close()
