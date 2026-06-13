"""Retroactively apply the 15-min cooldown to historical AG Short rows.

AG Short's live cooldown deployed 2026-03-25 evening (commit a73994c) and has
0 flicker pairs from Mar 26 on. This removes only the PRE-DEPLOY flicker noise
(within-15-min dupes), matching what the DB would hold if the cooldown had
always been live. Greedy walk identical to should_notify_ag().

SAFETY (AG Short TRADES REAL MONEY): protect any row referenced by
real_trade_orders / auto_trade_orders / options_trade_orders. Delete only
unreferenced dupes. Full backup before delete.

DRY RUN default. Pass --apply to execute.
"""
import os, sys, json, psycopg2
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')

COOLDOWN_MIN = 15
APPLY = "--apply" in sys.argv
BACKUP = "_tmp_ag_short_deleted_backup.json"

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("""
    SELECT DISTINCT s.id FROM setup_log s
    LEFT JOIN real_trade_orders r ON r.setup_log_id = s.id
    LEFT JOIN auto_trade_orders a ON a.setup_log_id = s.id
    LEFT JOIN options_trade_orders o ON o.setup_log_id = s.id
    WHERE s.setup_name='AG Short'
      AND (r.setup_log_id IS NOT NULL OR a.setup_log_id IS NOT NULL OR o.setup_log_id IS NOT NULL)
""")
protected = {r[0] for r in cur.fetchall()}

cur.execute("""
    SELECT id, ts, (ts AT TIME ZONE 'America/New_York')::date AS et_date
    FROM setup_log WHERE setup_name='AG Short' ORDER BY ts
""")
rows = cur.fetchall()

anchor = None
keep_ids, delete_ids, protected_kept = [], [], []
per_day = defaultdict(lambda: {"total": 0, "keep": 0, "delete": 0})
from datetime import date
DEPLOY = date(2026, 3, 26)

for (rid, ts, et_date) in rows:
    per_day[et_date]["total"] += 1
    within = anchor is not None and (ts - anchor).total_seconds() / 60 < COOLDOWN_MIN
    if not within:
        anchor = ts
        keep_ids.append(rid); per_day[et_date]["keep"] += 1
    else:
        if rid in protected:
            protected_kept.append(rid); per_day[et_date]["keep"] += 1
        else:
            delete_ids.append(rid); per_day[et_date]["delete"] += 1

# Verify the claim: every deletable row is before Mar 26
cur.execute("SELECT id, (ts AT TIME ZONE 'America/New_York')::date FROM setup_log WHERE id = ANY(%s)",
            (delete_ids,)) if delete_ids else None
del_dates = cur.fetchall() if delete_ids else []
post_deploy_deletes = [(i, d) for i, d in del_dates if d >= DEPLOY]

print(f"AG Short total rows:         {len(rows)}")
print(f"  KEEP (past cooldown):      {len(keep_ids)}")
print(f"  PROTECTED (has trade→kept):{len(protected_kept)}  ids={sorted(protected_kept)}")
print(f"  DELETE (pre-deploy dupes): {len(delete_ids)}")
print(f"  All protected ids:         {sorted(protected)}")
print(f"  Deletes on/after Mar 26:   {len(post_deploy_deletes)}  (MUST be 0) {post_deploy_deletes}")

noisy = sorted([d for d in per_day if per_day[d]["delete"] > 0])
print(f"\nDays with deletions ({len(noisy)}):")
for d in noisy:
    p = per_day[d]
    print(f"  {d}: {p['total']:3d} → {p['keep']:2d} kept  ({p['delete']} deleted)")

if delete_ids:
    cur.execute("SELECT row_to_json(s) FROM setup_log s WHERE id = ANY(%s)", (delete_ids,))
    backup = [r[0] for r in cur.fetchall()]
    with open(BACKUP, "w", encoding="utf-8") as f:
        json.dump(backup, f, indent=2, default=str)
    print(f"\nBacked up {len(backup)} rows → {BACKUP}")

if APPLY and delete_ids and not post_deploy_deletes:
    cur.execute("DELETE FROM setup_log WHERE id = ANY(%s)", (delete_ids,))
    conn.commit()
    print(f"\n*** DELETED {len(delete_ids)} AG Short pre-deploy dupe rows. ***")
    cur.execute("SELECT COUNT(*) FROM setup_log WHERE setup_name='AG Short'")
    print(f"AG Short rows remaining: {cur.fetchone()[0]}")
elif APPLY and post_deploy_deletes:
    print("\n!!! ABORTED: found post-deploy deletes — investigate before applying.")
elif not APPLY:
    print("\n[DRY RUN] No rows deleted. Re-run with --apply to execute.")

cur.close(); conn.close()
