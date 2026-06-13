"""Retroactively apply S191 15-min cooldown to historical GEX Long rows.

Greedy walk (matches should_notify in setup_detector.py): keep first fire,
suppress any fire < 15 min after the last KEPT fire, keep next fire past 15 min.
Suppressed rows under S191 would never have been written → delete them.

SAFETY: never delete a row referenced by auto_trade_orders / options_trade_orders
(SIM trade records). Those are PROTECTED and kept even inside a cooldown window.
real_trade_orders has 0 GEX Long refs (portal-only setup).

DRY RUN by default. Pass --apply to actually DELETE (backs up first regardless).
"""
import os, sys, json, psycopg2
from collections import defaultdict
from datetime import timezone
sys.stdout.reconfigure(encoding='utf-8')

COOLDOWN_MIN = 15
APPLY = "--apply" in sys.argv
BACKUP = "_tmp_gex_long_deleted_backup.json"

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Protected ids (have SIM child records)
cur.execute("""
    SELECT DISTINCT s.id FROM setup_log s
    LEFT JOIN auto_trade_orders a ON a.setup_log_id = s.id
    LEFT JOIN options_trade_orders o ON o.setup_log_id = s.id
    WHERE s.setup_name='GEX Long' AND (a.setup_log_id IS NOT NULL OR o.setup_log_id IS NOT NULL)
""")
protected = {r[0] for r in cur.fetchall()}

# All GEX Long rows ordered by time
cur.execute("""
    SELECT id, ts, (ts AT TIME ZONE 'America/New_York')::date AS et_date,
           grade, paradigm, direction, outcome_result, outcome_pnl
    FROM setup_log WHERE setup_name='GEX Long' ORDER BY ts
""")
rows = cur.fetchall()

anchor = None
keep_ids, delete_ids, protected_kept = [], [], []
per_day = defaultdict(lambda: {"total": 0, "keep": 0, "delete": 0})

for (rid, ts, et_date, grade, para, dir_, ocres, ocpnl) in rows:
    per_day[et_date]["total"] += 1
    within = anchor is not None and (ts - anchor).total_seconds() / 60 < COOLDOWN_MIN
    if not within:
        # this fire is KEPT and becomes the new anchor
        anchor = ts
        keep_ids.append(rid)
        per_day[et_date]["keep"] += 1
    else:
        # would be suppressed by the 15-min floor
        if rid in protected:
            protected_kept.append(rid)
            per_day[et_date]["keep"] += 1
            # NOTE: protected row does NOT reset anchor (it wouldn't exist under S191),
            # matching what the live cooldown would have done.
        else:
            delete_ids.append(rid)
            per_day[et_date]["delete"] += 1

print(f"GEX Long total rows:        {len(rows)}")
print(f"  KEEP (past cooldown):     {len(keep_ids)}")
print(f"  PROTECTED (SIM child→kept):{len(protected_kept)}  ids={sorted(protected_kept)}")
print(f"  DELETE (cooldown noise):  {len(delete_ids)}")
print(f"  Protected ids total:      {sorted(protected)}")

# Show the noisy days (where deletes happen)
noisy = sorted([d for d in per_day if per_day[d]["delete"] > 0])
print(f"\nDays with deletions ({len(noisy)}):")
for d in noisy:
    p = per_day[d]
    print(f"  {d}: {p['total']:3d} → {p['keep']:2d} kept  ({p['delete']} deleted)")

# Back up the delete set (full rows) before any deletion
if delete_ids:
    cur.execute("SELECT row_to_json(s) FROM setup_log s WHERE id = ANY(%s)", (delete_ids,))
    backup = [r[0] for r in cur.fetchall()]
    with open(BACKUP, "w", encoding="utf-8") as f:
        json.dump(backup, f, indent=2, default=str)
    print(f"\nBacked up {len(backup)} rows → {BACKUP}")

if APPLY and delete_ids:
    cur.execute("DELETE FROM setup_log WHERE id = ANY(%s)", (delete_ids,))
    conn.commit()
    print(f"\n*** DELETED {len(delete_ids)} GEX Long cooldown rows. ***")
    cur.execute("SELECT COUNT(*) FROM setup_log WHERE setup_name='GEX Long'")
    print(f"GEX Long rows remaining: {cur.fetchone()[0]}")
elif not APPLY:
    print("\n[DRY RUN] No rows deleted. Re-run with --apply to execute.")

cur.close(); conn.close()
