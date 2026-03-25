"""
Skew Charm v2 Grade Recomputation & DB Update
==============================================
1. Backs up old grades to CSV
2. Recomputes v2 grades for all SC trades (trade_date < 2026-03-23)
3. Updates production DB in a single atomic transaction
4. Verifies result
"""

import os, sys, csv, subprocess, json
from datetime import datetime, time as dtime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

ET = ZoneInfo("America/New_York")

# ── Get DATABASE_URL from Railway ──
def get_db_url():
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return url
    # Try Railway CLI
    try:
        result = subprocess.run(
            ["railway", "variables", "--json"],
            capture_output=True, text=True, timeout=15,
            cwd=r"G:\My Drive\Python\MyProject\GitHub\0dtealpha"
        )
        if result.returncode == 0:
            d = json.loads(result.stdout)
            url = d.get("DATABASE_URL", "")
            if url:
                return url
    except Exception as e:
        print(f"Railway CLI failed: {e}")
    print("ERROR: DATABASE_URL not found"); sys.exit(1)

DB_URL = get_db_url()
print(f"DB: {DB_URL[:40]}...")

CSV_PATH = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\sc_grade_backup_pre_v2.csv"

# ══════════════════════════════════════════════════════════════════════
# V2 scoring constants — EXACT copy from setup_detector.py lines 2393-2468
# ══════════════════════════════════════════════════════════════════════

_GOOD_PARADIGMS = {"GEX-PURE", "SIDIAL-EXTREME", "SIDIAL-MESSY", "AG-TARGET",
                   "BOFA-LIS", "BofA-LIS", "AG-PURE", "GEX-MESSY"}
_BAD_PARADIGMS = {"GEX-LIS", "AG-LIS"}


def compute_v2_score(paradigm, fired_time, vix, abs_charm, skew_change_pct):
    """
    Compute v2 score and grade.
    Returns (total_score, grade, components_dict).
    """
    # 1. Paradigm subtype (0-30) — strongest predictor
    p_val = str(paradigm) if paradigm else ""
    if p_val in _GOOD_PARADIGMS:
        para_score = 30
    elif p_val in _BAD_PARADIGMS:
        para_score = 0
    else:
        para_score = 15

    # 2. Time of day INVERTED (0-25) — morning = best
    t = fired_time
    if t < dtime(12, 0):
        time_score = 25
    elif t < dtime(14, 0):
        time_score = 15
    elif t < dtime(14, 30):
        time_score = 10
    elif t < dtime(15, 0):
        time_score = 3   # 14:30 dead zone
    elif t < dtime(15, 30):
        time_score = 8
    else:
        time_score = 0   # 15:30+ death zone

    # 3. VIX regime (0-20)
    _vix = float(vix) if vix is not None else 22.0
    if _vix < 22:
        vix_score = 20
    elif _vix < 25:
        vix_score = 12
    else:
        vix_score = 5

    # 4. Charm INVERTED (0-15) — low abs(charm) = best
    ac = abs(abs_charm) if abs_charm is not None else 50_000_000  # default to mid
    if ac < 50_000_000:
        charm_score = 15
    elif ac < 100_000_000:
        charm_score = 10
    elif ac < 250_000_000:
        charm_score = 5
    else:
        charm_score = 0

    # 5. Skew magnitude (0-10) — use 3 as conservative floor (r=-0.03)
    abs_change = abs(skew_change_pct) if skew_change_pct is not None else 3.5
    if abs_change >= 7:
        skew_score = 10
    elif abs_change >= 5:
        skew_score = 7
    else:
        skew_score = 3

    total = para_score + time_score + vix_score + charm_score + skew_score

    # Grade thresholds
    if total >= 80:
        grade = "A+"
    elif total >= 65:
        grade = "A"
    elif total >= 50:
        grade = "B"
    elif total >= 35:
        grade = "C"
    else:
        grade = "LOG"

    return total, grade, {
        "para": para_score, "time": time_score, "vix": vix_score,
        "charm": charm_score, "skew": skew_score
    }


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ══════════════════════════════════════════════════════════════════
    # STEP 1: Backup old grades to CSV
    # ══════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("STEP 1: Backing up old SC grades to CSV")
    print("=" * 70)

    cur.execute("""
        SELECT id, ts::date as trade_date, grade, score
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
        ORDER BY ts;
    """)
    all_sc = cur.fetchall()
    print(f"  Total SC trades in DB: {len(all_sc)}")

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "trade_date", "old_grade", "old_score"])
        for row in all_sc:
            writer.writerow([row["id"], row["trade_date"], row["grade"], row["score"]])

    print(f"  Saved to: {CSV_PATH}")
    print(f"  Rows backed up: {len(all_sc)}")

    # Old grade distribution
    old_dist = {}
    for row in all_sc:
        g = row["grade"]
        old_dist[g] = old_dist.get(g, 0) + 1
    print(f"  Old grade distribution: {dict(sorted(old_dist.items()))}")

    # ══════════════════════════════════════════════════════════════════
    # STEP 2: Recompute v2 grades for pre-v2 trades (ts < 2026-03-23)
    # ══════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("STEP 2: Recomputing v2 grades")
    print("=" * 70)

    # Get all SC trades that need recomputation (before v2 was deployed)
    cur.execute("""
        SELECT id, ts, grade, score, paradigm, vix
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
        AND ts < '2026-03-23'::date
        ORDER BY ts;
    """)
    pre_v2_trades = cur.fetchall()
    print(f"  Pre-v2 SC trades (before 2026-03-23): {len(pre_v2_trades)}")

    # Also get trades on/after 2026-03-23 (already v2, skip update)
    cur.execute("""
        SELECT id, ts, grade, score
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
        AND ts >= '2026-03-23'::date
        ORDER BY ts;
    """)
    post_v2_trades = cur.fetchall()
    print(f"  Post-v2 SC trades (2026-03-23+): {len(post_v2_trades)} (will NOT be updated)")

    # For each pre-v2 trade, get charm from volland_snapshots
    updates = []
    charm_found = 0
    charm_missing = 0

    for t in pre_v2_trades:
        trade_ts = t["ts"]

        # Get charm from volland snapshot closest to trade time (within 5 min before)
        cur.execute("""
            SELECT payload->'statistics'->>'aggregatedCharm' as agg_charm, ts
            FROM volland_snapshots
            WHERE ts <= %s
            AND ts >= %s - interval '5 minutes'
            AND payload->'statistics' IS NOT NULL
            ORDER BY ts DESC LIMIT 1;
        """, (trade_ts, trade_ts))
        vol_row = cur.fetchone()

        charm_value = None
        if vol_row and vol_row["agg_charm"]:
            charm_str = vol_row["agg_charm"]
            try:
                cleaned = charm_str.replace("$", "").replace(",", "").strip()
                charm_value = float(cleaned)
                charm_found += 1
            except (ValueError, TypeError):
                charm_missing += 1
        else:
            charm_missing += 1

        # Convert ts to ET time-of-day
        if hasattr(trade_ts, 'astimezone'):
            ts_et = trade_ts.astimezone(ET)
            fired_time = ts_et.time()
        elif hasattr(trade_ts, 'replace'):
            ts_et = trade_ts.replace(tzinfo=timezone.utc).astimezone(ET)
            fired_time = ts_et.time()
        else:
            fired_time = dtime(12, 0)

        # Compute v2 score (skew_change_pct not in DB — use default 3)
        new_score, new_grade, components = compute_v2_score(
            paradigm=t["paradigm"],
            fired_time=fired_time,
            vix=t["vix"],
            abs_charm=charm_value,
            skew_change_pct=None  # conservative floor
        )

        updates.append({
            "id": t["id"],
            "old_grade": t["grade"],
            "old_score": t["score"],
            "new_grade": new_grade,
            "new_score": new_score,
            "components": components,
            "charm_value": charm_value,
        })

    print(f"  Charm data found: {charm_found}, missing: {charm_missing}")
    print(f"  Recomputed: {len(updates)} trades")

    # Show new grade distribution for recomputed trades
    new_dist = {}
    for u in updates:
        g = u["new_grade"]
        new_dist[g] = new_dist.get(g, 0) + 1
    print(f"  New v2 grade distribution (pre-v2 trades): {dict(sorted(new_dist.items()))}")

    # Show changes
    changed = [u for u in updates if u["old_grade"] != u["new_grade"] or u["old_score"] != u["new_score"]]
    unchanged = [u for u in updates if u["old_grade"] == u["new_grade"] and u["old_score"] == u["new_score"]]
    print(f"  Changed: {len(changed)}, Unchanged: {len(unchanged)}")

    # ══════════════════════════════════════════════════════════════════
    # STEP 3: UPDATE the database (atomic transaction)
    # ══════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("STEP 3: Updating production database")
    print("=" * 70)

    if len(updates) == 0:
        print("  No rows to update!")
    else:
        # Use a fresh connection for the atomic update transaction
        update_conn = psycopg2.connect(DB_URL)
        try:
            update_cur = update_conn.cursor()
            rows_updated = 0
            for u in updates:
                update_cur.execute("""
                    UPDATE setup_log
                    SET grade = %s, score = %s
                    WHERE id = %s
                """, (u["new_grade"], u["new_score"], u["id"]))
                rows_updated += update_cur.rowcount

            update_conn.commit()
            print(f"  Transaction committed. Rows updated: {rows_updated}")
        except Exception as e:
            update_conn.rollback()
            print(f"  ERROR: Transaction rolled back! {e}")
            update_conn.close()
            cur.close()
            conn.close()
            sys.exit(1)
        finally:
            update_conn.close()

    # ══════════════════════════════════════════════════════════════════
    # STEP 4: Verify
    # ══════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("STEP 4: Verification")
    print("=" * 70)

    # Fresh connection for verification
    conn.close()
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Grade distribution with WR
    cur.execute("""
        SELECT grade, COUNT(*) as cnt,
               ROUND(AVG(CASE WHEN outcome_result = 'WIN' THEN 1.0 ELSE 0.0 END) * 100, 1) as wr
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
        GROUP BY grade ORDER BY grade;
    """)
    verify_rows = cur.fetchall()
    print("\n  Current DB grade distribution (all SC trades):")
    print(f"  {'Grade':<8} {'Count':>6} {'WR':>8}")
    print(f"  {'-'*8} {'-'*6} {'-'*8}")
    for r in verify_rows:
        wr_str = f"{r['wr']:.1f}%" if r['wr'] is not None else "n/a"
        print(f"  {r['grade']:<8} {r['cnt']:>6} {wr_str:>8}")

    # Check for v1-only grades that should no longer exist
    cur.execute("""
        SELECT grade, COUNT(*) as cnt
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
        AND grade = 'A-Entry'
        GROUP BY grade;
    """)
    v1_leftovers = cur.fetchall()
    if v1_leftovers:
        print(f"\n  WARNING: 'A-Entry' grade still exists! {v1_leftovers}")
    else:
        print(f"\n  CONFIRMED: No 'A-Entry' grades remain for SC trades.")

    # Check all grades are valid v2 grades
    cur.execute("""
        SELECT grade, COUNT(*) as cnt
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
        AND grade NOT IN ('A+', 'A', 'B', 'C', 'LOG')
        GROUP BY grade;
    """)
    invalid = cur.fetchall()
    if invalid:
        print(f"  WARNING: Non-v2 grades found: {invalid}")
    else:
        print(f"  CONFIRMED: All SC grades are valid v2 grades (A+, A, B, C, LOG).")

    # ══════════════════════════════════════════════════════════════════
    # STEP 5: Summary
    # ══════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("STEP 5: Summary")
    print("=" * 70)
    print(f"  Rows backed up to CSV:  {len(all_sc)}")
    print(f"  Rows recomputed:        {len(updates)}")
    print(f"  Rows actually changed:  {len(changed)}")
    print(f"  Post-v2 (untouched):    {len(post_v2_trades)}")
    print()
    print(f"  Old grade distribution: {dict(sorted(old_dist.items()))}")
    print(f"  New v2 grades (pre-v2): {dict(sorted(new_dist.items()))}")
    print()

    # Post-v2 trades grade dist (should already be v2)
    post_dist = {}
    for r in post_v2_trades:
        g = r["grade"]
        post_dist[g] = post_dist.get(g, 0) + 1
    if post_dist:
        print(f"  Post-v2 grades (already v2): {dict(sorted(post_dist.items()))}")
    print()
    print(f"  'A-Entry' remaining: {'YES (problem!)' if v1_leftovers else 'NONE (clean)'}")
    print(f"  CSV backup: {CSV_PATH}")
    print("\nDone.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
