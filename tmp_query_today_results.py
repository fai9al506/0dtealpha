#!/usr/bin/env python3
"""Query today's (Feb 26 2026) trading results from Railway PostgreSQL."""
import os, sys, json
import psycopg2
import psycopg2.extras

TRADE_DATE = '2026-02-26'

DATABASE_URL = os.environ.get('DATABASE_URL')
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set", file=sys.stderr)
    sys.exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def prow(r):
    d = dict(r)
    for k, v in d.items():
        if hasattr(v, 'isoformat'):
            d[k] = v.isoformat()
        elif isinstance(v, float):
            d[k] = round(v, 2) if v else v
    print(json.dumps(d, default=str, indent=2))
    print("-" * 80)

def ptable(rows):
    if not rows:
        print("(no data)")
        return
    header = list(rows[0].keys())
    widths = []
    for h in header:
        max_w = len(str(h))
        for r in rows:
            max_w = max(max_w, len(str(r[h]) if r[h] is not None else ''))
        widths.append(min(max_w + 1, 22))
    fmt = " | ".join(f"{{:>{w}}}" for w in widths)
    print(fmt.format(*header))
    print("-" * sum(w + 3 for w in widths))
    for r in rows:
        vals = [str(r[h]) if r[h] is not None else '' for h in header]
        print(fmt.format(*vals))

TZ = 'America/New_York'
DATE_FILTER = f"(ts AT TIME ZONE '{TZ}')::date = '{TRADE_DATE}'"

# ============================================================================
print("=" * 100)
print(f"QUERY 1: All setup_log trades on {TRADE_DATE} (ET)")
print("=" * 100)
cur.execute(f"""
SELECT id, setup_name, direction, grade, score, spot,
       outcome_result, outcome_pnl, outcome_first_event, outcome_max_profit, outcome_max_loss,
       ts AT TIME ZONE '{TZ}' as time_et,
       paradigm, lis, target, comments,
       abs_vol_ratio, abs_es_price, vix,
       outcome_target_level, outcome_stop_level, outcome_elapsed_min
FROM setup_log
WHERE {DATE_FILTER}
ORDER BY ts;
""")
rows = cur.fetchall()
if not rows:
    print("(no trades)")
else:
    for r in rows:
        prow(r)
    print(f"\nTotal rows: {len(rows)}")

# ============================================================================
print("\n" + "=" * 100)
print(f"QUERY 2: Summary stats by setup ({TRADE_DATE})")
print("=" * 100)
cur.execute(f"""
SELECT setup_name,
       COUNT(*) as trades,
       SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
       SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
       SUM(CASE WHEN outcome_result = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
       SUM(CASE WHEN outcome_result IS NULL THEN 1 ELSE 0 END) as open,
       ROUND(AVG(outcome_pnl)::numeric, 1) as avg_pnl,
       ROUND(SUM(outcome_pnl)::numeric, 1) as total_pnl,
       ROUND(MAX(outcome_max_profit)::numeric, 1) as best_max_profit,
       ROUND(MIN(outcome_max_loss)::numeric, 1) as worst_max_loss
FROM setup_log
WHERE {DATE_FILTER}
GROUP BY setup_name
ORDER BY total_pnl DESC;
""")
ptable(cur.fetchall())

# ============================================================================
print("\n" + "=" * 100)
print(f"QUERY 3: Grand total ({TRADE_DATE})")
print("=" * 100)
cur.execute(f"""
SELECT COUNT(*) as total_trades,
       SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
       SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
       SUM(CASE WHEN outcome_result = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
       SUM(CASE WHEN outcome_result IS NULL THEN 1 ELSE 0 END) as open,
       ROUND(SUM(outcome_pnl)::numeric, 1) as total_pnl,
       ROUND(AVG(outcome_pnl)::numeric, 1) as avg_pnl
FROM setup_log
WHERE {DATE_FILTER};
""")
r = cur.fetchone()
for k, v in dict(r).items():
    print(f"  {k}: {v}")

# ============================================================================
print("\n" + "=" * 100)
print(f"QUERY 4: DD Exhaustion filter analysis ({TRADE_DATE})")
print("=" * 100)
cur.execute(f"""
SELECT id,
       ts AT TIME ZONE '{TZ}' as time_et,
       direction, grade, score,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       paradigm, comments
FROM setup_log
WHERE {DATE_FILTER} AND setup_name = 'DD Exhaustion'
ORDER BY ts;
""")
rows = cur.fetchall()
if not rows:
    print("(no DD Exhaustion trades)")
else:
    for r in rows:
        prow(r)

# ============================================================================
print("\n" + "=" * 100)
print(f"QUERY 5: Auto trade orders (TS SIM) ({TRADE_DATE})")
print("=" * 100)
cur.execute(f"""
SELECT setup_log_id,
       state->>'setup_name' as setup,
       state->>'direction' as dir,
       state->>'status' as status,
       state->>'entry_price' as entry,
       state->>'t1_fill_price' as t1_fill,
       state->>'t2_fill_price' as t2_fill,
       state->>'stop_fill_price' as stop_fill,
       state->>'close_fill_price' as close_fill,
       state->>'created_at' as opened
FROM auto_trade_orders
WHERE (state->>'created_at')::date = '{TRADE_DATE}'
ORDER BY state->>'created_at';
""")
ptable(cur.fetchall())

# ============================================================================
print("\n" + "=" * 100)
print("QUERY 6: Cumulative all-time portal P&L")
print("=" * 100)
cur.execute("""
SELECT ROUND(SUM(outcome_pnl)::numeric, 1) as all_time_pnl,
       COUNT(*) as total_trades,
       SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
       SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
       SUM(CASE WHEN outcome_result = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
       ROUND(100.0 * SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) /
             NULLIF(SUM(CASE WHEN outcome_result IN ('WIN','LOSS') THEN 1 ELSE 0 END), 0), 1) as win_rate_pct
FROM setup_log WHERE outcome_result IS NOT NULL;
""")
r = cur.fetchone()
for k, v in dict(r).items():
    print(f"  {k}: {v}")

# ============================================================================
print("\n" + "=" * 100)
print(f"QUERY 7: ES Absorption signals ({TRADE_DATE})")
print("=" * 100)
cur.execute(f"""
SELECT id, ts AT TIME ZONE '{TZ}' as time_et,
       direction, grade, score,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       comments, abs_vol_ratio, abs_es_price
FROM setup_log
WHERE {DATE_FILTER} AND setup_name = 'ES Absorption'
ORDER BY ts;
""")
rows = cur.fetchall()
if not rows:
    print("(no ES Absorption signals)")
else:
    for r in rows:
        prow(r)

conn.close()
print("\n\nDone.")
