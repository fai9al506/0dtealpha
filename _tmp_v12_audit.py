import sys
sys.stdout.reconfigure(encoding='utf-8')

import psycopg2

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

conn = psycopg2.connect(DB)
cur = conn.cursor()

def run(label, sql):
    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"{'='*80}")
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if not rows:
        print("  (no rows)")
        return
    widths = [len(c) for c in cols]
    str_rows = []
    for r in rows:
        sr = [str(v) if v is not None else 'NULL' for v in r]
        str_rows.append(sr)
        for i, s in enumerate(sr):
            widths[i] = max(widths[i], len(s))
    fmt = "  " + " | ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*cols))
    print("  " + "-+-".join("-"*w for w in widths))
    for sr in str_rows:
        print(fmt.format(*sr))

# Date range
cur.execute("SELECT MIN(ts)::date, MAX(ts)::date, COUNT(*) FROM setup_log WHERE outcome_result IN ('WIN','LOSS');")
r = cur.fetchone()
print(f"DATA RANGE: {r[0]} to {r[1]}  ({r[2]} resolved trades)")

# 0. BASELINE
run("0. BASELINE - Unfiltered",
    "SELECT COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS');")

run("0. BASELINE - V12-fix filtered (notified=true)",
    "SELECT COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND notified = true;")

run("0. BASELINE - Blocked (notified=false)",
    "SELECT COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND notified = false;")

# 1. SETUP WHITELIST
run("1. SETUP WHITELIST - Blocked setups (notified=false) by setup_name",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND notified = false "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl) DESC;")

# 2. SC GRADE GATE
run("2. SC GRADE GATE - All SC trades by grade",
    "SELECT grade, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY grade ORDER BY grade;")

# 3. TIME GATES - SC/DD
run("3. TIME GATES - SC/DD by ET time bucket",
    "SELECT "
    "CASE "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30 THEN '14:30-15:00' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 15 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30 THEN '15:30-16:00' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 15 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') < 30 THEN '15:00-15:30' "
    "  ELSE 'Other' "
    "END as time_bucket, "
    "COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log "
    "WHERE setup_name IN ('Skew Charm', 'DD Exhaustion') AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY time_bucket ORDER BY time_bucket;")

# 3b. BofA time
run("3b. TIME GATES - BofA Scalp by ET time bucket",
    "SELECT "
    "CASE "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30 THEN '14:30-15:00' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 15 THEN '15:00+' "
    "  ELSE 'Before 14:30' "
    "END as time_bucket, "
    "COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log "
    "WHERE setup_name = 'BofA Scalp' AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY time_bucket ORDER BY time_bucket;")

# 4. ALIGNMENT GATE
run("4. ALIGNMENT GATE - Longs by alignment",
    "SELECT greek_alignment, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY greek_alignment ORDER BY greek_alignment;")

# 5. SC VIX EXEMPTION
run("5. SC VIX EXEMPTION - Non-SC longs at VIX>22 with overvix<2 (blocked by VIX gate)",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "AND setup_name != 'Skew Charm' "
    "AND vix > 22 AND (overvix IS NULL OR overvix < 2) "
    "AND greek_alignment >= 2 "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 5b. SC longs at VIX>22
run("5b. SC VIX EXEMPTION - SC longs at VIX>22 (exempted, passes filter)",
    "SELECT COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "AND setup_name = 'Skew Charm' "
    "AND vix > 22;")

# 6. SHORTS WHITELIST
run("6. SHORTS WHITELIST - Blocked shorts (not SC, not AG, not DD-with-align)",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('short','bearish') AND outcome_result IN ('WIN','LOSS') "
    "AND setup_name NOT IN ('Skew Charm', 'AG Short') "
    "AND NOT (setup_name = 'DD Exhaustion' AND greek_alignment != 0) "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 7. GEX-LIS SHORTS BLOCK
run("7. GEX-LIS SHORTS BLOCK - SC/DD shorts on GEX-LIS paradigm",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log "
    "WHERE setup_name IN ('Skew Charm','DD Exhaustion') AND direction IN ('short','bearish') "
    "AND paradigm = 'GEX-LIS' AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 8. AG-TARGET BLOCK
run("8. AG-TARGET BLOCK - AG Short on AG-TARGET paradigm",
    "SELECT COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE setup_name = 'AG Short' AND paradigm = 'AG-TARGET' AND outcome_result IN ('WIN','LOSS');")

# 9. SIDIAL-EXTREME LONGS
run("9. SIDIAL-EXTREME LONGS BLOCK",
    "SELECT setup_name, direction, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE paradigm = 'SIDIAL-EXTREME' AND direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY setup_name, direction ORDER BY SUM(outcome_pnl);")

# 10. GAP FILTER approximate
run("10. GAP FILTER (approximate) - All longs before 10:00 ET",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') < 10 "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 11. V12-pass LOSERS
run("11. V12-PASS LOSERS - Notified trades with negative PnL by setup+direction",
    "SELECT setup_name, direction, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND notified = true "
    "GROUP BY setup_name, direction "
    "HAVING SUM(outcome_pnl) < 0 "
    "ORDER BY SUM(outcome_pnl);")

# 12. BLOCKED BUT WINNING
run("12. BLOCKED BUT WINNING - notified=false with positive PnL",
    "SELECT setup_name, direction, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND notified = false "
    "GROUP BY setup_name, direction "
    "HAVING SUM(outcome_pnl) > 0 "
    "ORDER BY SUM(outcome_pnl) DESC;")

# 13. OVERALL COMPARISON
run("13. OVERALL COMPARISON",
    "SELECT 'Unfiltered' as filter, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') "
    "UNION ALL "
    "SELECT 'V12-fix' as filter, COUNT(*), "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END), "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1), "
    "SUM(outcome_pnl) "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND notified = true "
    "UNION ALL "
    "SELECT 'Blocked' as filter, COUNT(*), "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END), "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1), "
    "SUM(outcome_pnl) "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND notified = false;")

# BONUS
run("BONUS: V12-PASS full breakdown by setup+direction",
    "SELECT setup_name, direction, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "SUM(outcome_pnl) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND notified = true "
    "GROUP BY setup_name, direction "
    "ORDER BY SUM(outcome_pnl) DESC;")

conn.close()
print("\n\nDone.")
