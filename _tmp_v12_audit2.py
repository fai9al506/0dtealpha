"""
V12-fix Filter Audit

IMPORTANT: The setup_log table logs ALL fired setups with notified=TRUE always.
The _passes_live_filter() function gates Telegram/auto-trade but NOT DB logging.
Therefore we must RE-APPLY the filter rules in SQL to separate passed vs blocked.

V12-fix filter rules (from _passes_live_filter):
1. Setup whitelist: block VIX Divergence, IV Momentum, Vanna Butterfly
2. SC grade gate: block C and LOG grades on Skew Charm
3. Time gates: SC/DD blocked 14:30-15:00 and 15:30+; BofA blocked 14:30+
4. Gap filter: block longs before 10:00 on |gap|>30 (can't check - no gap column)
5. SIDIAL-EXTREME longs: block all longs on SIDIAL-EXTREME paradigm
6. Alignment gate: longs need align >= +2
7. VIX gate: non-SC longs blocked at VIX>22 unless overvix>=2
8. SC longs: exempt from VIX gate (pass always if align>=2 and not SIDIAL-EXT)
9. Shorts: SC and AG pass always (except GEX-LIS for SC/DD, AG-TARGET for AG)
10. DD shorts: pass only if align!=0
11. All other shorts: blocked
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
import psycopg2

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

conn = psycopg2.connect(DB)
cur = conn.cursor()

def run(label, sql, params=None):
    print(f"\n{'='*80}")
    print(f"  {label}")
    print(f"{'='*80}")
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if not rows:
        print("  (no rows)")
        return rows
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
    return rows

# The V12-fix filter as a SQL CASE expression
# Returns TRUE if the trade PASSES the filter, FALSE if BLOCKED
# NOTE: We cannot check _daily_gap_pts (rule 4) - it's not in the DB. Skip that rule.
V12_FILTER = """
CASE
  -- Rule 1: Setup whitelist (block non-whitelisted)
  WHEN setup_name IN ('VIX Divergence', 'IV Momentum', 'Vanna Butterfly') THEN FALSE
  -- Rule 2: SC grade gate (block C and LOG)
  WHEN setup_name = 'Skew Charm' AND grade IN ('C', 'LOG') THEN FALSE
  -- Rule 3a: SC/DD time gate 14:30-15:00
  WHEN setup_name IN ('Skew Charm', 'DD Exhaustion')
       AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14
       AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30 THEN FALSE
  -- Rule 3b: SC/DD time gate 15:30+
  WHEN setup_name IN ('Skew Charm', 'DD Exhaustion')
       AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 15
       AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30 THEN FALSE
  WHEN setup_name IN ('Skew Charm', 'DD Exhaustion')
       AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 16 THEN FALSE
  -- Rule 3c: BofA after 14:30
  WHEN setup_name = 'BofA Scalp'
       AND ((EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30)
            OR EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 15) THEN FALSE
  -- Rule 5: SIDIAL-EXTREME longs block
  WHEN direction IN ('long', 'bullish') AND paradigm = 'SIDIAL-EXTREME' THEN FALSE
  -- Rule 6: Alignment gate for longs (need >= +2)
  WHEN direction IN ('long', 'bullish') AND COALESCE(greek_alignment, 0) < 2 THEN FALSE
  -- Rule 7: VIX gate for non-SC longs (VIX>22 and overvix<2 = blocked)
  WHEN direction IN ('long', 'bullish') AND setup_name != 'Skew Charm'
       AND vix > 22 AND COALESCE(overvix, -99) < 2 THEN FALSE
  -- Rule 8: SC longs pass (already past align>=2 check)
  WHEN direction IN ('long', 'bullish') AND setup_name = 'Skew Charm' THEN TRUE
  -- Rule 9a: All longs that passed above checks = TRUE
  WHEN direction IN ('long', 'bullish') THEN TRUE
  -- SHORTS --
  -- Rule 9b: GEX-LIS block for SC/DD shorts
  WHEN setup_name IN ('Skew Charm', 'DD Exhaustion') AND paradigm = 'GEX-LIS' THEN FALSE
  -- Rule 9c: AG-TARGET block for AG Short
  WHEN setup_name = 'AG Short' AND paradigm = 'AG-TARGET' THEN FALSE
  -- Rule 9d: SC and AG shorts pass
  WHEN setup_name IN ('Skew Charm', 'AG Short') THEN TRUE
  -- Rule 10: DD shorts pass only if align != 0
  WHEN setup_name = 'DD Exhaustion' AND COALESCE(greek_alignment, 0) != 0 THEN TRUE
  -- Rule 11: All other shorts blocked
  ELSE FALSE
END
"""

# Date range
cur.execute("SELECT MIN(ts)::date, MAX(ts)::date, COUNT(*) FROM setup_log WHERE outcome_result IN ('WIN','LOSS');")
r = cur.fetchone()
print(f"DATA RANGE: {r[0]} to {r[1]}  ({r[2]} resolved trades)")
print(f"\nNOTE: All trades in setup_log have notified=TRUE (filter gates Telegram, not logging).")
print(f"This audit RE-APPLIES the V12-fix filter rules in SQL to separate passed vs blocked.")
print(f"Gap filter (rule 4) cannot be checked - _daily_gap_pts is not stored in DB.")

# 0. BASELINE
run("0. BASELINE - All resolved trades (unfiltered)",
    f"SELECT COUNT(*) as trades, "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS');")

run("0. BASELINE - V12-fix PASS (simulated)",
    f"SELECT COUNT(*) as trades, "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = TRUE;")

run("0. BASELINE - V12-fix BLOCKED (simulated)",
    f"SELECT COUNT(*) as trades, "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = FALSE;")

# 1. SETUP WHITELIST
run("1. SETUP WHITELIST - Blocked setups (VIX Divergence, IV Momentum, Vanna Butterfly)",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') "
    "AND setup_name IN ('VIX Divergence', 'IV Momentum', 'Vanna Butterfly') "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl) DESC;")

# Also show what OTHER setups exist but aren't in the shorts whitelist (for context)
run("1b. ALL setup names in the data",
    "SELECT setup_name, COUNT(*) as trades, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log WHERE outcome_result IN ('WIN','LOSS') "
    "GROUP BY setup_name ORDER BY COUNT(*) DESC;")

# 2. SC GRADE GATE
run("2. SC GRADE GATE - All SC trades by grade",
    "SELECT grade, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY grade ORDER BY grade;")

run("2b. SC GRADE GATE - What the gate blocks (C + LOG)",
    "SELECT grade, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log WHERE setup_name = 'Skew Charm' AND outcome_result IN ('WIN','LOSS') "
    "AND grade IN ('C', 'LOG') "
    "GROUP BY grade ORDER BY grade;")

# 3. TIME GATES
run("3. TIME GATES - SC/DD signals by ET time bucket",
    "SELECT "
    "CASE "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') < 10 THEN '09:30-10:00' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') < 11 THEN '10:00-11:00' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') < 12 THEN '11:00-12:00' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') < 13 THEN '12:00-13:00' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') < 14 THEN '13:00-14:00' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') < 30 THEN '14:00-14:30' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30 THEN '14:30-15:00 [BLOCKED]' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 15 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') < 30 THEN '15:00-15:30' "
    "  WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 15 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30 THEN '15:30-16:00 [BLOCKED]' "
    "  ELSE 'Other' "
    "END as time_bucket, "
    "COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log "
    "WHERE setup_name IN ('Skew Charm', 'DD Exhaustion') AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY time_bucket ORDER BY time_bucket;")

run("3b. TIME GATES - BofA Scalp by ET time bucket",
    "SELECT "
    "CASE "
    "  WHEN (EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30) "
    "       OR EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 15 THEN '14:30+ [BLOCKED]' "
    "  ELSE 'Before 14:30' "
    "END as time_bucket, "
    "COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log "
    "WHERE setup_name = 'BofA Scalp' AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY time_bucket ORDER BY time_bucket;")

# 4. ALIGNMENT GATE
run("4. ALIGNMENT GATE - ALL longs by alignment",
    "SELECT greek_alignment as align, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl, "
    "CASE WHEN COALESCE(greek_alignment, 0) >= 2 THEN 'PASS' ELSE 'BLOCKED' END as status "
    "FROM setup_log WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY greek_alignment ORDER BY greek_alignment;")

run("4b. ALIGNMENT GATE - Blocked longs (align < 2) by setup",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "AND COALESCE(greek_alignment, 0) < 2 "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 5. VIX GATE
run("5. VIX GATE - Non-SC longs at VIX>22 with overvix<2 and align>=2 (BLOCKED)",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "AND setup_name != 'Skew Charm' "
    "AND vix > 22 AND COALESCE(overvix, -99) < 2 "
    "AND COALESCE(greek_alignment, 0) >= 2 "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

run("5b. SC VIX EXEMPTION - SC longs at VIX>22 (PASSES despite VIX)",
    "SELECT COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "AND setup_name = 'Skew Charm' AND vix > 22 "
    "AND COALESCE(greek_alignment, 0) >= 2 "
    "AND grade NOT IN ('C', 'LOG');")

run("5c. OVERVIX OVERRIDE - Non-SC longs at VIX>22 BUT overvix>=2 (PASSES)",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "AND setup_name != 'Skew Charm' "
    "AND vix > 22 AND overvix >= 2 "
    "AND COALESCE(greek_alignment, 0) >= 2 "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 6. SHORTS WHITELIST
run("6. SHORTS WHITELIST - Blocked shorts (not SC/AG, not DD-with-align!=0)",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('short','bearish') AND outcome_result IN ('WIN','LOSS') "
    "AND setup_name NOT IN ('Skew Charm', 'AG Short') "
    "AND NOT (setup_name = 'DD Exhaustion' AND COALESCE(greek_alignment,0) != 0) "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 7. GEX-LIS SHORTS BLOCK
run("7. GEX-LIS SHORTS BLOCK - SC/DD shorts on GEX-LIS paradigm",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log "
    "WHERE setup_name IN ('Skew Charm','DD Exhaustion') AND direction IN ('short','bearish') "
    "AND paradigm = 'GEX-LIS' AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 8. AG-TARGET BLOCK
run("8. AG-TARGET BLOCK - AG Short on AG-TARGET paradigm",
    "SELECT COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log WHERE setup_name = 'AG Short' AND paradigm = 'AG-TARGET' AND outcome_result IN ('WIN','LOSS');")

# 9. SIDIAL-EXTREME LONGS
run("9. SIDIAL-EXTREME LONGS BLOCK",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log WHERE paradigm = 'SIDIAL-EXTREME' AND direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 9b. SIDIAL-EXTREME shorts for comparison
run("9b. SIDIAL-EXTREME SHORTS (not blocked - for comparison)",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log WHERE paradigm = 'SIDIAL-EXTREME' AND direction IN ('short','bearish') AND outcome_result IN ('WIN','LOSS') "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl) DESC;")

# 10. GAP FILTER (approximate)
run("10. GAP FILTER (approximate) - All longs before 10:00 ET",
    "SELECT setup_name, COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    "FROM setup_log "
    "WHERE direction IN ('long','bullish') AND outcome_result IN ('WIN','LOSS') "
    "AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') < 10 "
    "GROUP BY setup_name ORDER BY SUM(outcome_pnl);")

# 11. V12-PASS LOSERS
run("11. V12-PASS LOSERS - Setups that PASS filter but have negative PnL",
    f"SELECT setup_name, direction, COUNT(*) as trades, "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = TRUE "
    f"GROUP BY setup_name, direction "
    f"HAVING SUM(outcome_pnl) < 0 "
    f"ORDER BY SUM(outcome_pnl);")

# 12. BLOCKED BUT WINNING
run("12. BLOCKED BUT WINNING - Trades that V12 blocks but have positive PnL",
    f"SELECT setup_name, direction, COUNT(*) as trades, "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = FALSE "
    f"GROUP BY setup_name, direction "
    f"HAVING SUM(outcome_pnl) > 0 "
    f"ORDER BY SUM(outcome_pnl) DESC;")

# 13. OVERALL COMPARISON
run("13. OVERALL COMPARISON",
    f"SELECT 'Unfiltered' as filter, COUNT(*) as trades, "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') "
    f"UNION ALL "
    f"SELECT 'V12-fix PASS' as filter, COUNT(*), "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END), "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1), "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = TRUE "
    f"UNION ALL "
    f"SELECT 'V12-fix BLOCKED' as filter, COUNT(*), "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END), "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1), "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = FALSE;")

# BONUS: V12-PASS full breakdown
run("BONUS: V12-PASS full breakdown by setup+direction",
    f"SELECT setup_name, direction, COUNT(*) as trades, "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = TRUE "
    f"GROUP BY setup_name, direction "
    f"ORDER BY SUM(outcome_pnl) DESC;")

# BONUS2: V12-BLOCKED full breakdown
run("BONUS2: V12-BLOCKED full breakdown by setup+direction",
    f"SELECT setup_name, direction, COUNT(*) as trades, "
    f"SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    f"ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    f"ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = FALSE "
    f"GROUP BY setup_name, direction "
    f"ORDER BY SUM(outcome_pnl) DESC;")

# BONUS3: What REASON is each blocked trade blocked for?
run("BONUS3: Block REASON breakdown",
    "SELECT "
    "CASE "
    "  WHEN setup_name IN ('VIX Divergence', 'IV Momentum', 'Vanna Butterfly') THEN 'R1: Setup not whitelisted' "
    "  WHEN setup_name = 'Skew Charm' AND grade IN ('C', 'LOG') THEN 'R2: SC grade C/LOG' "
    "  WHEN setup_name IN ('Skew Charm', 'DD Exhaustion') "
    "       AND EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14 "
    "       AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30 THEN 'R3a: SC/DD 14:30-15:00' "
    "  WHEN setup_name IN ('Skew Charm', 'DD Exhaustion') "
    "       AND (EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 16 "
    "            OR (EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 15 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30)) "
    "       THEN 'R3b: SC/DD 15:30+' "
    "  WHEN setup_name = 'BofA Scalp' "
    "       AND ((EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') = 14 AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') >= 30) "
    "            OR EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') >= 15) THEN 'R3c: BofA 14:30+' "
    "  WHEN direction IN ('long', 'bullish') AND paradigm = 'SIDIAL-EXTREME' THEN 'R5: SIDIAL-EXT longs' "
    "  WHEN direction IN ('long', 'bullish') AND COALESCE(greek_alignment, 0) < 2 THEN 'R6: Long align < 2' "
    "  WHEN direction IN ('long', 'bullish') AND setup_name != 'Skew Charm' "
    "       AND vix > 22 AND COALESCE(overvix, -99) < 2 THEN 'R7: VIX gate (>22, no overvix)' "
    "  WHEN direction IN ('short', 'bearish') AND setup_name IN ('Skew Charm', 'DD Exhaustion') "
    "       AND paradigm = 'GEX-LIS' THEN 'R9b: SC/DD short GEX-LIS' "
    "  WHEN direction IN ('short', 'bearish') AND setup_name = 'AG Short' "
    "       AND paradigm = 'AG-TARGET' THEN 'R9c: AG short AG-TARGET' "
    "  WHEN direction IN ('short', 'bearish') AND setup_name NOT IN ('Skew Charm', 'AG Short') "
    "       AND NOT (setup_name = 'DD Exhaustion' AND COALESCE(greek_alignment,0) != 0) "
    "       THEN 'R11: Short not whitelisted' "
    "  ELSE 'R?: Unknown' "
    "END as block_reason, "
    "COUNT(*) as trades, "
    "SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins, "
    "ROUND(100.0 * SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) as wr_pct, "
    "ROUND(SUM(outcome_pnl)::numeric, 1) as pnl "
    f"FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND ({V12_FILTER}) = FALSE "
    "GROUP BY block_reason ORDER BY pnl;")

conn.close()
print("\n\nAudit complete.")
