import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True
cur = c.cursor()

WL = ('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence',
      'ES Absorption','SB Absorption','DD Exhaustion','GEX Long')

# Coverage of key columns among placed (skip_reason IS NULL) whitelist signals
cur.execute("""
  SELECT
    MIN(ts::date), MAX(ts::date), COUNT(*),
    COUNT(basket_pct), COUNT(live_pass) FILTER (WHERE live_pass),
    COUNT(mes_sim_outcome_pnl), COUNT(outcome_pnl),
    COUNT(outcome_elapsed_min)
  FROM setup_log
  WHERE setup_name = ANY(%s) AND real_trade_skip_reason IS NULL
""", (list(WL),))
r = cur.fetchone()
print("PLACED (skip_reason IS NULL) whitelist signals — full history")
print(f"  date range      : {r[0]} -> {r[1]}")
print(f"  total           : {r[2]}")
print(f"  basket_pct set  : {r[3]}")
print(f"  live_pass=true  : {r[4]}")
print(f"  mes_sim set     : {r[5]}")
print(f"  outcome_pnl set : {r[6]}")
print(f"  elapsed_min set : {r[7]}")
print()

# basket_pct coverage by month
cur.execute("""
  SELECT to_char(ts,'YYYY-MM') m, COUNT(*) tot,
         COUNT(basket_pct) bp, COUNT(mes_sim_outcome_pnl) ms
  FROM setup_log
  WHERE setup_name = ANY(%s) AND real_trade_skip_reason IS NULL
  GROUP BY 1 ORDER BY 1
""", (list(WL),))
print("by month (placed):  month  total  basket_pct  mes_sim")
for m, tot, bp, ms in cur.fetchall():
    print(f"   {m}   {tot:5}   {bp:8}   {ms:6}")
