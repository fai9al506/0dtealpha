# -*- coding: utf-8 -*-
import os, psycopg2, pandas as pd
conn=psycopg2.connect(os.environ["DATABASE_URL"])
# 1) how far back is the basket signal populated?
print("=== basket_pct coverage on setup_log (monthly) ===")
cov=pd.read_sql("""SELECT to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM') mo,
  count(*) sigs, count(basket_pct) has_basket
  FROM setup_log WHERE ts AT TIME ZONE 'America/New_York'>='2026-02-01'
  GROUP BY mo ORDER BY mo""",conn)
print(cov.to_string(index=False))
print("\n=== semi_basket table (the LIVE capture) date range ===")
sb=pd.read_sql("""SELECT min(et AT TIME ZONE 'America/New_York') first, max(et AT TIME ZONE 'America/New_York') last, count(*) n,
  count(distinct date(et AT TIME ZONE 'America/New_York')) days FROM semi_basket""",conn)
print(sb.to_string(index=False))
conn.close()
