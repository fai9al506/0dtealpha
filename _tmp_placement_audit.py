# -*- coding: utf-8 -*-
"""Placement audit: did the broker place MORE than the V16 signal set in June?
And do the placed trades pass the canonical V16 filter (live_pass)?"""
import os, psycopg2
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

# distinct setup_log_ids that real_trader actually placed, by month, with live_pass
cur.execute("""
  SELECT to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM') mon,
         COUNT(DISTINCT rto.setup_log_id) placed_signals,
         COUNT(DISTINCT rto.setup_log_id) FILTER (WHERE sl.live_pass) placed_v16pass,
         COUNT(DISTINCT rto.setup_log_id) FILTER (WHERE sl.live_pass IS NOT TRUE) placed_NOT_v16
  FROM real_trade_orders rto
  JOIN setup_log sl ON sl.id = rto.setup_log_id
  WHERE sl.ts AT TIME ZONE 'America/New_York' >= '2026-05-01'
  GROUP BY mon ORDER BY mon
""")
print("PLACED signals (distinct setup_log_id) vs V16-pass, by month:")
print(f"{'month':<9}{'placed':>9}{'v16_pass':>10}{'NOT_v16':>9}")
for mon,pl,v16,nv in cur.fetchall():
    print(f"{mon:<9}{pl:>9}{v16:>10}{nv:>9}")

# June: which setups were placed but FAIL v16 (over-placement)?
cur.execute("""
  SELECT sl.setup_name, sl.direction, sl.grade, sl.paradigm,
         COUNT(*) FILTER (WHERE sl.live_pass IS NOT TRUE) AS not_v16,
         COUNT(*) AS total
  FROM real_trade_orders rto
  JOIN setup_log sl ON sl.id=rto.setup_log_id
  WHERE to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-06'
  GROUP BY 1,2,3,4 ORDER BY not_v16 DESC LIMIT 25
""")
print("\nJUNE placed setups, flag those NOT passing V16 (over-placement candidates):")
print(f"{'setup':<16}{'dir':<8}{'grade':<6}{'paradigm':<16}{'not_v16':>8}{'total':>7}")
for sn,d,g,p,nv,t in cur.fetchall():
    print(f"{(sn or '')[:15]:<16}{(d or '')[:7]:<8}{(g or '')[:5]:<6}{(p or '')[:15]:<16}{nv:>8}{t:>7}")

# How many real_trade_orders ROWS (entries) per distinct signal in June -> stacking/fragmentation?
cur.execute("""
  SELECT COUNT(*) rows, COUNT(DISTINCT setup_log_id) sigs
  FROM real_trade_orders rto
  JOIN setup_log sl ON sl.id=rto.setup_log_id
  WHERE to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-06'
""")
rows,sigs=cur.fetchone()
print(f"\nJune real_trade_orders rows={rows} over distinct signals={sigs} (ratio {rows/sigs:.2f})")
print("Broker tsrt_daily_stmt counted 127 FIFO round-trips for June.")
conn.close()
