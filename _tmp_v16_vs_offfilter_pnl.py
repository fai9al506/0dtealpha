# -*- coding: utf-8 -*-
"""Decisive test: in June, did the OFF-V16 placed trades bleed while V16-pass stayed green?
Uses setup_log sim P&L (chain) + mes_sim (broker-proxy) split by live_pass, PLACED-only."""
import os, psycopg2
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

for mon in ('2026-05','2026-06'):
    cur.execute("""
      SELECT sl.live_pass,
             COUNT(*) n,
             COUNT(*) FILTER (WHERE sl.outcome_result='WIN') wins,
             ROUND(SUM(sl.outcome_pnl)::numeric,1) chain_pnl,
             ROUND(SUM(sl.mes_sim_outcome_pnl)::numeric,1) mes_pnl,
             ROUND(AVG(sl.outcome_pnl)::numeric,2) chain_avg
      FROM real_trade_orders rto
      JOIN setup_log sl ON sl.id=rto.setup_log_id
      WHERE to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM')=%s
        AND sl.outcome_result IS NOT NULL
      GROUP BY sl.live_pass ORDER BY sl.live_pass
    """,(mon,))
    print(f"\n=== {mon}  PLACED trades split by V16 pass ===")
    print(f"{'V16pass':<9}{'n':>5}{'wins':>6}{'WR%':>7}{'chain_pnl':>11}{'mes_pnl':>10}{'chain_avg':>11}")
    for lp,n,w,cp,mp,ca in cur.fetchall():
        wr=w/n*100 if n else 0
        print(f"{str(lp):<9}{n:>5}{w:>6}{wr:>7.1f}{float(cp or 0):>11}{float(mp) if mp is not None else 0:>10}{float(ca or 0):>11}")

# June: the 80 off-filter trades by setup
cur.execute("""
  SELECT sl.setup_name,
         COUNT(*) n,
         ROUND(SUM(sl.outcome_pnl)::numeric,1) chain_pnl
  FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
  WHERE to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-06'
    AND sl.live_pass IS NOT TRUE AND sl.outcome_result IS NOT NULL
  GROUP BY 1 ORDER BY chain_pnl
""")
print("\nJUNE off-V16 placed trades by setup (chain sim P&L):")
for sn,n,cp in cur.fetchall():
    print(f"  {sn:<16} n={n:<4} chain_pnl={float(cp or 0):>8}")
conn.close()
