import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True; cur = c.cursor()
def q(s, *a):
    cur.execute(s, a); return cur.fetchall()

print("=== JULY mes-sim contamination scan (|mes|>40 = suspicious) ===")
r = q("""SELECT count(*) FILTER (WHERE abs(mes_sim_outcome_pnl)>40),
                count(*) FILTER (WHERE mes_sim_outcome_pnl IS NOT NULL),
                min(mes_sim_outcome_pnl), max(mes_sim_outcome_pnl)
         FROM setup_log WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-07'""")
print("suspicious(|mes|>40):", r[0][0], " of mes-pop", r[0][1], " min/max:", r[0][2], r[0][3])

print("\n=== JULY: is longs-mes>chain driven by a few outliers? top 8 |mes-chain| longs ===")
r = q("""SELECT id, setup_name, direction, to_char(ts AT TIME ZONE 'America/New_York','MM-DD HH24:MI') et,
                outcome_pnl, mes_sim_outcome_pnl, outcome_result
         FROM setup_log
         WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-07'
           AND direction IN ('long','bullish') AND mes_sim_outcome_pnl IS NOT NULL
         ORDER BY abs(coalesce(mes_sim_outcome_pnl,0)-coalesce(outcome_pnl,0)) DESC LIMIT 8""")
for x in r: print(f"  lid{x[0]} {x[1][:12]:12} {x[3]} chain={float(x[4]):6.1f} mes={float(x[5]):6.1f} {x[6]}")

print("\n=== JULY per-DAY chain vs mes (all whitelist mes-covered rows) ===")
r = q("""SELECT to_char(ts AT TIME ZONE 'America/New_York','MM-DD') d,
                count(*) n,
                round(sum(outcome_pnl)::numeric,1) chain,
                round(sum(mes_sim_outcome_pnl)::numeric,1) mes
         FROM setup_log
         WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-07'
           AND mes_sim_outcome_pnl IS NOT NULL AND outcome_pnl IS NOT NULL
         GROUP BY 1 ORDER BY 1""")
for x in r: print(f"  {x[0]}: n={x[1]:3d} chain={float(x[2]):7.1f} mes={float(x[3]):7.1f}")

print("\n=== Any REAL broker trades in July? (real_trade_orders) ===")
r = q("""SELECT to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM-DD') d, count(*)
         FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
         WHERE to_char(sl.ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-07'
         GROUP BY 1 ORDER BY 1""")
print("  real July trade-days:", [(x[0], x[1]) for x in r] or "NONE (real trading off since Jul 1)")

print("\n=== tsrt_daily_stmt July (broker truth $) ===")
try:
    r = q("""SELECT day, net, n_trades FROM tsrt_daily_stmt WHERE day::text LIKE '2026-07%' ORDER BY day""")
    for x in r: print(f"  {x[0]}: net=${float(x[1]):.2f} trades={x[2]}")
    if not r: print("  none")
except Exception as e:
    print("  err", e)

print("\n=== SHORTS as % of each filter's July trades (the mechanism) ===")
# reuse: shorts capture ~17% vs longs; short-heavy filters inflated by chain
r = q("""SELECT CASE WHEN direction IN ('long','bullish') THEN 'LONG' ELSE 'SHORT' END dir,
                setup_name, count(*),
                round(sum(outcome_pnl)::numeric,1) chain,
                round(sum(mes_sim_outcome_pnl) FILTER (WHERE mes_sim_outcome_pnl IS NOT NULL)::numeric,1) mes
         FROM setup_log WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-07'
         GROUP BY 1,2 ORDER BY 1, chain DESC""")
for x in r: print(f"  {x[0]:5} {x[1][:16]:16} n={x[2]:3d} chain={float(x[3] or 0):7.1f} mes={float(x[4] or 0):7.1f}")
