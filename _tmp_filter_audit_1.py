import os, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()

def q(sql):
    cur.execute(sql); return cur.fetchall()

# Month coverage: rows, chain populated, mes populated
print("=== setup_log monthly coverage (2026) ===")
for m in ['2026-03','2026-04','2026-05','2026-06','2026-07']:
    r=q(f"""SELECT count(*),
        count(outcome_pnl),
        count(mes_sim_outcome_pnl),
        round(coalesce(sum(outcome_pnl),0)::numeric,1),
        round(coalesce(sum(mes_sim_outcome_pnl),0)::numeric,1)
      FROM setup_log
      WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')='{m}'""")[0]
    print(f"{m}: rows={r[0]:4d}  chain_pop={r[1]:4d}  mes_pop={r[2]:4d}  chainSum={r[3]:>8}  mesSum={r[4]:>8}")

print("\n=== JULY by direction: chain vs mes (all setup_log rows, no filter) ===")
r=q("""SELECT
    CASE WHEN direction IN ('long','bullish') THEN 'LONG' ELSE 'SHORT' END dir,
    count(*),
    count(mes_sim_outcome_pnl) mespop,
    round(coalesce(sum(outcome_pnl),0)::numeric,1) chain,
    round(coalesce(sum(mes_sim_outcome_pnl),0)::numeric,1) mes
  FROM setup_log
  WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-07'
  GROUP BY 1 ORDER BY 1""")
for x in r: print(f"{x[0]}: n={x[1]:4d} mes_pop={x[2]:4d}  chain={x[3]:>8}  mes={x[4]:>8}")

print("\n=== JULY chain vs mes on MATCHED population (mes not null) by direction ===")
r=q("""SELECT
    CASE WHEN direction IN ('long','bullish') THEN 'LONG' ELSE 'SHORT' END dir,
    count(*),
    round(sum(outcome_pnl)::numeric,1) chain,
    round(sum(mes_sim_outcome_pnl)::numeric,1) mes
  FROM setup_log
  WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')='2026-07'
    AND mes_sim_outcome_pnl IS NOT NULL AND outcome_pnl IS NOT NULL
  GROUP BY 1 ORDER BY 1""")
for x in r: print(f"{x[0]}: n_matched={x[1]:4d}  chain={x[2]:>8}  mes={x[3]:>8}")
