"""S233 step 0 — data landscape. What is in setup_log, how complete, what is excluded."""
import os, collections
from sqlalchemy import create_engine, text
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
E = create_engine(os.environ["DATABASE_URL"])

with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
    print("=== setups in setup_log since 2026-03-16 (n, resolved%, chain pts) ===")
    rows = c.execute(text("""
        SELECT setup_name,
               COUNT(*) n,
               COUNT(outcome_pnl) resolved,
               ROUND(SUM(outcome_pnl)::numeric,1) pts,
               ROUND(AVG(CASE WHEN outcome_pnl>0 THEN 1.0 ELSE 0.0 END)*100,0) wr
        FROM setup_log WHERE ts >= '2026-03-16'
        GROUP BY 1 ORDER BY n DESC""")).fetchall()
    for r in rows:
        print(f"  {r[0]:<24}{r[1]:>6}{'':>2}res {r[2]:>5} ({(r[2]/r[1]*100):>3.0f}%)  pts {str(r[3]):>9}  WR {r[4]}%")

    print("\n=== monthly signal counts + resolution (whitelist setups) ===")
    rows = c.execute(text("""
        SELECT to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM') m,
               COUNT(*) n, COUNT(outcome_pnl) res,
               COUNT(DISTINCT date(ts AT TIME ZONE 'America/New_York')) sess
        FROM setup_log
        WHERE ts >= '2026-02-01' AND setup_name IN
          ('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion','VIX Divergence','GEX Long')
        GROUP BY 1 ORDER BY 1""")).fetchall()
    for r in rows:
        print(f"  {r[0]}  signals {r[1]:>5}  resolved {r[2]:>5} ({r[2]/r[1]*100:>3.0f}%)  sessions {r[3]:>3}")

    print("\n=== columns available on setup_log ===")
    rows = c.execute(text("""SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name='setup_log' ORDER BY ordinal_position""")).fetchall()
    print("  " + ", ".join(r[0] for r in rows))

    print("\n=== VIX regime by month (daily mean/median of setup_log.vix) ===")
    rows = c.execute(text("""
        SELECT to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM') m,
               ROUND(AVG(vix)::numeric,1), ROUND(MIN(vix)::numeric,1), ROUND(MAX(vix)::numeric,1)
        FROM setup_log WHERE ts>='2026-02-01' AND vix IS NOT NULL GROUP BY 1 ORDER BY 1""")).fetchall()
    for r in rows:
        print(f"  {r[0]}  vix avg {r[1]}  min {r[2]}  max {r[3]}")
