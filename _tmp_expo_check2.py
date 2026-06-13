import os
from sqlalchemy import create_engine, text
engine=create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    conn.execution_options(isolation_level="AUTOCOMMIT")
    print("distinct greek:", [r[0] for r in conn.execute(text("SELECT DISTINCT greek FROM volland_exposure_points")).fetchall()])
    print("distinct expiration_option:", [r[0] for r in conn.execute(text("SELECT DISTINCT expiration_option FROM volland_exposure_points")).fetchall()])
    print("distinct ticker:", [r[0] for r in conn.execute(text("SELECT DISTINCT ticker FROM volland_exposure_points")).fetchall()])
    cov=conn.execute(text("SELECT MIN(ts_utc::date), MAX(ts_utc::date), COUNT(*) FROM volland_exposure_points")).fetchone()
    print("coverage:", cov)
    # how many strikes per (greek,expiration) in latest snapshot
    print("\n--- latest snapshot: rows per greek x expiration_option ---")
    rows=conn.execute(text("""
      SELECT greek, expiration_option, COUNT(*) n, MIN(strike) lo, MAX(strike) hi
      FROM volland_exposure_points
      WHERE ts_utc = (SELECT MAX(ts_utc) FROM volland_exposure_points)
      GROUP BY 1,2 ORDER BY 1,2""")).fetchall()
    for r in rows: print(f"  {r[0]:<12} {str(r[1]):<12} n={r[2]:<4} strikes {r[3]}-{r[4]}")
    # sample vanna rows near spot latest
    print("\n--- sample vanna rows (latest, top |value|) ---")
    s=conn.execute(text("""
      SELECT greek, expiration_option, strike, value, current_price
      FROM volland_exposure_points
      WHERE ts_utc=(SELECT MAX(ts_utc) FROM volland_exposure_points) AND greek ILIKE '%vanna%'
      ORDER BY ABS(value) DESC LIMIT 8""")).fetchall()
    for r in s: print(f"  {r[0]:<14} {str(r[1]):<10} strike={r[2]} value={r[3]} spot={r[4]}")
