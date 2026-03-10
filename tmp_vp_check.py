import sys, os
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Count all setups since id>450
r = c.execute(text("SELECT setup_name, count(*) as cnt FROM setup_log WHERE id > 450 GROUP BY setup_name ORDER BY cnt DESC")).fetchall()
print("=== Setup counts (id > 450) ===", flush=True)
for x in r:
    print(f"  {x[0]}: {x[1]}", flush=True)

# Check max setup_log id
r2 = c.execute(text("SELECT max(id) FROM setup_log")).fetchone()
print(f"Max setup_log id: {r2[0]}", flush=True)

# Check Railway logs for VP errors
r3 = c.execute(text("SELECT count(*) FROM es_range_bars WHERE trade_date = '2026-03-05' AND source = 'live'")).fetchone()
print(f"Range bars on Mar 5: {r3[0]}", flush=True)
r4 = c.execute(text("SELECT count(*) FROM es_range_bars WHERE trade_date = '2026-03-06' AND source = 'live'")).fetchone()
print(f"Range bars on Mar 6: {r4[0]}", flush=True)

# Check vanna exposure points
r5 = c.execute(text("SELECT count(*) FROM volland_exposure_points WHERE greek = 'vanna' AND ts_utc > '2026-03-05'")).fetchone()
print(f"Vanna exposure points since Mar 5: {r5[0]}", flush=True)

c.close()
