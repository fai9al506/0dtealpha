import os, sys
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

r = c.execute(text("SELECT direction, COUNT(*) as n, SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as w, COALESCE(SUM(outcome_pnl),0) as p FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND setup_name='ES Absorption' AND greek_alignment>=2 GROUP BY direction")).fetchall()
print("ES Absorption longs (align>=2):", flush=True)
for x in r:
    print(f"  {x[0]}: {x[1]}t {x[2]/x[1]*100:.0f}%WR {float(x[3]):+.1f}", flush=True)

r2 = c.execute(text("SELECT direction, COUNT(*) as n, SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as w, COALESCE(SUM(outcome_pnl),0) as p FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND setup_name='DD Exhaustion' AND greek_alignment=-1 GROUP BY direction")).fetchall()
print("DD align=-1:", flush=True)
for x in r2:
    print(f"  {x[0]}: {x[1]}t {x[2]/x[1]*100:.0f}%WR {float(x[3]):+.1f}", flush=True)

r3 = c.execute(text("SELECT direction, COUNT(*) as n, SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as w, COALESCE(SUM(outcome_pnl),0) as p FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND setup_name='DD Exhaustion' AND greek_alignment=3 GROUP BY direction")).fetchall()
print("DD align=3:", flush=True)
for x in r3:
    print(f"  {x[0]}: {x[1]}t {x[2]/x[1]*100:.0f}%WR {float(x[3]):+.1f}", flush=True)

# SC by direction
r4 = c.execute(text("SELECT direction, COUNT(*) as n, SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as w, COALESCE(SUM(outcome_pnl),0) as p FROM setup_log WHERE outcome_result IN ('WIN','LOSS') AND setup_name='Skew Charm' GROUP BY direction")).fetchall()
print("Skew Charm by direction:", flush=True)
for x in r4:
    print(f"  {x[0]}: {x[1]}t {x[2]/x[1]*100:.0f}%WR {float(x[3]):+.1f}", flush=True)

c.close()
print("done", flush=True)
