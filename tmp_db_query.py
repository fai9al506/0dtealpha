import os
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Get columns
r = c.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='setup_log' ORDER BY ordinal_position")).fetchall()
print("COLUMNS:", [x[0] for x in r])

# Get recent daily summary
r2 = c.execute(text("""
    SELECT ts::date as d, count(*) as n,
           sum(case when outcome_result like '%WIN%' then 1 else 0 end) as w,
           sum(case when outcome_result like '%LOSS%' then 1 else 0 end) as l,
           round(sum(coalesce(outcome_pnl,0))::numeric, 1) as pnl
    FROM setup_log
    WHERE ts >= '2026-03-03'
    GROUP BY ts::date
    ORDER BY ts::date
""")).fetchall()
print("\nDAILY SUMMARY:")
for x in r2:
    print(f"  {x[0]}  trades={x[1]}  W={x[2]}  L={x[3]}  PnL={x[4]}")

# Get recent trades detail
r3 = c.execute(text("""
    SELECT ts::date as d, to_char(ts, 'HH24:MI') as t,
           setup_name, direction, grade, outcome_result, outcome_pnl, greek_alignment
    FROM setup_log
    WHERE ts >= '2026-03-05'
    ORDER BY ts
""")).fetchall()
print(f"\nDETAIL (Mar 5+): {len(r3)} trades")
for x in r3:
    print(f"  {x[0]} {x[1]}  {x[2]:20s} {x[3]:5s} {x[4]:5s} {str(x[5] or 'OPEN'):15s} {float(x[6] or 0):+7.1f}  align={x[7]}")

c.close()
