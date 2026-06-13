import os, json
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
out={}
with engine.connect() as conn:
    for day in ["2026-06-09","2026-05-27"]:
        rows=conn.execute(text("""
            SELECT (ts AT TIME ZONE 'America/New_York')::text et, spot
            FROM chain_snapshots
            WHERE (ts AT TIME ZONE 'America/New_York')::date = DATE :d
              AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN TIME '09:30' AND TIME '16:00'
              AND spot IS NOT NULL
            ORDER BY ts ASC"""), {"d":day}).fetchall()
        series=[(r[0][11:16], float(r[1])) for r in rows]
        # thin to ~every other point if dense
        out[day]=series
        if series:
            lo=min(s for _,s in series); hi=max(s for _,s in series)
            print(f"{day}: {len(series)} pts, open {series[0][1]:.0f} close {series[-1][1]:.0f} low {lo:.0f} high {hi:.0f}")
        else:
            print(f"{day}: no data")
json.dump(out, open("_tmp_intraday.json","w"))
print("saved _tmp_intraday.json")
