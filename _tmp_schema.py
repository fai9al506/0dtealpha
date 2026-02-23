import os, json
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Check volland payload->statistics
r = c.execute(text("SELECT payload->'statistics' as stats FROM volland_snapshots WHERE ts >= '2026-02-20' ORDER BY ts DESC LIMIT 1")).fetchone()
stats = r[0] if r else {}
if isinstance(stats, str):
    stats = json.loads(stats)
print(f"stats type: {type(stats)}", flush=True)
print(f"stats keys: {list(stats.keys()) if isinstance(stats, dict) else 'not dict'}", flush=True)
if isinstance(stats, dict):
    for k, v in stats.items():
        print(f"  {k}: {str(v)[:150]}", flush=True)

c.close()
