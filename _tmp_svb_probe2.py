import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
with eng.connect() as c:
    row = c.execute(text("SELECT payload FROM volland_snapshots ORDER BY ts DESC LIMIT 1")).fetchone()
    p = row[0] if isinstance(row[0], dict) else json.loads(row[0])

    def walk(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                kp = f"{path}.{k}" if path else k
                if any(t in k.lower() for t in ("spot", "vol", "beta", "vix", "statistic", "paradigm")):
                    s = str(v)
                    print(f"{kp} = {s[:300]}")
                else:
                    walk(v, kp)

    walk(p)
    print("\nTop-level keys:", list(p.keys()))
    caps = p.get("captures", {})
    print("captures keys:", list(caps.keys()))
