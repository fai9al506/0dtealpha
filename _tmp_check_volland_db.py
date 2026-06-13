import os, json
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"])
with eng.connect() as c:
    r = c.execute(text("SELECT ts, payload FROM volland_snapshots ORDER BY ts DESC LIMIT 3"))
    for row in r:
        d = dict(row._mapping)
        p = d["payload"] if isinstance(d["payload"], dict) else json.loads(d["payload"]) if d["payload"] else {}
        print(f"\n=== {d['ts']} ===")
        # Print key fields
        for k in ("paradigm", "lis", "target", "dd_spx", "dd_spy", "charm", "lis_upper", "exposures", "spot_vol_beta"):
            v = p.get(k) if isinstance(p, dict) else None
            print(f"  {k}: {v}")
        # All keys
        if isinstance(p, dict):
            print(f"  statistics: {p.get('statistics')}")
            print(f"  spy_statistics: {p.get('spy_statistics')}")
            print(f"  captures keys: {list(p.get('captures',{}).keys()) if isinstance(p.get('captures'),dict) else type(p.get('captures'))}")
            print(f"  exposure_points_saved: {p.get('exposure_points_saved')}")
