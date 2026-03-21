import os, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
with e.connect() as conn:
    # Volland snapshots - payload is JSONB with statistics nested
    rows = conn.execute(text("""
        SELECT ts, payload
        FROM volland_snapshots
        WHERE payload IS NOT NULL
          AND payload->'statistics' IS NOT NULL
          AND payload->>'statistics' != '{}'
        ORDER BY ts
    """)).fetchall()
    print(f"Fetched {len(rows)} volland snapshots")

    data = []
    for r in rows:
        ts = str(r[0])
        p = r[1] if isinstance(r[1], dict) else json.loads(r[1])
        stats = p.get("statistics", {})
        if not stats:
            continue
        data.append({
            "ts": ts,
            "paradigm": stats.get("paradigm", ""),
            "dd_hedging": stats.get("delta_decay_hedging", stats.get("deltaDecayHedging", "")),
            "charm": stats.get("aggregated_charm", stats.get("aggregatedCharm", "")),
            "lis": stats.get("lines_in_sand", stats.get("linesInSand", "")),
            "svb": stats.get("spot_vol_beta", stats.get("spotVolBeta", "")),
        })

    with open("tmp_volland_stats.json", "w") as f:
        json.dump(data, f)
    print(f"Saved {len(data)} snapshots to tmp_volland_stats.json")

    # Setup log for reference
    cols = [c['name'] for c in __import__('sqlalchemy').inspect(e).get_columns('setup_log')]
    print(f"setup_log columns: {cols}")
