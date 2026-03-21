import os, json
import sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as c:
    # Get a sample payload to see structure
    row = c.execute(sa.text(
        "SELECT payload FROM volland_snapshots ORDER BY ts DESC LIMIT 1"
    )).fetchone()
    if row:
        payload = row.payload if isinstance(row.payload, dict) else json.loads(row.payload)
        print(json.dumps(list(payload.keys()), indent=2))
        # Check for paradigm-related keys
        for k in payload:
            if 'paradigm' in k.lower() or 'para' in k.lower():
                print(f'\n{k}: {payload[k]}')
        # Also check if statistics is nested
        if 'statistics' in payload:
            print(f'\nstatistics keys: {list(payload["statistics"].keys()) if isinstance(payload["statistics"], dict) else payload["statistics"]}')
    else:
        print('No rows found')
