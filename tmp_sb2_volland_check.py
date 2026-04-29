import os, json
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
with e.begin() as c:
    r = c.execute(text("""SELECT payload FROM volland_snapshots
        WHERE payload->>'error_event' IS NULL AND payload->'statistics' IS NOT NULL
        ORDER BY ts DESC LIMIT 1""")).fetchone()
    p = json.loads(r[0]) if isinstance(r[0], str) else r[0]
    print('TOP KEYS:', list(p.keys()))
    s = p.get('statistics', {})
    print('STATS KEYS:', list(s.keys()))
    print('SVB in top:', p.get('spot_vol_beta'))
    print('SVB in stats:', s.get('spotVolBeta'))
    print('paradigm:', s.get('paradigm'))
    print('charm:', s.get('aggregatedCharm'))
    print('dd:', s.get('delta_decay_hedging'))
