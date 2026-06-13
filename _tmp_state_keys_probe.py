import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
with eng.connect() as c:
    rows = c.execute(text("""
        SELECT rto.setup_log_id, rto.state, sl.setup_name, sl.direction,
               (sl.ts AT TIME ZONE 'America/New_York') AS et
        FROM real_trade_orders rto
        JOIN setup_log sl ON sl.id = rto.setup_log_id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
        ORDER BY rto.setup_log_id DESC LIMIT 2
    """)).fetchall()
for row in rows:
    st = row[1] if isinstance(row[1], dict) else json.loads(row[1])
    print(f"=== lid {row[0]} {row[2]} {row[3]} entry_ts={row[4]}")
    for k in sorted(st.keys()):
        print(f"  {k} = {str(st[k])[:90]}")
