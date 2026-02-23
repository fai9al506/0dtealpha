import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT DATE(ts AT TIME ZONE 'America/New_York') as trade_date,
               COUNT(*) as trades,
               MIN(ts AT TIME ZONE 'America/New_York')::time as first_trade,
               MAX(ts AT TIME ZONE 'America/New_York')::time as last_trade,
               STRING_AGG(DISTINCT setup_name, ', ') as setups
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY DATE(ts AT TIME ZONE 'America/New_York')
        ORDER BY trade_date
    """)).mappings().all()
    
    for r in rows:
        print(f"{r['trade_date']}  trades={r['trades']:>2}  {str(r['first_trade'])[:8]}-{str(r['last_trade'])[:8]}  setups: {r['setups']}")
