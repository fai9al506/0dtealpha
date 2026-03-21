import os, json
import sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as c:
    rows = c.execute(sa.text("""
        SELECT
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            MIN(vix) as min_vix, MAX(vix) as max_vix, AVG(vix) as avg_vix,
            MIN(overvix) as min_overvix, MAX(overvix) as max_overvix, AVG(overvix) as avg_overvix,
            COUNT(*) as trades
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND vix IS NOT NULL
        GROUP BY (ts AT TIME ZONE 'America/New_York')::date
        ORDER BY trade_date
    """)).fetchall()

    for r in rows:
        vix_str = f'VIX {float(r.avg_vix):.1f} ({float(r.min_vix):.1f}-{float(r.max_vix):.1f})'
        if r.avg_overvix is not None:
            ov_str = f'Overvix {float(r.avg_overvix):.2f} ({float(r.min_overvix):.2f}-{float(r.max_overvix):.2f})'
        else:
            ov_str = 'Overvix n/a'
        print(f'{r.trade_date}: {vix_str}, {ov_str}, {r.trades} trades')
