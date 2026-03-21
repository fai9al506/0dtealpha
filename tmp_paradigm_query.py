import os, sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as c:
    # Get all distinct paradigm values from volland_snapshots payload
    rows = c.execute(sa.text("""
        SELECT DISTINCT payload->>'paradigm' as paradigm, COUNT(*) as cnt
        FROM volland_snapshots
        WHERE payload->>'paradigm' IS NOT NULL
        GROUP BY payload->>'paradigm'
        ORDER BY cnt DESC
    """)).fetchall()
    print('All paradigm values in volland_snapshots:')
    for r in rows:
        print(f'  {r.paradigm}: {r.cnt} snapshots')

    # Get paradigm per trade from setup_log
    rows2 = c.execute(sa.text("""
        SELECT DISTINCT paradigm, COUNT(*) as cnt
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY paradigm
        ORDER BY cnt DESC
    """)).fetchall()
    print('\nParadigm values in setup_log (with outcomes):')
    for r in rows2:
        print(f'  {r.paradigm}: {r.cnt} trades')

    # Get Sidial-specific days and their trade outcomes
    rows3 = c.execute(sa.text("""
        SELECT ts::date as trade_date, paradigm, setup_name, direction, outcome_result, outcome_pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND paradigm ILIKE '%sidial%'
        ORDER BY ts
    """)).fetchall()
    print(f'\nSidial trades ({len(rows3)} total):')
    total = 0
    for r in rows3:
        pnl = float(r.outcome_pnl) if r.outcome_pnl else 0
        total += pnl
        print(f'  {r.trade_date} {r.setup_name:20s} {r.direction:5s} {r.paradigm:30s} {str(r.outcome_result):10s} {pnl:+.1f}')
    print(f'  Sidial total PnL: {total:+.1f} pts')
