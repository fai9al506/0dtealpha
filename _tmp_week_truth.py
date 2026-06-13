import os
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
with eng.connect() as c:
    rows = c.execute(text("""
        SELECT day, gross, comm, net, n_trades, n_wins
        FROM tsrt_daily_stmt WHERE day >= '2026-06-01' ORDER BY day
    """)).fetchall()
    print(f"{'day':<12}{'gross':>9}{'comm':>7}{'net':>9}{'n':>4}{'W':>4}")
    tg = tn = 0.0
    for r in rows:
        print(f"{str(r[0]):<12}{float(r[1]):>+9.1f}{float(r[2]):>7.0f}{float(r[3]):>+9.1f}{r[4]:>4}{r[5]:>4}")
        tg += float(r[1]); tn += float(r[3])
    print(f"{'WEEK':<12}{tg:>+9.1f}{'':>7}{tn:>+9.1f}")
