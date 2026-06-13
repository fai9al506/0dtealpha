import os, shutil
from datetime import date
from sqlalchemy import create_engine

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
engine = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

from app.eod_report import generate_trades_chart, _query_tsrt_trades

d = date(2026, 6, 5)
trades = _query_tsrt_trades(engine, d)
print(f"{len(trades)} TSRT trades on {d}:")
for t in trades:
    print(f"  lid={t['id']} {t['setup_name']:<16} {t['direction']:<8} "
          f"entry={t['entry']} exit={t['exit']} pts={t['pts']} ${t['dollars']} "
          f"{t['result']} reason={t['close_reason']}")

path = generate_trades_chart(engine, d)
print("chart:", path)
if path:
    shutil.copy(path, "_tmp_chart_test.png")
    print("copied to _tmp_chart_test.png")
