import sys, os, psycopg
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from psycopg.rows import dict_row
from datetime import date

conn = psycopg.connect(os.environ['DATABASE_URL'], row_factory=dict_row)

d = date(2026, 2, 28)
print(f'Feb 28, 2026 is a {d.strftime("%A")}')
print()

recent = conn.execute("""
    SELECT ts::date as trade_date, COUNT(*) as cnt,
           SUM(CASE WHEN outcome_result ILIKE '%WIN%' THEN 1 ELSE 0 END) as wins,
           SUM(CASE WHEN outcome_result ILIKE '%LOSS%' THEN 1 ELSE 0 END) as losses,
           SUM(COALESCE(outcome_pnl, 0)) as pnl
    FROM setup_log
    WHERE ts::date >= '2026-02-25'
    GROUP BY ts::date
    ORDER BY ts::date DESC
    LIMIT 5
""").fetchall()
print('Recent trading days:')
for r in recent:
    print(f'  {r["trade_date"]}  trades={r["cnt"]}  W={r["wins"]}  L={r["losses"]}  PnL={float(r["pnl"]):+.1f}')

es = conn.execute("""
    SELECT trade_date, source, COUNT(*) as cnt
    FROM es_range_bars
    WHERE trade_date >= '2026-02-25' AND status = 'closed'
    GROUP BY trade_date, source
    ORDER BY trade_date DESC
    LIMIT 10
""").fetchall()
print()
print('Recent ES range bar dates:')
for r in es:
    print(f'  {r["trade_date"]}  source={r["source"]}  bars={r["cnt"]}')

conn.close()
