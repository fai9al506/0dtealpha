"""lid 3629 forensic: when did tracker resolve WIN, what was ES doing, when did MES close fill."""
import os, json
from datetime import timedelta
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    row = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') AS et, setup_name, direction,
               abs_es_price, outcome_result, outcome_pnl, outcome_max_profit,
               outcome_max_loss, outcome_elapsed_min, outcome_stop_level,
               outcome_target_level, spot
        FROM setup_log WHERE id = 3629
    """)).fetchone()
    et = row[0]
    print("setup_log 3629:")
    print(f"  entry ts (ET):      {et}")
    print(f"  setup/dir:          {row[1]} {row[2]}")
    print(f"  abs_es_price:       {row[3]}")
    print(f"  spot at signal:     {row[11]}")
    print(f"  outcome:            {row[4]}  pnl={row[5]}  max_profit={row[6]}  max_loss={row[7]}")
    print(f"  elapsed_min:        {row[8]}  -> resolution ~{(et + timedelta(minutes=float(row[8]))).strftime('%H:%M:%S')} ET")
    print(f"  stop/target levels: {row[9]} / {row[10]}")

    st = c.execute(text("SELECT state FROM real_trade_orders WHERE setup_log_id = 3629")).fetchone()[0]
    if isinstance(st, str):
        st = json.loads(st)
    print("\nreal_trade_orders state:")
    for k in sorted(st.keys()):
        print(f"  {k} = {st[k]}")

    # ES 5pt range bars around entry -> +40 min
    print("\nES range bars (vps, 5pt) 13:20-14:30 ET:")
    bars = c.execute(text("""
        SELECT (ts_start AT TIME ZONE 'America/New_York') AS et, bar_open, bar_high, bar_low, bar_close
        FROM vps_es_range_bars
        WHERE trade_date = '2026-06-05' AND range_pts = 5.0
          AND (ts_start AT TIME ZONE 'America/New_York')::time BETWEEN '13:20' AND '14:30'
        ORDER BY ts_start
    """)).fetchall()
    for b in bars:
        print(f"  {b[0].strftime('%H:%M:%S')}  O {float(b[1]):.2f}  H {float(b[2]):.2f}  L {float(b[3]):.2f}  C {float(b[4]):.2f}")
