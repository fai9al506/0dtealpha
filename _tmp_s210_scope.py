"""S210 scope: all lids where FIFO rewrote close_fill_price but stop_fill_price
still holds the stale bot value (the invisible-rewrite class)."""
import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT setup_log_id, state
        FROM real_trade_orders
        WHERE state ? 'fifo_close_oid'
        ORDER BY setup_log_id
    """)).fetchall()

total = inconsistent = 0
for sid, st in rows:
    if isinstance(st, str):
        st = json.loads(st)
    total += 1
    sfp, cfp = st.get("stop_fill_price"), st.get("close_fill_price")
    if sfp is not None and cfp is not None and abs(float(sfp) - float(cfp)) > 0.001:
        inconsistent += 1
        print(f"lid {sid}: placed {str(st.get('ts_placed'))[:10]}  "
              f"stop_fp={sfp}  close_fp(FIFO)={cfp}  reason={st.get('close_reason')}")
print(f"\nFIFO-touched lids: {total}, with stale stop_fill_price: {inconsistent}")
