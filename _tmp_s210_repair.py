"""S210 one-off repair: for lids where FIFO rewrote close_fill_price but
stop_fill_price kept the stale bot value — sync stop to FIFO truth, preserving
the bot value in stop_fill_price_pre_fifo_reconcile. Idempotent."""
import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT setup_log_id, state FROM real_trade_orders
        WHERE state ? 'fifo_close_oid' ORDER BY setup_log_id
    """)).fetchall()

fixed = 0
for sid, st in rows:
    if isinstance(st, str):
        st = json.loads(st)
    sfp, cfp = st.get("stop_fill_price"), st.get("close_fill_price")
    if sfp is None or cfp is None or abs(float(sfp) - float(cfp)) <= 0.001:
        continue
    if "stop_fill_price_pre_fifo_reconcile" not in st:
        st["stop_fill_price_pre_fifo_reconcile"] = sfp
    st["stop_fill_price"] = cfp
    with eng.begin() as cx:
        cx.execute(text(
            "UPDATE real_trade_orders SET state = :st, updated_at = NOW() "
            "WHERE setup_log_id = :sid"
        ), {"st": json.dumps(st), "sid": sid})
    fixed += 1
    print(f"repaired lid {sid}: stop {sfp} -> {cfp} (pre saved)")

print(f"\nrepaired {fixed} lids")
