"""S210 diagnosis: which Jun 5 lids did fifo_reconcile actually rewrite, and
what do the exit-related fields look like per lid?"""
import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT sl.id, sl.setup_name, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
          AND rto.state->>'account_id' = '210VYX65'
        ORDER BY rto.state->>'ts_placed'
    """)).fetchall()

print(f"{'lid':<6}{'entry':>9}{'stop_fp':>9}{'close_fp':>10}{'pre_fifo':>10}{'fifo_oid':>12}  close_reason")
sum_eff = sum_fifo_close = 0.0
for lid, name, st in rows:
    if isinstance(st, str):
        st = json.loads(st)
    fp = st.get("fill_price")
    sfp = st.get("stop_fill_price")
    cfp = st.get("close_fill_price")
    pre = st.get("close_fill_price_pre_fifo_reconcile")
    foid = st.get("fifo_close_oid")
    eff = sfp or cfp
    if fp is not None and eff is not None:
        sum_eff += float(eff) - float(fp)
    if fp is not None and cfp is not None:
        sum_fifo_close += float(cfp) - float(fp)
    print(f"{lid:<6}{fp if fp else '':>9}{sfp if sfp else '-':>9}{cfp if cfp else '-':>10}"
          f"{pre if pre is not None else '-':>10}{foid if foid else '-':>12}  {st.get('close_reason','')}")

print(f"\nsum using effective exit (stop OR close) = {sum_eff:+.2f} pts")
print(f"sum using close_fill_price only          = {sum_fifo_close:+.2f} pts")
print("(broker truth for VYX65 Jun 5 longs: -76.5 pts gross per tsrt_daily_stmt trades)")
