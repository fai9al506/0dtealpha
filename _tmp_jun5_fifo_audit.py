"""Jun 5: per-lid pre-FIFO vs post-FIFO close fills — verify attribution vs money."""
import os, json
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT sl.id, (sl.ts AT TIME ZONE 'America/New_York') AS et,
               sl.setup_name, sl.direction, sl.outcome_pnl, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
        ORDER BY sl.ts
    """)).fetchall()

tot_post = tot_pre = 0.0
n_rewritten = 0
print(f"{'lid':<6}{'setup':<15}{'dir':<5}{'entry':>8}{'exit_now':>9}{'exit_pre':>9}{'pts_now':>8}{'pts_pre':>8}{'portal':>8}")
for lid, et, name, direction, ppnl, st in rows:
    st = st or {}
    fill = st.get("fill_price")
    exit_post = st.get("stop_fill_price") or st.get("close_fill_price")
    if fill is None or exit_post is None:
        continue
    exit_pre = st.get("close_fill_price_pre_fifo_reconcile")
    # stop fills aren't FIFO-rewritten; pre only exists when reconcile touched the lid
    eff_pre = exit_pre if exit_pre is not None else exit_post
    is_long = (direction or "").lower() in ("long", "bullish")
    sgn = 1 if is_long else -1
    pts_post = sgn * (float(exit_post) - float(fill))
    pts_pre = sgn * (float(eff_pre) - float(fill))
    tot_post += pts_post
    tot_pre += pts_pre
    mark = "  <-- FIFO-rewritten" if exit_pre is not None and float(exit_pre) != float(exit_post) else ""
    if exit_pre is not None and float(exit_pre) != float(exit_post):
        n_rewritten += 1
    print(f"{lid:<6}{name[:14]:<15}{direction[:4]:<5}{float(fill):>8.2f}{float(exit_post):>9.2f}"
          f"{float(eff_pre):>9.2f}{pts_post:>+8.2f}{pts_pre:>+8.2f}{float(ppnl or 0):>+8.1f}{mark}")

print(f"\nFIFO-rewritten lids: {n_rewritten}")
print(f"DAY TOTAL post-FIFO (attribution): {tot_post:+.2f} pts = ${tot_post*5:+.0f}")
print(f"DAY TOTAL pre-FIFO  (bot's own fills): {tot_pre:+.2f} pts = ${tot_pre*5:+.0f}")
