"""Decompose portal-vs-broker week gap (Jun 1-5):
portal sim of PLACED trades vs broker fills vs the unplaced remainder."""
import os, json
from collections import defaultdict
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
DAYS = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04", "2026-06-05"]

with eng.connect() as c:
    # placed trades: portal sim pnl + broker pts (bot-own fills)
    rows = c.execute(text("""
        SELECT (sl.ts AT TIME ZONE 'America/New_York')::date AS d,
               sl.id, sl.outcome_pnl, sl.direction, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = ANY(:days)
        ORDER BY sl.ts
    """), {"days": DAYS}).fetchall()

    day_sim = defaultdict(float)
    day_real = defaultdict(float)
    n_placed = defaultdict(int)
    for d, lid, ppnl, direction, st in rows:
        st = st or {}
        fill = st.get("fill_price")
        exit_p = (st.get("stop_fill_price_pre_fifo_reconcile") or st.get("stop_fill_price")
                  or st.get("close_fill_price_pre_fifo_reconcile") or st.get("close_fill_price"))
        if fill is None or exit_p is None:
            continue
        is_long = (direction or "").lower() in ("long", "bullish")
        real = (float(exit_p) - float(fill)) * (1 if is_long else -1)
        d = str(d)
        day_sim[d] += float(ppnl or 0)
        day_real[d] += real
        n_placed[d] += 1

    # signals that passed nothing-blocked-statically but were skipped by RISK STATE
    # (these appear in the portal V16 view but were never placed)
    rows = c.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
               real_trade_skip_reason, COUNT(*), SUM(COALESCE(outcome_pnl, 0))
        FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date = ANY(:days)
          AND real_trade_skip_reason IS NOT NULL
          AND real_trade_skip_reason NOT IN ('live_filter_block')
        GROUP BY 1, 2 ORDER BY 1, 4
    """), {"days": DAYS}).fetchall()

print(f"{'day':<12}{'placed':>7}{'sim_pts':>9}{'real_pts':>9}{'exec_gap':>9}")
ts = tr = 0.0
for d in DAYS:
    gap = day_real[d] - day_sim[d]
    print(f"{d:<12}{n_placed[d]:>7}{day_sim[d]:>+9.1f}{day_real[d]:>+9.1f}{gap:>+9.1f}")
    ts += day_sim[d]; tr += day_real[d]
print(f"{'WEEK':<12}{sum(n_placed.values()):>7}{ts:>+9.1f}{tr:>+9.1f}{tr-ts:>+9.1f}")
print(f"\nportal shows +92.8p over 67t -> unplaced portion = {92.8 - ts:+.1f}p over ~{67 - sum(n_placed.values())}t")

print("\n=== risk-state skips (in portal view, NOT placed) by day/reason ===")
for r in rows:
    print(f"  {r[0]}  {r[1]:<28} n={r[2]:<3} sim_pnl_sum={float(r[3] or 0):+.1f}p")
