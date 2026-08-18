import os, psycopg2
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True
cur = c.cursor()

# Jun 18 — all whitelist signals (placed + skipped) with basket + skip reason
cur.execute("""
  SELECT ts, setup_name, direction, basket_pct, real_trade_skip_reason,
         outcome_pnl, mes_sim_outcome_pnl, outcome_result, live_pass, grade
  FROM setup_log
  WHERE ts::date = '2026-06-18'
    AND setup_name IN ('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence',
                       'ES Absorption','SB Absorption','DD Exhaustion','GEX Long')
  ORDER BY ts
""")
print("Jun-18 whitelist signals (basket_pct, skip_reason, live_pass)")
print(f"  {'time':6} {'setup':14} {'dir':6} {'basket':>7} {'skip_reason':22} {'V16':4} {'chain':>6} {'res':>7}")
for ts,nm,d,bp,skip,op,mp,res,lp,g in cur.fetchall():
    t = ts.astimezone(ET)
    bps = f"{bp:.2f}" if bp is not None else "  null"
    op = f"{op:.1f}" if op is not None else "  -"
    print(f"  {t.strftime('%H:%M'):6} {nm[:14]:14} {(d or '')[:6]:6} {bps:>7} {(skip or '-')[:22]:22} {str(lp):4} {op:>6} {(res or '-'):>7}")
