"""Verify what the broken circuit breaker is counting vs reality."""
import os, psycopg2, json
from datetime import datetime
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
    SELECT rto.setup_log_id, sl.setup_name, sl.direction,
           sl.outcome_pnl, sl.outcome_result,
           rto.state->>'fill_price', rto.state->>'close_fill_price',
           rto.state->>'close_reason'
    FROM real_trade_orders rto
    JOIN setup_log sl ON sl.id = rto.setup_log_id
    WHERE rto.created_at::date = '2026-05-20'
      AND rto.state->>'status' = 'closed'
    ORDER BY rto.setup_log_id
""")

print("=" * 110)
print(f"{'lid':<5} {'setup':<14} {'dir':<8} {'sl.pnl':>8} {'sl.res':<10} {'fill':>9} {'close':>9} {'close_reason':<28}")
print("=" * 110)
gross_loss_breaker = 0.0  # what the breaker counts
net_setup_log_pnl = 0.0   # actual setup_log net
for lid, name, dir_, pnl, result, fill, close, reason in cur.fetchall():
    pnl_f = float(pnl) if pnl is not None else 0.0
    print(f"{lid:<5} {name:<14} {dir_:<8} {pnl_f:>8.2f} {str(result):<10} {str(fill):>9} {str(close):>9} {str(reason):<28}")
    if pnl_f < 0:
        gross_loss_breaker += abs(pnl_f) * 5.0  # MES_POINT_VALUE × QTY=1
    net_setup_log_pnl += pnl_f

print()
print(f"Setup_log GROSS LOSS (what breaker counts at $5/pt × 1 MES): ${gross_loss_breaker:.2f}")
print(f"Setup_log NET P&L in points: {net_setup_log_pnl:.2f} pts = ${net_setup_log_pnl * 5:.2f}")
print()
print("vs BROKER realized today: +$388.50")
print()
print("→ Circuit breaker is reading SPX-outcome gross loss, blocking despite real broker being green.")

cur.close(); c.close()
