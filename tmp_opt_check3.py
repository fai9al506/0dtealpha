import os, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# All 16 options trades with entry/close/pnl
rows = c.execute(text("""
    SELECT setup_log_id,
      state->>'setup_name' as setup,
      state->>'direction' as dir,
      state->>'symbol' as sym,
      state->>'strike' as strike,
      (state->>'entry_price')::float as entry,
      (state->>'close_price')::float as close_price,
      state->>'status' as status,
      state->>'ts_placed' as placed
    FROM options_trade_orders
    WHERE setup_log_id IN (
        SELECT id FROM setup_log
        WHERE id >= 672 AND id <= 720
    )
    ORDER BY setup_log_id
""")).fetchall()

print(f"OPTIONS TRADES MAR 11: {len(rows)}", flush=True)
print(f"{'ID':>5} {'Setup':<20} {'Dir':<8} {'Strike':>6} {'Entry':>7} {'Close':>7} {'P&L':>8} {'Status':<8}", flush=True)
print("-" * 80, flush=True)
total_pnl = 0
for r in rows:
    entry = r[5] or 0
    close = r[6] or 0
    pnl = (close - entry) * 100  # SPX options = $100 multiplier
    total_pnl += pnl
    print(f"#{r[0]:>4} {r[1]:<20} {r[2]:<8} {r[4]:>6} ${entry:>6.2f} ${close:>6.2f} ${pnl:>+7.0f} {r[7]:<8}", flush=True)

print(f"\nTOTAL P&L: ${total_pnl:+.0f} ({len(rows)} trades)", flush=True)
wins = sum(1 for r in rows if (r[6] or 0) > (r[5] or 0))
losses = len(rows) - wins
print(f"WINS: {wins}, LOSSES: {losses}, WR: {wins/len(rows)*100:.0f}%", flush=True)

c.close()
