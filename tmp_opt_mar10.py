import os, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Get all options trades where ts_placed is Mar 10
rows = c.execute(text("""
    SELECT setup_log_id,
      state->>'setup_name' as setup,
      state->>'direction' as dir,
      state->>'strike' as strike,
      state->>'symbol' as sym,
      (state->>'entry_price')::float as entry,
      (state->>'close_price')::float as close_p,
      state->>'ts_placed' as placed,
      state->>'status' as st
    FROM options_trade_orders
    WHERE (state->>'ts_placed')::text LIKE '2026-03-10%'
    ORDER BY setup_log_id
""")).fetchall()

print(f"\nMAR 10 OPTIONS TRADES: {len(rows)}", flush=True)
print(f"{'ID':>5} {'Setup':<18} {'Dir':<8} {'Strike':>6} {'Symbol':<25} {'Entry':>7} {'Close':>7} {'P&L':>8} {'Status'}", flush=True)
print("-" * 105, flush=True)

total = 0
wins = 0
for r in rows:
    entry = r[5] if r[5] else 0
    close = r[6] if r[6] else 0
    pnl = (close - entry) * 100 if entry and close else 0
    total += pnl
    if pnl > 0:
        wins += 1
    print(f"#{r[0]:>4} {r[1]:<18} {r[2]:<8} {r[3] or '?':>6} {r[4] or '?':<25} ${entry:>6.2f} ${close:>6.2f} ${pnl:>+7.0f} {r[8]}", flush=True)

losses = len(rows) - wins
print(f"\nTOTAL P&L: ${total:+,.0f}", flush=True)
print(f"Trades: {len(rows)}, Wins: {wins}, Losses: {losses}, WR: {wins/len(rows)*100:.0f}%" if rows else "", flush=True)

c.close()
