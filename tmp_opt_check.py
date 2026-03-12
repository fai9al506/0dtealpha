import os, sys
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

rows = c.execute(text("SELECT setup_log_id, state->>'setup_name', state->>'direction', state->>'status', state->>'entry_price', state->>'option_symbol', state->>'realized_pnl', state->>'created_at', state->>'close_reason' FROM options_trade_orders WHERE setup_log_id IN (SELECT id FROM setup_log WHERE created_at >= '2026-03-11' AND created_at < '2026-03-12') ORDER BY setup_log_id")).fetchall()
print(f"OPT MAR 11: {len(rows)}", flush=True)
for r in rows:
    print(f"  #{r[0]} {r[1]} {r[2]} | {r[3]} | entry={r[4]} | sym={r[5]} | pnl={r[6]} | {r[7]} | {r[8]}", flush=True)

if not rows:
    print("No options trades for Mar 11", flush=True)

rows2 = c.execute(text("SELECT setup_log_id, state->>'setup_name', state->>'status', state->>'realized_pnl', state->>'created_at' FROM options_trade_orders ORDER BY setup_log_id DESC LIMIT 10")).fetchall()
print("RECENT 10:", flush=True)
for r in rows2:
    print(f"  #{r[0]} {r[1]} {r[2]} pnl={r[3]} {r[4]}", flush=True)

rows3 = c.execute(text("SELECT id, created_at, direction, spot, outcome, outcome_pts, greek_alignment FROM setup_log WHERE created_at >= '2026-03-11' AND created_at < '2026-03-12' AND setup_name='Skew Charm' ORDER BY created_at")).fetchall()
print(f"SKEW CHARM MAR 11: {len(rows3)}", flush=True)
for r in rows3:
    print(f"  #{r[0]} {r[1]} {r[2]} spot={float(r[3]):.1f} {r[4]} {r[5]} align={r[6]}", flush=True)

c.close()
