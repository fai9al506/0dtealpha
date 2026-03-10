"""Paradigm shifts and options analysis for March 6."""
import psycopg2, os, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Paradigm shifts
print("=== PARADIGM SHIFTS (March 6) ===", flush=True)
cur.execute("""
SELECT ts AT TIME ZONE 'EST' as ts_et,
       statistics->>'paradigm' as paradigm,
       statistics->>'lis' as lis,
       statistics->>'aggregatedCharm' as charm
FROM volland_snapshots
WHERE ts::date = '2026-03-06' AND statistics IS NOT NULL
  AND statistics->>'paradigm' IS NOT NULL
ORDER BY ts
""")
rows = cur.fetchall()
prev_paradigm = None
for r in rows:
    if r[1] != prev_paradigm:
        print(f"  {str(r[0])[:16]}  {r[1]:20}  LIS: {r[2]:>8}  Charm: {r[3]}", flush=True)
        prev_paradigm = r[1]
print(f"Total snapshots with paradigm: {len(rows)}", flush=True)

# Options trade P&L reconstruction
print("\n=== OPTIONS TRADE RECONSTRUCTION (March 6) ===", flush=True)
# Get all options for March 6
cur.execute("""
SELECT setup_log_id, state
FROM options_trade_orders
WHERE setup_log_id >= 540 AND setup_log_id <= 590
ORDER BY setup_log_id
""")
rows = cur.fetchall()
total_options_pnl = 0
for r in rows:
    s = r[1]
    entry = float(s.get('entry_price', 0) or 0)
    exit_p = float(s.get('exit_price', 0) or 0)
    pnl = float(s.get('pnl_dollars', 0) or 0)
    symbol = s.get('symbol', '?')
    direction = s.get('direction', '?')
    status = s.get('status', '?')
    setup_id = r[0]

    # Get corresponding setup_log outcome
    cur.execute("SELECT outcome_result, outcome_pnl, spot FROM setup_log WHERE id = %s", (setup_id,))
    sl = cur.fetchone()
    outcome = sl[0] if sl else '?'
    sl_pnl = float(sl[1]) if sl and sl[1] else 0
    spot = float(sl[2]) if sl and sl[2] else 0

    print(f"  #{setup_id} {direction:5} {symbol:22} entry=${entry:.2f}  exit=${exit_p:.2f}  pnl=${pnl:.2f}  portal={outcome}({sl_pnl:+.1f}pts)  spot={spot:.0f}", flush=True)
    total_options_pnl += pnl

print(f"\nDB total options PnL: ${total_options_pnl:.2f}", flush=True)

# What was ES doing at key times?
print("\n=== KEY PRICE POINTS (March 6 from setup_log) ===", flush=True)
cur.execute("""
SELECT ts AT TIME ZONE 'EST' as ts_et, round(spot::numeric, 0) as spot, setup_name, direction, outcome_result
FROM setup_log
WHERE ts::date = '2026-03-06'
ORDER BY ts
""")
for r in cur.fetchall():
    print(f"  {str(r[0])[:16]} SPX={r[1]} {r[2]:16} {r[3]:8} {r[4] or ''}", flush=True)

conn.close()
