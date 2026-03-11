"""Check SIM auto-trader open positions and recent activity"""
import os, sys, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Check auto_trade_orders table
print("-- AUTO_TRADE_ORDERS (SIM) --")
r = c.execute(text("""
    SELECT setup_log_id, state
    FROM auto_trade_orders
    ORDER BY setup_log_id DESC
    LIMIT 10
""")).fetchall()
for row in r:
    s = row[1] if isinstance(row[1], dict) else json.loads(row[1])
    status = s.get('status', '?')
    setup = s.get('setup_name', '?')
    direction = s.get('direction', '?')
    entry = s.get('entry_price', '?')
    print("  #%s %s %s %s status=%s" % (row[0], setup, direction, entry, status))
    if status in ('open', 'partial', 'active'):
        print("    FULL STATE:", json.dumps(s, indent=2, default=str))

# Check _setup_open_trades via the internal state
# These are tracked in memory, but we can check setup_log for unresolved trades
print("\n-- UNRESOLVED SETUP_LOG (today, no outcome) --")
r2 = c.execute(text("""
    SELECT id, to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t,
           setup_name, direction, grade, spot, outcome_result
    FROM setup_log
    WHERE ts::date = '2026-03-10' AND grade != 'LOG' AND outcome_result IS NULL
    ORDER BY ts
""")).fetchall()
for row in r2:
    print("  #%s %s %s %s [%s] spot=%.1f  result=%s" % (
        row[0], row[1], row[2], row[3], row[4], row[5], row[6] or 'STILL OPEN'))

# Check options_trade_orders too
print("\n-- OPTIONS_TRADE_ORDERS --")
try:
    r3 = c.execute(text("""
        SELECT setup_log_id, state
        FROM options_trade_orders
        ORDER BY setup_log_id DESC
        LIMIT 5
    """)).fetchall()
    for row in r3:
        s = row[1] if isinstance(row[1], dict) else json.loads(row[1])
        status = s.get('status', '?')
        print("  #%s status=%s setup=%s" % (row[0], status, s.get('setup_name', '?')))
        if status in ('open', 'partial', 'active', 'filled'):
            print("    FULL:", json.dumps(s, indent=2, default=str))
except Exception as ex:
    print("  (table error: %s)" % ex)

c.close()
