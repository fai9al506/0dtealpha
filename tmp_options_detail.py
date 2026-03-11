"""Check options_trade_orders detail for Mar 10"""
import os, sys, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

r = c.execute(text("""
    SELECT oto.setup_log_id, oto.state
    FROM options_trade_orders oto
    JOIN setup_log sl ON sl.id = oto.setup_log_id
    WHERE sl.ts::date = '2026-03-10'
    ORDER BY sl.ts
""")).fetchall()

for row in r:
    lid = row[0]
    s = row[1] if isinstance(row[1], dict) else json.loads(row[1])
    # Show all keys
    print("#%s %s %s" % (lid, s.get("setup_name","?"), s.get("symbol","?")))
    for k in sorted(s.keys()):
        v = s[k]
        if v is not None and v != "" and v != 0:
            print("  %s: %s" % (k, v))
    print()

c.close()
