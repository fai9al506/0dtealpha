"""Rule B detail: which trades does '3 consecutive same-direction losses -> block direction 120min' remove?"""
import os, json
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
from datetime import timedelta

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("""
    SELECT r.setup_log_id, r.state, l.setup_name, l.direction, l.ts, l.outcome_pnl, l.paradigm
    FROM real_trade_orders r JOIN setup_log l ON l.id = r.setup_log_id
    WHERE l.ts >= '2026-04-01' ORDER BY l.ts
""")
trades = []
for lid, state, name, d, ts, pnl, para in cur.fetchall():
    st = state if isinstance(state, dict) else json.loads(state or "{}")
    t = ts.astimezone(ET)
    sign = 1 if (d or "").lower() in ("long", "bullish", "buy") else -1
    fill = st.get("fill_price"); close_p = st.get("close_fill_price")
    qty = float(st.get("qty") or 1)
    if fill is not None and close_p is not None:
        usd = (float(close_p) - float(fill)) * sign * 5.0 * qty
    elif pnl is not None:
        usd = float(pnl) * 5.0
    else:
        continue
    trades.append(dict(lid=lid, name=name, dir="L" if sign > 0 else "S", t=t, d=t.date(), usd=usd, para=para))

by_day = defaultdict(list)
for x in trades:
    by_day[x["d"]].append(x)

LOSS = -20.0
removed = []
trigger_days = set()
for d, xs in sorted(by_day.items()):
    consec = defaultdict(int); block = {}
    for x in sorted(xs, key=lambda z: z["t"]):
        k = x["dir"]
        if k in block and x["t"] < block[k]:
            removed.append(x); continue
        if x["usd"] <= LOSS:
            consec[k] += 1
        else:
            consec[k] = 0
        if consec[k] >= 3:
            block[k] = x["t"] + timedelta(minutes=120)
            trigger_days.add(d)

print(f"trigger days: {sorted(trigger_days)}")
print(f"removed {len(removed)} trades, ${sum(x['usd'] for x in removed):+.2f}:")
per_day = defaultdict(float)
for x in removed:
    per_day[x["d"]] += x["usd"]
    print(f"  {x['d']} {x['t'].strftime('%H:%M')} {x['name']:15s} {x['dir']} {x['para']} ${x['usd']:+.2f}")
print("\nper-day removed P&L (negative = rule saved that much):")
for d, v in sorted(per_day.items()):
    print(f"  {d}: ${v:+.2f}")
c.close()
