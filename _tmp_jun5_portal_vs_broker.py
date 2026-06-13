"""Jun 5 per-lid: portal chain-sim P&L vs broker MES fills — locate the gap."""
import os
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT sl.id, (sl.ts AT TIME ZONE 'America/New_York') AS et,
               sl.setup_name, sl.direction, sl.outcome_pnl, sl.outcome_result,
               rto.state
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (sl.ts AT TIME ZONE 'America/New_York')::date = '2026-06-05'
        ORDER BY sl.ts
    """)).fetchall()

print(f"{'lid':<6}{'time':<7}{'setup':<15}{'dir':<5}{'portal':>8}{'broker':>8}{'gap':>7}  reason")
tot_p = tot_b = 0.0
n_b = 0
for lid, et, name, direction, ppnl, pres, st in rows:
    st = st or {}
    fill = st.get("fill_price")
    exit_p = st.get("stop_fill_price") or st.get("close_fill_price")
    portal = float(ppnl) if ppnl is not None else None
    if fill is None or exit_p is None:
        print(f"{lid:<6}{et.strftime('%H:%M'):<7}{name[:14]:<15}{direction[:4]:<5}"
              f"{portal if portal is not None else '?':>8}{'NO FILL':>8}{'':>7}  "
              f"(not counted in broker total)")
        continue
    is_long = (direction or "").lower() in ("long", "bullish")
    broker = ((float(exit_p) - float(fill)) if is_long else (float(fill) - float(exit_p)))
    gap = broker - (portal or 0)
    flag = " <-- BIG" if abs(gap) >= 3 else ""
    print(f"{lid:<6}{et.strftime('%H:%M'):<7}{name[:14]:<15}{direction[:4]:<5}"
          f"{portal:>+8.1f}{broker:>+8.2f}{gap:>+7.2f}  {st.get('close_reason','')}{flag}")
    tot_p += (portal or 0)
    tot_b += broker
    n_b += 1

print(f"\nfilled trades: {n_b}")
print(f"portal total (same {n_b} lids): {tot_p:+.1f} pts = ${tot_p*5:+.0f}")
print(f"broker total:                  {tot_b:+.2f} pts = ${tot_b*5:+.0f}")
print(f"gap: {tot_b-tot_p:+.2f} pts = ${(tot_b-tot_p)*5:+.0f}")
