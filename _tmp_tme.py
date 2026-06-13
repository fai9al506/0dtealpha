import os, psycopg, json
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
cur.execute("SELECT setup_log_id,state FROM real_trade_orders WHERE state->>'close_reason'='trail_market_exit'")
rows=cur.fetchall()
print(f"total trail_market_exit closes ever: {len(rows)}")
data=[]
for lid,st in rows:
    s=st if isinstance(st,dict) else json.loads(st)
    cur.execute("SELECT ts,setup_name,direction,outcome_pnl,outcome_result FROM setup_log WHERE id=%s",(lid,))
    r=cur.fetchone()
    if not r: continue
    ts,name,dirn,opnl,ores=r
    e=s.get("fill_price"); x=s.get("close_fill_price") or s.get("stop_fill_price")
    short=str(dirn).lower() in ("short","bearish")
    bpts=((e-x) if short else (x-e)) if (e and x) else None
    data.append((ts.astimezone(ET), lid, name, str(dirn), bpts, float(opnl or 0)))
data.sort()
print(f"date range: {data[0][0].date()} -> {data[-1][0].date()}\n")
print(f"{'date':>10} {'lid':>5} {'setup':<14}{'dir':<7}{'brokerPt':>8}{'portalPt':>9}{'gap(p-b)':>9}")
tb=tp=0.0
for dt,lid,name,dirn,bpts,opnl in data:
    if bpts is None: continue
    gap=opnl-bpts; tb+=bpts; tp+=opnl
    print(f"{str(dt.date()):>10} {lid:>5} {name:<14}{dirn:<7}{bpts:>+8.1f}{opnl:>+9.1f}{gap:>+9.1f}")
print(f"\n  broker total {tb:+.1f}pt (${tb*5:+.0f}) | portal total {tp:+.1f}pt (${tp*5:+.0f}) | GAP {tp-tb:+.1f}pt (${(tp-tb)*5:+.0f})")
