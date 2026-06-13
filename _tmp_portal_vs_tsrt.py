import os, psycopg, json
from datetime import date
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
d = date(2026,6,11)
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()

# 1) PORTAL V16 view = live_pass=true, sum outcome_pnl (portal points)
cur.execute("""SELECT id,ts,setup_name,direction,grade,outcome_result,outcome_pnl,
   mes_sim_outcome_pnl, live_pass
   FROM setup_log WHERE ts::date=%s AND live_pass=true ORDER BY ts""",(d,))
v16 = cur.fetchall()
psum = sum(float(r[6] or 0) for r in v16)
msum = sum(float(r[7]) for r in v16 if r[7] is not None)
print(f"=== PORTAL V16 VIEW (live_pass=true): {len(v16)} trades, sum outcome_pnl = {psum:+.1f} pt (~${psum*5:+.0f} @1MES) ===")
print(f"    (MES-sim col sum where present: {msum:+.1f} pt)")

# which V16 trades were actually placed on broker?
cur.execute("SELECT setup_log_id FROM real_trade_orders")
placed_ids = set(r[0] for r in cur.fetchall())
print(f"\n  trade-by-trade (PLACED? = hit real broker):")
in_v16_not_placed = []
for r in v16:
    lid,ts,name,dirn,grade,res,pnl,msim,lp = r
    t=ts.astimezone(ET).strftime("%H:%M")
    placed = "PLACED " if lid in placed_ids else "  ---  "
    if lid not in placed_ids: in_v16_not_placed.append(r)
    print(f"   {placed} lid {lid} {t} {name:<16} {str(dirn):<7} {str(grade):<3} -> {res} {float(pnl or 0):+.1f}pt")

# 2) the breaker-blocked-but-V16-counted trades
print(f"\n=== V16 counts these but broker DID NOT place (breaker/live-state) ===")
blk_sum=0.0
for r in in_v16_not_placed:
    lid,ts,name,dirn,grade,res,pnl,msim,lp=r
    cur.execute("SELECT real_trade_skip_reason FROM setup_log WHERE id=%s",(lid,))
    sr=cur.fetchone()[0]
    blk_sum+=float(pnl or 0)
    print(f"   lid {lid} {ts.astimezone(ET).strftime('%H:%M')} {name:<16} {str(dirn):<7} {res} {float(pnl or 0):+.1f}pt  skip={sr}")
print(f"   --> these unplaced trades sum {blk_sum:+.1f}pt of the portal view that broker never took")
print(f"\n=== BROKER TRUTH (real_trade_orders, bot fills): -55.5pt gross / -$277.50 / NET -$289.50 ===")
