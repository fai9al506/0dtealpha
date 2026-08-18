import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
cur=c.cursor()
DEAD=0.15
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','SB Absorption')
def confirm(bp,d):
    if bp is None: return True
    bp=float(bp); il=d in ('long','bullish')
    if abs(bp)<DEAD: return False
    return (bp>0)==il

print("V16-SB confirmed whitelist signals, Jun 16-18 — placement breakdown\n")
cur.execute("""SELECT ts, setup_name, direction, basket_pct, real_trade_skip_reason,
               COALESCE(mes_sim_outcome_pnl,outcome_pnl)
               FROM setup_log WHERE setup_name=ANY(%s) AND ts::date>='2026-06-16' AND ts::date<='2026-06-18'
               AND COALESCE(mes_sim_outcome_pnl,outcome_pnl) IS NOT NULL
               ORDER BY ts""",(list(WL),))
from collections import defaultdict
agg=defaultdict(lambda:[0,0.0])  # reason -> [count, simpts]
placed=[]; skipped=[]
for ts,nm,d,bp,reason,p in cur.fetchall():
    if not confirm(bp,d): continue
    p=float(p); key = reason if reason else "PLACED (no skip reason)"
    agg[key][0]+=1; agg[key][1]+=p
    (skipped if reason else placed).append((ts,nm,d,p,reason))

print(f"  {'reason':32} {'n':>3} {'sim_pts':>9} {'sim_$':>8}")
for k in sorted(agg, key=lambda x:-agg[x][0]):
    n,pts=agg[k]; print(f"  {k:32} {n:>3} {pts:>+9.1f} {pts*5:>+8.0f}")

print(f"\n  PLACED: {len(placed)} signals, {sum(p for _,_,_,p,_ in placed):+.1f} sim pts")
print(f"  SKIPPED: {len(skipped)} signals, {sum(p for _,_,_,p,_ in skipped):+.1f} sim pts")
print("\n  --- skipped detail ---")
for ts,nm,d,p,reason in skipped:
    print(f"   {str(ts)[:16]} {nm:14} {str(d):7} {p:>+6.1f}  <- {reason}")
c.close()
