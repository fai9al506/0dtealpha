import os, psycopg2, json
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
cur=c.cursor()
DEAD=0.15
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','SB Absorption')
def confirm(bp,d):
    if bp is None: return True
    bp=float(bp); il=d in ('long','bullish')
    if abs(bp)<DEAD: return False
    return (bp>0)==il

S,E='2026-06-16','2026-06-19'

print("="*70)
print("A) V16-SB SIM trades (confirmed whitelist signals), Jun 16-18")
print("="*70)
cur.execute("""SELECT ts, setup_name, direction, grade, basket_pct,
               COALESCE(mes_sim_outcome_pnl,outcome_pnl)
               FROM setup_log WHERE setup_name=ANY(%s) AND ts>=%s AND ts<%s
               AND COALESCE(mes_sim_outcome_pnl,outcome_pnl) IS NOT NULL
               ORDER BY ts""",(list(WL),S,E))
sim=cur.fetchall(); tot=0
print(f"  {'time':16} {'setup':14} {'dir':6} {'gr':3} {'bp':>6} {'simP':>7}")
for ts,nm,d,g,bp,p in sim:
    if not confirm(bp,d): continue
    p=float(p); tot+=p
    print(f"  {str(ts)[:16]:16} {nm:14} {str(d):6} {str(g):3} {('%.2f'%bp) if bp is not None else '  - ':>6} {p:>+7.1f}")
print(f"  ---> {sum(1 for ts,nm,d,g,bp,p in sim if confirm(bp,d))} confirmed sim trades, net {tot:+.1f} pts = ${tot*5:+.0f}")

print("\n"+"="*70)
print("B) ACTUAL TSRT real broker round-trips, Jun 16-18 (tsrt_daily_stmt.trades)")
print("="*70)
cur.execute("SELECT day, trades FROM tsrt_daily_stmt WHERE day>=%s AND day<%s ORDER BY day",(S,E))
rtot=0; n=0
print(f"  {'entry_et':16} {'acct':10} {'dir':6} {'entry':>8} {'exit':>8} {'pts':>6} {'usd':>8}")
for day,tr in cur.fetchall():
    arr = tr if isinstance(tr,list) else json.loads(tr)
    for t in arr:
        n+=1; pts=float(t.get('pts',0)); usd=float(t.get('usd_gross',0)); rtot+=usd
        print(f"  {t.get('entry_et',''):16} {t.get('account',''):10} {t.get('dir',''):6} "
              f"{t.get('entry',0):>8} {t.get('exit',0):>8} {pts:>+6.2f} {usd:>+8.2f}")
print(f"  ---> {n} real round-trips, gross ${rtot:+.2f}  (net after ~$1/RT comm: ${rtot-n:+.2f})")

print("\n"+"="*70)
print("WHY 11%:")
print(f"  SIM says: take ALL confirmed signals -> {sum(1 for ts,nm,d,g,bp,p in sim if confirm(bp,d))} trades, ${tot*5:+.0f}")
print(f"  REAL did: 1-MES cap-1 account placed -> {n} round-trips, ${rtot:+.2f} gross")
print(f"  Gap = (signals the small account could NOT take) + (fills/comm on the ones it did)")
c.close()
