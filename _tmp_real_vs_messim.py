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

# the 12 PLACED V16-SB-confirmed signals, Jun 16-18, sim columns SEPARATED
cur.execute("""SELECT ts, setup_name, direction, basket_pct,
               outcome_pnl, mes_sim_outcome_pnl, mes_sim_outcome_result, outcome_result
               FROM setup_log
               WHERE setup_name=ANY(%s) AND ts::date BETWEEN '2026-06-16' AND '2026-06-18'
                 AND real_trade_skip_reason IS NULL
                 AND COALESCE(mes_sim_outcome_pnl,outcome_pnl) IS NOT NULL
               ORDER BY ts""",(list(WL),))
rows=[r for r in cur.fetchall() if confirm(r[3], r[2])]

print("12 PLACED V16-SB trades — chain-sim vs MES-sim (Jun 16-18)\n")
print(f"  {'time':16} {'setup':14} {'dir':7} {'chainP':>7} {'mesP':>7} {'mes_res':>8}")
chain=mes=0; mes_pop=0
for ts,nm,d,bp,op,mp,mr,orr in rows:
    op=float(op) if op is not None else 0
    has_mes = mp is not None
    mpv=float(mp) if has_mes else None
    chain+=op
    if has_mes: mes+=mpv; mes_pop+=1
    print(f"  {str(ts)[:16]:16} {nm:14} {str(d):7} {op:>+7.1f} "
          f"{('%+.1f'%mpv) if has_mes else '  null':>7} {str(mr) if mr else '-':>8}")
print(f"\n  signals={len(rows)}  mes_sim populated on {mes_pop}/{len(rows)}")
print(f"  CHAIN-sim total: {chain:+.1f} pts (${chain*5:+.0f})")
print(f"  MES-sim total  : {mes:+.1f} pts (${mes*5:+.0f})   [only on the {mes_pop} populated]")

# real broker truth for those days
cur.execute("SELECT day, net, trades FROM tsrt_daily_stmt WHERE day BETWEEN '2026-06-16' AND '2026-06-18' ORDER BY day")
rnet=0; rpts=0; nrt=0
for day,net,tr in cur.fetchall():
    rnet+=float(net)
    arr = tr if isinstance(tr,list) else (json.loads(tr) if tr else [])
    for t in arr: rpts+=float(t.get('pts',0)); nrt+=1
print(f"\n  REAL broker: {nrt} round-trips, {rpts:+.1f} pts gross (${rpts*5:+.0f}) | net after comm ${rnet:+.2f}")
print(f"\n  ==> chain-sim ${chain*5:+.0f}  vs  MES-sim ${mes*5:+.0f}  vs  REAL ${rpts*5:+.0f} gross")
if mes>0: print(f"  ==> REAL / MES-sim capture = {100*rpts/mes:.0f}%")
if chain>0: print(f"  ==> REAL / chain-sim capture = {100*rpts/chain:.0f}%")
c.close()
