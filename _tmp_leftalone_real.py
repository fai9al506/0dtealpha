import os, sys, psycopg2
sys.path.insert(0,'app'); from mes_sim_backfill import mes_walk
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
def bars_from(tstr):
    cur.execute("""SELECT ts_start, ts_end, bar_open, bar_high, bar_low, bar_close FROM vps_es_range_bars
        WHERE trade_date='2026-06-30' AND symbol LIKE '%%ES%%' AND (ts_end AT TIME ZONE 'America/New_York')::time >= %s ORDER BY ts_end""",(tstr,))
    return [(r[0],r[1],float(r[2]),float(r[3]),float(r[4]),float(r[5])) for r in cur.fetchall()]
# (entry, start_time, sl, act, gap) — GEX Long v6 = SL14/act15/gap5 ; ES Abs = SL8/act8/gap3 (params at fire time)
trades=[('4552 GEX Long',7557.5,'14:23',14,15,5),('4555 GEX Long',7555.0,'14:38',14,15,5),('4553 ES Abs',7553.75,'14:33',8,8,3)]
print('LEFT-ALONE on the REAL ES path (long, 2 MES each):')
tot=0
for nm,ent,t,sl,act,gap in trades:
    b=bars_from(t)
    o=mes_walk(b, ent, True, sl, None, 0, act, gap, 360)
    d=o['pnl']*2*5
    print(f"  {nm}: entry {ent}  MFE +{o['mfe']:.1f} (act={act} -> trail {'FIRED' if o['mfe']>=act else 'NEVER activated'})  exit {o['exit_price']:.2f} reason={o['reason']}  = {o['pnl']:+.1f}pt = ${d:+.0f}")
    tot+=d
print(f"  >>> LEFT-ALONE TOTAL (real ES): ${tot:+.0f}")
c.close()
