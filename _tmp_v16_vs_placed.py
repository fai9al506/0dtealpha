import os, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo
ET=ZoneInfo('America/New_York')
c=psycopg2.connect(os.environ['DATABASE_URL']); cur=c.cursor()
today=datetime.now(ET).date()
# all setup_log today
cur.execute("""
 SELECT sl.id, sl.setup_name, sl.direction, sl.grade, sl.ts, sl.outcome_pnl, sl.outcome_result,
        sl.real_trade_skip_reason, sl.mes_sim_outcome_pnl,
        (rto.setup_log_id IS NOT NULL) AS placed
 FROM setup_log sl
 LEFT JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
 WHERE sl.ts::date=%s
 ORDER BY sl.ts
""",(today,))
rows=cur.fetchall()
print(f"ALL setup_log today: {len(rows)} rows\n")
print(f"{'id':<5}{'setup':<13}{'dir':<6}{'gr':<4}{'time':<8}{'pnl':>7}{'mes':>7}{'res':<8}{'plcd':<5} skip_reason")
placed_pnl=0; placed_n=0
for id_,name,dir_,gr,ts,pnl,res,skip,mes,placed in rows:
    t=ts.astimezone(ET).strftime('%H:%M')
    pf=float(pnl) if pnl is not None else 0
    mf=float(mes) if mes is not None else None
    print(f"{id_:<5}{name[:12]:<13}{dir_[:5]:<6}{str(gr):<4}{t:<8}{pf:>7.1f}{(mf if mf is not None else 0):>7.1f}{str(res)[:7]:<8}{str(placed):<5} {skip or ''}")
    if placed: placed_pnl+=pf; placed_n+=1
print(f"\nPLACED: {placed_n} trades, sum SPX outcome_pnl = {placed_pnl:.1f} pts")
cur.close(); c.close()
