import os, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
today = datetime.now(ET).date()
print("Checking ET date:", today)
cur.execute("""
  SELECT rto.setup_log_id, sl.setup_name, sl.direction, sl.grade,
         sl.ts, sl.outcome_pnl, sl.outcome_result,
         rto.state->>'fill_price', rto.state->>'close_fill_price',
         rto.state->>'stop_fill_price', rto.state->>'close_reason', rto.state->>'status'
  FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
  WHERE rto.created_at::date = %s
  ORDER BY rto.setup_log_id
""", (today,))
rows = cur.fetchall()
print(f"\n{len(rows)} TSRT orders today\n")
print(f"{'lid':<5}{'setup':<13}{'dir':<7}{'gr':<4}{'time':<9}{'pnl':>8}{'res':<9}{'fill':>9}{'close':>9}{'stop':>9} {'reason':<22}{'status'}")
for lid,name,dir_,gr,ts,pnl,res,fill,close,stop,reason,status in rows:
    t = ts.astimezone(ET).strftime('%H:%M:%S') if ts else ''
    pf = float(pnl) if pnl is not None else 0
    print(f"{lid:<5}{name[:12]:<13}{dir_[:6]:<7}{str(gr):<4}{t:<9}{pf:>8.1f}{str(res)[:8]:<9}{str(fill):>9}{str(close):>9}{str(stop):>9} {str(reason)[:21]:<22}{status}")
cur.close(); c.close()
