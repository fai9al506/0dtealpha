import os, psycopg2, json
from datetime import datetime
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
today = datetime.now(ET).date()
cur.execute("""
  SELECT rto.setup_log_id, sl.setup_name, sl.direction, rto.state
  FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
  WHERE rto.created_at::date = %s ORDER BY rto.setup_log_id
""",(today,))
MES=5.0; total=0.0
print(f"{'lid':<5}{'setup':<12}{'dir':<6}{'fill':>9}{'exit':>9}{'qty':>4}{'$pnl':>9}  reason")
for lid,name,dir_,st in cur.fetchall():
    fill=st.get('fill_price'); close=st.get('close_fill_price') or st.get('stop_fill_price')
    qty=st.get('qty') or st.get('filled_qty') or 1
    reason=st.get('close_reason','')
    if fill and close:
        f=float(fill); x=float(close)
        pts = (x-f) if dir_=='long' else (f-x)
        d = pts*MES*float(qty)
        total+=d
        print(f"{lid:<5}{name[:11]:<12}{dir_[:5]:<6}{f:>9.2f}{x:>9.2f}{str(qty):>4}{d:>9.2f}  {reason}")
print(f"\nBROKER-FILL NET (bot's own view): ${total:.2f}")
# breaker / cap state
cur.execute("SELECT key, value FROM kv_store WHERE key ILIKE '%breaker%' OR key ILIKE '%cap%' OR key ILIKE '%lockout%'") if False else None
cur.close(); c.close()
