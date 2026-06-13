import os, psycopg, json
from datetime import date
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()
for d in [date(2026,6,9), date(2026,6,10), date(2026,6,11)]:
    cur.execute("""SELECT s.id,s.ts,s.setup_name,s.direction,s.grade,r.state
      FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id=s.id
      WHERE s.ts::date=%s ORDER BY s.ts""",(d,))
    rows=cur.fetchall()
    g=0.0; lw=ls=0; sw=ss=0; lp=0.0; sp=0.0
    for lid,ts,name,dirn,grade,state in rows:
        st=state if isinstance(state,dict) else json.loads(state)
        e=st.get("fill_price"); x=st.get("close_fill_price")
        if x is None: x=st.get("stop_fill_price")
        if e is None or x is None: continue
        short=str(dirn).lower() in ("short","bearish")
        pts=(e-x) if short else (x-e)
        usd=pts*5*(st.get("quantity",1) or 1); g+=usd
        if short:
            sp+=usd; sw+=usd>0; ss+=usd<=0
        else:
            lp+=usd; lw+=usd>0; ls+=usd<=0
    # breaker blocks
    cur.execute("""SELECT count(*),min(ts) FROM setup_log WHERE ts::date=%s AND real_trade_skip_reason='daily_loss_limit'""",(d,))
    nb,mt=cur.fetchone()
    bt = mt.astimezone(ET).strftime('%H:%M') if mt else "—"
    print(f"{d}  GROSS ${g:+.0f}  | LONGS ${lp:+.0f} ({lw}W/{ls}L)  SHORTS ${sp:+.0f} ({sw}W/{ss}L)  | breaker: {nb} blocks, 1st@{bt}")
