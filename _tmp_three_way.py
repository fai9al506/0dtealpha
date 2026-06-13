import os, psycopg, json
from datetime import date
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
d = date(2026,6,11)
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()
cur.execute("SELECT setup_log_id, state FROM real_trade_orders")
broker={}
for lid,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    broker[lid]=st
cur.execute("""SELECT id,ts,setup_name,direction,outcome_pnl,mes_sim_outcome_pnl
   FROM setup_log WHERE ts::date=%s ORDER BY ts""",(d,))
print(f"{'lid':>5} {'time':>5} {'setup':<14} {'dir':<7} {'chain':>7} {'mes-sim':>8} {'broker$':>8}")
cs=ms=bk=0.0
for lid,ts,name,dirn,opnl,mpnl in cur.fetchall():
    if lid not in broker: continue
    st=broker[lid]
    e=st.get("fill_price"); x=st.get("close_fill_price") or st.get("stop_fill_price")
    short=str(dirn).lower() in ("short","bearish")
    busd = ((e-x) if short else (x-e))*5 if (e and x) else None
    t=ts.astimezone(ET).strftime("%H:%M")
    cs+=float(opnl or 0); ms+=float(mpnl or 0); bk+=(busd or 0)
    print(f"{lid:>5} {t:>5} {name:<14} {str(dirn):<7} {float(opnl or 0):>+7.1f} {float(mpnl or 0) if mpnl is not None else 0:>+8.1f} {busd if busd is not None else 0:>+8.0f}")
print(f"{'':>5} {'':>5} {'TOTAL (placed)':<14} {'':<7} {cs:>+7.1f} {ms:>+8.1f} {bk:>+8.0f}")
print(f"\n  chain-sim pts ${cs*5:+.0f} | mes-sim pts ${ms*5:+.0f} | broker ${bk:+.0f}")
