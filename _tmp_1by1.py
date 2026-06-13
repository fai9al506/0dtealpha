import os, psycopg, json
from datetime import date
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
d = date(2026,6,11)
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()
cur.execute("SELECT setup_log_id, state FROM real_trade_orders")
broker={lid:(s if isinstance(s,dict) else json.loads(s)) for lid,s in cur.fetchall()}

cur.execute("""SELECT id,ts,setup_name,direction,grade,outcome_result,outcome_pnl,
   mes_sim_outcome_pnl,real_trade_skip_reason,live_pass
   FROM setup_log WHERE ts::date=%s AND live_pass=true ORDER BY ts""",(d,))
rows=cur.fetchall()
print(f"PORTAL V16 (live_pass=true): {len(rows)} trades\n")
print(f"{'#':>2} {'lid':>4} {'time':>5} {'setup':<16}{'dir':<7}{'gr':<3}| {'PORTAL':>13} | {'MESsim':>6} | {'BROKER':<40}")
print("-"*120)
pW=pL=0; psum=0.0; bsum=0.0; placed_n=0
for i,(lid,ts,name,dirn,grade,res,pnl,mpnl,skip,lp) in enumerate(rows,1):
    t=ts.astimezone(ET).strftime("%H:%M")
    pnl=float(pnl or 0); psum+=pnl
    if res=="WIN": pW+=1
    elif res=="LOSS": pL+=1
    pstr=f"{res or '?':<4} {pnl:+6.1f}pt"
    mstr=f"{float(mpnl):+5.1f}" if mpnl is not None else "  -  "
    if lid in broker:
        st=broker[lid]; placed_n+=1
        e=st.get("fill_price"); x=st.get("close_fill_price") or st.get("stop_fill_price")
        short=str(dirn).lower() in ("short","bearish")
        busd=((e-x) if short else (x-e))*5 if (e and x) else None
        if busd is not None: bsum+=busd
        bstr=f"PLACED in {e} out {x} = ${busd:+.0f} [{st.get('close_reason','')}]"
    else:
        bstr=f"NOT PLACED — blocked: {skip}"
    print(f"{i:>2} {lid:>4} {t:>5} {name:<16}{str(dirn):<7}{str(grade):<3}| {pstr:>13} | {mstr:>6} | {bstr:<40}")
print("-"*120)
print(f"PORTAL: {pW}W {pL}L  sum {psum:+.1f}pt (${psum*5:+.0f})   |  BROKER placed {placed_n}/19, sum ${bsum:+.0f}")

# any broker trade NOT in the 19?
v16ids={r[0] for r in rows}
extra=[lid for lid in broker if lid not in v16ids]
cur.execute("SELECT id,setup_name,direction,grade,outcome_result,live_pass,real_trade_skip_reason FROM setup_log WHERE id = ANY(%s)",(extra,))
print("\nBROKER-PLACED but NOT in portal's 19:")
for r in cur.fetchall():
    st=broker[r[0]]; e=st.get("fill_price"); x=st.get("close_fill_price") or st.get("stop_fill_price")
    short=str(r[2]).lower() in ("short","bearish"); busd=((e-x) if short else (x-e))*5 if (e and x) else None
    print(f"  lid {r[0]} {r[1]} {r[2]} {r[3]} broker ${busd:+.0f} | live_pass={r[5]} skip={r[6]} portal_outcome={r[4]}")
