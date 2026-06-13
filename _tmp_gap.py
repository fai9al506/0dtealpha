import os, psycopg, json
from zoneinfo import ZoneInfo
ET=ZoneInfo("America/New_York")
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
for label,reason in [("trail_market_exit (early-MES class)", ("outcome_close_trail_market_exit","trail_market_exit")),
                     ("spx_trail_exit (healthy S131 path)", ("outcome_close_spx_trail_exit",))]:
    cur.execute("SELECT setup_log_id,state FROM real_trade_orders WHERE state->>'close_reason'=ANY(%s)",(list(reason),))
    data=[]
    for lid,st in cur.fetchall():
        s=st if isinstance(st,dict) else json.loads(st)
        cur.execute("SELECT ts,setup_name,direction,outcome_pnl,mes_sim_outcome_pnl FROM setup_log WHERE id=%s",(lid,))
        r=cur.fetchone()
        if not r: continue
        ts,name,dirn,opnl,mpnl=r
        e=s.get("fill_price"); x=s.get("close_fill_price") or s.get("stop_fill_price")
        short=str(dirn).lower() in ("short","bearish")
        bpts=((e-x) if short else (x-e)) if (e and x) else None
        if bpts is None: continue
        data.append((ts.astimezone(ET),lid,name,bpts,float(opnl or 0),float(mpnl) if mpnl is not None else None))
    data.sort()
    print(f"\n=== {label}: n={len(data)}  {data[0][0].date()} -> {data[-1][0].date()} ===")
    tb=tp=tm=0.0; mn=0
    for dt,lid,name,bpts,opnl,mpnl in data:
        tb+=bpts; tp+=opnl
        ms = f"{mpnl:+.1f}" if mpnl is not None else "  - "
        if mpnl is not None: tm+=mpnl; mn+=1
        print(f"  {dt.date()} lid{lid:>5} {name:<13} broker{bpts:>+6.1f}  portal{opnl:>+6.1f}  mes-sim {ms:>6}  gap(p-b){opnl-bpts:>+6.1f}")
    print(f"  TOTALS: broker {tb:+.1f}p (${tb*5:+.0f}) | portal {tp:+.1f}p (${tp*5:+.0f}) | gap {tp-tb:+.1f}p (${(tp-tb)*5:+.0f})")
    if mn: print(f"          mes-sim (n={mn}) {tm:+.1f}p (${tm*5:+.0f}) — the realistic 'should-have-got'")
