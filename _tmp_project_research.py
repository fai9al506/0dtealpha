import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True
cur=c.cursor()
def q(s,a=None):
    cur.execute(s,a or ()); return cur.fetchall()

print("########## 1. WHEN DID REAL MONEY ACTUALLY START? ##########")
for t in ('tsrt_daily_stmt','real_trade_orders'):
    try:
        if t=='tsrt_daily_stmt':
            r=q("SELECT MIN(day),MAX(day),COUNT(*) FROM tsrt_daily_stmt")[0]
        else:
            r=q("SELECT MIN(ts)::date,MAX(ts)::date,COUNT(*) FROM real_trade_orders")[0]
        print(f"  {t:22} first={r[0]} last={r[1]} rows={r[2]}")
    except Exception as e:
        print(f"  {t}: {e}")

print("\n########## 2. REAL-MONEY DAILY CURVE (tsrt_daily_stmt = broker truth) ##########")
rows=q("SELECT day, net, n_trades, n_wins FROM tsrt_daily_stmt ORDER BY day")
cum=0; peak=0; maxdd=0
print(f"  {'day':12} {'net$':>9} {'cum$':>10} {'dd$':>9}  trades  WR")
for day,net,nt,nw in rows:
    net=float(net); cum+=net; peak=max(peak,cum); dd=cum-peak; maxdd=min(maxdd,dd)
    wr = f"{100*nw/nt:.0f}%" if nt else "-"
    print(f"  {str(day):12} {net:>9.2f} {cum:>10.2f} {dd:>9.2f}   {nt:>4}  {wr:>4}")
print(f"  ===> real net total = {cum:+.2f}$  | maxDD = {maxdd:.2f}$  | days = {len(rows)}")

print("\n########## 3. SIGNAL-LAYER SIM CURVE BY MONTH (setup_log, whitelist) ##########")
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','SB Absorption')
rows=q("""
  SELECT to_char(ts,'YYYY-MM') ym,
         COUNT(*) n,
         SUM(CASE WHEN COALESCE(mes_sim_outcome_pnl,outcome_pnl)>0 THEN 1 ELSE 0 END) w,
         SUM(COALESCE(mes_sim_outcome_pnl,outcome_pnl)) netpts
  FROM setup_log
  WHERE setup_name=ANY(%s) AND COALESCE(mes_sim_outcome_pnl,outcome_pnl) IS NOT NULL
  GROUP BY 1 ORDER BY 1
""",(list(WL),))
print(f"  {'month':9} {'n':>5} {'WR':>5} {'sim_net_pts':>12} {'sim_$@5/pt':>11}")
for ym,n,w,net in rows:
    net=float(net)
    print(f"  {ym:9} {n:>5} {100*w/n:>4.0f}% {net:>12.1f} {net*5:>11.0f}")

print("\n########## 4. CAPTURE: REAL $ vs SIM $ in the REAL era (per month) ##########")
# real net by month from tsrt
rmonth={}
for day,net,nt,nw in q("SELECT day,net,n_trades,n_wins FROM tsrt_daily_stmt ORDER BY day"):
    ym=str(day)[:7]; rmonth[ym]=rmonth.get(ym,0)+float(net)
# sim net by month (whitelist) for same months
for ym in sorted(rmonth):
    sim=q("""SELECT SUM(COALESCE(mes_sim_outcome_pnl,outcome_pnl))
             FROM setup_log WHERE setup_name=ANY(%s)
             AND to_char(ts,'YYYY-MM')=%s
             AND COALESCE(mes_sim_outcome_pnl,outcome_pnl) IS NOT NULL""",(list(WL),ym))[0][0]
    sim_usd=float(sim)*5 if sim else 0
    real=rmonth[ym]
    ratio = f"{100*real/sim_usd:.0f}%" if sim_usd>0 else "n/a"
    print(f"  {ym}: real={real:+8.2f}$  sim(all whitelist)={sim_usd:+9.0f}$  real/sim={ratio}")
print("  (note: sim = ALL whitelist signals; real = what a 1-MES cap-1 acct actually placed. Ratio is capacity+capture combined, not pure capture.)")
c.close()
