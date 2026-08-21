# -*- coding: utf-8 -*-
"""Size Problem 2 (execution gap) across the whole broker-truth era.
Per day: broker actual pts vs portal chain-sim & MES-sim of the SAME placed trades.
Gap = broker - sim. Express in $ (1 MES = $5/pt) + commission."""
import os, json, psycopg2
from collections import defaultdict
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

# broker truth per day
cur.execute("SELECT day, gross, comm, net, trades FROM tsrt_daily_stmt ORDER BY day")
brk={}
for day,g,c,n,tr in cur.fetchall():
    js=tr if isinstance(tr,list) else json.loads(tr or '[]')
    brk[str(day)]={'gross':float(g or 0),'comm':float(c or 0),'net':float(n or 0),
                   'pts':sum(t.get('pts',0) for t in js),'rt':len(js)}

# portal sim (chain + mes) of PLACED trades, per day
cur.execute("""
  SELECT date(sl.ts AT TIME ZONE 'America/New_York') d,
         ROUND(SUM(sl.outcome_pnl)::numeric,1) chain,
         ROUND(SUM(sl.mes_sim_outcome_pnl)::numeric,1) mes,
         COUNT(DISTINCT sl.id) n
  FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
  WHERE sl.outcome_result IS NOT NULL
    AND date(sl.ts AT TIME ZONE 'America/New_York') >= '2026-05-15'
  GROUP BY d""")
sim={str(d):{'chain':float(c or 0),'mes':float(m) if m is not None else None,'n':n} for d,c,m,n in cur.fetchall()}

days=sorted(brk)
print(f"{'date':<12}{'plc_n':>6}{'rt':>4}{'chainSIM':>9}{'mesSIM':>8}{'brkPTS':>8}{'gap_chain':>10}{'gap_mes':>9}{'brkNET$':>9}")
tot=defaultdict(float); mes_days=0
for d in days:
    s=sim.get(d,{}); b=brk[d]
    ch=s.get('chain',0); me=s.get('mes'); bp=b['pts']
    gc=bp-ch; gm=(bp-me) if me is not None else None
    print(f"{d:<12}{s.get('n',0):>6}{b['rt']:>4}{ch:>9.1f}{(me if me is not None else 0):>8.1f}{bp:>8.1f}{gc:>10.1f}{(gm if gm is not None else 0):>9.1f}{b['net']:>9.1f}")
    tot['chain']+=ch; tot['brk_pts']+=bp; tot['brk_net']+=b['net']; tot['comm']+=b['comm']; tot['gross']+=b['gross']
    if me is not None: tot['mes']+=me; tot['gap_mes']+=gm; mes_days+=1
    tot['gap_chain']+=gc

print("-"*86)
print(f"{'TOTAL':<12}{'':>6}{'':>4}{tot['chain']:>9.1f}{tot['mes']:>8.1f}{tot['brk_pts']:>8.1f}{tot['gap_chain']:>10.1f}{tot['gap_mes']:>9.1f}{tot['brk_net']:>9.1f}")

D=5.0  # $/pt per MES
print(f"""
=== EXECUTION GAP SIZING (broker-truth era {days[0]} -> {days[-1]}) ===
 Portal chain-sim (placed)  : {tot['chain']:+.1f} pts  = ${tot['chain']*D:+,.0f}
 Portal MES-sim   (placed)  : {tot['mes']:+.1f} pts  = ${tot['mes']*D:+,.0f}   (mes-sim covers {mes_days}/{len(days)} days)
 Broker GROSS               : {tot['brk_pts']:+.1f} pts  = ${tot['gross']:+,.0f}
 Commission                 :                 ${-tot['comm']:+,.0f}
 Broker NET                 :                 ${tot['brk_net']:+,.0f}

 GAP chain->broker (pts)    : {tot['gap_chain']:+.1f} pts  = ${tot['gap_chain']*D:+,.0f}   <- total execution drag vs optimistic sim
 GAP mes->broker  (pts)     : {tot['gap_mes']:+.1f} pts  = ${tot['gap_mes']*D:+,.0f}   <- slippage BEYOND the S55 MES model
 of which commission        :                 ${-tot['comm']:+,.0f}

 chain->mes (modeling)      : {tot['mes']-tot['chain']:+.1f} pts  = ${(tot['mes']-tot['chain'])*D:+,.0f}
""")

# SB-era only (Jun 16+): does the gap persist after the filter fix?
print("=== SB-era only (Jun 16-18) — does execution gap survive the SB fix? ===")
sbtot=defaultdict(float)
for d in ['2026-06-16','2026-06-17','2026-06-18']:
    if d in brk:
        s=sim.get(d,{}); b=brk[d]
        sbtot['chain']+=s.get('chain',0); sbtot['brk']+=b['pts']; sbtot['net']+=b['net']
print(f" chain-sim {sbtot['chain']:+.1f} pts (${sbtot['chain']*D:+.0f}) vs broker {sbtot['brk']:+.1f} pts net ${sbtot['net']:+.0f}"
      f"  -> gap ${(sbtot['brk']-sbtot['chain'])*D:+.0f}")
conn.close()
