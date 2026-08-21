# -*- coding: utf-8 -*-
"""Phase 4: (A) validate 1-min sim by signal-density (prove churn hypothesis);
(B) trail-param grid on the CLEAN per-signal basis, per setup, for V16-SB trades."""
import os, sys, json, pickle, bisect, psycopg2
from collections import defaultdict
sys.path.insert(0,'.')
from app.mes_sim_backfill import mes_walk
univ,bars,bt=pickle.load(open("_tmp_1min_univ.pkl","rb"))
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()
def bars_after(ts,mm):
    e0=ts.timestamp(); e1=e0+mm*60
    i=bisect.bisect_left(bt,e0); out=[]
    j=i
    while j<len(bars) and bt[j]<=e1: out.append(bars[j]); j+=1
    return out

# ---- (A) validation by signal density ----
cur.execute("SELECT setup_log_id FROM real_trade_orders"); placed=set(r[0] for r in cur.fetchall())
cur.execute("SELECT id, date(ts AT TIME ZONE 'America/New_York') FROM setup_log WHERE id=ANY(%s)",(list(placed),))
etd={r[0]:str(r[1]) for r in cur.fetchall()}
cur.execute("SELECT day, trades FROM tsrt_daily_stmt ORDER BY day")
brk={}
for day,tr in cur.fetchall():
    js=tr if isinstance(tr,list) else json.loads(tr or '[]'); brk[str(day)]=sum(t.get('pts',0) for t in js)
agg=defaultdict(lambda:[0.0,0.0,0])
for u in univ:
    if u['id'] not in placed: continue
    d=etd.get(u['id'])
    if d: agg[d][0]+=u['chain_pnl']; agg[d][1]+=u['s1_pnl']; agg[d][2]+=1
lo=[0.0,0.0]; hi=[0.0,0.0]; lo_n=hi_n=0
for d,(ch,s1,n) in agg.items():
    if d not in brk: continue
    if n<=5: lo[0]+=s1; lo[1]+=brk[d]; lo_n+=1
    else: hi[0]+=s1; hi[1]+=brk[d]; hi_n+=1
print("=== (A) VALIDATION by signal density (1-min sim vs broker) ===")
print(f"  LOW-signal days (<=5 placed): {lo_n} days   1min={lo[0]:+.1f}  broker={lo[1]:+.1f}  gap={lo[0]-lo[1]:+.1f}")
print(f"  HIGH-signal days (>5 placed): {hi_n} days   1min={hi[0]:+.1f}  broker={hi[1]:+.1f}  gap={hi[0]-hi[1]:+.1f}")
print("  -> if LOW gap small & HIGH gap large+positive, the loss is POSITION CHURN on busy days,")
print("     not the trail. The clean per-signal sim is only valid on low-churn days.\n")

# ---- (B) trail grid on clean per-signal basis (V16-SB pass), per setup ----
def maxdd(seq):
    cum=0; peak=0; dd=0
    for x in seq:
        cum+=x; peak=max(peak,cum); dd=min(dd,cum-peak)
    return dd
def run_grid(trades, sl_default):
    # trades: list of dicts with ts,entry,is_long,elapsed
    grid={}
    for act in (6,8,10,12,15,20):
        for gap in (3,5,8,10):
            pnls=[]
            for u in trades:
                mm=max(u['elapsed']+30,60)
                r=mes_walk(bars_after(u['ts'],mm),u['entry'],u['is_long'],sl_default,None,0,act,gap,mm)
                pnls.append(r['pnl'])
            n=len(pnls); tot=sum(pnls); wr=sum(1 for p in pnls if p>0)/n*100
            grid[(act,gap)]=(round(tot,1),round(wr,1),round(maxdd(pnls),1))
    return grid

SETUPS=['Skew Charm','ES Absorption','AG Short','DD Exhaustion']
SL={'Skew Charm':14,'ES Absorption':8,'AG Short':12,'DD Exhaustion':20}
for sn in SETUPS:
    sub=[u for u in univ if u['setup']==sn and u['live_pass']]
    if len(sub)<15:
        print(f"\n### {sn}: only {len(sub)} V16-SB trades — skip (too few)"); continue
    print(f"\n### {sn} — {len(sub)} V16-SB trades, SL={SL[sn]}  (clean 1-min per-signal basis)")
    g=run_grid(sub,SL[sn])
    # current live params
    print(f"  {'act\\gap':<8}"+"".join(f"{f'g{gap}':>16}" for gap in (3,5,8,10)))
    for act in (6,8,10,12,15,20):
        cells=[]
        for gap in (3,5,8,10):
            tot,wr,dd=g[(act,gap)]
            cells.append(f"{tot:>6.0f}/{wr:>3.0f}%/{dd:>5.0f}")
        print(f"  a{act:<7}"+"".join(f"{c:>16}" for c in cells))
    best=max(g.items(),key=lambda kv:kv[1][0])
    bestwr=max(g.items(),key=lambda kv:kv[1][1])
    print(f"  cells = totalPnL / WR% / maxDD (points). best PnL: a{best[0][0]}/g{best[0][1]} = {best[1]}")
    print(f"  best WR:  a{bestwr[0][0]}/g{bestwr[0][1]} = {bestwr[1]}")
conn.close()
