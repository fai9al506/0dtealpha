# -*- coding: utf-8 -*-
import os, sys, pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
from mes_sim_backfill import mes_walk
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, abs_es_price, outcome_pnl, mes_sim_outcome_pnl, outcome_result FROM setup_log "
        f"WHERE setup_name='ES Absorption' AND abs_es_price IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')>='2026-05-01' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
    base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
    print(f"ES Abs V16 trades since May 1: {len(base)}")
    # bars per trade
    def get_bars(et_naive):
        d=et_naive.date()
        q=conn.execute(text("""SELECT ts_start, ts_end, bar_open, bar_high, bar_low, bar_close FROM vps_es_range_bars
            WHERE trade_date=:d AND symbol LIKE '%ES%' AND (ts_end AT TIME ZONE 'America/New_York') >= :t ORDER BY ts_end"""),
            {"d":str(d),"t":et_naive}).fetchall()
        return [(r[0],r[1],float(r[2]),float(r[3]),float(r[4]),float(r[5])) for r in q]
    trades=[]
    for r in base:
        et=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
        il=r['direction'] in ('long','bullish')
        bars=get_bars(et)
        if bars: trades.append((r,il,bars))
print(f"with Sierra bars: {len(trades)}\n")
def run(act,gap,sl=8):
    tot=0; w=0; n=0
    for r,il,bars in trades:
        o=mes_walk(bars, float(r['abs_es_price']), il, sl, None, 0, act, gap, 360)
        tot+=o['pnl']; w+= (o['pnl']>0); n+=1
    return n,tot,100*w/n if n else 0
print(f"{'scheme':<22}{'n':>4}{'totPts':>9}{'WR':>6}{'$@1MES':>9}")
for lab,act,gap in [('CURRENT act8/gap3',8,3),('user act8/gap2',8,2),('act8/gap1',8,1),('act8/gap4',8,4),
                    ('act6/gap2',6,2),('act10/gap2',10,2),('act6/gap3',6,3),('act10/gap5',10,5)]:
    n,tot,wr=run(act,gap)
    star=' <-- current' if lab.startswith('CURRENT') else (' <-- proposed' if 'gap2'==f'gap{gap}' and act==8 else '')
    print(f"{lab:<22}{n:>4}{tot:>+9.1f}{wr:>5.0f}%{tot*5:>+9.0f}{star}")

# era-split by month
import collections
by=collections.defaultdict(list)
for r,il,bars in trades:
    mo=r['ts'].astimezone(lf.ET).strftime('%Y-%m'); by[mo].append((r,il,bars))
def run_set(tset,act,gap,sl=8):
    tot=0;w=0;n=0
    for r,il,bars in tset:
        o=mes_walk(bars,float(r['abs_es_price']),il,sl,None,0,act,gap,360); tot+=o['pnl'];w+=(o['pnl']>0);n+=1
    return n,tot,100*w/n if n else 0
print("\n=== ERA-SPLIT (does it beat current in EVERY month?) ===")
print(f"{'month':<9}{'n':>4}  current(8/3)   your(8/2)    act6/gap2")
for mo in sorted(by):
    ts=by[mo]
    a=run_set(ts,8,3); b=run_set(ts,8,2); c=run_set(ts,6,2)
    print(f"{mo:<9}{a[0]:>4}  {a[1]:>+7.1f}p     {b[1]:>+7.1f}p    {c[1]:>+7.1f}p")

print("\n=== act6 gap2 vs gap3 (your 'safer act6' question) ===")
print(f"{'month':<9}{'n':>4}  current(8/3)   act6/gap3    act6/gap2")
for mo in sorted(by):
    ts=by[mo]; a=run_set(ts,8,3); g3=run_set(ts,6,3); g2=run_set(ts,6,2)
    print(f"{mo:<9}{a[0]:>4}  {a[1]:>+7.1f}p     {g3[1]:>+7.1f}p   {g2[1]:>+7.1f}p")
tA=run_set(trades,8,3); t3=run_set(trades,6,3); t2=run_set(trades,6,2)
print(f"{'TOTAL':<9}{tA[0]:>4}  {tA[1]:>+7.1f}p     {t3[1]:>+7.1f}p   {t2[1]:>+7.1f}p")
print(f"{'WR':<9}{'':>4}  {tA[2]:>7.0f}%     {t3[2]:>7.0f}%   {t2[2]:>7.0f}%")
