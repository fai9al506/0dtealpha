"""Backtest 'follow Dark Matter's weekly bias' vs our baseline.
For each of his 8 plan-weeks, apply his directional bias as a filter on our
quality-traded signals, and compare P&L to baseline (take everything).

Overlay rule (from his plans):
  EXTREME/SHORT week  -> SHORTS only (drop counter-trend longs)
  LONG week           -> LONGS only (he's long-biased; fade-rips are scalps)
  RANGE/NEUTRAL week  -> BOTH (our normal mean-reversion)
P&L = outcome_pnl*5 ($@1MES), quality set, 15-min dedup.
"""
import os
from collections import defaultdict
from datetime import timedelta, date
from sqlalchemy import create_engine, text
engine=create_engine(os.environ['DATABASE_URL'])

HIS=[("2026-04-13","LONG"),("2026-04-20","RANGE"),("2026-04-27","RANGE"),
     ("2026-05-04","RANGE"),("2026-05-11","LONG"),("2026-05-18","RANGE"),
     ("2026-05-25","LONG"),("2026-06-01","RANGE"),("2026-06-08","SHORT")]

with engine.connect() as conn:
    rows=conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York') et, setup_name, direction, grade,
               greek_alignment, outcome_pnl
        FROM setup_log
        WHERE outcome_pnl IS NOT NULL
          AND setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption','AG Short')
        ORDER BY ts ASC""")).fetchall()

def quality(s,d,g,a):
    if g in ('C','LOG',None): return False
    islong=d in ('long','bullish'); aa=a or 0
    if s=='ES Absorption' and g not in ('A','A+'): return False
    if s=='DD Exhaustion' and islong and (aa<0 or aa>=3): return False
    return True

last={};T=[]
for et,s,d,g,a,p in rows:
    islong=d in ('long','bullish');key=(s,'L' if islong else 'S')
    if key in last and (et-last[key])<timedelta(minutes=15):continue
    last[key]=et
    if not quality(s,d,g,a):continue
    T.append({"d":et.date().isoformat(),"islong":islong,"usd":float(p)*5})

def wkdays(mon):
    y,m,dd=map(int,mon.split("-"));st=date(y,m,dd);return [(st+timedelta(days=i)).isoformat() for i in range(5)]

print(f"{'week':<12}{'his_bias':<8}{'baseLONG':>9}{'baseSHORT':>10}{'BASE tot':>9}{'  ':>2}{'DM-overlay':>11}{'  delta':>9}")
base_sum=dm_sum=0
post=("2026-05-18","2026-05-25","2026-06-01","2026-06-08")
post_base=post_dm=0
for mon,bias in HIS:
    days=set(wkdays(mon))
    wk=[t for t in T if t['d'] in days]
    L=[t for t in wk if t['islong']];S=[t for t in wk if not t['islong']]
    base=sum(t['usd'] for t in wk)
    if bias=="SHORT": dm=sum(t['usd'] for t in S)            # shorts only
    elif bias=="LONG": dm=sum(t['usd'] for t in L)           # longs only
    else: dm=base                                            # both
    base_sum+=base; dm_sum+=dm
    if mon in post: post_base+=base; post_dm+=dm
    print(f"{mon:<12}{bias:<8}{sum(t['usd'] for t in L):>+9.0f}{sum(t['usd'] for t in S):>+10.0f}"
          f"{base:>+9.0f}  {dm:>+11.0f}{dm-base:>+9.0f}")
print("-"*72)
print(f"{'TOTAL 8wk':<20}{'':>9}{'':>10}{base_sum:>+9.0f}  {dm_sum:>+11.0f}{dm_sum-base_sum:>+9.0f}")
print(f"{'POST-V16 (last4wk)':<20}{'':>9}{'':>10}{post_base:>+9.0f}  {post_dm:>+11.0f}{post_dm-post_base:>+9.0f}")
