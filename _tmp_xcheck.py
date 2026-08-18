# -*- coding: utf-8 -*-
"""CROSS-CHECK my passes_v16 reconstruction vs portal known: Jun13+ base = 98t / -207.1 pts."""
import os, sys, pandas as pd
from datetime import timedelta
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE (ts AT TIME ZONE 'America/New_York')::date >= '2026-06-13' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]          # V16-base (no basket)
sb  =[dict(r) for r in rows if lf.passes_v16_sb(r,gaps)]       # V16-SB (open 0/0/1, stamped basket)
def summ(lst,lab,dedup):
    if dedup:
        for r in lst: r['et']=r['ts'].astimezone(lf.ET)
        lst=sorted(lst,key=lambda r:r['et']); keep=[]; last={}
        for r in lst:
            k=(r['setup_name'], r['direction'] in ('long','bullish'))
            if k in last and (r['et']-last[k])<timedelta(minutes=15): continue
            last[k]=r['et']; keep.append(r)
        lst=keep
    n=len(lst); pnl=sum(float(r['outcome_pnl']) for r in lst); w=sum(1 for r in lst if r['outcome_result']=='WIN')
    print(f"  {lab:<28} n={n:<4} W={w} WR={100*w/n if n else 0:.0f}%  pts={pnl:+.1f}")
print("Jun13+ (portal says: V16-base 98t / -207.1 pts ; V16-SB 27t / -57.5):")
print(" RAW (no dedup):"); summ(base,'V16-base',False); summ(sb,'V16-SB open0/0/1',False)
print(" DEDUP 15min:");    summ(base,'V16-base',True);  summ(sb,'V16-SB open0/0/1',True)
