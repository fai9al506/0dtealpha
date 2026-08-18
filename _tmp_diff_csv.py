# -*- coding: utf-8 -*-
import os, sys, pandas as pd
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
# portal CSV (ground truth V16-base)
csv=pd.read_csv(r"C:/Users/Faisa/Downloads/trade_log_2026-06-24.csv",encoding='utf-8-sig')
csv.columns=[c.strip() for c in csv.columns]
csv['ID']=csv['ID'].astype(int); csv['P&L']=pd.to_numeric(csv['P&L'],errors='coerce')
portal_ids=set(csv['ID']); portal_pnl=dict(zip(csv['ID'],csv['P&L']))
print(f"PORTAL CSV: {len(csv)} trades, pts={csv['P&L'].sum():+.1f}, W={(csv['Result']=='WIN').sum()} L={(csv['Result']=='LOSS').sum()} EXP={(csv['Result']=='EXPIRED').sum()}")
# my reconstruction
eng=create_engine(os.environ["DATABASE_URL"])
with eng.connect() as conn:
    gaps=lf.load_gaps(conn)
    rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
        f"WHERE id >= 4031 AND id <= 4357 AND outcome_pnl IS NOT NULL ORDER BY id")).mappings().all()
mine={r['id']:dict(r) for r in rows if lf.passes_v16(r,gaps)}
mine_ids=set(mine)
print(f"MINE (passes_v16): {len(mine_ids)} trades, pts={sum(float(mine[i]['outcome_pnl']) for i in mine_ids):+.1f}\n")

only_mine=sorted(mine_ids-portal_ids)
only_portal=sorted(portal_ids-mine_ids)
print(f"=== IN MINE, NOT PORTAL ({len(only_mine)}) — I wrongly INCLUDE these ===")
for i in only_mine:
    m=mine[i]; print(f"  {i}  {m['setup_name']:<16} {m['direction']:<8} grade={m['grade']} para={m['paradigm']} pnl={float(m['outcome_pnl']):+.1f} res={m['outcome_result']}")
print(f"\n=== IN PORTAL, NOT MINE ({len(only_portal)}) — I wrongly EXCLUDE these ===")
for i in only_portal:
    r=csv[csv['ID']==i].iloc[0]; print(f"  {i}  {r['Setup']:<16} {r['Direction']:<8} grade={r['Grade']} pnl={r['P&L']:+.1f} res={r['Result']}")
# P&L mismatches on common
print(f"\n=== COMMON IDs with P&L mismatch ===")
common=sorted(mine_ids & portal_ids); nmm=0
for i in common:
    a=round(float(mine[i]['outcome_pnl']),1); b=round(float(portal_pnl[i]),1)
    if abs(a-b)>0.15:
        nmm+=1; print(f"  {i} {mine[i]['setup_name']:<14} mine={a:+.1f} portal={b:+.1f} (diff {a-b:+.1f})")
print(f"  ({nmm} mismatches, {len(common)} common ids)")
