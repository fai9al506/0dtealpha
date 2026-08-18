# -*- coding: utf-8 -*-
"""Is the LIVE basket capture broken? And which V16 trades BYPASSED the SB gate via fail-open, at what cost?"""
import os, sys, pandas as pd, numpy as np
from sqlalchemy import create_engine, text
sys.path.insert(0,'app'); import live_filter as lf
eng=create_engine(os.environ["DATABASE_URL"])
conn=eng.connect()
# 1) capture quality per day (last 12 trading days)
print("=== semi_basket LIVE capture quality (per ET day) ===")
q=pd.read_sql(text("""SELECT date(et AT TIME ZONE 'America/New_York') d, count(*) rows,
   min(n_names) min_names, max(n_names) max_names,
   to_char(min(et AT TIME ZONE 'America/New_York'),'HH24:MI') first, to_char(max(et AT TIME ZONE 'America/New_York'),'HH24:MI') last
   FROM semi_basket GROUP BY d ORDER BY d DESC LIMIT 12"""),conn)
print(q.to_string(index=False))
print("  (healthy day = ~390 rows 09:30->15:59, all 6 names)")

# 2) V16 trades in the GATE-ACTIVE era (Jun-16+): how many fail-opened (basket_pct NULL)?
gaps=lf.load_gaps(conn)
rows=conn.execute(text(f"SELECT {lf.COLS}, outcome_pnl, outcome_result FROM setup_log "
    f"WHERE (ts AT TIME ZONE 'America/New_York')::date >= '2026-06-16' AND outcome_pnl IS NOT NULL ORDER BY ts")).mappings().all()
base=[dict(r) for r in rows if lf.passes_v16(r,gaps)]
for r in base: r['et']=r['ts'].astimezone(lf.ET).replace(tzinfo=None)
df=pd.DataFrame(base)
df['pts']=df['outcome_pnl'].astype(float); df['stamped']=pd.to_numeric(df['basket_pct'],errors='coerce')
df['failopen']=df['stamped'].isna()
print(f"\n=== V16 trades since gate went live (Jun-16+): {len(df)} ===")
print(f"  stamped basket present: {(~df['failopen']).sum()}   FAIL-OPEN (NULL basket -> bypassed gate): {df['failopen'].sum()}")
