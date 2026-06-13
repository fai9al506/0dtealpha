import os, psycopg, json
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
cur.execute("SELECT ts, spot, columns, rows FROM chain_snapshots WHERE ts::date='2026-06-11' ORDER BY ts LIMIT 1")
ts,spot,cols,rows=cur.fetchone()
cols=cols if isinstance(cols,list) else json.loads(cols)
rows=rows if isinstance(rows,list) else json.loads(rows)
print("spot:",spot,"ncols:",len(cols),"nrows:",len(rows))
print("columns:",cols)
print("\nsample rows (near spot):")
# find strike col index
for i,r in enumerate(rows[:3]): print("  ",r)
# dte0_gex key_levels sample
cur.execute("SELECT scan_date,spot,key_levels FROM dte0_gex_scans WHERE symbol IN ('$SPXW.X','SPX') AND scan_date='2026-06-11' ORDER BY scan_ts LIMIT 1")
r=cur.fetchone()
if r: print("\ndte0 key_levels 6/11:",json.dumps(r[2])[:400] if r[2] else None)
