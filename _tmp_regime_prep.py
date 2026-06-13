import os, psycopg
conn=psycopg.connect(os.environ["DATABASE_URL"], autocommit=True); cur=conn.cursor()
# dte0_gex_scans: daily SPX gamma regime source?
for t in ('dte0_gex_scans','chain_snapshots','semi_basket'):
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position",(t,))
    cols=[r[0] for r in cur.fetchall()]
    print(f"{t}: {cols}")
print()
# dte0 sample totals for SPX
cur.execute("""SELECT scan_date, symbol, totals FROM dte0_gex_scans WHERE symbol IN ('$SPXW.X','SPX') ORDER BY scan_ts DESC LIMIT 3""")
for r in cur.fetchall(): print("dte0 sample:",r[0],r[1],str(r[2])[:200])
# date coverage of dte0
cur.execute("SELECT min(scan_date),max(scan_date),count(distinct scan_date) FROM dte0_gex_scans WHERE symbol IN ('$SPXW.X','SPX')")
print("dte0 SPX coverage:",cur.fetchone())
# semi_basket coverage
cur.execute("SELECT count(*),min(et),max(et) FROM semi_basket")
print("semi_basket:",cur.fetchone())
