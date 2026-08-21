import os, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
cur.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_name='chain_snapshots' ORDER BY ordinal_position")
print("chain_snapshots:",[(r[0]) for r in cur.fetchall()])
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='volland_exposure_points' ORDER BY ordinal_position")
print("volland_exposure_points:",[r[0] for r in cur.fetchall()])
# how is chain stored - rows per strike or json?
cur.execute("SELECT ts, spot FROM chain_snapshots ORDER BY ts DESC LIMIT 1")
print("latest chain row sample:", cur.fetchone())
cur.execute("SELECT count(*) FROM chain_snapshots WHERE ts=(SELECT max(ts) FROM chain_snapshots)")
print("rows at latest ts:", cur.fetchone()[0])
c.close()
