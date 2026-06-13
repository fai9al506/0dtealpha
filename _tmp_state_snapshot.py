import os, psycopg2, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("SELECT ts, spot FROM chain_snapshots ORDER BY ts DESC LIMIT 1")
ts, spot = cur.fetchone()
print(f"SPX spot:  {spot}  ({ts})")

cur.execute("SELECT ts, payload FROM volland_snapshots ORDER BY ts DESC LIMIT 1")
vts, payload = cur.fetchone()
print(f"Volland ts: {vts}")
stats = payload.get('statistics', {})
# print full stats keys + values for any non-null
print("--- statistics (non-null) ---")
for k, v in stats.items():
    if v is not None and v != '' and v != {}:
        if isinstance(v, dict) and len(str(v)) > 200:
            print(f"  {k}: <dict len {len(v)}>")
        elif isinstance(v, list) and len(v) > 5:
            print(f"  {k}: <list len {len(v)}>")
        else:
            print(f"  {k}: {v}")

cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='vps_es_range_bars' ORDER BY ordinal_position""")
print("\nvps_es_range_bars cols:", [r[0] for r in cur.fetchall()])

cur.execute("SELECT * FROM vps_es_range_bars WHERE range_pts=5 ORDER BY ts_end DESC LIMIT 1")
desc = [c[0] for c in cur.description]
row = cur.fetchone()
if row:
    print("--- last ES 5pt bar ---")
    for k, v in zip(desc, row):
        print(f"  {k}: {v}")

cur.close(); conn.close()
