import json, os, psycopg2
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute("SELECT columns, rows FROM chain_snapshots ORDER BY ts DESC LIMIT 1")
row = cur.fetchone()
cols = json.loads(row[0]) if isinstance(row[0], str) else row[0]
data = json.loads(row[1]) if isinstance(row[1], str) else row[1]
with open("tmp_cols_output.txt", "w") as f:
    f.write(f"Columns: {cols}\n")
    f.write(f"Num rows: {len(data)}\n")
    if data:
        f.write(f"Sample row 0: {data[0]}\n")
        f.write(f"Sample row mid: {data[len(data)//2]}\n")
cur.close()
conn.close()
