import os, psycopg2
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True
cur = c.cursor()
for t in ("setup_log", "real_trade_orders"):
    cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                   WHERE table_name=%s ORDER BY ordinal_position""", (t,))
    print(f"=== {t} ===")
    for name, dt in cur.fetchall():
        print(f"  {name:32} {dt}")
    print()
