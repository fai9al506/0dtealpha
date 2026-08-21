import os, psycopg2
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()
cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_name='setup_log' ORDER BY ordinal_position""")
print([r[0] for r in cur.fetchall()])
conn.close()
