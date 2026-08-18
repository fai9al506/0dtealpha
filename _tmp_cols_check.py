import os, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
cur.execute("""SELECT column_name FROM information_schema.columns
  WHERE table_name='setup_log' ORDER BY ordinal_position""")
cols=[r[0] for r in cur.fetchall()]
print("setup_log cols:", cols)
# sample a row to see spot/from_open fields
cur.execute("SELECT * FROM setup_log WHERE id=4658")
names=[d[0] for d in cur.description]
row=cur.fetchone()
d=dict(zip(names,row))
for k in ('spot','entry','entry_price','from_open','signal_es_price','abs_details'):
    if k in d: print(f"  {k} = {str(d[k])[:120]}")
c.close()
