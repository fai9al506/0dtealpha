"""Upload report HTML to a temp DB table so the Railway container can relay it
to Telegram (local ISP SNI-blocks api.telegram.org). Quick insert + commit —
no long transaction (DB discipline rule)."""
import os, sys, psycopg2

fname = sys.argv[1]
caption = sys.argv[2]
with open(fname, encoding='utf-8') as f:
    content = f.read()

dsn = os.environ['DATABASE_URL'].replace('postgresql+psycopg2', 'postgresql')
c = psycopg2.connect(dsn)
c.autocommit = True
cur = c.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS tmp_report_transfer (id INT PRIMARY KEY, fname TEXT, caption TEXT, content TEXT)")
cur.execute("INSERT INTO tmp_report_transfer (id, fname, caption, content) VALUES (1, %s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET fname=EXCLUDED.fname, caption=EXCLUDED.caption, content=EXCLUDED.content",
            (fname, caption, content))
print(f"uploaded {fname} ({len(content)//1024} KB) to tmp_report_transfer")
c.close()
