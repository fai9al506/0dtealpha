import os, psycopg, requests
c = psycopg.connect(os.environ['DATABASE_URL'])
c.autocommit = True
cur = c.cursor()
cur.execute("SELECT fname, caption, content FROM tmp_report_transfer WHERE id=1")
f, cap, h = cur.fetchone()
r = requests.post("https://api.telegram.org/bot" + os.environ['TELEGRAM_BOT_TOKEN'] + "/sendDocument",
                  data={"chat_id": "-1003792574755", "caption": cap},
                  files={"document": (f, h.encode("utf-8"), "text/html")}, timeout=90)
ok = r.json().get("ok")
print("telegram:", r.status_code, ok)
if ok:
    cur.execute("DROP TABLE tmp_report_transfer")
    print("temp table dropped")
c.close()
