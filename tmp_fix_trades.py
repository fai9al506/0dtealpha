import os, psycopg2
c = psycopg2.connect(os.environ['DATABASE_URL'])
r = c.cursor()

# Delete stale trade #278 (bar 199 at 13:31, signal logged at 13:38 when ES was 6899.5)
r.execute("DELETE FROM setup_log WHERE id = 278")
print(f"Deleted trade #278: {r.rowcount} row(s)")

# Fix trade #275: max_loss was -91.5 (SPX contamination), actual is -6.75
r.execute("UPDATE setup_log SET outcome_max_loss = -6.75 WHERE id = 275")
print(f"Fixed trade #275 max_loss: {r.rowcount} row(s)")

c.commit()
c.close()
print("Done")
