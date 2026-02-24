from sqlalchemy import create_engine, text
import os, sys
e = create_engine(os.environ['DATABASE_URL'])
with e.connect() as c:
    count = c.execute(text("SELECT COUNT(*) FROM economic_events")).scalar()
    print(f"Total events in DB: {count}")

    if count == 0:
        print("No events found — calendar may not have been fetched yet")
        sys.exit(0)

    print("\n--- USD events this week ---")
    rows = c.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as t, title, impact, forecast, previous, actual
        FROM economic_events
        WHERE country = 'USD'
        ORDER BY ts
    """)).fetchall()

    for r in rows:
        t = str(r[0])[:16]
        title = r[1][:42]
        impact = r[2] or ""
        fcast = r[3] or "-"
        prev = r[4] or "-"
        actual = r[5] or "-"
        marker = " ***" if impact == "High" else ""
        print(f"  {t}  {impact:8s}  {title:42s}  F={fcast:10s}  P={prev:10s}  A={actual}{marker}")

    print(f"\n--- All countries summary ---")
    rows2 = c.execute(text("""
        SELECT country, COUNT(*), SUM(CASE WHEN impact='High' THEN 1 ELSE 0 END) as high_ct
        FROM economic_events GROUP BY country ORDER BY COUNT(*) DESC
    """)).fetchall()
    for r in rows2:
        print(f"  {r[0]:5s}  {r[1]:3d} events  ({r[2]} high-impact)")

sys.stdout.flush()
