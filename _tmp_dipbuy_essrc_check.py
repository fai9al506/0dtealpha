"""Check ES bar sources: coverage + columns, for a finer dip-buy sim."""
import os, psycopg2
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

for tbl in ("vps_es_range_bars", "es_delta_bars", "es_range_bars"):
    try:
        cur.execute(f"""select column_name from information_schema.columns
                        where table_name='{tbl}' order by ordinal_position""")
        cols = [r[0] for r in cur.fetchall()]
        print(f"\n{tbl}: {cols}")
        cur.execute(f"select min(ts_start), max(ts_start), count(*) from {tbl}")
        print("  coverage:", cur.fetchall()[0])
    except Exception as e:
        conn.rollback()
        print(f"\n{tbl}: ERR {e}")

# per-day bar counts vps 5pt
try:
    cur.execute("""select (ts_start at time zone 'America/New_York')::date d, count(*)
                   from vps_es_range_bars where range_pts=5
                   group by d order by d limit 100""")
    rows = cur.fetchall()
    print(f"\nvps 5pt days: {len(rows)} first={rows[0] if rows else None} last={rows[-1] if rows else None}")
except Exception as e:
    conn.rollback(); print("vps day count ERR", e)

# es_delta_bars day coverage
try:
    cur.execute("""select min(bar_ts), max(bar_ts), count(*) from es_delta_bars""")
    print("es_delta_bars coverage:", cur.fetchall()[0])
except Exception as e:
    conn.rollback(); print("es_delta_bars ERR", e)
conn.close()
