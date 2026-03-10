import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()
r = c.execute(text("""
    SELECT
        COUNT(*) as total,
        COUNT(greek_alignment) as has_align,
        COUNT(*) - COUNT(greek_alignment) as missing
    FROM setup_log
""")).fetchone()
print(f"Total: {r[0]}, Has alignment: {r[1]}, Missing: {r[2]}")

if r[2] > 0:
    missing = c.execute(text("""
        SELECT id, setup_name, ts FROM setup_log
        WHERE greek_alignment IS NULL
        ORDER BY ts LIMIT 20
    """)).fetchall()
    print("Missing rows:")
    for m in missing:
        print(f"  #{m[0]} {m[1]} {str(m[2])[:16]}")
c.close()
