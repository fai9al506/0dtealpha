import os
from sqlalchemy import create_engine, text
eng = create_engine(os.environ["DATABASE_URL"], isolation_level="AUTOCOMMIT")
with eng.connect() as c:
    # post-V16 graded DD longs: skip reason breakdown
    rows = c.execute(text("""
        SELECT COALESCE(sl.real_trade_skip_reason,
                        CASE WHEN rto.setup_log_id IS NOT NULL THEN '>> PLACED <<' ELSE '(null, not placed)' END) reason,
               count(*)
        FROM setup_log sl
        LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.setup_name='DD Exhaustion'
          AND lower(sl.direction) IN ('long','bullish')
          AND sl.grade IS NOT NULL AND sl.grade != 'LOG'
          AND sl.ts >= '2026-05-18' AND sl.ts < '2026-06-04'
        GROUP BY 1 ORDER BY 2 DESC
    """)).fetchall()
    print("=== post-V16 graded DD LONG signals (May18-Jun3): skip reasons ===")
    tot = 0
    for r, n in rows:
        print(f"  {n:>4}  {r}")
        tot += n
    print(f"  {tot:>4}  TOTAL")
