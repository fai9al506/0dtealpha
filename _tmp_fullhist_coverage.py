import os
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    r = conn.execute(text("""
        SELECT MIN((ts AT TIME ZONE 'America/New_York')::date),
               MAX((ts AT TIME ZONE 'America/New_York')::date),
               COUNT(*) FILTER (WHERE outcome_pnl IS NOT NULL),
               COUNT(*) FILTER (WHERE mes_sim_outcome_pnl IS NOT NULL),
               COUNT(*)
        FROM setup_log
    """)).fetchone()
    print("setup_log date range:", r[0], "->", r[1])
    print("rows w/ outcome_pnl:", r[2], " | w/ mes_sim:", r[3], " | total:", r[4])
    print()
    # SC/DD/ES Abs long counts w/ outcome by month
    rows = conn.execute(text("""
        SELECT to_char((ts AT TIME ZONE 'America/New_York'),'YYYY-MM') as mo,
               setup_name,
               COUNT(*) FILTER (WHERE outcome_pnl IS NOT NULL) as n_out
        FROM setup_log
        WHERE setup_name IN ('Skew Charm','DD Exhaustion','ES Absorption')
          AND direction IN ('long','bullish')
        GROUP BY 1,2 ORDER BY 1,2
    """)).fetchall()
    print(f"{'month':<9}{'setup':<16}{'n_with_outcome':>14}")
    for mo, s, n in rows:
        print(f"{mo:<9}{s:<16}{n:>14}")
