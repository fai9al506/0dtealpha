"""Quick S2-S7 data check for LOG-ONLY setups."""
import os
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

setups = [
    "SB2 Absorption",
    "IV Momentum",
    "Vanna Butterfly",
    "VIX Compression",
    "GEX Long",
    "GEX Velocity",
]

with engine.connect() as c:
    for name in setups:
        total = c.execute(text("SELECT COUNT(*) FROM setup_log WHERE setup_name=:n"), {"n": name}).scalar()
        r = c.execute(text("""
            SELECT COUNT(*) as t,
                   SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as w,
                   SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as l,
                   SUM(CASE WHEN outcome_result='EXPIRED' THEN 1 ELSE 0 END) as x,
                   ROUND(COALESCE(SUM(outcome_pnl),0)::numeric,1) as p
            FROM setup_log WHERE setup_name=:n AND outcome_result IS NOT NULL
        """), {"n": name}).mappings().first()

        wr = f"{100*int(r['w'])/int(r['t']):.0f}%" if int(r['t']) > 0 else "n/a"
        print(f"{name:20s} | logged={total:3d} | resolved={int(r['t']):3d} | W={r['w']} L={r['l']} X={r['x']} | WR={wr:>4s} | PnL={r['p']}")

    # Date ranges
    dr = c.execute(text("""
        SELECT setup_name, MIN(created_at)::date as first, MAX(created_at)::date as last
        FROM setup_log
        WHERE setup_name IN ('SB2 Absorption','IV Momentum','Vanna Butterfly','VIX Compression','GEX Long','GEX Velocity')
        GROUP BY setup_name ORDER BY setup_name
    """)).mappings().all()
    print("\n--- Date Ranges ---")
    for r in dr:
        print(f"{r['setup_name']:20s} | {r['first']} -> {r['last']}")
