import os
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    cols = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='setup_log' ORDER BY ordinal_position
    """)).fetchall()
    print("setup_log columns:")
    print(", ".join(c[0] for c in cols))
    print()

    # All Jun 9 signals
    rows = conn.execute(text("""
        SELECT (ts AT TIME ZONE 'America/New_York')::text as et,
               setup_name, direction, grade, paradigm, spot, vix, overvix,
               outcome_result, outcome_pnl, real_trade_skip_reason
        FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date = DATE '2026-06-09'
        ORDER BY ts ASC
    """)).fetchall()
    print(f"Total Jun 9 setup_log signals: {len(rows)}\n")
    print(f"{'time':<6}{'setup':<13}{'dir':<8}{'gr':<3}{'paradigm':<13}{'spot':>8}{'vix':>6}{'ovx':>5}{'res':>5}{'pnl':>7}  rt_skip")
    for r in rows:
        et, setup, d, g, par, spot, vix, ovx, res, pnl, rtskip = r
        sp = f"{spot:.1f}" if spot is not None else "-"
        vx = f"{vix:.1f}" if vix is not None else "-"
        ov = f"{ovx:+.1f}" if ovx is not None else "-"
        pn = f"{pnl:+.1f}" if pnl is not None else "-"
        print(f"{et[11:16]:<6}{str(setup)[:12]:<13}{str(d)[:7]:<8}{str(g):<3}{str(par)[:12]:<13}{sp:>8}{vx:>6}{ov:>5}{str(res)[:4]:>5}{pn:>7}  {str(rtskip)[:34] if rtskip else ''}")
