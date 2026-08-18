# -*- coding: utf-8 -*-
"""One-time: backfill mes_sim_* for DD Exhaustion (Mar 23 -> today), forcing the
CURRENT continuous trail (SL12 / act10 / gap10, no BE; S224). Stored per-row trail
params are the stale 20/5 so we override them explicitly. Then NULL the Jun 15-16
Sierra-contaminated dates (ES bars ~55-60pt low -> phantom short profit)."""
import os, sys
sys.path.insert(0,'.')
from sqlalchemy import create_engine, text
from app import mes_sim_backfill as msb

eng = create_engine(os.environ['DATABASE_URL'].replace('postgresql://','postgresql+psycopg://'))

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT sl.id, sl.ts, sl.setup_name, sl.direction, sl.spot, sl.outcome_elapsed_min,
               (rto.state->>'signal_es_price')::float AS sig_es,
               (rto.state->>'fill_price')::float AS fill_px
        FROM setup_log sl
        LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.setup_name='DD Exhaustion' AND sl.outcome_result IS NOT NULL
          AND sl.ts >= '2026-03-23'
        ORDER BY sl.ts ASC
    """)).fetchall()

print(f"DD rows to backfill: {len(rows)}")
computed=written=skipped=0
for r in rows:
    sim = msb.compute_mes_sim_outcome(
        engine=eng, setup_log_id=r[0], setup_name=r[2], direction=r[3],
        signal_ts=r[1], spx_spot=r[4],
        trail_sl=12, trail_activation=10, trail_gap=10,   # FORCE current config
        signal_es_price=r[6], fill_price=r[7], outcome_elapsed_min=r[5],
    )
    if sim is None:
        skipped+=1; continue
    computed+=1
    if msb.write_mes_sim_columns(eng, r[0], sim): written+=1
print(f"computed={computed} written={written} skipped(no bars/inputs)={skipped}")

# Clean Jun 15-16 contamination (mirror prior session's NULLing for SC/AG)
with eng.begin() as c:
    n = c.execute(text("""
        UPDATE setup_log SET mes_sim_outcome_pnl=NULL, mes_sim_outcome_result=NULL, mes_sim_max_fav=NULL
        WHERE setup_name='DD Exhaustion'
          AND date(ts AT TIME ZONE 'America/New_York') IN ('2026-06-15','2026-06-16')
    """)).rowcount
print(f"NULLed Jun15-16 contaminated DD rows: {n}")

# sanity: coverage now + contamination scan (any |pnl|>40 or max_fav>40 = suspect)
with eng.connect() as c:
    cov = c.execute(text("""SELECT count(*) total, count(mes_sim_outcome_pnl) cov
        FROM setup_log WHERE setup_name='DD Exhaustion' AND outcome_result IS NOT NULL AND ts>='2026-03-23'""")).fetchone()
    print(f"DD coverage: {cov[1]}/{cov[0]}")
    sus = c.execute(text("""SELECT date(ts AT TIME ZONE 'America/New_York') d, id, direction,
            round(mes_sim_outcome_pnl::numeric,1), round(mes_sim_max_fav::numeric,1)
        FROM setup_log WHERE setup_name='DD Exhaustion' AND mes_sim_max_fav > 40 ORDER BY 1""")).fetchall()
    print(f"suspect (max_fav>40): {len(sus)}")
    for s in sus[:15]: print("  ", str(s[0]), "lid",s[1], s[2], "pnl",s[3], "maxF",s[4])
