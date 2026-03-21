import os, json, sys
import sqlalchemy as sa

engine = sa.create_engine(os.environ['DATABASE_URL'])
with engine.connect() as c:
    rows = c.execute(sa.text("""
        SELECT
            id,
            (ts AT TIME ZONE 'America/New_York')::date as trade_date,
            ts AT TIME ZONE 'America/New_York' as ts_et,
            setup_name, direction, grade, score,
            paradigm, greek_alignment,
            outcome_result, outcome_pnl,
            spot, outcome_target_level, outcome_stop_level,
            outcome_max_profit, outcome_max_loss,
            vix, overvix, spot_vol_beta,
            charm_limit_entry,
            EXTRACT(DOW FROM (ts AT TIME ZONE 'America/New_York')::date) as day_of_week,
            EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') as hour_et,
            EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') as minute_et
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND outcome_result != 'EXPIRED'
        ORDER BY ts
    """)).fetchall()

    results = []
    for r in rows:
        results.append({
            'id': r.id,
            'trade_date': str(r.trade_date),
            'ts_et': str(r.ts_et),
            'setup_name': r.setup_name,
            'direction': r.direction,
            'grade': r.grade,
            'score': float(r.score) if r.score else None,
            'paradigm': r.paradigm,
            'alignment': int(r.greek_alignment) if r.greek_alignment is not None else None,
            'outcome': r.outcome_result,
            'outcome_pnl': float(r.outcome_pnl) if r.outcome_pnl else 0,
            'spot': float(r.spot) if r.spot else 0,
            'target': float(r.outcome_target_level) if r.outcome_target_level else None,
            'stop': float(r.outcome_stop_level) if r.outcome_stop_level else None,
            'max_profit': float(r.outcome_max_profit) if r.outcome_max_profit else None,
            'max_loss': float(r.outcome_max_loss) if r.outcome_max_loss else None,
            'vix': float(r.vix) if r.vix else None,
            'overvix': float(r.overvix) if r.overvix else None,
            'svb': float(r.spot_vol_beta) if r.spot_vol_beta else None,
            'charm_limit': float(r.charm_limit_entry) if r.charm_limit_entry else None,
            'dow': int(r.day_of_week),
            'hour': int(r.hour_et),
            'minute': int(r.minute_et)
        })

    print(json.dumps(results))
    print(f'\n---TOTAL: {len(results)} trades---', file=sys.stderr)
