import os
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, setup_name, direction, grade,
               spot, target, outcome_result, outcome_pnl,
               outcome_target_level, outcome_stop_level,
               outcome_max_profit, outcome_max_loss,
               outcome_elapsed_min
        FROM setup_log
        WHERE ts AT TIME ZONE 'America/New_York' >= '2026-02-19'
          AND ts AT TIME ZONE 'America/New_York' < '2026-02-20'
          AND grade IN ('A', 'B', 'LOG')
        ORDER BY ts
    """)).mappings().all()

    print(f"{'ID':>5} {'Time':>5} {'Setup':<16} {'Dir':<6} {'G':>1} {'Spot':>7} {'Target':>7} {'Stop':>7} {'Result':<8} {'PnL':>7} {'MaxP':>6} {'Min'}")
    print("-" * 100)
    for r in rows:
        t = str(r['ts_et'])[11:16]
        tgt = f"{r['target']:.1f}" if r['target'] else "-"
        stop = f"{r['outcome_stop_level']:.1f}" if r['outcome_stop_level'] else "-"
        pnl = f"{r['outcome_pnl']:+.1f}" if r['outcome_pnl'] is not None else "OPEN"
        res = r['outcome_result'] or 'OPEN'
        mp = f"{r['outcome_max_profit']:.1f}" if r['outcome_max_profit'] is not None else "-"
        em = f"{r['outcome_elapsed_min']}m" if r['outcome_elapsed_min'] else "-"
        print(f"#{r['id']:>4} {t} {r['setup_name']:<16} {r['direction']:<6} {r['grade']:>1} {r['spot']:>7.1f} {tgt:>7} {stop:>7} {res:<8} {pnl:>7} {mp:>6} {em}")

    print(f"\nTotal: {len(rows)} trades")

    # Summary by setup
    print("\n=== Summary by Setup ===")
    setups = {}
    for r in rows:
        name = r['setup_name']
        if name not in setups:
            setups[name] = {'count': 0, 'wins': 0, 'pnl': 0.0, 'trades': []}
        setups[name]['count'] += 1
        if r['outcome_pnl'] is not None:
            setups[name]['pnl'] += r['outcome_pnl']
            if r['outcome_result'] == 'WIN':
                setups[name]['wins'] += 1
        setups[name]['trades'].append(r)

    for name, s in setups.items():
        wr = f"{s['wins']/s['count']*100:.0f}%" if s['count'] > 0 else "-"
        print(f"  {name:<16} {s['count']} trades  {s['wins']}W  WR={wr}  PnL={s['pnl']:+.1f} pts")
