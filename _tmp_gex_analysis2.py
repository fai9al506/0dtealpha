"""GEX Long: Volland data at signal time + max profit on losers"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.environ['DATABASE_URL']
if DB_URL.startswith('postgresql://'):
    DB_URL = DB_URL.replace('postgresql://', 'postgresql+psycopg://', 1)

engine = create_engine(DB_URL)
with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
               ts as ts_utc, spot, target,
               outcome_result, outcome_pnl,
               outcome_max_profit, outcome_max_loss,
               outcome_elapsed_min
        FROM setup_log
        WHERE setup_name = 'GEX Long' AND outcome_result IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    # Volland data at signal time
    print("=" * 120)
    print("VOLLAND SNAPSHOT AT SIGNAL TIME")
    print("=" * 120)
    for r in rows:
        ts_utc = r['ts_utc']
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        vol = conn.execute(text("""
            SELECT ts,
                   payload->>'paradigm' as paradigm,
                   payload->>'ddHedging' as dd_hedging,
                   payload->>'aggregatedCharm' as charm
            FROM volland_snapshots
            WHERE ts BETWEEN :ts1 AND :ts2
              AND payload->>'paradigm' IS NOT NULL
            ORDER BY ts ASC
            LIMIT 1
        """), {"ts1": ts_utc - __import__('datetime').timedelta(minutes=3),
               "ts2": ts_utc + __import__('datetime').timedelta(minutes=3)}).mappings().first()

        ts_et = r['ts_et']
        if vol:
            dd = vol['dd_hedging'] or 'N/A'
            charm = vol['charm'] or 'N/A'
            par = vol['paradigm'] or 'N/A'
            print(f"  #{r['id']:>3} {ts_et.strftime('%m/%d %H:%M')} paradigm={par:>12} DD={dd:>18} charm={charm:>15} | {r['outcome_result']:>7} {pnl:>+6.1f}")
        else:
            print(f"  #{r['id']:>3} {ts_et.strftime('%m/%d %H:%M')} [no volland data] | {r['outcome_result']:>7} {pnl:>+6.1f}")

    # Max profit on losers
    print("\n" + "=" * 120)
    print("MAX PROFIT ON LOSING TRADES")
    print("=" * 120)
    for r in rows:
        if r['outcome_result'] != 'LOSS':
            continue
        spot = float(r['spot']) if r['spot'] else 0
        mp = float(r['outcome_max_profit']) if r['outcome_max_profit'] else 0
        ml = float(r['outcome_max_loss']) if r['outcome_max_loss'] else 0
        tgt = float(r['target']) if r['target'] else 0
        tgt_dist = tgt - spot if tgt else 0
        em = r['outcome_elapsed_min'] or 0
        print(f"  #{r['id']:>3} {r['ts_et'].strftime('%m/%d %H:%M')} tgt_dist={tgt_dist:>5.1f} maxProfit={mp:>+6.1f} maxLoss={ml:>+6.1f} stopped_after={em}min")

    # Price action after entry — query playback_snapshots for each losing trade
    print("\n" + "=" * 120)
    print("PRICE PATH AFTER ENTRY (losers only, from playback_snapshots)")
    print("=" * 120)
    for r in rows:
        if r['outcome_result'] != 'LOSS':
            continue
        ts_utc = r['ts_utc']
        spot = float(r['spot'])
        snapshots = conn.execute(text("""
            SELECT ts AT TIME ZONE 'America/New_York' as ts_et,
                   (payload->>'spot')::float as price
            FROM playback_snapshots
            WHERE ts BETWEEN :ts1 AND :ts2
            ORDER BY ts ASC
            LIMIT 30
        """), {"ts1": ts_utc, "ts2": ts_utc + __import__('datetime').timedelta(minutes=60)}).mappings().all()

        print(f"\n  #{r['id']:>3} {r['ts_et'].strftime('%m/%d %H:%M')} entry={spot:.1f}")
        if snapshots:
            for s in snapshots[:15]:
                price = s['price']
                diff = price - spot
                bar = '+' * int(max(0, diff/2)) + '-' * int(max(0, -diff/2))
                print(f"    {s['ts_et'].strftime('%H:%M')} {price:>7.1f} ({diff:>+6.1f}) {bar}")
        else:
            print(f"    [no playback snapshots found]")
