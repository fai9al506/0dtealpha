"""Verify DD Exhaustion trade #14: SHORT at 6861, Feb 19"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

with engine.begin() as conn:
    # 1. Get the setup_log entry
    r = conn.execute(text("""
        SELECT id, ts, setup_name, direction, spot, grade, score,
               outcome_result, outcome_pnl, outcome_stop_level,
               outcome_first_event, outcome_max_profit, outcome_max_loss
        FROM setup_log
        WHERE id = 139
    """)).mappings().first()

    print(f"ID={r['id']} ts={r['ts']} dir={r['direction']} spot={r['spot']}")
    print(f"  outcome: {r['outcome_result']} PNL={r['outcome_pnl']} stop={r['outcome_stop_level']}")
    print(f"  first_event={r['outcome_first_event']} max_profit={r['outcome_max_profit']} max_loss={r['outcome_max_loss']}")

    entry = float(r['spot'])
    initial_stop = entry + 12  # SHORT: stop above
    print(f"\n  Entry={entry}, Initial SL={initial_stop}")
    print(f"  Stored stop_level={r['outcome_stop_level']} => implies max_fav={(entry - float(r['outcome_stop_level'])) + 5:.1f}")

    # 2. Full price path from playback_snapshots (from entry to EOD)
    prices = conn.execute(text("""
        SELECT ts, spot FROM playback_snapshots
        WHERE ts >= :start AND ts <= :end
        ORDER BY ts ASC
    """), {"start": r['ts'], "end": "2026-02-19 21:00:00+00:00"}).mappings().all()

    print(f"\n  === FULL PRICE PATH ({len(prices)} snapshots, entry to EOD) ===")
    min_price = 99999
    max_price = 0
    for i, p in enumerate(prices):
        price = float(p['spot'])
        if price < min_price:
            min_price = price
        if price > max_price:
            max_price = price
        profit = entry - price  # SHORT profit
        if i < 10 or i % 10 == 0 or price < entry - 15 or price > initial_stop - 1:
            print(f"    [{i:3d}] {p['ts']} spot={price:.1f} profit={profit:+.1f}")

    print(f"\n  Session extremes in snapshots: LOW={min_price:.1f} HIGH={max_price:.1f}")
    print(f"  Max favorable for SHORT: {entry - min_price:+.1f}")
    print(f"  Max adverse for SHORT: {max_price - entry:+.1f}")

    # 3. Check chain_snapshots for session H/L data
    print("\n  === CHAIN SNAPSHOTS (session H/L from TradeStation) ===")
    chain = conn.execute(text("""
        SELECT ts, spot, session_high, session_low
        FROM (
            SELECT ts,
                   (payload->>'spot')::float AS spot,
                   (payload->>'session_high')::float AS session_high,
                   (payload->>'session_low')::float AS session_low
            FROM chain_snapshots
            WHERE ts::date = '2026-02-19' AND ts >= :start
            ORDER BY ts ASC
        ) sub
        WHERE session_low IS NOT NULL
        LIMIT 60
    """), {"start": r['ts']}).mappings().all()

    for i, c in enumerate(chain[:30]):
        if c['session_low']:
            print(f"    [{i:3d}] {c['ts']} spot={c['spot']:.1f} sess_H={c['session_high']} sess_L={c['session_low']}")
