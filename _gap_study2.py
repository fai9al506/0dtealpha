"""
Supplemental gap study: verify today's gap, and deeper first-30-min analysis.
"""
import os
from datetime import date, time as dtime
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

def main():
    # ── Verify today's gap computation ──
    print("="*80)
    print("  VERIFY TODAY'S GAP (2026-03-27)")
    print("="*80)
    q = text("""
        SELECT
            date(ts AT TIME ZONE 'America/New_York') AS td,
            MIN(ts) AS first_ts,
            MAX(ts) AS last_ts,
            (SELECT cs.spot FROM chain_snapshots cs WHERE cs.ts = MIN(t.ts) AND cs.spot IS NOT NULL LIMIT 1) AS day_open,
            (SELECT cs.spot FROM chain_snapshots cs WHERE cs.ts = MAX(t.ts) AND cs.spot IS NOT NULL LIMIT 1) AS day_close
        FROM chain_snapshots t
        WHERE spot IS NOT NULL
          AND date(ts AT TIME ZONE 'America/New_York') IN ('2026-03-26', '2026-03-27')
        GROUP BY date(ts AT TIME ZONE 'America/New_York')
        ORDER BY td
    """)
    with engine.begin() as conn:
        rows = conn.execute(q).mappings().all()
    for r in rows:
        print(f"  {r['td']}: open={r['day_open']}, close={r['day_close']}, first_ts={r['first_ts']}, last_ts={r['last_ts']}")
    if len(rows) >= 2:
        prev_close = rows[0]['day_close']
        today_open = rows[1]['day_open']
        gap = round(today_open - prev_close, 1)
        print(f"\n  Gap = {today_open} - {prev_close} = {gap:+.1f} pts")

    # Also check what _compute_daily_gap would see (prev day's LAST spot)
    q2 = text("""
        SELECT spot as close_price, ts
        FROM chain_snapshots
        WHERE spot IS NOT NULL
          AND date(ts AT TIME ZONE 'America/New_York') < '2026-03-27'
        ORDER BY ts DESC LIMIT 1
    """)
    with engine.begin() as conn:
        r = conn.execute(q2).mappings().first()
    print(f"\n  _compute_daily_gap logic: prev_close = {r['close_price']} at {r['ts']}")

    # Today's first spot
    q3 = text("""
        SELECT spot, ts
        FROM chain_snapshots
        WHERE spot IS NOT NULL
          AND date(ts AT TIME ZONE 'America/New_York') = '2026-03-27'
        ORDER BY ts ASC LIMIT 5
    """)
    with engine.begin() as conn:
        rows3 = conn.execute(q3).mappings().all()
    print(f"  Today's first snapshots:")
    for r in rows3:
        gap_here = round(r['spot'] - float(rows[0]['day_close']), 1) if len(rows) >= 2 else None
        print(f"    {r['ts']}: spot={r['spot']} (gap={gap_here:+.1f})" if gap_here else f"    {r['ts']}: spot={r['spot']}")

    # ── First 30 min longs on gap-down: per-trade detail ──
    print(f"\n{'='*80}")
    print("  FIRST 30 MIN LONGS ON ANY GAP DAY — ALL TRADES")
    print(f"{'='*80}")

    # Get ALL early longs that pass V12-base
    q4 = text("""
        WITH gaps AS (
            SELECT
                date(ts AT TIME ZONE 'America/New_York') AS td,
                MIN(spot) FILTER (WHERE ts = sub.first_ts) AS day_open
            FROM chain_snapshots,
            LATERAL (
                SELECT MIN(t2.ts) AS first_ts
                FROM chain_snapshots t2
                WHERE t2.spot IS NOT NULL
                  AND date(t2.ts AT TIME ZONE 'America/New_York') = date(chain_snapshots.ts AT TIME ZONE 'America/New_York')
            ) sub
            WHERE spot IS NOT NULL
            GROUP BY date(ts AT TIME ZONE 'America/New_York')
        )
        SELECT
            s.id, s.ts,
            date(s.ts AT TIME ZONE 'America/New_York') AS trade_date,
            (s.ts AT TIME ZONE 'America/New_York')::time AS entry_time_et,
            s.setup_name, s.direction, s.grade, s.greek_alignment,
            s.outcome_result, s.outcome_pnl, s.spot, s.vix, s.overvix, s.paradigm
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL
          AND s.outcome_pnl IS NOT NULL
          AND s.direction IN ('long', 'bullish')
          AND (s.ts AT TIME ZONE 'America/New_York')::time < '10:00:00'
        ORDER BY s.ts
    """)
    with engine.begin() as conn:
        early_longs = conn.execute(q4).mappings().all()

    print(f"\n  All early (<10:00) longs from setup_log: {len(early_longs)}")
    print(f"  {'Date':>12} | {'Time':>8} | {'Setup':>18} | {'Grd':>3} | {'Alg':>3} | {'Out':>7} | {'PnL':>7} | {'VIX':>5} | {'Paradigm':>12}")
    print(f"  {'-'*12}-+-{'-'*8}-+-{'-'*18}-+-{'-'*3}-+-{'-'*3}-+-{'-'*7}-+-{'-'*7}-+-{'-'*5}-+-{'-'*12}")
    for r in early_longs:
        print(f"  {r['trade_date']} | {str(r['entry_time_et'])[:8]:>8} | {r['setup_name']:>18} | {(r['grade'] or '?')[:3]:>3} | {r['greek_alignment'] or 0:>3} | {r['outcome_result']:>7} | {r['outcome_pnl']:>+6.1f} | {r['vix'] or 0:>5.1f} | {r['paradigm'] or 'n/a':>12}")

    # ── Specifically: SC longs on gap-down days, first 30 min vs rest ──
    print(f"\n{'='*80}")
    print("  SC LONGS: FIRST 30 MIN (09:30-10:00) vs REST — ALL DAYS")
    print(f"{'='*80}")

    q5 = text("""
        SELECT
            s.id, s.ts,
            date(s.ts AT TIME ZONE 'America/New_York') AS trade_date,
            (s.ts AT TIME ZONE 'America/New_York')::time AS entry_time_et,
            s.setup_name, s.direction, s.grade, s.greek_alignment,
            s.outcome_result, s.outcome_pnl, s.vix, s.overvix, s.paradigm
        FROM setup_log s
        WHERE s.outcome_result IS NOT NULL
          AND s.outcome_pnl IS NOT NULL
          AND s.setup_name = 'Skew Charm'
          AND s.direction IN ('long', 'bullish')
        ORDER BY s.ts
    """)
    with engine.begin() as conn:
        sc_longs = [dict(r) for r in conn.execute(q5).mappings().all()]

    # Apply V12-base filter
    from datetime import time as dtime
    def passes_v12_base(t):
        if t['grade'] and t['grade'] in ('C', 'LOG'):
            return False
        et = t['entry_time_et']
        if dtime(14, 30) <= et < dtime(15, 0):
            return False
        if et >= dtime(15, 30):
            return False
        align = t['greek_alignment'] or 0
        if align < 2:
            return False
        return True

    sc_v12 = [t for t in sc_longs if passes_v12_base(t)]
    early = [t for t in sc_v12 if t['entry_time_et'] < dtime(10, 0)]
    rest = [t for t in sc_v12 if t['entry_time_et'] >= dtime(10, 0)]

    def stats(trades, label):
        if not trades:
            print(f"  {label}: 0 trades")
            return
        pnl = sum(t['outcome_pnl'] for t in trades)
        wins = sum(1 for t in trades if t['outcome_result'] == 'WIN')
        wr = wins / len(trades) * 100
        avg = pnl / len(trades)
        print(f"  {label}: {len(trades)}t, {wins}W/{len(trades)-wins}L, WR={wr:.1f}%, PnL={pnl:+.1f}, Avg={avg:+.1f}")

    stats(sc_v12, "SC longs V12-base (all times)")
    stats(early, "SC longs BEFORE 10:00")
    stats(rest, "SC longs AFTER 10:00")

    # ── SC longs: 09:30-10:00 performance by outcome ──
    print(f"\n  SC longs before 10:00 — detail:")
    for t in early:
        print(f"    {t['trade_date']} {str(t['entry_time_et'])[:8]} grade={t['grade']} align={t['greek_alignment']} -> {t['outcome_result']} {t['outcome_pnl']:+.1f}")

    # ── What if we blocked SC longs first 30 min on ALL days? ──
    print(f"\n{'='*80}")
    print("  WHAT IF: Block SC longs before 10:00 on ALL days (not just gap days)?")
    print(f"{'='*80}")

    if early:
        blocked_pnl = sum(t['outcome_pnl'] for t in early)
        print(f"  Would block: {len(early)} trades, PnL={blocked_pnl:+.1f}")
        print(f"  If blocked PnL is negative → filter helps. If positive → filter hurts.")

if __name__ == "__main__":
    main()
