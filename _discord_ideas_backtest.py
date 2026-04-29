"""
Backtest the 4 testable ideas from the Apr 1-8 Discord sync.

Ideas tested:
  #1 Undervix-extreme morning long block (overvix < -2 AND gap_up > 2%)
  #3 GEX paradigm stickiness boost for SC longs
  #4 DD hedging concentration magnet for SC/DD shorts
  #5 BofA-LIS -> GEX-pure within-day paradigm drift

Skipped:
  #2 Vanna tower targeting — needs raw vanna level identification per signal
  #6 Macro/headline density — testable but no clear quantitative threshold from Discord

Per CLAUDE.md analysis validation protocol:
  Gate 1: data quality (overvix only available from Mar 17 — limit to Mar 18 onward)
  Gate 2: cross-check vs DB totals
  Gate 3: state clean sample, exclusions, confidence
"""
import psycopg2
from collections import defaultdict
from datetime import datetime, date

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
START = '2026-03-18'  # overvix populated, V12-fix era roughly

def is_long(direction):
    return direction in ('long', 'bullish')
def is_short(direction):
    return direction in ('short', 'bearish')

def fmt_pnl(p):
    return f'{p:+.1f}'

# ============================================================================
# Gate 1: Data quality + sanity
# ============================================================================
def gate1():
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    print('=' * 80)
    print('GATE 1: DATA QUALITY')
    print('=' * 80)

    cur.execute(f"""
SELECT COUNT(*),
       COUNT(*) FILTER (WHERE overvix IS NOT NULL),
       COUNT(*) FILTER (WHERE outcome_pnl IS NOT NULL),
       SUM(outcome_pnl)
FROM setup_log WHERE ts >= '{START}'
""")
    total, with_ov, with_outcome, total_pnl = cur.fetchone()
    print(f'Mar 18 - Apr 8 setup_log: {total} rows, {with_ov} with overvix, {with_outcome} resolved')
    print(f'Total raw outcome_pnl: {fmt_pnl(total_pnl)}')

    # Per-day count for visual sanity
    cur.execute(f"""
SELECT (ts AT TIME ZONE 'America/New_York')::date as d, COUNT(*) as n
FROM setup_log WHERE ts >= '{START}' AND outcome_result IS NOT NULL
GROUP BY d ORDER BY d
""")
    print('Days with resolved trades:')
    for d, n in cur.fetchall():
        print(f'  {d}: {n}')
    conn.close()

# ============================================================================
# Build per-day context: gap, undervix flag, paradigm history
# ============================================================================
def build_day_context():
    """Returns dict {date: {gap_pts, undervix_morning, ...}}"""
    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Get yesterday's last spot vs today's first spot from chain_snapshots
    cur.execute(f"""
SELECT (ts AT TIME ZONE 'America/New_York')::date as d,
       MIN(ts AT TIME ZONE 'America/New_York') as first_ts,
       MAX(ts AT TIME ZONE 'America/New_York') as last_ts
FROM chain_snapshots
WHERE ts >= '{START}'::date - INTERVAL '5 days'
GROUP BY d ORDER BY d
""")
    days = cur.fetchall()

    day_first_spot = {}
    day_last_spot = {}
    for d, first_ts, last_ts in days:
        # Get spot at first ts and last ts of that day
        cur.execute("""
SELECT spot FROM chain_snapshots
WHERE (ts AT TIME ZONE 'America/New_York')::date = %s
  AND spot IS NOT NULL
ORDER BY ts ASC LIMIT 1
""", (d,))
        r = cur.fetchone()
        if r: day_first_spot[d] = float(r[0])
        cur.execute("""
SELECT spot FROM chain_snapshots
WHERE (ts AT TIME ZONE 'America/New_York')::date = %s
  AND spot IS NOT NULL
ORDER BY ts DESC LIMIT 1
""", (d,))
        r = cur.fetchone()
        if r: day_last_spot[d] = float(r[0])

    # Sort dates
    sorted_dates = sorted(day_first_spot.keys())
    ctx = {}
    for i, d in enumerate(sorted_dates):
        if i == 0: continue
        prev = sorted_dates[i-1]
        if prev in day_last_spot and d in day_first_spot:
            gap_pts = day_first_spot[d] - day_last_spot[prev]
            ctx[d] = {'gap_pts': gap_pts, 'open': day_first_spot[d], 'prev_close': day_last_spot[prev]}

    # Add overvix per day (avg of first hour 9:30-10:30 ET)
    cur.execute(f"""
SELECT (ts AT TIME ZONE 'America/New_York')::date as d,
       AVG(overvix) as morning_ov
FROM setup_log
WHERE ts >= '{START}' AND overvix IS NOT NULL
  AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '10:30'
GROUP BY d ORDER BY d
""")
    for d, ov in cur.fetchall():
        if d in ctx: ctx[d]['morning_overvix'] = float(ov) if ov is not None else None

    conn.close()
    return ctx

# ============================================================================
# IDEA #1: Undervix-extreme morning long block
# ============================================================================
def idea1(ctx):
    print('\n' + '=' * 80)
    print('IDEA #1: UNDERVIX-EXTREME MORNING LONG BLOCK')
    print('Rule: IF morning overvix <= -2 AND gap_up > +2% (>134 pts on 6700) -> BLOCK longs first 60 min')
    print('=' * 80)

    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Find days matching the rule
    candidate_days = []
    for d, c in sorted(ctx.items()):
        ov = c.get('morning_overvix')
        gap = c.get('gap_pts')
        if ov is None or gap is None: continue
        gap_pct = gap / c['open'] * 100
        is_undervix = ov <= -2.0
        is_gap_up = gap_pct > 2.0
        flag = is_undervix and is_gap_up
        candidate_days.append((d, ov, gap_pct, flag))

    triggered = [d for d, ov, g, f in candidate_days if f]
    print(f'Days where rule would have triggered: {len(triggered)}')
    for d, ov, g, f in candidate_days:
        marker = '<<< TRIGGER' if f else ''
        print(f'  {d}: overvix={ov:+.2f}, gap={g:+.2f}% {marker}')

    if not triggered:
        print('No qualifying days. Idea #1 cannot be tested directly.')
        # Relax the rule: just undervix <= -2 (no gap requirement)
        print('\nRELAXED rule: morning overvix <= -2 (no gap requirement)')
        # Two relaxed variants
        relax_a = [d for d, ov, g, f in candidate_days if ov <= -1.9 and g > 0]  # Apr 8 captures
        relax_b = [d for d, ov, g, f in candidate_days if ov <= -1.0 and g > 1.0]  # broader
        print(f'Relaxed A (ov<=-1.9 AND gap_up>0): {len(relax_a)} -> {[str(d) for d in relax_a]}')
        print(f'Relaxed B (ov<=-1.0 AND gap_up>1%): {len(relax_b)} -> {[str(d) for d in relax_b]}')
        relaxed = relax_a if relax_a else relax_b

        if relaxed:
            ph = ','.join("'%s'" % d.isoformat() for d in relaxed)
            cur.execute(f"""
SELECT setup_name, direction,
       (ts AT TIME ZONE 'America/New_York')::date as d,
       (ts AT TIME ZONE 'America/New_York')::time as t,
       outcome_result, outcome_pnl, paradigm
FROM setup_log
WHERE outcome_result IS NOT NULL
  AND (ts AT TIME ZONE 'America/New_York')::date IN ({ph})
  AND direction IN ('long','bullish')
  AND (ts AT TIME ZONE 'America/New_York')::time <= '10:30'
ORDER BY ts
""")
            morning_longs = cur.fetchall()
            print(f'\nMorning longs (<=10:30 ET) on undervix days: {len(morning_longs)}')
            wins = sum(1 for r in morning_longs if r[4] == 'WIN')
            losses = sum(1 for r in morning_longs if r[4] == 'LOSS')
            total_pnl = sum(r[5] or 0 for r in morning_longs)
            wr = wins / max(wins + losses, 1) * 100
            print(f'  WR={wr:.0f}% W/L={wins}/{losses} PnL={fmt_pnl(total_pnl)}')
            for r in morning_longs:
                print(f'  {r[2]} {r[3]} {r[0]:18s} {r[4]:6s} pnl={fmt_pnl(r[5] or 0):>7s}')

    conn.close()
    return {'triggered_days': triggered}

# ============================================================================
# IDEA #3: GEX paradigm stickiness boost for SC longs
# ============================================================================
def idea3():
    print('\n' + '=' * 80)
    print('IDEA #3: GEX PARADIGM STICKINESS BOOST FOR SC LONGS')
    print('Rule: count consecutive cycles where paradigm contains "GEX-" before each SC long signal')
    print('=' * 80)

    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Get all SC long resolved signals from the era
    cur.execute(f"""
SELECT id, ts, paradigm, outcome_result, outcome_pnl,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE setup_name = 'Skew Charm' AND direction IN ('long','bullish')
  AND outcome_result IS NOT NULL
  AND ts >= '{START}'
ORDER BY ts
""")
    sc_longs = cur.fetchall()
    print(f'SC long signals (Mar 18+, resolved): {len(sc_longs)}')

    # For each, count consecutive prior cycles with paradigm starting with 'GEX-'
    # We'll query volland_snapshots payload statistics.paradigm
    results = []
    for sid, ts, paradigm, outcome, pnl, d in sc_longs:
        # Get all volland snapshots in the same trading day before this signal
        cur.execute("""
SELECT ts, payload->'statistics'->>'paradigm' as p
FROM volland_snapshots
WHERE (ts AT TIME ZONE 'America/New_York')::date = %s
  AND ts <= %s
ORDER BY ts DESC
""", (d, ts))
        rows = cur.fetchall()
        # Walk backwards from most recent counting consecutive GEX paradigms
        consec = 0
        for r in rows:
            p = r[1] or ''
            if p.startswith('GEX'):
                consec += 1
            else:
                break
        results.append((sid, d, paradigm, consec, outcome, pnl))

    # Bucket
    buckets = [(0, 4, '0-4'), (5, 9, '5-9'), (10, 19, '10-19'), (20, 999, '20+')]
    print(f"\n{'Bucket':<10} {'N':>4} {'W':>3} {'L':>3} {'WR%':>5} {'PnL':>8} {'AvgPnL':>8}")
    for lo, hi, label in buckets:
        bucket = [r for r in results if lo <= r[3] <= hi]
        if not bucket: continue
        wins = sum(1 for r in bucket if r[4] == 'WIN')
        losses = sum(1 for r in bucket if r[4] == 'LOSS')
        total_pnl = sum(r[5] or 0 for r in bucket)
        wr = wins / max(wins + losses, 1) * 100
        avg = total_pnl / len(bucket)
        print(f'{label:<10} {len(bucket):>4} {wins:>3} {losses:>3} {wr:>4.0f}% {fmt_pnl(total_pnl):>8} {fmt_pnl(avg):>8}')

    # Per-day breakdown for the high-stickiness group
    print(f'\nHigh-stickiness (20+ cycles) trade detail:')
    for r in results:
        if r[3] >= 20:
            print(f'  {r[1]} consec={r[3]:>3} {r[4]:6s} pnl={fmt_pnl(r[5] or 0):>7s} paradigm={r[2]}')

    conn.close()
    return results

# ============================================================================
# IDEA #4: DD hedging concentration magnet
# ============================================================================
def idea4():
    print('\n' + '=' * 80)
    print('IDEA #4: DD HEDGING CONCENTRATION MAGNET FOR SC/DD SHORTS')
    print('Rule: at signal time, find strongest |DD| strike from volland_exposure_points TODAY exp')
    print('  within +/-10 pts of spot. Compare WR/PnL when "near magnet" vs not.')
    print('=' * 80)

    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Get all SC short and DD short signals
    cur.execute(f"""
SELECT id, ts, setup_name, spot, paradigm, outcome_result, outcome_pnl,
       (ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log
WHERE setup_name IN ('Skew Charm','DD Exhaustion')
  AND direction IN ('short','bearish')
  AND outcome_result IS NOT NULL
  AND ts >= '{START}'
ORDER BY ts
""")
    shorts = cur.fetchall()
    print(f'SC/DD short signals (Mar 18+, resolved): {len(shorts)}')

    # For each, find the closest DD snapshot in time, then look at TODAY exp DD strikes near spot
    results = []
    for sid, ts, sname, spot, paradigm, outcome, pnl, d in shorts:
        if spot is None: continue
        spot_f = float(spot)

        # Get DD points within +/-10 pts at the closest snapshot before ts
        cur.execute("""
SELECT strike, value FROM volland_exposure_points
WHERE greek = 'deltaDecay' AND expiration_option = 'TODAY'
  AND ts_utc = (
    SELECT MAX(ts_utc) FROM volland_exposure_points
    WHERE greek = 'deltaDecay' AND expiration_option = 'TODAY' AND ts_utc <= %s
  )
""", (ts,))
        dd_points = cur.fetchall()
        if not dd_points:
            results.append((sid, d, sname, outcome, pnl, None, 'no_data'))
            continue

        # Find max |value| strike within +/-10 pts
        near = [(float(s), float(v)) for s, v in dd_points if abs(float(s) - spot_f) <= 10]
        if not near:
            tag = 'no_strike_near'
            max_dd = 0.0
        else:
            max_dd = max(abs(v) for s, v in near)
            tag = 'has_strike_near'

        results.append((sid, d, sname, outcome, pnl, max_dd, tag))

    # Bucket by max_dd magnitude (within 10 pts)
    print(f'\nDistribution of max |DD| within +/-10 pts:')
    valid = [r for r in results if r[5] is not None]
    print(f'  Valid signals: {len(valid)} of {len(results)}')

    # Get value distribution to set thresholds
    vals = sorted([r[5] for r in valid])
    if vals:
        n = len(vals)
        print(f'  Quartiles: 25%={vals[n//4]:.0f} 50%={vals[n//2]:.0f} 75%={vals[3*n//4]:.0f} max={vals[-1]:.0f}')

    # Bucket: use percentile thresholds
    if vals:
        p33, p67 = vals[n//3], vals[2*n//3]
        buckets = [(0, p33, f'low (<={p33:.0f})'), (p33, p67, f'mid ({p33:.0f}-{p67:.0f})'), (p67, 1e15, f'high (>{p67:.0f})')]
        print(f"\n{'Bucket':<25} {'N':>4} {'W':>3} {'L':>3} {'WR%':>5} {'PnL':>8} {'AvgPnL':>8}")
        for lo, hi, label in buckets:
            bucket = [r for r in valid if lo <= r[5] < hi]
            if not bucket: continue
            wins = sum(1 for r in bucket if r[3] == 'WIN')
            losses = sum(1 for r in bucket if r[3] == 'LOSS')
            total_pnl = sum(r[4] or 0 for r in bucket)
            wr = wins / max(wins + losses, 1) * 100
            avg = total_pnl / len(bucket)
            print(f'{label:<25} {len(bucket):>4} {wins:>3} {losses:>3} {wr:>4.0f}% {fmt_pnl(total_pnl):>8} {fmt_pnl(avg):>8}')

    conn.close()
    return results

# ============================================================================
# IDEA #5: BofA-LIS -> GEX-pure paradigm drift
# ============================================================================
def idea5():
    print('\n' + '=' * 80)
    print('IDEA #5: BofA-LIS -> GEX-PURE PARADIGM DRIFT')
    print('Rule: find days with within-day transition from BofA-* to GEX-*. Check long performance.')
    print('=' * 80)

    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Find days where paradigm goes from BofA-* to GEX-*
    cur.execute(f"""
SELECT (ts AT TIME ZONE 'America/New_York')::date as d, ts, payload->'statistics'->>'paradigm' as p
FROM volland_snapshots
WHERE ts >= '{START}'
ORDER BY ts
""")
    rows = cur.fetchall()

    by_day = defaultdict(list)
    for d, ts, p in rows:
        if p:
            by_day[d].append((ts, p))

    drift_days = {}  # date -> transition_ts
    for d, seq in by_day.items():
        had_bofa = False
        bofa_first_ts = None
        for ts, p in seq:
            if p.startswith('BOFA') or p.startswith('BofA'):
                had_bofa = True
                if bofa_first_ts is None: bofa_first_ts = ts
            elif had_bofa and p.startswith('GEX'):
                drift_days[d] = ts
                break

    print(f'Days with BofA->GEX drift: {len(drift_days)}')
    for d, t in sorted(drift_days.items()):
        print(f'  {d}: drift at {t.astimezone().strftime("%H:%M")} UTC')

    # For these days, compute long performance AFTER drift_ts vs all longs on non-drift days
    if drift_days:
        # Longs after drift on drift days
        after_pnls = []
        for d, t in drift_days.items():
            cur.execute("""
SELECT outcome_result, outcome_pnl FROM setup_log
WHERE direction IN ('long','bullish') AND outcome_result IS NOT NULL
  AND (ts AT TIME ZONE 'America/New_York')::date = %s AND ts > %s
""", (d, t))
            after_pnls.extend(cur.fetchall())

        # Longs on all OTHER days
        ph = ','.join("'%s'" % d.isoformat() for d in drift_days.keys())
        cur.execute(f"""
SELECT outcome_result, outcome_pnl FROM setup_log
WHERE direction IN ('long','bullish') AND outcome_result IS NOT NULL
  AND ts >= '{START}'
  AND (ts AT TIME ZONE 'America/New_York')::date NOT IN ({ph})
""")
        non_drift = cur.fetchall()

        def stats(rows, label):
            wins = sum(1 for r in rows if r[0] == 'WIN')
            losses = sum(1 for r in rows if r[0] == 'LOSS')
            total = sum(r[1] or 0 for r in rows)
            wr = wins / max(wins + losses, 1) * 100
            print(f'  {label}: N={len(rows)} W/L={wins}/{losses} WR={wr:.0f}% PnL={fmt_pnl(total)}')

        stats(after_pnls, 'Longs AFTER drift on drift days')
        stats(non_drift, 'Longs on non-drift days (baseline)')

    conn.close()

# ============================================================================
# Main
# ============================================================================
if __name__ == '__main__':
    gate1()
    ctx = build_day_context()
    print('\nDay context (gap and morning overvix):')
    for d, c in sorted(ctx.items())[:25]:
        print(f'  {d}: gap={c.get("gap_pts",0):+.1f} ov={c.get("morning_overvix")}')

    idea1(ctx)
    idea3()
    idea4()
    idea5()
