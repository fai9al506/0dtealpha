"""V12 filtered stats after cleanup."""
import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["DATABASE_URL"])

with e.connect() as c:
    # Get all resolved trades with filter-relevant fields
    rows = c.execute(text("""
        SELECT id, setup_name, direction, grade, outcome_result, outcome_pnl,
               greek_alignment, vix, overvix, paradigm,
               ts AT TIME ZONE 'America/New_York' as t
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        ORDER BY ts
    """)).mappings().all()

    # V12 filter logic (mirrors _passes_live_filter)
    def passes_v12(r):
        name = r['setup_name']
        d = r['direction']
        align = int(r['greek_alignment']) if r['greek_alignment'] is not None else 0
        vix = float(r['vix']) if r['vix'] else None
        overvix = float(r['overvix']) if r['overvix'] else None
        paradigm = r['paradigm'] or ''
        grade = r['grade'] or ''
        t = r['t']
        hr, mn = t.hour, t.minute
        t_min = hr * 60 + mn

        # SC grade gate: only A+/A/B
        if name == 'Skew Charm' and grade in ('C', 'LOG'):
            return False

        # SC/DD blocked 14:30-15:00 (charm dead zone)
        if name in ('Skew Charm', 'DD Exhaustion') and 14*60+30 <= t_min < 15*60:
            return False

        # SC/DD blocked 15:30+
        if name in ('Skew Charm', 'DD Exhaustion') and t_min >= 15*60+30:
            return False

        # BofA blocked after 14:30
        if name == 'BofA Scalp' and t_min >= 14*60+30:
            return False

        # SC/DD shorts blocked when paradigm = GEX-LIS
        if name in ('Skew Charm', 'DD Exhaustion') and d == 'bearish' and paradigm == 'GEX-LIS':
            return False

        # Gap-up longs block (V12) - can't check without gap data, skip this gate

        if d == 'bullish':
            # Longs: alignment >= +2 AND (SC exempt OR VIX <= 22 OR overvix >= +2)
            if align < 2:
                return False
            if name == 'Skew Charm':
                return True  # SC exempt from VIX gate
            if vix is not None and vix <= 22:
                return True
            if overvix is not None and overvix >= 2:
                return True
            return False
        else:
            # Shorts whitelist: SC (all), AG (all), DD (align!=0)
            if name == 'Skew Charm':
                return True
            if name == 'AG Short':
                return True
            if name == 'DD Exhaustion' and align != 0:
                return True
            return False

    # Apply filter
    filtered = [r for r in rows if passes_v12(r)]
    unfiltered_pnl = sum(float(r['outcome_pnl']) for r in rows if r['outcome_pnl'])

    print(f"=== V12 Filter Results (after cleanup) ===\n")
    print(f"Unfiltered: {len(rows)} trades, PnL={unfiltered_pnl:+.1f}")
    print(f"V12 filtered: {len(filtered)} trades\n")

    # Per-setup breakdown
    from collections import defaultdict
    stats = defaultdict(lambda: {'t': 0, 'w': 0, 'l': 0, 'x': 0, 'pnl': 0})
    for r in filtered:
        s = stats[r['setup_name']]
        pnl = float(r['outcome_pnl']) if r['outcome_pnl'] else 0
        s['t'] += 1
        s['pnl'] += pnl
        if r['outcome_result'] == 'WIN': s['w'] += 1
        elif r['outcome_result'] == 'LOSS': s['l'] += 1
        else: s['x'] += 1

    print(f"{'Setup':22s} | {'Trades':>6s} | {'W':>3s} | {'L':>3s} | {'X':>3s} | {'WR':>5s} | {'PnL':>8s}")
    print("-" * 75)
    gt = gw = gl = gx = 0
    gpnl = 0
    for name in sorted(stats, key=lambda n: -stats[n]['pnl']):
        s = stats[name]
        wr = f"{100*s['w']/(s['w']+s['l']):.0f}%" if (s['w']+s['l']) > 0 else "n/a"
        print(f"{name:22s} | {s['t']:6d} | {s['w']:3d} | {s['l']:3d} | {s['x']:3d} | {wr:>5s} | {s['pnl']:+8.1f}")
        gt += s['t']; gw += s['w']; gl += s['l']; gx += s['x']; gpnl += s['pnl']

    print("-" * 75)
    gwr = f"{100*gw/(gw+gl):.0f}%" if (gw+gl) > 0 else "n/a"
    print(f"{'V12 TOTAL':22s} | {gt:6d} | {gw:3d} | {gl:3d} | {gx:3d} | {gwr:>5s} | {gpnl:+8.1f}")
    print(f"\nV12 improvement: {gpnl - unfiltered_pnl:+.1f} pts ({gpnl:+.1f} vs {unfiltered_pnl:+.1f})")
