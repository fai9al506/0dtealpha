"""
CRITICAL: simulate V12-fix filter on the SC/DD shorts, then test DD-magnet
on top. Real-world deployment impact = incremental beyond V12-fix.

V12-fix blocks for SC/DD shorts:
  - 14:30-15:00 ET (charm dead zone)
  - 15:30-16:00 ET (too little time)
  - paradigm contains 'GEX-LIS' (LIS = support floor for shorts)

We do NOT need the long-side V12-fix rules here.
"""
import psycopg2
from datetime import time

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
START = '2026-03-18'

def fmt(p): return f'{p:+.1f}'

def passes_v12fix(t, paradigm):
    """Returns True if SC/DD short should pass V12-fix."""
    # Block 14:30-15:00
    if time(14, 30) <= t <= time(15, 0):
        return False
    # Block 15:30+
    if t >= time(15, 30):
        return False
    # Block GEX-LIS
    if paradigm and 'GEX-LIS' in paradigm:
        return False
    return True

def main():
    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    cur.execute(f"""
SELECT s.id, s.ts, s.setup_name, s.spot, s.paradigm, s.outcome_result, s.outcome_pnl,
       (s.ts AT TIME ZONE 'America/New_York')::date as d,
       (s.ts AT TIME ZONE 'America/New_York')::time as t
FROM setup_log s
WHERE setup_name IN ('Skew Charm','DD Exhaustion')
  AND direction IN ('short','bearish')
  AND outcome_result IS NOT NULL
  AND ts >= '{START}'
ORDER BY ts
""")
    shorts = cur.fetchall()

    enriched = []
    for sid, ts, sname, spot, paradigm, outcome, pnl, d, t in shorts:
        if spot is None: continue
        spot_f = float(spot)
        cur.execute("""
SELECT MAX(ts_utc) FROM volland_exposure_points
WHERE greek = 'deltaDecay' AND expiration_option = 'TODAY' AND ts_utc < %s
""", (ts,))
        dd_ts = cur.fetchone()[0]
        if dd_ts is None: continue
        cur.execute("""
SELECT strike, value FROM volland_exposure_points
WHERE greek = 'deltaDecay' AND expiration_option = 'TODAY' AND ts_utc = %s
""", (dd_ts,))
        pts = cur.fetchall()
        near = [(float(s), float(v)) for s, v in pts if abs(float(s) - spot_f) <= 10]
        max_dd = max((abs(v) for s, v in near), default=0.0)
        passes = passes_v12fix(t, paradigm or '')
        enriched.append({
            'id': sid, 'ts': ts, 'd': d, 't': t, 'setup': sname,
            'paradigm': paradigm or '', 'outcome': outcome, 'pnl': pnl or 0,
            'max_dd': max_dd, 'v12fix_pass': passes
        })

    def stats(rows):
        wins = sum(1 for r in rows if r['outcome'] == 'WIN')
        losses = sum(1 for r in rows if r['outcome'] == 'LOSS')
        total = sum(r['pnl'] for r in rows)
        wr = wins / max(wins+losses, 1) * 100
        return f'N={len(rows)} W/L={wins}/{losses} WR={wr:.0f}% PnL={fmt(total)}'

    print('=' * 80)
    print('UNFILTERED (raw SC/DD shorts)')
    print('=' * 80)
    print(' ', stats(enriched))

    print('\n=== APPLY V12-FIX (14:30-15:00 + 15:30+ + GEX-LIS block) ===')
    after_v12 = [r for r in enriched if r['v12fix_pass']]
    blocked_v12 = [r for r in enriched if not r['v12fix_pass']]
    print(' After V12-fix:', stats(after_v12))
    print(' Blocked by V12-fix:', stats(blocked_v12))

    print('\n=== ON TOP OF V12-FIX, ADD |DD|>=2.3B BLOCK ===')
    THRESH = 2.3e9
    after_v12_lowdd = [r for r in after_v12 if r['max_dd'] < THRESH]
    after_v12_highdd = [r for r in after_v12 if r['max_dd'] >= THRESH]
    print(' KEPT (V12-fix pass + low DD):', stats(after_v12_lowdd))
    print(' BLOCKED (V12-fix pass but high DD):', stats(after_v12_highdd))

    base_pnl = sum(r['pnl'] for r in after_v12)
    new_pnl = sum(r['pnl'] for r in after_v12_lowdd)
    incr = new_pnl - base_pnl
    print(f'\n V12-fix only PnL: {fmt(base_pnl)}')
    print(f' V12-fix + DD block PnL: {fmt(new_pnl)}')
    print(f' INCREMENTAL improvement: {fmt(incr)} pts ({incr/abs(base_pnl)*100:+.0f}%)')

    print('\n=== THRESHOLD SWEEP ON V12-FIX-PASSED TRADES ===')
    for th in [1.0e9, 1.5e9, 2.0e9, 2.3e9, 2.5e9, 3.0e9, 3.5e9, 4.0e9]:
        kept = [r for r in after_v12 if r['max_dd'] < th]
        blocked = [r for r in after_v12 if r['max_dd'] >= th]
        kept_pnl = sum(r['pnl'] for r in kept)
        blocked_pnl = sum(r['pnl'] for r in blocked)
        wins_k = sum(1 for r in kept if r['outcome'] == 'WIN')
        losses_k = sum(1 for r in kept if r['outcome'] == 'LOSS')
        wr_k = wins_k/max(wins_k+losses_k,1)*100
        wins_b = sum(1 for r in blocked if r['outcome'] == 'WIN')
        losses_b = sum(1 for r in blocked if r['outcome'] == 'LOSS')
        wr_b = wins_b/max(wins_b+losses_b,1)*100
        print(f' th={th/1e9:.1f}B  kept N={len(kept):>3} WR={wr_k:>3.0f}% PnL={fmt(kept_pnl):>8}   '
              f'blocked N={len(blocked):>3} WR={wr_b:>3.0f}% PnL={fmt(blocked_pnl):>8}   '
              f'incr={fmt(kept_pnl - base_pnl):>7}')

    print('\n=== PER-DAY (V12-fix vs V12-fix + DD block) ===')
    from collections import defaultdict
    by_day = defaultdict(lambda: {'v12': [], 'v12_dd': [], 'block': []})
    for r in after_v12:
        by_day[r['d']]['v12'].append(r)
        if r['max_dd'] < THRESH:
            by_day[r['d']]['v12_dd'].append(r)
        else:
            by_day[r['d']]['block'].append(r)

    print(f'  {"Date":<12} {"V12-fix only":<18} {"V12+DD":<18} {"Blocked":<14} {"Incr"}')
    total_incr = 0.0
    for d in sorted(by_day.keys()):
        b = by_day[d]
        v12_pnl = sum(r['pnl'] for r in b['v12'])
        v12dd_pnl = sum(r['pnl'] for r in b['v12_dd'])
        blk_pnl = sum(r['pnl'] for r in b['block'])
        incr = v12dd_pnl - v12_pnl
        total_incr += incr
        print(f'  {str(d):<12} N={len(b["v12"]):<2}{fmt(v12_pnl):>9}   '
              f'N={len(b["v12_dd"]):<2}{fmt(v12dd_pnl):>9}   '
              f'N={len(b["block"]):<2}{fmt(blk_pnl):>9}   {fmt(incr)}')
    print(f'\n  Total incremental: {fmt(total_incr)} pts over {len(by_day)} days')

    # OOS check on V12-fix-passed trades
    print('\n=== OOS TEST (after V12-fix) ===')
    sorted_e = sorted(after_v12, key=lambda r: r['ts'])
    half = len(sorted_e) // 2
    train = sorted_e[:half]
    test = sorted_e[half:]
    print(f'Train: N={len(train)} ({train[0]["d"]} - {train[-1]["d"]})')
    print(f'Test:  N={len(test)} ({test[0]["d"]} - {test[-1]["d"]})')

    train_kept = [r for r in train if r['max_dd'] < THRESH]
    test_kept = [r for r in test if r['max_dd'] < THRESH]
    train_unfilt_pnl = sum(r['pnl'] for r in train)
    train_filt_pnl = sum(r['pnl'] for r in train_kept)
    test_unfilt_pnl = sum(r['pnl'] for r in test)
    test_filt_pnl = sum(r['pnl'] for r in test_kept)
    print(f' Train unfilt: {fmt(train_unfilt_pnl)} -> filt: {fmt(train_filt_pnl)}  incr: {fmt(train_filt_pnl - train_unfilt_pnl)}')
    print(f' Test  unfilt: {fmt(test_unfilt_pnl)} -> filt: {fmt(test_filt_pnl)}   incr: {fmt(test_filt_pnl - test_unfilt_pnl)}')

    conn.close()

if __name__ == '__main__':
    main()
