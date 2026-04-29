"""
Follow-up validation for Idea #4 (DD hedging magnet) — the standout finding.

Questions:
  1. Does the high-DD signal add value BEYOND the existing GEX-LIS short block?
  2. Per-day breakdown (which days drove the signal?)
  3. Is the threshold robust if shifted +/- 20%?
  4. Does it work for SC and DD separately, or only combined?

Also: Idea #6 quick test — does intraday VIX stdev (proxy for headline density) correlate
with any setup performance?

And: cross-check Gate 2 — total raw PnL Mar 18-Apr 8 should match independent query.
"""
import psycopg2
from collections import defaultdict

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
START = '2026-03-18'

def fmt(p):
    return f'{p:+.1f}'

def stats(rows, label, has_pnl_idx=4, has_outcome_idx=3):
    wins = sum(1 for r in rows if r[has_outcome_idx] == 'WIN')
    losses = sum(1 for r in rows if r[has_outcome_idx] == 'LOSS')
    expired = sum(1 for r in rows if r[has_outcome_idx] == 'EXPIRED')
    total = sum(r[has_pnl_idx] or 0 for r in rows)
    wr = wins / max(wins + losses, 1) * 100
    avg = total / max(len(rows), 1)
    return f'{label}: N={len(rows)} W/L/E={wins}/{losses}/{expired} WR={wr:.0f}% PnL={fmt(total)} avg={fmt(avg)}'

def main():
    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    print('=' * 80)
    print('GATE 2: CROSS-CHECK')
    print('=' * 80)
    cur.execute(f"""
SELECT setup_name, COUNT(*), SUM(outcome_pnl)
FROM setup_log
WHERE outcome_result IS NOT NULL AND ts >= '{START}'
GROUP BY setup_name ORDER BY 3 DESC NULLS LAST
""")
    grand_total = 0.0
    grand_n = 0
    for s, n, p in cur.fetchall():
        p = p or 0
        grand_total += p
        grand_n += n
        print(f'  {s:25s} N={n:>4} PnL={fmt(p):>8}')
    print(f'  TOTAL: N={grand_n} PnL={fmt(grand_total)}')

    print('\n' + '=' * 80)
    print('IDEA #4 DEEP DIVE: DD HEDGING MAGNET')
    print('=' * 80)

    # Pull all SC/DD shorts with their max |DD| within +/-10 pts at signal time
    cur.execute(f"""
SELECT s.id, s.ts, s.setup_name, s.spot, s.paradigm, s.outcome_result, s.outcome_pnl,
       (s.ts AT TIME ZONE 'America/New_York')::date as d
FROM setup_log s
WHERE setup_name IN ('Skew Charm','DD Exhaustion')
  AND direction IN ('short','bearish')
  AND outcome_result IS NOT NULL
  AND ts >= '{START}'
ORDER BY ts
""")
    shorts = cur.fetchall()
    print(f'SC/DD shorts: {len(shorts)}')

    enriched = []
    for sid, ts, sname, spot, paradigm, outcome, pnl, d in shorts:
        if spot is None: continue
        spot_f = float(spot)
        cur.execute("""
SELECT strike, value FROM volland_exposure_points
WHERE greek = 'deltaDecay' AND expiration_option = 'TODAY'
  AND ts_utc = (
    SELECT MAX(ts_utc) FROM volland_exposure_points
    WHERE greek = 'deltaDecay' AND expiration_option = 'TODAY' AND ts_utc <= %s
  )
""", (ts,))
        pts = cur.fetchall()
        near = [(float(s), float(v)) for s, v in pts if abs(float(s) - spot_f) <= 10]
        max_dd = max((abs(v) for s, v in near), default=0.0)
        # Find sign of dominant DD
        if near:
            dom = max(near, key=lambda x: abs(x[1]))
            dd_sign = 'pos' if dom[1] > 0 else 'neg'
        else:
            dd_sign = 'none'
        enriched.append({
            'id': sid, 'ts': ts, 'setup': sname, 'spot': spot_f, 'paradigm': paradigm or '',
            'outcome': outcome, 'pnl': pnl or 0, 'd': d, 'max_dd': max_dd, 'dd_sign': dd_sign
        })

    # Use 2.3B threshold from prior analysis (top tercile)
    THRESH = 2.3e9

    print('\nQ1: High-DD signal vs paradigm filter overlap')
    high_dd = [r for r in enriched if r['max_dd'] >= THRESH]
    low_dd = [r for r in enriched if r['max_dd'] < THRESH]
    is_lis = lambda r: 'LIS' in r['paradigm']

    high_dd_lis = [r for r in high_dd if is_lis(r)]
    high_dd_notlis = [r for r in high_dd if not is_lis(r)]
    low_dd_lis = [r for r in low_dd if is_lis(r)]
    low_dd_notlis = [r for r in low_dd if not is_lis(r)]

    def stat_dict(rows, label):
        wins = sum(1 for r in rows if r['outcome'] == 'WIN')
        losses = sum(1 for r in rows if r['outcome'] == 'LOSS')
        expired = sum(1 for r in rows if r['outcome'] == 'EXPIRED')
        total = sum(r['pnl'] for r in rows)
        wr = wins / max(wins + losses, 1) * 100
        avg = total / max(len(rows), 1)
        print(f'  {label}: N={len(rows):>3} W/L/E={wins}/{losses}/{expired} WR={wr:.0f}% PnL={fmt(total):>8} avg={fmt(avg):>5}')

    stat_dict(high_dd_lis, '|DD|>=2.3B  AND  paradigm has LIS')
    stat_dict(high_dd_notlis, '|DD|>=2.3B  AND  paradigm NO LIS    <-- KEY: incremental signal')
    stat_dict(low_dd_lis, '|DD|<2.3B   AND  paradigm has LIS')
    stat_dict(low_dd_notlis, '|DD|<2.3B   AND  paradigm NO LIS    <-- baseline')

    print('\nQ2: Per setup_name (does the rule work for both SC and DD?)')
    for sname in ['Skew Charm', 'DD Exhaustion']:
        rows_high = [r for r in enriched if r['setup'] == sname and r['max_dd'] >= THRESH]
        rows_low = [r for r in enriched if r['setup'] == sname and r['max_dd'] < THRESH]
        stat_dict(rows_high, f'{sname:15s} HIGH DD')
        stat_dict(rows_low, f'{sname:15s} LOW DD')

    print('\nQ3: Threshold robustness')
    for thresh, label in [(1.5e9, '1.5B'), (2.0e9, '2.0B'), (2.3e9, '2.3B'), (3.0e9, '3.0B'), (4.0e9, '4.0B')]:
        high = [r for r in enriched if r['max_dd'] >= thresh]
        low = [r for r in enriched if r['max_dd'] < thresh]
        wins_h = sum(1 for r in high if r['outcome'] == 'WIN')
        losses_h = sum(1 for r in high if r['outcome'] == 'LOSS')
        wr_h = wins_h / max(wins_h + losses_h, 1) * 100
        pnl_h = sum(r['pnl'] for r in high)
        wins_l = sum(1 for r in low if r['outcome'] == 'WIN')
        losses_l = sum(1 for r in low if r['outcome'] == 'LOSS')
        wr_l = wins_l / max(wins_l + losses_l, 1) * 100
        pnl_l = sum(r['pnl'] for r in low)
        print(f'  thresh={label:>5}  HIGH N={len(high):>3} WR={wr_h:>3.0f}% PnL={fmt(pnl_h):>8}   '
              f'LOW N={len(low):>3} WR={wr_l:>3.0f}% PnL={fmt(pnl_l):>8}')

    print('\nQ4: Per-day breakdown')
    by_day = defaultdict(lambda: {'all': [], 'high_dd': [], 'low_dd': []})
    for r in enriched:
        by_day[r['d']]['all'].append(r)
        (by_day[r['d']]['high_dd'] if r['max_dd'] >= THRESH else by_day[r['d']]['low_dd']).append(r)

    print(f'  {"Date":<12} {"All":>10} {"HighDD":>15} {"LowDD":>15} {"Block savings":>15}')
    total_savings = 0.0
    for d in sorted(by_day.keys()):
        b = by_day[d]
        all_pnl = sum(r['pnl'] for r in b['all'])
        high_pnl = sum(r['pnl'] for r in b['high_dd'])
        low_pnl = sum(r['pnl'] for r in b['low_dd'])
        savings = -high_pnl  # blocking high_dd "saves" their losses (or removes wins)
        total_savings += savings
        print(f'  {str(d):<12} N={len(b["all"]):<2}{fmt(all_pnl):>6} '
              f'N={len(b["high_dd"]):<2}{fmt(high_pnl):>10} '
              f'N={len(b["low_dd"]):<2}{fmt(low_pnl):>10} {fmt(savings):>15}')
    print(f'  TOTAL block savings: {fmt(total_savings)}')

    # Total impact: PnL with vs without filter
    total_with_filter = sum(r['pnl'] for r in low_dd)
    total_unfiltered = sum(r['pnl'] for r in enriched)
    print(f'\n  Unfiltered total PnL (SC/DD shorts only): {fmt(total_unfiltered)}')
    print(f'  After |DD|>=2.3B block:                   {fmt(total_with_filter)}')
    print(f'  IMPROVEMENT:                               {fmt(total_with_filter - total_unfiltered)}')

    # Confidence interval (rough)
    n_blocked = len(high_dd)
    n_kept = len(low_dd)
    print(f'\n  Trades blocked: {n_blocked} ({n_blocked/len(enriched)*100:.0f}% of population)')
    print(f'  Trades kept:    {n_kept}')

    # ============================================================================
    # IDEA #6: Macro day proxy
    # ============================================================================
    print('\n' + '=' * 80)
    print('IDEA #6 (LIGHT TEST): VIX intraday stdev as headline-density proxy')
    print('Test: per-day, compute stdev of vix from setup_log signals. Group all setups.')
    print('=' * 80)

    cur.execute(f"""
SELECT (ts AT TIME ZONE 'America/New_York')::date as d,
       STDDEV(vix) as vix_std,
       AVG(vix) as vix_avg,
       COUNT(*) as n,
       SUM(outcome_pnl) as pnl,
       SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
       SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses
FROM setup_log
WHERE outcome_result IS NOT NULL AND ts >= '{START}' AND vix IS NOT NULL
GROUP BY d ORDER BY d
""")
    days = cur.fetchall()
    # Categorize by stdev tercile
    stds = sorted([r[1] for r in days if r[1] is not None])
    if stds:
        n = len(stds)
        p33, p67 = stds[n//3], stds[2*n//3]
        low_d, mid_d, high_d = [], [], []
        for d, std, avg, cnt, pnl, w, l in days:
            std = std or 0
            tup = (d, std, avg, cnt, pnl or 0, w, l)
            if std <= p33: low_d.append(tup)
            elif std <= p67: mid_d.append(tup)
            else: high_d.append(tup)

        for label, group in [('low_std (calm)', low_d), ('mid_std', mid_d), ('high_std (volatile/headlines)', high_d)]:
            n_t = sum(r[3] for r in group)
            tot_pnl = sum(r[4] for r in group)
            tot_w = sum(r[5] for r in group)
            tot_l = sum(r[6] for r in group)
            wr = tot_w / max(tot_w + tot_l, 1) * 100
            print(f'  {label:<32}: {len(group)} days, {n_t} trades, WR={wr:.0f}% PnL={fmt(tot_pnl)}')

        print('\n  Per-day raw:')
        for d, std, avg, cnt, pnl, w, l in days:
            wr = w / max(w + l, 1) * 100
            print(f'    {d}: vix_std={std or 0:.3f} avg={avg:.2f} N={cnt} WR={wr:>3.0f}% PnL={fmt(pnl or 0)}')

    conn.close()

if __name__ == '__main__':
    main()
