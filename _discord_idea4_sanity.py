"""
Sanity-check the +248 pts finding for Idea #4 (DD hedging magnet).
Per CLAUDE.md: claims of >50% improvement are red flags — verify before deploying.

Checks:
  1. Look-ahead: confirm DD snapshot ts_utc < signal ts (not equal/after)
  2. Snapshot freshness: distribution of (signal_ts - dd_snapshot_ts)
  3. Out-of-sample: split temporally, train threshold on first half, test on second
  4. Time-of-day confound: bucket by time slot, see if high-DD is concentrated in bad slots
  5. Outlier check: distribution of per-trade PnL in HIGH bucket
  6. Trade-by-trade detail of HIGH bucket (60 trades)
"""
import psycopg2
from collections import defaultdict
from datetime import timedelta

DB = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
START = '2026-03-18'
THRESH = 2.3e9

def fmt(p): return f'{p:+.1f}'

def main():
    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Pull SC/DD shorts with the matching DD snapshot ts
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
    print(f'SC/DD shorts: {len(shorts)}')

    enriched = []
    stale_hist = []
    for sid, ts, sname, spot, paradigm, outcome, pnl, d, t in shorts:
        if spot is None: continue
        spot_f = float(spot)

        # Get the DD snapshot ts used and compute staleness
        cur.execute("""
SELECT MAX(ts_utc) FROM volland_exposure_points
WHERE greek = 'deltaDecay' AND expiration_option = 'TODAY' AND ts_utc < %s
""", (ts,))
        dd_ts = cur.fetchone()[0]
        if dd_ts is None:
            continue
        stale_sec = (ts - dd_ts).total_seconds()
        stale_hist.append(stale_sec)

        # Pull the DD points within +/-10 pts using ONLY snapshots STRICTLY before ts
        cur.execute("""
SELECT strike, value FROM volland_exposure_points
WHERE greek = 'deltaDecay' AND expiration_option = 'TODAY' AND ts_utc = %s
""", (dd_ts,))
        pts = cur.fetchall()
        near = [(float(s), float(v)) for s, v in pts if abs(float(s) - spot_f) <= 10]
        max_dd = max((abs(v) for s, v in near), default=0.0)

        enriched.append({
            'id': sid, 'ts': ts, 'd': d, 't': t, 'setup': sname, 'spot': spot_f,
            'paradigm': paradigm or '', 'outcome': outcome, 'pnl': pnl or 0,
            'max_dd': max_dd, 'stale_sec': stale_sec
        })

    print(f'Enriched (look-ahead-safe): {len(enriched)}')

    # ---- 1. Look-ahead check ----
    print('\n=== 1. LOOK-AHEAD CHECK ===')
    print('All DD snapshots are STRICTLY before signal ts (ts_utc < ts).')
    print(f'Confirmed: enriched count = {len(enriched)} (vs original 186)')

    # ---- 2. Staleness ----
    print('\n=== 2. SNAPSHOT FRESHNESS ===')
    if stale_hist:
        stale_hist.sort()
        n = len(stale_hist)
        print(f'  count={n}')
        print(f'  min={stale_hist[0]:.0f}s p25={stale_hist[n//4]:.0f}s p50={stale_hist[n//2]:.0f}s p75={stale_hist[3*n//4]:.0f}s p95={stale_hist[int(n*0.95)]:.0f}s max={stale_hist[-1]:.0f}s')
        # How many are >5min stale?
        stale_5 = sum(1 for s in stale_hist if s > 300)
        stale_10 = sum(1 for s in stale_hist if s > 600)
        print(f'  >5min stale: {stale_5} ({stale_5/n*100:.0f}%)')
        print(f'  >10min stale: {stale_10} ({stale_10/n*100:.0f}%)')

    # ---- 3. Out-of-sample temporal split ----
    print('\n=== 3. OUT-OF-SAMPLE TEMPORAL SPLIT ===')
    # Sort by date, split in half
    sorted_e = sorted(enriched, key=lambda r: r['ts'])
    half = len(sorted_e) // 2
    train = sorted_e[:half]
    test = sorted_e[half:]
    print(f'Train: {len(train)} trades ({train[0]["d"]} - {train[-1]["d"]})')
    print(f'Test:  {len(test)} trades ({test[0]["d"]} - {test[-1]["d"]})')

    def stats(rows):
        wins = sum(1 for r in rows if r['outcome'] == 'WIN')
        losses = sum(1 for r in rows if r['outcome'] == 'LOSS')
        total = sum(r['pnl'] for r in rows)
        wr = wins/max(wins+losses,1)*100
        return len(rows), wr, total

    # On train: try multiple thresholds, pick the one that maximizes filtered PnL
    print('\n  Train tuning:')
    best = (None, -1e9)
    for th in [1.0e9, 1.5e9, 2.0e9, 2.3e9, 2.5e9, 3.0e9, 3.5e9, 4.0e9]:
        kept = [r for r in train if r['max_dd'] < th]
        n_k, wr_k, p_k = stats(kept)
        print(f'    th={th/1e9:.1f}B  kept={n_k} WR={wr_k:.0f}% PnL={fmt(p_k)}')
        if p_k > best[1]:
            best = (th, p_k, n_k, wr_k)
    print(f'  Best train threshold: {best[0]/1e9:.1f}B (PnL={fmt(best[1])})')

    # Apply best train threshold to test
    print(f'\n  Apply train-best threshold {best[0]/1e9:.1f}B to TEST set:')
    test_unfilt = stats(test)
    test_kept = [r for r in test if r['max_dd'] < best[0]]
    test_blocked = [r for r in test if r['max_dd'] >= best[0]]
    n_kept, wr_kept, p_kept = stats(test_kept)
    n_blk, wr_blk, p_blk = stats(test_blocked)
    print(f'    Test unfiltered: N={test_unfilt[0]} WR={test_unfilt[1]:.0f}% PnL={fmt(test_unfilt[2])}')
    print(f'    Test KEPT (low DD): N={n_kept} WR={wr_kept:.0f}% PnL={fmt(p_kept)}')
    print(f'    Test BLOCKED:        N={n_blk} WR={wr_blk:.0f}% PnL={fmt(p_blk)}')
    improvement = p_kept - test_unfilt[2]
    print(f'    OOS improvement: {fmt(improvement)} pts')

    # Same with fixed 2.3B threshold
    print(f'\n  Apply FIXED 2.3B threshold to TEST set (no peeking):')
    test_kept_23 = [r for r in test if r['max_dd'] < 2.3e9]
    test_blocked_23 = [r for r in test if r['max_dd'] >= 2.3e9]
    n_kept, wr_kept, p_kept = stats(test_kept_23)
    n_blk, wr_blk, p_blk = stats(test_blocked_23)
    print(f'    Test KEPT: N={n_kept} WR={wr_kept:.0f}% PnL={fmt(p_kept)}')
    print(f'    Test BLOCKED: N={n_blk} WR={wr_blk:.0f}% PnL={fmt(p_blk)}')
    improvement = p_kept - test_unfilt[2]
    print(f'    OOS improvement: {fmt(improvement)} pts')

    # ---- 4. Time-of-day confound ----
    print('\n=== 4. TIME-OF-DAY CONFOUND CHECK ===')
    print('Bucket signals by time slot, separately for HIGH and LOW DD:')
    slots = [
        ('09:30-10:30', '09:30:00', '10:29:59'),
        ('10:30-12:00', '10:30:00', '11:59:59'),
        ('12:00-14:00', '12:00:00', '13:59:59'),
        ('14:00-15:30', '14:00:00', '15:29:59'),
        ('15:30-16:00', '15:30:00', '16:00:00'),
    ]
    print(f'  {"slot":<14} {"HIGH N/WR/PnL":<20} {"LOW N/WR/PnL":<20}')
    for label, lo, hi in slots:
        from datetime import time
        lo_t = time.fromisoformat(lo)
        hi_t = time.fromisoformat(hi)
        in_slot = [r for r in enriched if lo_t <= r['t'] <= hi_t]
        h = [r for r in in_slot if r['max_dd'] >= THRESH]
        l = [r for r in in_slot if r['max_dd'] < THRESH]
        n_h, wr_h, p_h = stats(h)
        n_l, wr_l, p_l = stats(l)
        print(f'  {label:<14} {f"{n_h}/{wr_h:.0f}%/{fmt(p_h)}":<20} {f"{n_l}/{wr_l:.0f}%/{fmt(p_l)}":<20}')

    # ---- 5. Outlier check ----
    print('\n=== 5. OUTLIER CHECK (HIGH DD bucket per-trade PnL) ===')
    high = [r for r in enriched if r['max_dd'] >= THRESH]
    pnls = sorted([r['pnl'] for r in high])
    n = len(pnls)
    print(f'  N={n}')
    if n:
        print(f'  min={pnls[0]:.1f} p10={pnls[n//10]:.1f} p25={pnls[n//4]:.1f} p50={pnls[n//2]:.1f} p75={pnls[3*n//4]:.1f} p90={pnls[9*n//10]:.1f} max={pnls[-1]:.1f}')
        print(f'  mean={sum(pnls)/n:+.2f}')
        # Drop top/bottom 10%, recompute
        trimmed = pnls[n//10 : n - n//10]
        print(f'  Trimmed mean (10/90 cut): {sum(trimmed)/len(trimmed):+.2f}')

    # Same for LOW
    print('\n  LOW DD bucket per-trade PnL:')
    low = [r for r in enriched if r['max_dd'] < THRESH]
    pnls_l = sorted([r['pnl'] for r in low])
    n_l = len(pnls_l)
    if n_l:
        print(f'  N={n_l}')
        print(f'  min={pnls_l[0]:.1f} p25={pnls_l[n_l//4]:.1f} p50={pnls_l[n_l//2]:.1f} p75={pnls_l[3*n_l//4]:.1f} max={pnls_l[-1]:.1f}')
        print(f'  mean={sum(pnls_l)/n_l:+.2f}')

    # ---- 6. Trade detail of HIGH bucket ----
    print('\n=== 6. HIGH DD BUCKET TRADE DETAIL ===')
    print(f'  {"date":<11} {"time":<9} {"setup":<14} {"paradigm":<13} {"|DD|/1B":>8} {"out":<8} {"pnl":>7}')
    for r in sorted(high, key=lambda x: x['ts']):
        print(f'  {str(r["d"]):<11} {str(r["t"])[:8]:<9} {r["setup"]:<14} {r["paradigm"]:<13} {r["max_dd"]/1e9:>8.2f} {r["outcome"]:<8} {fmt(r["pnl"]):>7}')

    conn.close()

if __name__ == '__main__':
    main()
