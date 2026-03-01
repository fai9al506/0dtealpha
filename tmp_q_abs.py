import psycopg2, os, sys, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# 1. Range bars on Feb 25 (live source)
cur.execute("""
SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, bar_delta,
       bar_buy_volume, bar_sell_volume,
       cvd_open, cvd_high, cvd_low, cvd_close,
       ts_start AT TIME ZONE 'America/New_York' as ts_start_et,
       ts_end AT TIME ZONE 'America/New_York' as ts_end_et
FROM es_range_bars
WHERE trade_date = '2026-02-25'
  AND source = 'live'
ORDER BY bar_idx
""")
cols = [d[0] for d in cur.description]
rows = [dict(zip(cols, r)) for r in cur.fetchall()]
sys.stdout.write(f'Total range bars Feb 25 (live): {len(rows)}\n\n')

# Show ALL bars with volume analysis
sys.stdout.write(f'{"idx":>4} {"time_s":>12} {"time_e":>12} {"open":>8} {"high":>8} {"low":>8} {"close":>8} {"vol":>6} {"delta":>7} {"buy":>6} {"sell":>6} {"cvd_c":>8} {"vRatio":>6}\n')
sys.stdout.write('=' * 130 + '\n')

vols = [r['bar_volume'] for r in rows]
for i, r in enumerate(rows):
    ts = r['ts_start_et']
    te = r['ts_end_et']
    if not ts:
        continue
    # Rolling 10-bar avg volume
    start_i = max(0, i - 10)
    avg_vol = sum(vols[start_i:i]) / max(1, i - start_i) if i > 0 else vols[0]
    vol_ratio = r['bar_volume'] / avg_vol if avg_vol > 0 else 0
    marker = ' ***' if vol_ratio >= 1.4 else ''
    sys.stdout.write(f'{r["bar_idx"]:>4} {str(ts.time()):>12} {str(te.time()):>12} {r["bar_open"]:>8.2f} {r["bar_high"]:>8.2f} {r["bar_low"]:>8.2f} {r["bar_close"]:>8.2f} {r["bar_volume"]:>6} {r["bar_delta"]:>7} {r["bar_buy_volume"]:>6} {r["bar_sell_volume"]:>6} {r["cvd_close"]:>8} {vol_ratio:>6.1f}{marker}\n')

# 2. Check ES Absorption signals fired on Feb 25
sys.stdout.write('\n=== ES Absorption signals Feb 25 ===\n')
cur.execute("""
SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
       direction, grade, score, abs_vol_ratio, abs_es_price,
       outcome_result, outcome_pnl, comments
FROM setup_log
WHERE setup_name = 'ES Absorption'
  AND ts >= '2026-02-25 14:00:00 UTC'
  AND ts < '2026-02-26 00:00:00 UTC'
ORDER BY ts
""")
absorption_rows = cur.fetchall()
if not absorption_rows:
    sys.stdout.write('  (none)\n')
for r in absorption_rows:
    sys.stdout.write(f'  #{r[0]} {r[1]} {r[2]} grade={r[3]} score={r[4]} vol_ratio={r[5]} es={r[6]} result={r[7]} pnl={r[8]}\n')
    if r[9]:
        sys.stdout.write(f'    comments: {r[9][:200]}\n')

# 3. Identify potential swing points manually (pivot left=2, right=2)
sys.stdout.write('\n=== Manual swing detection (left=2, right=2) ===\n')
swings = []
for i in range(2, len(rows) - 2):
    lo = rows[i]['bar_low']
    hi = rows[i]['bar_high']
    # Check swing low: low <= both neighbors
    is_low = (lo <= rows[i-1]['bar_low'] and lo <= rows[i-2]['bar_low'] and
              lo <= rows[i+1]['bar_low'] and lo <= rows[i+2]['bar_low'])
    # Check swing high: high >= both neighbors
    is_high = (hi >= rows[i-1]['bar_high'] and hi >= rows[i-2]['bar_high'] and
               hi >= rows[i+1]['bar_high'] and hi >= rows[i+2]['bar_high'])
    if is_low:
        swings.append(('LOW', i, rows[i]))
    if is_high:
        swings.append(('HIGH', i, rows[i]))

for typ, i, r in swings:
    ts = r['ts_start_et']
    sys.stdout.write(f'  {typ:>4} idx={r["bar_idx"]:>3} {str(ts.time()):>12} price={r["bar_low"] if typ=="LOW" else r["bar_high"]:.2f} cvd={r["cvd_close"]} vol={r["bar_volume"]}\n')

# 4. Find consecutive same-type swings and check for divergence
sys.stdout.write('\n=== Swing-to-swing divergences ===\n')
prev_lows = []
prev_highs = []
for typ, i, r in swings:
    if typ == 'LOW':
        for pt, pi, pr in prev_lows:
            price_old = pr['bar_low']
            price_new = r['bar_low']
            cvd_old = pr['cvd_close']
            cvd_new = r['cvd_close']
            # Sell exhaustion: lower low + higher CVD
            if price_new < price_old and cvd_new > cvd_old:
                sys.stdout.write(f'  SELL_EXHAUSTION (BUY): idx {pr["bar_idx"]}->{r["bar_idx"]} price {price_old:.2f}->{price_new:.2f} cvd {cvd_old}->{cvd_new}\n')
            # Sell absorption: higher low + lower CVD
            elif price_new > price_old and cvd_new < cvd_old:
                sys.stdout.write(f'  SELL_ABSORPTION (BUY): idx {pr["bar_idx"]}->{r["bar_idx"]} price {price_old:.2f}->{price_new:.2f} cvd {cvd_old}->{cvd_new}\n')
        prev_lows.append((typ, i, r))
    else:
        for pt, pi, pr in prev_highs:
            price_old = pr['bar_high']
            price_new = r['bar_high']
            cvd_old = pr['cvd_close']
            cvd_new = r['cvd_close']
            # Buy exhaustion: higher high + lower CVD
            if price_new > price_old and cvd_new < cvd_old:
                sys.stdout.write(f'  BUY_EXHAUSTION (SELL): idx {pr["bar_idx"]}->{r["bar_idx"]} price {price_old:.2f}->{price_new:.2f} cvd {cvd_old}->{cvd_new}\n')
            # Buy absorption: lower high + higher CVD
            elif price_new < price_old and cvd_new > cvd_old:
                sys.stdout.write(f'  BUY_ABSORPTION (SELL): idx {pr["bar_idx"]}->{r["bar_idx"]} price {price_old:.2f}->{price_new:.2f} cvd {cvd_old}->{cvd_new}\n')
        prev_highs.append((typ, i, r))

sys.stdout.flush()
conn.close()
