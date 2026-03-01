"""Simulate zone-revisit detection on Feb 25 data to verify it catches the 10:35 signal."""
import psycopg2, os, sys

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("""
SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, bar_delta,
       cvd_close, ts_start AT TIME ZONE 'America/New_York', ts_end AT TIME ZONE 'America/New_York'
FROM es_range_bars
WHERE trade_date = '2026-02-25' AND source = 'live'
ORDER BY bar_idx
""")
rows = cur.fetchall()
conn.close()

bars = []
for r in rows:
    bars.append({
        'idx': r[0], 'open': r[1], 'high': r[2], 'low': r[3],
        'close': r[4], 'volume': r[5], 'delta': r[6], 'cvd': r[7],
        'ts_s': r[8], 'ts_e': r[9], 'status': 'closed'
    })

# Simulate zone tracking and detection
RANGE_PTS = 5.0
MIN_AWAY = 5
VOL_WINDOW = 10
MIN_VOL_RATIO = 1.4
CVD_Z_MIN = 0.5
CVD_STD_WINDOW = 20

zones = {}  # {zone_key: {"cvd": float, "bar_idx": int, "price": float}}
signals = []

for i in range(1, len(bars)):
    trigger = bars[i]
    trigger_idx = trigger['idx']
    ts = trigger['ts_s']

    # Skip pre-10AM
    if ts and ts.hour < 10:
        # Still update zones for pre-10AM bars (all except trigger)
        for b in bars[:i]:
            zk = str(int(b['low'] // RANGE_PTS))
            zones[zk] = {"cvd": b['cvd'], "bar_idx": b['idx'], "price": b['low']}
        continue

    # Volume gate
    start_v = max(0, i - VOL_WINDOW)
    recent_vols = [bars[j]['volume'] for j in range(start_v, i)]
    if not recent_vols:
        continue
    vol_avg = sum(recent_vols) / len(recent_vols)
    if vol_avg <= 0:
        continue
    vol_ratio = trigger['volume'] / vol_avg
    if vol_ratio < MIN_VOL_RATIO:
        # Update zones and continue
        zk = str(int(trigger['low'] // RANGE_PTS))
        zones[zk] = {"cvd": trigger['cvd'], "bar_idx": trigger_idx, "price": trigger['low']}
        continue

    # CVD std
    start_c = max(1, i - CVD_STD_WINDOW)
    deltas = [bars[j]['cvd'] - bars[j-1]['cvd'] for j in range(start_c, i + 1)]
    if len(deltas) < 5:
        continue
    mean_d = sum(deltas) / len(deltas)
    cvd_std = (sum((d - mean_d) ** 2 for d in deltas) / len(deltas)) ** 0.5
    if cvd_std < 1:
        cvd_std = 1

    # Zone revisit check (before updating zone with trigger)
    zone_key = str(int(trigger['low'] // RANGE_PTS))
    if zone_key in zones:
        prev = zones[zone_key]
        bars_away = trigger_idx - prev['bar_idx']
        if bars_away >= MIN_AWAY:
            cvd_diff = trigger['cvd'] - prev['cvd']
            cvd_z = abs(cvd_diff) / cvd_std
            if cvd_z >= CVD_Z_MIN:
                direction = "BULLISH" if cvd_diff > 0 else "BEARISH"
                pattern = "zone_accumulation" if cvd_diff > 0 else "zone_distribution"
                signals.append({
                    'bar_idx': trigger_idx,
                    'time': str(ts.time()) if ts else '?',
                    'direction': direction,
                    'pattern': pattern,
                    'zone': zone_key,
                    'price': trigger['low'],
                    'prev_cvd': prev['cvd'],
                    'curr_cvd': trigger['cvd'],
                    'cvd_diff': cvd_diff,
                    'cvd_z': round(cvd_z, 2),
                    'vol_ratio': round(vol_ratio, 1),
                    'bars_away': bars_away,
                    'prev_bar': prev['bar_idx'],
                })

    # Update zone with trigger bar
    zones[zone_key] = {"cvd": trigger['cvd'], "bar_idx": trigger_idx, "price": trigger['low']}

    # Also update zones for any skipped bars
    for b in bars[:i]:
        zk = str(int(b['low'] // RANGE_PTS))
        if zk not in zones or zones[zk]['bar_idx'] < b['idx']:
            zones[zk] = {"cvd": b['cvd'], "bar_idx": b['idx'], "price": b['low']}

sys.stdout.write(f'=== Zone-revisit signals (Feb 25, {len(signals)} found) ===\n')
for s in signals:
    sys.stdout.write(f'  bar {s["bar_idx"]:>3} {s["time"]:>15} {s["direction"]:>8} {s["pattern"]:>20} '
                     f'zone={s["price"]:.0f} prev_cvd={s["prev_cvd"]:>6} curr_cvd={s["curr_cvd"]:>6} '
                     f'diff={s["cvd_diff"]:>+7} z={s["cvd_z"]:.2f} vol={s["vol_ratio"]:.1f}x '
                     f'away={s["bars_away"]}bars (prev=bar{s["prev_bar"]})\n')

# Highlight the 10:30-10:45 window
sys.stdout.write(f'\n=== Signals in 10:30-10:45 window ===\n')
for s in signals:
    if '10:3' in s['time'] or '10:4' in s['time']:
        sys.stdout.write(f'  *** bar {s["bar_idx"]} {s["time"]} {s["direction"]} {s["pattern"]} '
                         f'z={s["cvd_z"]} vol={s["vol_ratio"]}x away={s["bars_away"]}\n')

sys.stdout.flush()
