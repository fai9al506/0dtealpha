"""Backtest zone-revisit detection across all days in es_range_bars."""
import psycopg2, os, sys
from collections import defaultdict

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get all available dates and sources
cur.execute("""
SELECT DISTINCT trade_date, source, count(*) as bars
FROM es_range_bars
WHERE status = 'closed'
GROUP BY trade_date, source
ORDER BY trade_date, source
""")
date_info = cur.fetchall()
sys.stdout.write(f'=== Available data ===\n')
for d in date_info:
    sys.stdout.write(f'  {d[0]} source={d[1]} bars={d[2]}\n')

# Get all range bars, grouped by date+source
# IMPORTANT: filter by source to avoid contamination
cur.execute("""
SELECT trade_date, source, bar_idx, bar_open, bar_high, bar_low, bar_close,
       bar_volume, bar_delta, cvd_close,
       ts_start AT TIME ZONE 'America/New_York',
       ts_end AT TIME ZONE 'America/New_York'
FROM es_range_bars
WHERE status = 'closed'
ORDER BY trade_date, source, bar_idx
""")
rows = cur.fetchall()
conn.close()

# Group by (date, source)
days = defaultdict(list)
for r in rows:
    key = (str(r[0]), r[1])
    days[key].append({
        'idx': r[2], 'open': r[3], 'high': r[4], 'low': r[5],
        'close': r[6], 'volume': r[7], 'delta': r[8], 'cvd': r[9],
        'ts_s': r[10], 'ts_e': r[11],
    })

# Settings
RANGE_PTS = 5.0
MIN_AWAY = 5
VOL_WINDOW = 10
MIN_VOL_RATIO = 1.4
CVD_Z_MIN = 0.5
CVD_STD_WINDOW = 20

# Outcome evaluation: after signal, track price for N bars
# WIN = price moves target_pts in direction within max_bars
# LOSS = price moves stop_pts against direction within max_bars
TARGETS = [5, 10, 15]
STOP = 12
MAX_BARS = 40  # max bars to wait for outcome

all_signals = []

for (trade_date, source), bars in sorted(days.items()):
    if len(bars) < 30:
        continue

    zones = {}
    signals_today = []

    for i in range(1, len(bars)):
        trigger = bars[i]
        trigger_idx = trigger['idx']
        ts = trigger['ts_s']

        # Update zones for all prior bars (except trigger — checked first)
        for b in bars[:i]:
            zk = str(int(b['low'] // RANGE_PTS))
            if zk not in zones or zones[zk]['bar_idx'] < b['idx']:
                zones[zk] = {"cvd": b['cvd'], "bar_idx": b['idx'], "price": b['low']}

        # Skip pre-10AM ET
        if ts and ts.hour < 10:
            continue
        # Skip post-16:00 ET
        if ts and ts.hour >= 16:
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

        # Zone revisit check
        zone_key = str(int(trigger['low'] // RANGE_PTS))
        if zone_key in zones:
            prev = zones[zone_key]
            bars_away = trigger_idx - prev['bar_idx']
            if bars_away >= MIN_AWAY:
                cvd_diff = trigger['cvd'] - prev['cvd']
                cvd_z = abs(cvd_diff) / cvd_std
                if cvd_z >= CVD_Z_MIN:
                    direction = "bullish" if cvd_diff > 0 else "bearish"
                    pattern = "zone_accumulation" if cvd_diff > 0 else "zone_distribution"

                    # Evaluate outcome: track future bars
                    entry_price = trigger['close']
                    outcomes = {}
                    max_fav = 0
                    max_adv = 0
                    for j in range(i + 1, min(i + 1 + MAX_BARS, len(bars))):
                        fb = bars[j]
                        if direction == "bullish":
                            fav = fb['high'] - entry_price
                            adv = entry_price - fb['low']
                        else:
                            fav = entry_price - fb['low']
                            adv = fb['high'] - entry_price
                        max_fav = max(max_fav, fav)
                        max_adv = max(max_adv, adv)

                        for tgt in TARGETS:
                            k = f't{tgt}'
                            if k not in outcomes:
                                if fav >= tgt:
                                    outcomes[k] = 'WIN'
                                elif adv >= STOP:
                                    outcomes[k] = 'LOSS'
                        if 'stop' not in outcomes and adv >= STOP:
                            outcomes['stop'] = j - i

                    # Fill missing outcomes as EXPIRED
                    for tgt in TARGETS:
                        k = f't{tgt}'
                        if k not in outcomes:
                            outcomes[k] = 'EXPIRED'

                    sig = {
                        'date': trade_date,
                        'source': source,
                        'bar_idx': trigger_idx,
                        'bar_i': i,
                        'time': str(ts.time())[:8] if ts else '?',
                        'direction': direction,
                        'pattern': pattern,
                        'price': entry_price,
                        'zone': trigger['low'],
                        'prev_cvd': prev['cvd'],
                        'curr_cvd': trigger['cvd'],
                        'cvd_z': round(cvd_z, 2),
                        'vol_ratio': round(vol_ratio, 1),
                        'bars_away': bars_away,
                        'max_fav': round(max_fav, 2),
                        'max_adv': round(max_adv, 2),
                        'outcomes': outcomes,
                    }
                    signals_today.append(sig)
                    all_signals.append(sig)

        # Update zone with trigger
        zones[zone_key] = {"cvd": trigger['cvd'], "bar_idx": trigger_idx, "price": trigger['low']}

# === Results ===
sys.stdout.write(f'\n=== ZONE-REVISIT BACKTEST: {len(all_signals)} signals across {len(days)} day-sources ===\n\n')

# Per-signal detail
sys.stdout.write(f'{"date":>12} {"src":>7} {"time":>10} {"dir":>8} {"pattern":>20} {"price":>8} {"z":>5} {"vol":>5} {"away":>4} {"maxF":>6} {"maxA":>6} {"t5":>7} {"t10":>7} {"t15":>7}\n')
sys.stdout.write('=' * 140 + '\n')
for s in all_signals:
    sys.stdout.write(f'{s["date"]:>12} {s["source"]:>7} {s["time"]:>10} {s["direction"]:>8} {s["pattern"]:>20} '
                     f'{s["price"]:>8.2f} {s["cvd_z"]:>5.1f} {s["vol_ratio"]:>5.1f} {s["bars_away"]:>4} '
                     f'{s["max_fav"]:>6.1f} {s["max_adv"]:>6.1f} '
                     f'{s["outcomes"].get("t5","?"):>7} {s["outcomes"].get("t10","?"):>7} {s["outcomes"].get("t15","?"):>7}\n')

# Summary stats per target
sys.stdout.write(f'\n=== SUMMARY (all signals, stop={STOP}) ===\n')
for tgt in TARGETS:
    k = f't{tgt}'
    wins = sum(1 for s in all_signals if s['outcomes'].get(k) == 'WIN')
    losses = sum(1 for s in all_signals if s['outcomes'].get(k) == 'LOSS')
    expired = sum(1 for s in all_signals if s['outcomes'].get(k) == 'EXPIRED')
    total = wins + losses + expired
    wr = wins / total * 100 if total > 0 else 0
    pnl = wins * tgt + losses * (-STOP)
    sys.stdout.write(f'  Target {tgt:>2}pt: {wins}W/{losses}L/{expired}E ({total} total) WR={wr:.1f}% PnL={pnl:+.0f}\n')

# By direction
for d in ['bullish', 'bearish']:
    sigs = [s for s in all_signals if s['direction'] == d]
    sys.stdout.write(f'\n  --- {d.upper()} ({len(sigs)} signals) ---\n')
    for tgt in TARGETS:
        k = f't{tgt}'
        wins = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        losses = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        expired = sum(1 for s in sigs if s['outcomes'].get(k) == 'EXPIRED')
        total = wins + losses + expired
        wr = wins / total * 100 if total > 0 else 0
        pnl = wins * tgt + losses * (-STOP)
        sys.stdout.write(f'    Target {tgt:>2}pt: {wins}W/{losses}L/{expired}E WR={wr:.1f}% PnL={pnl:+.0f}\n')

# By pattern
for p in ['zone_accumulation', 'zone_distribution']:
    sigs = [s for s in all_signals if s['pattern'] == p]
    sys.stdout.write(f'\n  --- {p} ({len(sigs)} signals) ---\n')
    for tgt in TARGETS:
        k = f't{tgt}'
        wins = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        losses = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        expired = sum(1 for s in sigs if s['outcomes'].get(k) == 'EXPIRED')
        total = wins + losses + expired
        wr = wins / total * 100 if total > 0 else 0
        pnl = wins * tgt + losses * (-STOP)
        sys.stdout.write(f'    Target {tgt:>2}pt: {wins}W/{losses}L/{expired}E WR={wr:.1f}% PnL={pnl:+.0f}\n')

# By z-score bands
sys.stdout.write(f'\n=== BY Z-SCORE BAND ===\n')
z_bands = [(0.5, 1.0), (1.0, 2.0), (2.0, 5.0), (5.0, 100.0)]
for lo, hi in z_bands:
    sigs = [s for s in all_signals if lo <= s['cvd_z'] < hi]
    if not sigs:
        continue
    wins10 = sum(1 for s in sigs if s['outcomes'].get('t10') == 'WIN')
    losses10 = sum(1 for s in sigs if s['outcomes'].get('t10') == 'LOSS')
    total = len(sigs)
    wr = wins10 / total * 100 if total > 0 else 0
    avg_fav = sum(s['max_fav'] for s in sigs) / total
    sys.stdout.write(f'  z={lo:.1f}-{hi:.1f}: {total} sigs, t10 WR={wr:.1f}% ({wins10}W/{losses10}L), avg_maxFav={avg_fav:.1f}\n')

# By time of day
sys.stdout.write(f'\n=== BY HOUR ===\n')
for h in range(10, 17):
    sigs = [s for s in all_signals if s['time'].startswith(f'{h:02d}:')]
    if not sigs:
        continue
    wins10 = sum(1 for s in sigs if s['outcomes'].get('t10') == 'WIN')
    losses10 = sum(1 for s in sigs if s['outcomes'].get('t10') == 'LOSS')
    total = len(sigs)
    wr = wins10 / total * 100 if total > 0 else 0
    pnl = wins10 * 10 + losses10 * (-STOP)
    sys.stdout.write(f'  {h:02d}:xx: {total} sigs, t10 WR={wr:.1f}% PnL={pnl:+.0f}\n')

# Max favorable excursion distribution
sys.stdout.write(f'\n=== MAX FAVORABLE EXCURSION ===\n')
fav_bands = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 100)]
for lo, hi in fav_bands:
    count = sum(1 for s in all_signals if lo <= s['max_fav'] < hi)
    sys.stdout.write(f'  {lo:>2}-{hi:<3} pts: {count} signals\n')

sys.stdout.flush()
