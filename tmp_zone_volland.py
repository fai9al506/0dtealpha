"""Cross-reference zone-revisit signals with Volland paradigm, LIS, and target."""
import psycopg2, os, sys, re
from collections import defaultdict

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get all range bars grouped by (date, source=live only)
cur.execute("""
SELECT trade_date, bar_idx, bar_open, bar_high, bar_low, bar_close,
       bar_volume, bar_delta, cvd_close,
       ts_start AT TIME ZONE 'America/New_York',
       ts_end AT TIME ZONE 'America/New_York'
FROM es_range_bars
WHERE status = 'closed' AND source = 'live'
ORDER BY trade_date, bar_idx
""")
rows = cur.fetchall()

days = defaultdict(list)
for r in rows:
    days[str(r[0])].append({
        'idx': r[1], 'open': r[2], 'high': r[3], 'low': r[4],
        'close': r[5], 'volume': r[6], 'delta': r[7], 'cvd': r[8],
        'ts_s': r[9], 'ts_e': r[10],
    })

# Get Volland snapshots (paradigm, LIS, target from payload->statistics)
cur.execute("""
SELECT ts AT TIME ZONE 'America/New_York' as ts_et,
       payload->'statistics' as stats
FROM volland_snapshots
WHERE payload->'statistics' IS NOT NULL
  AND payload->>'exposure_points_saved' != '0'
  AND ts >= '2026-02-18'
ORDER BY ts
""")
volland_rows = cur.fetchall()
conn.close()

# Parse Volland snapshots into timeline
volland_timeline = []
for ts_et, stats in volland_rows:
    if not stats or not isinstance(stats, dict):
        continue
    paradigm = (stats.get('paradigm') or '').strip()
    lis_raw = stats.get('lines_in_sand') or ''
    # Parse LIS value (take midpoint of range like "$6,935 - $6,955")
    lis_val = None
    lis_matches = re.findall(r'[\d,]+\.?\d*', lis_raw.replace(',', ''))
    if lis_matches:
        try:
            vals = [float(m) for m in lis_matches]
            lis_val = sum(vals) / len(vals)  # midpoint
        except:
            pass
    # Parse target
    target_raw = stats.get('target') or ''
    target_val = None
    if target_raw and target_raw != 'None':
        t_match = re.search(r'[\d,]+\.?\d*', str(target_raw).replace(',', ''))
        if t_match:
            try:
                target_val = float(t_match.group())
            except:
                pass

    volland_timeline.append({
        'ts': ts_et, 'paradigm': paradigm, 'lis': lis_val, 'target': target_val,
        'date': str(ts_et.date()) if ts_et else None,
    })

def find_volland(ts_et):
    """Find the closest Volland snapshot at or before the given timestamp."""
    best = None
    for v in volland_timeline:
        if v['ts'] and ts_et and v['ts'] <= ts_et:
            best = v
        elif v['ts'] and ts_et and v['ts'] > ts_et:
            break
    return best

# Settings
RANGE_PTS = 5.0
MIN_AWAY = 5
VOL_WINDOW = 10
MIN_VOL_RATIO = 1.4
CVD_Z_MIN = 0.5
CVD_STD_WINDOW = 20
STOP = 12
MAX_BARS = 40
TARGETS = [5, 10, 15]

all_signals = []

for trade_date, bars in sorted(days.items()):
    if len(bars) < 30:
        continue

    zones = {}

    for i in range(1, len(bars)):
        trigger = bars[i]
        trigger_idx = trigger['idx']
        ts = trigger['ts_s']

        for b in bars[:i]:
            zk = str(int(b['low'] // RANGE_PTS))
            if zk not in zones or zones[zk]['bar_idx'] < b['idx']:
                zones[zk] = {"cvd": b['cvd'], "bar_idx": b['idx'], "price": b['low']}

        if ts and ts.hour < 10:
            continue
        if ts and ts.hour >= 16:
            continue

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

        start_c = max(1, i - CVD_STD_WINDOW)
        deltas = [bars[j]['cvd'] - bars[j-1]['cvd'] for j in range(start_c, i + 1)]
        if len(deltas) < 5:
            continue
        mean_d = sum(deltas) / len(deltas)
        cvd_std = (sum((d - mean_d) ** 2 for d in deltas) / len(deltas)) ** 0.5
        if cvd_std < 1:
            cvd_std = 1

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
                    for tgt in TARGETS:
                        k = f't{tgt}'
                        if k not in outcomes:
                            outcomes[k] = 'EXPIRED'

                    # Get Volland data at signal time
                    vl = find_volland(ts) if ts else None
                    paradigm = vl['paradigm'] if vl else ''
                    lis = vl['lis'] if vl else None
                    target = vl['target'] if vl else None

                    # Compute LIS distance
                    # ES and SPX differ by ~15-20pts, use approximate SPX
                    spx_approx = entry_price - 17  # rough ES->SPX offset
                    lis_dist = abs(spx_approx - lis) if lis else None
                    target_dist = (target - spx_approx) if target else None  # positive = target above

                    # Paradigm alignment
                    para_upper = paradigm.upper()
                    if direction == "bullish":
                        para_aligned = "GEX" in para_upper
                    else:
                        para_aligned = "AG" in para_upper

                    # Is spot above or below LIS?
                    above_lis = (spx_approx > lis) if lis else None

                    sig = {
                        'date': trade_date,
                        'bar_idx': trigger_idx,
                        'time': str(ts.time())[:8] if ts else '?',
                        'direction': direction,
                        'pattern': pattern,
                        'price': entry_price,
                        'cvd_z': round(cvd_z, 2),
                        'vol_ratio': round(vol_ratio, 1),
                        'max_fav': round(max_fav, 2),
                        'max_adv': round(max_adv, 2),
                        'outcomes': outcomes,
                        'paradigm': paradigm,
                        'lis': lis,
                        'lis_dist': round(lis_dist, 1) if lis_dist is not None else None,
                        'above_lis': above_lis,
                        'target': target,
                        'target_dist': round(target_dist, 1) if target_dist is not None else None,
                        'para_aligned': para_aligned,
                    }
                    all_signals.append(sig)

        zones[zone_key] = {"cvd": trigger['cvd'], "bar_idx": trigger_idx, "price": trigger['low']}

# === Results ===
n = len(all_signals)
sys.stdout.write(f'=== ZONE-REVISIT + VOLLAND: {n} signals (live source only) ===\n\n')

# Detail
sys.stdout.write(f'{"date":>12} {"time":>10} {"dir":>8} {"price":>8} {"z":>5} {"paradigm":>15} {"aligned":>7} {"LIS":>8} {"lisDist":>7} {"abvLIS":>6} {"tgt":>8} {"tgtDist":>7} {"t5":>7} {"t10":>7} {"maxF":>6}\n')
sys.stdout.write('=' * 160 + '\n')
for s in all_signals:
    sys.stdout.write(f'{s["date"]:>12} {s["time"]:>10} {s["direction"]:>8} {s["price"]:>8.2f} {s["cvd_z"]:>5.1f} '
                     f'{s["paradigm"]:>15} {"YES" if s["para_aligned"] else "no":>7} '
                     f'{s["lis"] or 0:>8.0f} {str(s["lis_dist"]):>7} {"Y" if s["above_lis"] else "N" if s["above_lis"] is not None else "?":>6} '
                     f'{s["target"] or 0:>8.0f} {str(s["target_dist"]):>7} '
                     f'{s["outcomes"].get("t5","?"):>7} {s["outcomes"].get("t10","?"):>7} {s["max_fav"]:>6.1f}\n')

# === Paradigm alignment analysis ===
sys.stdout.write(f'\n=== BY PARADIGM ALIGNMENT (direction matches paradigm) ===\n')
for aligned_label, aligned_val in [("ALIGNED", True), ("NOT aligned", False)]:
    sigs = [s for s in all_signals if s['para_aligned'] == aligned_val]
    if not sigs:
        continue
    for tgt in TARGETS:
        k = f't{tgt}'
        w = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        l = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        e = sum(1 for s in sigs if s['outcomes'].get(k) == 'EXPIRED')
        t = w + l + e
        wr = w / t * 100 if t else 0
        pnl = w * tgt + l * (-STOP)
        sys.stdout.write(f'  {aligned_label:>12} t{tgt:>2}: {w}W/{l}L/{e}E ({t}) WR={wr:.1f}% PnL={pnl:+.0f}\n')

# === By paradigm type ===
sys.stdout.write(f'\n=== BY PARADIGM TYPE ===\n')
paradigms = set(s['paradigm'].upper().split('-')[0] if s['paradigm'] else 'NONE' for s in all_signals)
for p in sorted(paradigms):
    sigs = [s for s in all_signals if (s['paradigm'].upper().split('-')[0] if s['paradigm'] else 'NONE') == p]
    if not sigs:
        continue
    for tgt in [5, 10]:
        k = f't{tgt}'
        w = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        l = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        t = len(sigs)
        wr = w / t * 100 if t else 0
        pnl = w * tgt + l * (-STOP)
        sys.stdout.write(f'  {p:>12} ({t} sigs) t{tgt}: {w}W/{l}L WR={wr:.1f}% PnL={pnl:+.0f}\n')

# === LIS distance bands ===
sys.stdout.write(f'\n=== BY LIS DISTANCE ===\n')
lis_bands = [(0, 5), (5, 10), (10, 20), (20, 50)]
for lo, hi in lis_bands:
    sigs = [s for s in all_signals if s['lis_dist'] is not None and lo <= s['lis_dist'] < hi]
    if not sigs:
        continue
    for tgt in [5, 10]:
        k = f't{tgt}'
        w = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        l = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        t = len(sigs)
        wr = w / t * 100 if t else 0
        pnl = w * tgt + l * (-STOP)
        sys.stdout.write(f'  LIS {lo:>2}-{hi:<2}pts ({t} sigs) t{tgt}: {w}W/{l}L WR={wr:.1f}% PnL={pnl:+.0f}\n')

# === Above vs Below LIS ===
sys.stdout.write(f'\n=== ABOVE vs BELOW LIS ===\n')
for label, val in [("ABOVE LIS", True), ("BELOW LIS", False)]:
    sigs = [s for s in all_signals if s['above_lis'] == val]
    if not sigs:
        continue
    for tgt in [5, 10]:
        k = f't{tgt}'
        w = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        l = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        t = len(sigs)
        wr = w / t * 100 if t else 0
        pnl = w * tgt + l * (-STOP)
        sys.stdout.write(f'  {label:>12} ({t} sigs) t{tgt}: {w}W/{l}L WR={wr:.1f}% PnL={pnl:+.0f}\n')

# === Bullish above LIS vs bullish below LIS ===
sys.stdout.write(f'\n=== BULLISH: ABOVE vs BELOW LIS ===\n')
for label, val in [("ABOVE LIS", True), ("BELOW LIS", False)]:
    sigs = [s for s in all_signals if s['above_lis'] == val and s['direction'] == 'bullish']
    if not sigs:
        continue
    for tgt in [5, 10]:
        k = f't{tgt}'
        w = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        l = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        t = len(sigs)
        wr = w / t * 100 if t else 0
        pnl = w * tgt + l * (-STOP)
        avg_fav = sum(s['max_fav'] for s in sigs) / t
        sys.stdout.write(f'  Bull {label:>10} ({t} sigs) t{tgt}: {w}W/{l}L WR={wr:.1f}% PnL={pnl:+.0f} avgMaxFav={avg_fav:.1f}\n')

# === Target distance (bullish: how far above? if target is above spot) ===
sys.stdout.write(f'\n=== BULLISH: BY TARGET DISTANCE (target above spot) ===\n')
td_bands = [(-100, 0), (0, 10), (10, 20), (20, 40), (40, 100)]
for lo, hi in td_bands:
    sigs = [s for s in all_signals if s['direction'] == 'bullish' and s['target_dist'] is not None and lo <= s['target_dist'] < hi]
    if not sigs:
        continue
    for tgt in [5, 10]:
        k = f't{tgt}'
        w = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        l = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        t = len(sigs)
        wr = w / t * 100 if t else 0
        pnl = w * tgt + l * (-STOP)
        sys.stdout.write(f'  TgtDist {lo:>4}-{hi:<3}pts ({t} sigs) t{tgt}: {w}W/{l}L WR={wr:.1f}% PnL={pnl:+.0f}\n')

# === Combined: paradigm aligned + above LIS + morning ===
sys.stdout.write(f'\n=== BEST FILTER COMBOS (bullish only) ===\n')
bull = [s for s in all_signals if s['direction'] == 'bullish']

filters = {
    "All bullish": lambda s: True,
    "Morning (10-12)": lambda s: s['time'][:2] in ('10', '11'),
    "Para aligned": lambda s: s['para_aligned'],
    "Above LIS": lambda s: s['above_lis'] == True,
    "Morning+Aligned": lambda s: s['time'][:2] in ('10', '11') and s['para_aligned'],
    "Morning+AboveLIS": lambda s: s['time'][:2] in ('10', '11') and s['above_lis'] == True,
    "Aligned+AboveLIS": lambda s: s['para_aligned'] and s['above_lis'] == True,
    "Morning+Aligned+AboveLIS": lambda s: s['time'][:2] in ('10', '11') and s['para_aligned'] and s['above_lis'] == True,
    "LIS<10 dist": lambda s: s['lis_dist'] is not None and s['lis_dist'] < 10,
    "Morning+LIS<10": lambda s: s['time'][:2] in ('10', '11') and s['lis_dist'] is not None and s['lis_dist'] < 10,
}

for label, filt in filters.items():
    sigs = [s for s in bull if filt(s)]
    if not sigs:
        sys.stdout.write(f'  {label:>30}: 0 sigs\n')
        continue
    for tgt in [5, 10]:
        k = f't{tgt}'
        w = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        l = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        t = len(sigs)
        wr = w / t * 100 if t else 0
        pnl = w * tgt + l * (-STOP)
        avg_fav = sum(s['max_fav'] for s in sigs) / t
        sys.stdout.write(f'  {label:>30} ({t:>2} sigs) t{tgt}: {w}W/{l}L WR={wr:.1f}% PnL={pnl:+.0f} avgMF={avg_fav:.1f}\n')

sys.stdout.flush()
