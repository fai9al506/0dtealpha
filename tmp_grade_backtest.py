"""Backtest zone-revisit + swing detection with NEW grading (LIS side + target dir)
across all days in es_range_bars, cross-referencing Volland snapshots."""
import psycopg2, os, sys, re
from collections import defaultdict

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# ── 1. Load all range bars ──
cur.execute("""
SELECT trade_date, source, bar_idx, bar_open, bar_high, bar_low, bar_close,
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
        'idx': r[2], 'open': r[3], 'high': r[4], 'low': r[5],
        'close': r[6], 'volume': r[7], 'delta': r[8], 'cvd': r[9],
        'ts_s': r[10], 'ts_e': r[11],
    })

sys.stdout.write(f'Loaded {sum(len(v) for v in days.values())} bars across {len(days)} days\n')

# ── 2. Load Volland snapshots (paradigm, LIS, target, DD hedging) ──
# Get all snapshots with statistics, ordered by time
cur.execute("""
SELECT ts AT TIME ZONE 'America/New_York', payload
FROM volland_snapshots
WHERE payload->>'error_event' IS NULL
  AND payload->'statistics' IS NOT NULL
ORDER BY ts
""")
volland_rows = cur.fetchall()
conn.close()

# Parse Volland snapshots into a time-sorted list
volland_snaps = []
for vr in volland_rows:
    ts = vr[0]
    payload = vr[1] if isinstance(vr[1], dict) else {}
    stats = payload.get('statistics', {})
    if not stats or not isinstance(stats, dict):
        continue
    # Extract fields
    paradigm = (stats.get('paradigm') or '').upper()
    lis_str = stats.get('lines_in_sand') or stats.get('linesInSand') or ''
    target_str = stats.get('target') or ''
    dd_str = stats.get('delta_decay_hedging') or stats.get('deltaDecayHedging') or ''

    # Parse LIS value
    lis_val = None
    lis_match = re.search(r'[\d,]+\.?\d*', lis_str.replace(',', ''))
    if lis_match:
        lis_val = float(lis_match.group())

    # Parse target value
    target_val = None
    target_match = re.search(r'[\d,]+\.?\d*', str(target_str).replace('$', '').replace(',', ''))
    if target_match:
        target_val = float(target_match.group())

    volland_snaps.append({
        'ts': ts, 'paradigm': paradigm, 'lis': lis_val,
        'target': target_val, 'dd': dd_str,
    })

sys.stdout.write(f'Loaded {len(volland_snaps)} Volland snapshots\n\n')


def get_volland_at(ts):
    """Find the most recent Volland snapshot at or before ts."""
    best = None
    for vs in volland_snaps:
        if vs['ts'] <= ts:
            best = vs
        else:
            break
    return best


# ── 3. Detection settings ──
RANGE_PTS = 5.0
MIN_AWAY = 5
VOL_WINDOW = 10
MIN_VOL_RATIO = 1.4
CVD_Z_MIN = 0.5
CVD_STD_WINDOW = 20
PIVOT_LEFT = 2
PIVOT_RIGHT = 2

# Outcome settings
TARGETS = [5, 10, 15]
STOP = 12
MAX_BARS = 40

# Grading weights (NEW — matches deployed code)
W_DIV = 25
W_VOL = 25
W_DD = 10
W_PARA = 10
W_LIS = 10
W_LIS_SIDE = 10
W_TARGET_DIR = 10
TOTAL_W = W_DIV + W_VOL + W_DD + W_PARA + W_LIS + W_LIS_SIDE + W_TARGET_DIR

# Grade thresholds
GRADE_THRESHOLDS = {"A+": 75, "A": 55, "B": 35}


def compute_grade(composite):
    if composite >= GRADE_THRESHOLDS["A+"]:
        return "A+"
    elif composite >= GRADE_THRESHOLDS["A"]:
        return "A"
    elif composite >= GRADE_THRESHOLDS["B"]:
        return "B"
    return "C"


def divergence_score(cvd_z, price_atr):
    """0-100 score from CVD z-score and price ATR."""
    z_pts = min(60, cvd_z / 3.0 * 60)
    p_pts = min(40, price_atr / 2.0 * 40)
    return min(100, z_pts + p_pts)


# ── 4. Run detection per day ──
all_signals = []

for trade_date, bars in sorted(days.items()):
    if len(bars) < 30:
        continue

    zones = {}
    swings = []
    last_swing_type = None

    for i in range(1, len(bars)):
        trigger = bars[i]
        trigger_idx = trigger['idx']
        ts = trigger['ts_s']

        # ── Update zones (all bars except trigger) ──
        for b in bars[:i]:
            zk = str(int(b['low'] // RANGE_PTS))
            if zk not in zones or zones[zk]['bar_idx'] < b['idx']:
                zones[zk] = {"cvd": b['cvd'], "bar_idx": b['idx'], "price": b['low']}

        # Skip outside 10:00-16:00
        if ts and (ts.hour < 10 or ts.hour >= 16):
            continue

        # ── Volume gate ──
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

        # ── CVD std ──
        start_c = max(1, i - CVD_STD_WINDOW)
        deltas = [bars[j]['cvd'] - bars[j-1]['cvd'] for j in range(start_c, i + 1)]
        if len(deltas) < 5:
            continue
        mean_d = sum(deltas) / len(deltas)
        cvd_std = (sum((d - mean_d) ** 2 for d in deltas) / len(deltas)) ** 0.5
        if cvd_std < 1:
            cvd_std = 1

        # ATR proxy
        atr_moves = [abs(bars[j]['close'] - bars[j-1]['close']) for j in range(start_c, min(start_c + CVD_STD_WINDOW, len(bars)))]
        atr = sum(atr_moves) / len(atr_moves) if atr_moves else 1.0
        if atr < 0.01:
            atr = 0.01

        # ── Pivot detection (simplified) ──
        if i >= PIVOT_LEFT + PIVOT_RIGHT:
            cand_idx = i - PIVOT_RIGHT
            cand = bars[cand_idx]
            # Check for swing low
            is_low = True
            for k in range(cand_idx - PIVOT_LEFT, cand_idx + PIVOT_RIGHT + 1):
                if k == cand_idx or k < 0 or k >= len(bars):
                    continue
                if bars[k]['low'] < cand['low']:
                    is_low = False
                    break
            # Check for swing high
            is_high = True
            for k in range(cand_idx - PIVOT_LEFT, cand_idx + PIVOT_RIGHT + 1):
                if k == cand_idx or k < 0 or k >= len(bars):
                    continue
                if bars[k]['high'] > cand['high']:
                    is_high = False
                    break

            if is_low and (last_swing_type != "L"):
                swings.append({"type": "L", "price": cand['low'], "cvd": cand['cvd'], "bar_idx": cand['idx']})
                last_swing_type = "L"
            elif is_high and (last_swing_type != "H"):
                swings.append({"type": "H", "price": cand['high'], "cvd": cand['cvd'], "bar_idx": cand['idx']})
                last_swing_type = "H"

        # ── Collect divergences ──
        bullish_divs = []
        bearish_divs = []

        # Swing-to-swing scan
        swing_lows = [s for s in swings if s['type'] == 'L']
        swing_highs = [s for s in swings if s['type'] == 'H']

        for si in range(1, len(swing_lows)):
            s1, s2 = swing_lows[si-1], swing_lows[si]
            if trigger_idx - s2['bar_idx'] > 40:
                continue
            cvd_gap = abs(s2['cvd'] - s1['cvd'])
            cvd_z = cvd_gap / cvd_std
            if cvd_z < CVD_Z_MIN:
                continue
            price_dist = abs(s2['price'] - s1['price'])
            price_atr = price_dist / atr
            score = divergence_score(cvd_z, price_atr)
            if s2['price'] < s1['price'] and s2['cvd'] > s1['cvd']:
                bullish_divs.append({'pattern': 'sell_exhaustion', 'cvd_z': cvd_z, 'score': score, 'price_atr': price_atr})
            elif s2['price'] >= s1['price'] and s2['cvd'] < s1['cvd']:
                bullish_divs.append({'pattern': 'sell_absorption', 'cvd_z': cvd_z, 'score': score, 'price_atr': price_atr})

        for si in range(1, len(swing_highs)):
            s1, s2 = swing_highs[si-1], swing_highs[si]
            if trigger_idx - s2['bar_idx'] > 40:
                continue
            cvd_gap = abs(s2['cvd'] - s1['cvd'])
            cvd_z = cvd_gap / cvd_std
            if cvd_z < CVD_Z_MIN:
                continue
            price_dist = abs(s2['price'] - s1['price'])
            price_atr = price_dist / atr
            score = divergence_score(cvd_z, price_atr)
            if s2['price'] > s1['price'] and s2['cvd'] < s1['cvd']:
                bearish_divs.append({'pattern': 'buy_exhaustion', 'cvd_z': cvd_z, 'score': score, 'price_atr': price_atr})
            elif s2['price'] <= s1['price'] and s2['cvd'] > s1['cvd']:
                bearish_divs.append({'pattern': 'buy_absorption', 'cvd_z': cvd_z, 'score': score, 'price_atr': price_atr})

        # Zone-revisit scan
        zone_key = str(int(trigger['low'] // 5.0))
        if zone_key in zones:
            prev = zones[zone_key]
            bars_away = trigger_idx - prev['bar_idx']
            if bars_away >= MIN_AWAY:
                cvd_diff = trigger['cvd'] - prev['cvd']
                cvd_z = abs(cvd_diff) / cvd_std if cvd_std > 0 else 0
                if cvd_z >= CVD_Z_MIN:
                    zone_score = round(min(100, cvd_z / 3.0 * 100), 1)
                    if cvd_diff > 0:
                        bullish_divs.append({'pattern': 'zone_accumulation', 'cvd_z': cvd_z, 'score': zone_score, 'price_atr': 0})
                    else:
                        bearish_divs.append({'pattern': 'zone_distribution', 'cvd_z': cvd_z, 'score': zone_score, 'price_atr': 0})

        # Update zone with trigger
        zones[zone_key] = {"cvd": trigger['cvd'], "bar_idx": trigger_idx, "price": trigger['low']}

        # Pick best direction
        best_bull = max(bullish_divs, key=lambda d: d['score']) if bullish_divs else None
        best_bear = max(bearish_divs, key=lambda d: d['score']) if bearish_divs else None

        if not best_bull and not best_bear:
            continue

        if best_bull and best_bear:
            if best_bull['score'] >= best_bear['score']:
                direction, best = 'bullish', best_bull
            else:
                direction, best = 'bearish', best_bear
        elif best_bull:
            direction, best = 'bullish', best_bull
        else:
            direction, best = 'bearish', best_bear

        # ── Volume raw score ──
        if vol_ratio >= 3.0:
            vol_raw = 3
        elif vol_ratio >= 2.0:
            vol_raw = 2
        else:
            vol_raw = 1

        # ── Volland confluence ──
        dd_raw = 0
        para_raw = 0
        lis_raw = 0
        lis_side_raw = 0
        target_dir_raw = 0
        lis_val = None
        lis_dist = None
        target_val = None
        paradigm_str = ""
        dd_hedging = ""

        # ES price ~ SPX + ~15-20 pts offset. Use trigger close as ES price
        # and approximate SPX = ES - 17
        es_price = trigger['close']
        spx_approx = es_price - 17  # rough offset

        if ts:
            vsnap = get_volland_at(ts)
            if vsnap:
                paradigm_str = vsnap['paradigm']
                dd_hedging = vsnap['dd']
                lis_val = vsnap['lis']
                target_val = vsnap['target']

                # DD hedging alignment
                if direction == 'bullish' and 'long' in dd_hedging.lower():
                    dd_raw = 1
                elif direction == 'bearish' and 'short' in dd_hedging.lower():
                    dd_raw = 1

                # Paradigm alignment
                if direction == 'bullish' and 'GEX' in paradigm_str:
                    para_raw = 1
                elif direction == 'bearish' and 'AG' in paradigm_str:
                    para_raw = 1

                # LIS proximity (using SPX approx)
                if lis_val is not None:
                    lis_dist = abs(spx_approx - lis_val)
                    if lis_dist <= 5:
                        lis_raw = 2
                    elif lis_dist <= 15:
                        lis_raw = 1

                    # LIS side scoring (NEW)
                    if direction == 'bullish' and spx_approx < lis_val:
                        lis_side_raw = 2
                    elif direction == 'bullish' and spx_approx <= lis_val + 5:
                        lis_side_raw = 1
                    elif direction == 'bearish' and spx_approx > lis_val:
                        lis_side_raw = 2
                    elif direction == 'bearish' and spx_approx >= lis_val - 5:
                        lis_side_raw = 1

                # Target direction scoring (NEW)
                if target_val is not None:
                    target_above = target_val > spx_approx
                    if direction == 'bullish' and target_above:
                        target_dir_raw = 2
                    elif direction == 'bearish' and not target_above:
                        target_dir_raw = 2

        # ── Composite score (NEW weights) ──
        div_score = best['score']
        vol_score = {1: 33, 2: 67, 3: 100}.get(vol_raw, 33)
        dd_score = 100 if dd_raw else 0
        para_score = 100 if para_raw else 0
        lis_score = {0: 0, 1: 50, 2: 100}.get(lis_raw, 0)
        lis_side_score = {0: 0, 1: 50, 2: 100}.get(lis_side_raw, 0)
        target_dir_score = {0: 0, 2: 100}.get(target_dir_raw, 0)

        composite = (
            div_score * W_DIV + vol_score * W_VOL + dd_score * W_DD
            + para_score * W_PARA + lis_score * W_LIS
            + lis_side_score * W_LIS_SIDE + target_dir_score * W_TARGET_DIR
        ) / TOTAL_W

        # OLD composite (for comparison — original 5 factors)
        old_lis_score = {0: 0, 1: 50, 2: 100}.get(lis_raw, 0)
        old_composite = (
            div_score * 25 + vol_score * 25 + dd_score * 15
            + para_score * 15 + old_lis_score * 20
        ) / 100

        grade = compute_grade(composite)
        old_grade = compute_grade(old_composite)

        # ── Evaluate outcome ──
        entry_price = trigger['close']
        outcomes = {}
        max_fav = 0
        max_adv = 0
        for j in range(i + 1, min(i + 1 + MAX_BARS, len(bars))):
            fb = bars[j]
            if direction == 'bullish':
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

        sig = {
            'date': trade_date,
            'bar_idx': trigger_idx,
            'bar_i': i,
            'time': str(ts.time())[:8] if ts else '?',
            'direction': direction,
            'pattern': best['pattern'],
            'price': entry_price,
            'cvd_z': round(best['cvd_z'], 2),
            'vol_ratio': round(vol_ratio, 1),
            # Grading
            'composite': round(composite, 1),
            'old_composite': round(old_composite, 1),
            'grade': grade,
            'old_grade': old_grade,
            'div_score': round(div_score, 1),
            'vol_raw': vol_raw,
            'dd_raw': dd_raw,
            'para_raw': para_raw,
            'lis_raw': lis_raw,
            'lis_side_raw': lis_side_raw,
            'target_dir_raw': target_dir_raw,
            'paradigm': paradigm_str[:12],
            'lis_val': lis_val,
            'lis_dist': round(lis_dist, 1) if lis_dist is not None else None,
            'target_val': target_val,
            # Outcomes
            'max_fav': round(max_fav, 2),
            'max_adv': round(max_adv, 2),
            'outcomes': outcomes,
        }
        all_signals.append(sig)


# ════════════════════════ RESULTS ════════════════════════

sys.stdout.write(f'\n{"="*120}\n')
sys.stdout.write(f'GRADING BACKTEST: {len(all_signals)} signals across {len(days)} days\n')
sys.stdout.write(f'{"="*120}\n\n')

# ── Per-signal detail ──
hdr = f'{"date":>12} {"time":>8} {"dir":>7} {"pattern":>18} {"price":>8} {"z":>5} {"vol":>4} '
hdr += f'{"grade":>5} {"old":>5} {"comp":>5} {"oldC":>5} '
hdr += f'{"DD":>2} {"Pa":>2} {"LIS":>3} {"Side":>4} {"Tgt":>3} '
hdr += f'{"maxF":>6} {"maxA":>6} {"t5":>7} {"t10":>7} {"t15":>7}'
sys.stdout.write(hdr + '\n')
sys.stdout.write('-' * 150 + '\n')

for s in all_signals:
    line = f'{s["date"]:>12} {s["time"]:>8} {s["direction"]:>7} {s["pattern"]:>18} {s["price"]:>8.2f} '
    line += f'{s["cvd_z"]:>5.1f} {s["vol_ratio"]:>4.1f} '
    line += f'{s["grade"]:>5} {s["old_grade"]:>5} {s["composite"]:>5.1f} {s["old_composite"]:>5.1f} '
    line += f'{s["dd_raw"]:>2} {s["para_raw"]:>2} {s["lis_raw"]:>3} {s["lis_side_raw"]:>4} {s["target_dir_raw"]:>3} '
    line += f'{s["max_fav"]:>6.1f} {s["max_adv"]:>6.1f} '
    line += f'{s["outcomes"].get("t5","?"):>7} {s["outcomes"].get("t10","?"):>7} {s["outcomes"].get("t15","?"):>7}'
    sys.stdout.write(line + '\n')

# ── Summary by NEW grade ──
sys.stdout.write(f'\n{"="*80}\n')
sys.stdout.write(f'PERFORMANCE BY NEW GRADE (stop={STOP})\n')
sys.stdout.write(f'{"="*80}\n')

for g in ['A+', 'A', 'B', 'C']:
    sigs = [s for s in all_signals if s['grade'] == g]
    if not sigs:
        continue
    sys.stdout.write(f'\n  --- Grade {g} ({len(sigs)} signals) ---\n')
    for tgt in TARGETS:
        k = f't{tgt}'
        wins = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        losses = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        expired = sum(1 for s in sigs if s['outcomes'].get(k) == 'EXPIRED')
        total = wins + losses + expired
        wr = wins / total * 100 if total > 0 else 0
        pnl = wins * tgt + losses * (-STOP)
        sys.stdout.write(f'    t{tgt:>2}: {wins}W/{losses}L/{expired}E WR={wr:.1f}% PnL={pnl:+.0f}\n')
    avg_fav = sum(s['max_fav'] for s in sigs) / len(sigs)
    avg_adv = sum(s['max_adv'] for s in sigs) / len(sigs)
    sys.stdout.write(f'    Avg maxFav={avg_fav:.1f} maxAdv={avg_adv:.1f}\n')

# ── Summary by OLD grade ──
sys.stdout.write(f'\n{"="*80}\n')
sys.stdout.write(f'PERFORMANCE BY OLD GRADE (for comparison)\n')
sys.stdout.write(f'{"="*80}\n')

for g in ['A+', 'A', 'B', 'C']:
    sigs = [s for s in all_signals if s['old_grade'] == g]
    if not sigs:
        continue
    sys.stdout.write(f'\n  --- Old Grade {g} ({len(sigs)} signals) ---\n')
    for tgt in TARGETS:
        k = f't{tgt}'
        wins = sum(1 for s in sigs if s['outcomes'].get(k) == 'WIN')
        losses = sum(1 for s in sigs if s['outcomes'].get(k) == 'LOSS')
        expired = sum(1 for s in sigs if s['outcomes'].get(k) == 'EXPIRED')
        total = wins + losses + expired
        wr = wins / total * 100 if total > 0 else 0
        pnl = wins * tgt + losses * (-STOP)
        sys.stdout.write(f'    t{tgt:>2}: {wins}W/{losses}L/{expired}E WR={wr:.1f}% PnL={pnl:+.0f}\n')

# ── Grade changes ──
sys.stdout.write(f'\n{"="*80}\n')
sys.stdout.write(f'GRADE CHANGES (old -> new)\n')
sys.stdout.write(f'{"="*80}\n')

upgrades = [(s, s['old_grade'], s['grade']) for s in all_signals if s['grade'] != s['old_grade']]
grade_order = {'C': 0, 'B': 1, 'A': 2, 'A+': 3}

for s, old_g, new_g in upgrades:
    arrow = '^' if grade_order.get(new_g, 0) > grade_order.get(old_g, 0) else 'v'
    t10 = s['outcomes'].get('t10', '?')
    sys.stdout.write(f'  {s["date"]} {s["time"]} {s["direction"]:>7} {s["pattern"]:>18} '
                     f'{old_g:>3} -> {new_g:>3} {arrow} '
                     f'(side={s["lis_side_raw"]} tgt={s["target_dir_raw"]}) '
                     f't10={t10} maxF={s["max_fav"]:.1f}\n')

sys.stdout.write(f'\n  Total changes: {len(upgrades)} / {len(all_signals)} signals\n')
up_count = sum(1 for _, o, n in upgrades if grade_order.get(n, 0) > grade_order.get(o, 0))
down_count = sum(1 for _, o, n in upgrades if grade_order.get(n, 0) < grade_order.get(o, 0))
sys.stdout.write(f'  Upgrades: {up_count}, Downgrades: {down_count}\n')

# ── LIS Side impact ──
sys.stdout.write(f'\n{"="*80}\n')
sys.stdout.write(f'LIS SIDE IMPACT (t10 WR by side score)\n')
sys.stdout.write(f'{"="*80}\n')

for side_val in [0, 1, 2]:
    sigs = [s for s in all_signals if s['lis_side_raw'] == side_val]
    if not sigs:
        continue
    wins = sum(1 for s in sigs if s['outcomes'].get('t10') == 'WIN')
    losses = sum(1 for s in sigs if s['outcomes'].get('t10') == 'LOSS')
    total = len(sigs)
    wr = wins / total * 100 if total > 0 else 0
    pnl = wins * 10 + losses * (-STOP)
    label = {0: 'Wrong side', 1: 'Near right side', 2: 'Right side'}[side_val]
    sys.stdout.write(f'  Side={side_val} ({label:>15}): {total:>3} sigs, t10 WR={wr:.1f}% ({wins}W/{losses}L) PnL={pnl:+.0f}\n')

# ── Target Dir impact ──
sys.stdout.write(f'\n{"="*80}\n')
sys.stdout.write(f'TARGET DIRECTION IMPACT (t10 WR)\n')
sys.stdout.write(f'{"="*80}\n')

for tdir in [0, 2]:
    sigs = [s for s in all_signals if s['target_dir_raw'] == tdir]
    if not sigs:
        continue
    wins = sum(1 for s in sigs if s['outcomes'].get('t10') == 'WIN')
    losses = sum(1 for s in sigs if s['outcomes'].get('t10') == 'LOSS')
    total = len(sigs)
    wr = wins / total * 100 if total > 0 else 0
    pnl = wins * 10 + losses * (-STOP)
    label = {0: 'No confirm / conflict', 2: 'Target confirms'}[tdir]
    sys.stdout.write(f'  TgtDir={tdir} ({label:>22}): {total:>3} sigs, t10 WR={wr:.1f}% ({wins}W/{losses}L) PnL={pnl:+.0f}\n')

# No Volland data
no_vol = [s for s in all_signals if s['lis_val'] is None and s['target_val'] is None]
if no_vol:
    sys.stdout.write(f'\n  ({len(no_vol)} signals had no Volland data available)\n')

# ── Combined: right side + target confirms ──
sys.stdout.write(f'\n{"="*80}\n')
sys.stdout.write(f'COMBINED CONFLUENCE (t10)\n')
sys.stdout.write(f'{"="*80}\n')

combos = [
    ('Both confirm (side=2 + tgt=2)', lambda s: s['lis_side_raw'] == 2 and s['target_dir_raw'] == 2),
    ('One confirms (side=2 or tgt=2)', lambda s: (s['lis_side_raw'] == 2) != (s['target_dir_raw'] == 2)),
    ('Neither confirms', lambda s: s['lis_side_raw'] < 2 and s['target_dir_raw'] < 2),
]

for label, filt in combos:
    sigs = [s for s in all_signals if filt(s)]
    if not sigs:
        continue
    wins = sum(1 for s in sigs if s['outcomes'].get('t10') == 'WIN')
    losses = sum(1 for s in sigs if s['outcomes'].get('t10') == 'LOSS')
    total = len(sigs)
    wr = wins / total * 100 if total > 0 else 0
    pnl = wins * 10 + losses * (-STOP)
    avg_fav = sum(s['max_fav'] for s in sigs) / len(sigs)
    sys.stdout.write(f'  {label:>35}: {total:>3} sigs, t10 WR={wr:.1f}% ({wins}W/{losses}L) PnL={pnl:+.0f} avgMaxF={avg_fav:.1f}\n')

# ── Per-pattern with new grade ──
sys.stdout.write(f'\n{"="*80}\n')
sys.stdout.write(f'BY PATTERN + GRADE (t10)\n')
sys.stdout.write(f'{"="*80}\n')

patterns = sorted(set(s['pattern'] for s in all_signals))
for pat in patterns:
    pat_sigs = [s for s in all_signals if s['pattern'] == pat]
    sys.stdout.write(f'\n  {pat} ({len(pat_sigs)} total):\n')
    for g in ['A+', 'A', 'B', 'C']:
        sigs = [s for s in pat_sigs if s['grade'] == g]
        if not sigs:
            continue
        wins = sum(1 for s in sigs if s['outcomes'].get('t10') == 'WIN')
        losses = sum(1 for s in sigs if s['outcomes'].get('t10') == 'LOSS')
        expired = sum(1 for s in sigs if s['outcomes'].get('t10') == 'EXPIRED')
        total = len(sigs)
        wr = wins / total * 100 if total > 0 else 0
        pnl = wins * 10 + losses * (-STOP)
        sys.stdout.write(f'    {g:>3}: {wins}W/{losses}L/{expired}E WR={wr:.1f}% PnL={pnl:+.0f}\n')

sys.stdout.flush()
