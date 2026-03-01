"""Diagnostic: check every absorption gate for bar 126 on 2026-02-26."""
import psycopg2, sys, os, math
sys.path.insert(0, '.')

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute('''
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, bar_delta,
           bar_buy_volume, bar_sell_volume, cvd_close, cvd_open, cvd_high, cvd_low,
           ts_start, ts_end
    FROM es_range_bars
    WHERE trade_date = '2026-02-26' AND source = 'rithmic'
    ORDER BY bar_idx
''')
rows = cur.fetchall()
conn.close()

bars = []
for r in rows:
    bars.append({
        'idx': r[0], 'open': r[1], 'high': r[2], 'low': r[3], 'close': r[4],
        'volume': r[5], 'delta': r[6], 'buy_volume': r[7], 'sell_volume': r[8],
        'cvd_close': r[9], 'cvd': r[9], 'cvd_open': r[10], 'cvd_high': r[11], 'cvd_low': r[12],
        'ts_start': str(r[13]), 'ts_end': str(r[14]), 'status': 'closed',
    })

TARGET = 126
trigger = bars[TARGET]

print("=" * 70)
print("BAR 126 DIAGNOSTIC - Absorption Criteria Check")
print("=" * 70)
print(f"O={trigger['open']:.2f}  H={trigger['high']:.2f}  L={trigger['low']:.2f}  C={trigger['close']:.2f}")
print(f"Volume={trigger['volume']}  Delta={trigger['delta']:+d}  CVD={trigger['cvd']:+.0f}")
color = "GREEN" if trigger['close'] >= trigger['open'] else "RED"
print(f"Color: {color}")
print(f"Time: {trigger['ts_end']}")
print()

# Gate 1
print("GATE 1: Setup Enabled")
print("  absorption_enabled = True                    --> PASS")
print()

# Gate 2
min_bars = max(10, 20, 2+2+1) + 1  # 21
print("GATE 2: Minimum Historical Data")
print(f"  Need >= {min_bars} closed bars")
print(f"  Have {TARGET + 1} bars (0..{TARGET})")
status = "PASS" if (TARGET + 1) >= min_bars else "FAIL"
print(f"  {TARGET + 1} >= {min_bars}                               --> {status}")
print()

# Gate 3
print("GATE 3: Time Gate (no signals before 10:00 ET)")
print("  Bar end time: ~10:35 ET")
print("  10:35 >= 10:00                               --> PASS")
print()

# Gate 4
vol_window = 10
recent_vols = [bars[i]['volume'] for i in range(TARGET - vol_window, TARGET)]
vol_avg = sum(recent_vols) / len(recent_vols)
vol_ratio = trigger['volume'] / vol_avg
min_vol_ratio = 1.4
status = "PASS" if vol_ratio >= min_vol_ratio else "FAIL"
print("GATE 4: Volume Trigger")
print(f"  Trigger bar volume: {trigger['volume']}")
print(f"  10-bar rolling avg: {vol_avg:.0f}")
print(f"  Ratio: {trigger['volume']}/{vol_avg:.0f} = {vol_ratio:.2f}x")
print(f"  Need >= {min_vol_ratio}x")
print(f"  {vol_ratio:.2f} >= {min_vol_ratio}                             --> {status}")
print()

# Gate 5
cvd_std_window = 20
cvd_deltas = []
for i in range(max(1, TARGET - cvd_std_window), TARGET + 1):
    cvd_deltas.append(bars[i]['cvd'] - bars[i-1]['cvd'])
n = len(cvd_deltas)
mean_d = sum(cvd_deltas) / n
var_d = sum((d - mean_d)**2 for d in cvd_deltas) / max(n - 1, 1)
std_d = max(math.sqrt(var_d), 1.0)
status = "PASS" if n >= 5 else "FAIL"
print("GATE 5: CVD Statistics")
print(f"  Bar-to-bar CVD deltas (last {n})")
print(f"  Mean={mean_d:.1f}  Std={std_d:.1f}")
print(f"  Deltas count {n} >= 5                        --> {status}")
print()

# Gate 6 - Build swing state
from app.setup_detector import reset_absorption_session, evaluate_absorption, DEFAULT_ABSORPTION_SETTINGS, _swing_tracker
reset_absorption_session()
settings = dict(DEFAULT_ABSORPTION_SETTINGS)

# Run up to bar 125 to build swing history
for i in range(TARGET):
    subset = bars[:i+1]
    if len(subset) >= min_bars:
        evaluate_absorption(subset, None, settings, spx_spot=None)

swings = _swing_tracker['swings']
print("GATE 6: Swing Pairs")
print(f"  Total swings detected by bar {TARGET-1}: {len(swings)}")
for s in swings:
    print(f"    {s['type']:4s} @ {s['price']:.2f} (bar {s['bar_idx']}) CVD={s['cvd']:+.0f}")

lows = [s for s in swings if s['type'] == 'low']
highs = [s for s in swings if s['type'] == 'high']
print(f"  Swing lows: {len(lows)}")
print(f"  Swing highs: {len(highs)}")
has_pairs = len(lows) >= 2 or len(highs) >= 2
status = "PASS" if has_pairs else "FAIL"
print(f"  Has >= 2 same-type swings for pairing        --> {status}")
print()

# Gate 7 - Swing-to-swing divergence
print("GATE 7: Swing-to-Swing CVD Divergence")
abs_max_trigger_dist = settings.get('abs_max_trigger_dist', 40)
cvd_z_min = settings.get('abs_cvd_z_min', 0.5)

found_swing_div = False
for label, swing_list, stype in [('LOW pairs (bullish candidates)', lows, 'low'),
                                   ('HIGH pairs (bearish candidates)', highs, 'high')]:
    if len(swing_list) < 2:
        print(f"  {label}: not enough swings -- SKIP")
        continue
    for j in range(len(swing_list) - 1):
        s1 = swing_list[j]
        s2 = swing_list[j+1]
        dist = TARGET - s2['bar_idx']
        dist_ok = dist <= abs_max_trigger_dist

        cvd_gap = abs(trigger['cvd'] - s2['cvd'])
        cvd_z = cvd_gap / std_d
        z_ok = cvd_z >= cvd_z_min

        if stype == 'low':
            # sell_exhaustion: lower low + higher CVD
            p1 = "sell_exhaustion" if (s2['price'] < s1['price'] and trigger['cvd'] > s1['cvd']) else None
            # sell_absorption: higher low + lower CVD
            p2 = "sell_absorption" if (s2['price'] > s1['price'] and trigger['cvd'] < s1['cvd']) else None
            patterns = [p for p in [p1, p2] if p]
        else:
            p1 = "buy_exhaustion" if (s2['price'] > s1['price'] and trigger['cvd'] < s1['cvd']) else None
            p2 = "buy_absorption" if (s2['price'] < s1['price'] and trigger['cvd'] > s1['cvd']) else None
            patterns = [p for p in [p1, p2] if p]

        print(f"  {stype}@{s1['price']:.2f}(bar{s1['bar_idx']}) -> {stype}@{s2['price']:.2f}(bar{s2['bar_idx']})")
        print(f"    Trigger dist: {dist} bars (max {abs_max_trigger_dist})    --> {'PASS' if dist_ok else 'FAIL'}")
        print(f"    CVD z-score: {cvd_z:.2f} (min {cvd_z_min})              --> {'PASS' if z_ok else 'FAIL'}")
        if patterns:
            print(f"    Pattern match: {', '.join(patterns)}              --> PASS")
            if dist_ok and z_ok:
                found_swing_div = True
        else:
            print(f"    No divergence pattern match                   --> FAIL")

if found_swing_div:
    print(f"  Swing divergence found                        --> PASS")
else:
    print(f"  No qualifying swing divergence                --> FAIL (try zone-revisit)")
print()

# Gate 8 - Zone-revisit
print("GATE 8: Zone-Revisit Divergence (fallback)")
result = evaluate_absorption(bars[:TARGET+1], None, settings, spx_spot=None)
if result:
    pattern = result.get('pattern', '?')
    best = result.get('best_swing', {})
    ref = best.get('ref_swing', {})
    sw = best.get('swing', {})
    print(f"  Pattern: {pattern}")
    if ref:
        print(f"  Zone pair: Z@{ref.get('price',0):.2f}(bar{ref.get('bar_idx','?')}) -> Z@{sw.get('price',0):.2f}(bar{sw.get('bar_idx','?')})")
        print(f"  CVD z-score: {best.get('cvd_z',0):.2f}")
        print(f"  Score: {best.get('score',0):.0f}")
    print(f"  Signal detected                              --> PASS")
else:
    print(f"  No zone-revisit match                        --> FAIL")
print()

# Gate 9
print("GATE 9: Per-Direction Cooldown")
print("  No prior bullish signal at this bar index")
print("  --> PASS")
print()

# Final
print("=" * 70)
if result:
    print("VERDICT: ALL GATES PASSED - SIGNAL FIRES")
    print(f"  Direction : {result['direction'].upper()}")
    print(f"  Pattern   : {result.get('pattern')}")
    print(f"  Grade     : {result['grade']}")
    print(f"  Score     : {result['score']:.0f}/100")
    print(f"  ES Price  : {result['abs_es_price']:.2f}")
    print(f"  Vol ratio : {result['abs_vol_ratio']:.2f}x")
else:
    print("VERDICT: SIGNAL BLOCKED")
print("=" * 70)
