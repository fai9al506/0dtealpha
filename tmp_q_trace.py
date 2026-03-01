import psycopg2, os, sys

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get range bars for Feb 25 with proper 10-bar rolling avg (excluding current)
cur.execute("""
SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume, bar_delta,
       cvd_close, ts_start AT TIME ZONE 'America/New_York', ts_end AT TIME ZONE 'America/New_York'
FROM es_range_bars
WHERE trade_date = '2026-02-25' AND source = 'live'
ORDER BY bar_idx
""")
rows = cur.fetchall()

# Recalculate volume ratios using trailing 10-bar window (like detector does)
sys.stdout.write('=== Bars 50-70 with proper 10-bar trailing vol avg ===\n')
sys.stdout.write(f'{"idx":>4} {"time_s":>12} {"high":>8} {"low":>8} {"close":>8} {"vol":>6} {"delta":>7} {"cvd_c":>8} {"10barAvg":>8} {"vRatio":>6} {"trigger":>7}\n')
sys.stdout.write('=' * 110 + '\n')

vols = [r[5] for r in rows]
for i, r in enumerate(rows):
    idx, op, hi, lo, cl, vol, delta, cvd, ts_s, ts_e = r
    if idx < 50 or idx > 75:
        continue
    # 10-bar trailing avg (excluding current bar, like detector)
    start = max(0, i - 10)
    trailing = vols[start:i]
    avg_v = sum(trailing) / len(trailing) if trailing else vol
    ratio = vol / avg_v if avg_v > 0 else 0
    trigger = 'YES' if ratio >= 1.4 else ''
    sys.stdout.write(f'{idx:>4} {str(ts_s.time()):>12} {hi:>8.2f} {lo:>8.2f} {cl:>8.2f} {vol:>6} {delta:>7} {cvd:>8} {avg_v:>8.0f} {ratio:>6.1f} {trigger:>7}\n')

# Also check: what swings would the detector see at bars 61-64?
# The detector uses alternating L-H-L-H with adaptive invalidation
# Let me trace the swing sequence
sys.stdout.write('\n=== Swing trace (all bars, alternating L-H-L-H) ===\n')
# Using pivot_left=2, pivot_right=2

all_bars = []
for r in rows:
    all_bars.append({
        'idx': r[0], 'open': r[1], 'high': r[2], 'low': r[3],
        'close': r[4], 'volume': r[5], 'delta': r[6], 'cvd': r[7],
        'ts_s': r[8], 'ts_e': r[9]
    })

# Simulate swing detection with alternating enforcement
swings = []
last_type = None

for i in range(2, len(all_bars) - 2):
    b = all_bars[i]
    # Check swing low
    is_low = (b['low'] <= all_bars[i-1]['low'] and b['low'] <= all_bars[i-2]['low'] and
              b['low'] <= all_bars[i+1]['low'] and b['low'] <= all_bars[i+2]['low'])
    # Check swing high
    is_high = (b['high'] >= all_bars[i-1]['high'] and b['high'] >= all_bars[i-2]['high'] and
               b['high'] >= all_bars[i+1]['high'] and b['high'] >= all_bars[i+2]['high'])

    if not is_low and not is_high:
        continue

    if is_low and is_high:
        if last_type == 'L':
            is_low = False
        elif last_type == 'H':
            is_high = False
        else:
            is_high = False

    if is_low:
        new = {'type': 'L', 'price': b['low'], 'cvd': b['cvd'], 'idx': b['idx'], 'ts': b['ts_s']}
        if not swings or last_type is None:
            swings.append(new)
            last_type = 'L'
        elif last_type == 'L':
            # Same direction: lower low replaces
            if new['price'] <= swings[-1]['price']:
                swings[-1] = new
        else:
            swings.append(new)
            last_type = 'L'
    elif is_high:
        new = {'type': 'H', 'price': b['high'], 'cvd': b['cvd'], 'idx': b['idx'], 'ts': b['ts_s']}
        if not swings or last_type is None:
            swings.append(new)
            last_type = 'H'
        elif last_type == 'H':
            if new['price'] >= swings[-1]['price']:
                swings[-1] = new
        else:
            swings.append(new)
            last_type = 'H'

    # Print swing state after processing bars up to idx 70
    if b['idx'] <= 70:
        swing_str = ' | '.join([f"{s['type']}:{s['idx']}@{s['price']:.2f}(cvd={s['cvd']})" for s in swings[-6:]])
        if is_low or is_high:
            sys.stdout.write(f'  Bar {b["idx"]:>3} pivot={"L" if is_low else "H"} -> swings: [{swing_str}]\n')

# Show the swing state at bar 61 (when signal fired) and bar 63-64
sys.stdout.write('\n=== Swing lows and highs at bar 63-64 time ===\n')
lows = [s for s in swings if s['type'] == 'L' and s['idx'] <= 64]
highs = [s for s in swings if s['type'] == 'H' and s['idx'] <= 64]
sys.stdout.write('Swing lows:\n')
for s in lows:
    sys.stdout.write(f'  idx={s["idx"]:>3} price={s["price"]:.2f} cvd={s["cvd"]}\n')
sys.stdout.write('Swing highs:\n')
for s in highs:
    sys.stdout.write(f'  idx={s["idx"]:>3} price={s["price"]:.2f} cvd={s["cvd"]}\n')

# Now check divergences for bullish at bars 63-64
sys.stdout.write('\n=== Bullish divergences available at bar 63-64 ===\n')
for i in range(1, len(lows)):
    s1, s2 = lows[i-1], lows[i]
    if 64 - s2['idx'] > 40:
        continue
    cvd_gap = abs(s2['cvd'] - s1['cvd'])
    price_dist = abs(s2['price'] - s1['price'])
    # sell exhaustion: lower low + higher CVD
    if s2['price'] < s1['price'] and s2['cvd'] > s1['cvd']:
        sys.stdout.write(f'  SELL_EXHAUST: {s1["idx"]}->{s2["idx"]} price {s1["price"]:.2f}->{s2["price"]:.2f} cvd {s1["cvd"]}->{s2["cvd"]} gap={cvd_gap}\n')
    elif s2['price'] >= s1['price'] and s2['cvd'] < s1['cvd']:
        sys.stdout.write(f'  SELL_ABSORB: {s1["idx"]}->{s2["idx"]} price {s1["price"]:.2f}->{s2["price"]:.2f} cvd {s1["cvd"]}->{s2["cvd"]} gap={cvd_gap}\n')

sys.stdout.write('\n=== Bearish divergences available at bar 63-64 ===\n')
for i in range(1, len(highs)):
    s1, s2 = highs[i-1], highs[i]
    if 64 - s2['idx'] > 40:
        continue
    cvd_gap = abs(s2['cvd'] - s1['cvd'])
    if s2['price'] > s1['price'] and s2['cvd'] < s1['cvd']:
        sys.stdout.write(f'  BUY_EXHAUST: {s1["idx"]}->{s2["idx"]} price {s1["price"]:.2f}->{s2["price"]:.2f} cvd {s1["cvd"]}->{s2["cvd"]} gap={cvd_gap}\n')
    elif s2['price'] <= s1['price'] and s2['cvd'] > s1['cvd']:
        sys.stdout.write(f'  BUY_ABSORB: {s1["idx"]}->{s2["idx"]} price {s1["price"]:.2f}->{s2["price"]:.2f} cvd {s1["cvd"]}->{s2["cvd"]} gap={cvd_gap}\n')

sys.stdout.flush()
conn.close()
