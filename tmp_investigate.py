import os, psycopg2, json, sys

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

print("=" * 80)
print("Check ALL Rithmic bars 165-180 with full detail")
print("=" * 80)
cur.execute("""
SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume,
       bar_buy_volume, bar_sell_volume, bar_delta, cvd_close,
       ts_start, ts_end
FROM es_range_bars
WHERE trade_date = '2026-02-26' AND source = 'rithmic'
AND bar_idx >= 165 AND bar_idx <= 180
ORDER BY bar_idx
""")
rows = cur.fetchall()
print(f"Found {len(rows)} bars")
for r in rows:
    bar_close = float(r[4])
    marker = ""
    if abs(bar_close - 6899.25) < 0.01:
        marker = " <<< CLOSE=6899.25 (matches abs_es_price)"
    if abs(bar_close - 6892.75) < 0.01:
        marker = " <<< CLOSE=6892.75 (bar 176 in our earlier output)"
    print(f"  Bar {r[0]}: O={r[1]} H={r[2]} L={r[3]} C={r[4]} Vol={r[5]} Buy={r[6]} Sell={r[7]} Delta={r[8]} CVD={r[9]}")
    print(f"           {r[10]} - {r[11]}{marker}")

print()
print("=" * 80)
print("Check TS live bars in same range")
print("=" * 80)
cur.execute("""
SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, bar_volume,
       bar_buy_volume, bar_sell_volume, bar_delta, cvd_close,
       ts_start, ts_end
FROM es_range_bars
WHERE trade_date = '2026-02-26' AND source = 'live'
AND bar_idx >= 130 AND bar_idx <= 145
ORDER BY bar_idx
""")
rows = cur.fetchall()
print(f"Found {len(rows)} bars")
for r in rows:
    bar_close = float(r[4])
    marker = ""
    if abs(bar_close - 6899.25) < 0.01 or abs(bar_close - 6899.5) < 0.5:
        marker = " <<< CLOSE near 6899.25"
    print(f"  Bar {r[0]}: O={r[1]} H={r[2]} L={r[3]} C={r[4]} Vol={r[5]} Buy={r[6]} Sell={r[7]} Delta={r[8]} CVD={r[9]}")
    print(f"           {r[10]} - {r[11]}{marker}")

print()
print("=" * 80)
print("Check: are there any DUPLICATE bar_idx in Rithmic today? (reconnect issue)")
print("=" * 80)
cur.execute("""
SELECT bar_idx, COUNT(*) as cnt
FROM es_range_bars
WHERE trade_date = '2026-02-26' AND source = 'rithmic'
GROUP BY bar_idx
HAVING COUNT(*) > 1
ORDER BY bar_idx
""")
rows = cur.fetchall()
if rows:
    print(f"Found {len(rows)} duplicate bar_idx values!")
    for r in rows:
        print(f"  Bar {r[0]}: {r[1]} copies")
else:
    print("No duplicates found")

print()
print("=" * 80)
print("Check: First and last bar of the day + total count")
print("=" * 80)
cur.execute("""
SELECT MIN(bar_idx), MAX(bar_idx), COUNT(*)
FROM es_range_bars
WHERE trade_date = '2026-02-26' AND source = 'rithmic'
""")
r = cur.fetchone()
print(f"First bar: {r[0]}, Last bar: {r[1]}, Total: {r[2]}")

# Also check for gaps in bar_idx
cur.execute("""
SELECT bar_idx FROM es_range_bars
WHERE trade_date = '2026-02-26' AND source = 'rithmic'
ORDER BY bar_idx
""")
indices = [r[0] for r in cur.fetchall()]
gaps = []
for i in range(len(indices) - 1):
    if indices[i+1] - indices[i] != 1:
        gaps.append((indices[i], indices[i+1]))
if gaps:
    print(f"Gaps in bar_idx: {gaps}")
else:
    print("No gaps in bar_idx (continuous)")

conn.close()
