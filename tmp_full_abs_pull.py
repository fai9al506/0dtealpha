"""Pull ALL ES Absorption trades with full swing + trigger bar data."""
import psycopg2, os, json, sys
sys.stdout.reconfigure(encoding='utf-8')

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get ALL ES Absorption trades
cur.execute("""
SELECT id, ts, direction, grade, score, spot, abs_es_price,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       abs_details::text, paradigm, ts::date as trade_date
FROM setup_log
WHERE setup_name = 'ES Absorption'
ORDER BY id
""")

trades = []
for r in cur.fetchall():
    abs_d = json.loads(r[11]) if r[11] else {}
    best = abs_d.get('best_swing', {})
    if not best:
        continue

    ref_sw = best.get('ref_swing', {})
    rec_sw = best.get('swing', {})
    bar_idx = abs_d.get('bar_idx')
    trade_date = str(r[13])

    # Get trigger bar data
    trig_bar = None
    if bar_idx is not None:
        # Try rithmic first, then live
        for src in ['rithmic', 'live']:
            cur.execute("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume, bar_delta, cvd_close, ts_start
                FROM es_range_bars
                WHERE trade_date = %s AND source = %s AND bar_idx = %s
            """, (trade_date, src, bar_idx))
            row = cur.fetchone()
            if row:
                trig_bar = {
                    'bar_idx': row[0], 'open': float(row[1]), 'high': float(row[2]),
                    'low': float(row[3]), 'close': float(row[4]),
                    'volume': int(row[5]), 'delta': int(row[6]),
                    'cvd': float(row[7]), 'ts_start': str(row[8])
                }
                break

    trades.append({
        'id': r[0], 'ts': str(r[1]), 'direction': r[2],
        'grade': r[3], 'score': r[4],
        'spot': float(r[5]) if r[5] else None,
        'es_price': float(r[6]) if r[6] else None,
        'result': r[7], 'pnl': float(r[8]) if r[8] else 0,
        'max_profit': float(r[9]) if r[9] else 0,
        'max_loss': float(r[10]) if r[10] else 0,
        'abs_details': abs_d,
        'paradigm': r[12],
        'trade_date': trade_date,
        'trigger_bar': trig_bar
    })

print(f"Pulled {len(trades)} ES Absorption trades with swing data", file=sys.stderr)

# Count by date
from collections import Counter
dates = Counter(t['trade_date'] for t in trades)
for d, c in sorted(dates.items()):
    has_trig = sum(1 for t in trades if t['trade_date'] == d and t.get('trigger_bar'))
    print(f"  {d}: {c} trades ({has_trig} with trigger bar)", file=sys.stderr)

json.dump(trades, open('abs_data_all.json', 'w'), default=str)
print(f"Saved to abs_data_all.json", file=sys.stderr)
conn.close()
