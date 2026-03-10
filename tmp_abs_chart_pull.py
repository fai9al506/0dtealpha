"""Pull ES range bars + absorption trades for March 2 chart."""
import psycopg2, os, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get all range bars for March 2
cur.execute("""
SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
       bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
       cvd_close, ts_start, ts_end, source
FROM es_range_bars
WHERE trade_date = '2026-03-02' AND source = 'rithmic'
ORDER BY bar_idx
""")
bars = []
for r in cur.fetchall():
    bars.append({
        'idx': r[0], 'open': float(r[1]), 'high': float(r[2]),
        'low': float(r[3]), 'close': float(r[4]),
        'volume': int(r[5]), 'buy_vol': int(r[6]), 'sell_vol': int(r[7]),
        'delta': int(r[8]), 'cvd': float(r[9]),
        'ts_start': str(r[10]), 'ts_end': str(r[11]), 'source': r[12]
    })

# If no rithmic, try live
if not bars:
    cur.execute("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
           bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
           cvd_close, ts_start, ts_end, source
    FROM es_range_bars
    WHERE trade_date = '2026-03-02' AND source = 'live'
    ORDER BY bar_idx
    """)
    for r in cur.fetchall():
        bars.append({
            'idx': r[0], 'open': float(r[1]), 'high': float(r[2]),
            'low': float(r[3]), 'close': float(r[4]),
            'volume': int(r[5]), 'buy_vol': int(r[6]), 'sell_vol': int(r[7]),
            'delta': int(r[8]), 'cvd': float(r[9]),
            'ts_start': str(r[10]), 'ts_end': str(r[11]), 'source': r[12]
        })

# Get ES Absorption trades for March 2
cur.execute("""
SELECT id, ts, direction, grade, score, spot, abs_es_price,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       abs_details::text, paradigm, comments
FROM setup_log
WHERE setup_name = 'ES Absorption' AND ts::date = '2026-03-02'
ORDER BY id
""")
trades = []
for r in cur.fetchall():
    abs_d = json.loads(r[11]) if r[11] else {}
    trades.append({
        'id': r[0], 'ts': str(r[1]), 'direction': r[2],
        'grade': r[3], 'score': r[4], 'spot': float(r[5]) if r[5] else None,
        'es_price': float(r[6]) if r[6] else None,
        'result': r[7], 'pnl': float(r[8]) if r[8] else 0,
        'max_profit': float(r[9]) if r[9] else 0,
        'max_loss': float(r[10]) if r[10] else 0,
        'bar_idx': abs_d.get('bar_idx'),
        'pattern': abs_d.get('pattern', ''),
        'paradigm': r[12],
        'best_swing': abs_d.get('best_swing', {}),
    })

print(json.dumps({'bars': bars, 'trades': trades}, default=str))
conn.close()
