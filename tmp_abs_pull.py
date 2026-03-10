"""Pull ES Absorption data from Railway DB and dump to JSON for local Excel generation."""
import psycopg2, os, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get ALL ES Absorption trades
cur.execute("""
SELECT s.id, s.ts, s.direction, s.grade, s.score, s.spot,
       s.outcome_result, s.outcome_pnl, s.outcome_max_profit, s.outcome_max_loss,
       s.paradigm, s.abs_es_price, s.abs_details::text, s.comments, s.abs_vol_ratio
FROM setup_log s
WHERE s.setup_name = 'ES Absorption'
ORDER BY s.id
""")
trades = cur.fetchall()

results = []
for t in trades:
    tid, ts, direction, grade, score, spot, result, pnl, max_profit, max_loss, \
        paradigm, es_price, abs_details_str, comments, vol_ratio_db = t

    abs_d = json.loads(abs_details_str) if abs_details_str else {}
    trigger_bar_idx = abs_d.get('bar_idx')
    trade_date = str(ts)[:10] if ts else None

    # Get trigger bar from es_range_bars
    trig_data = None
    if trigger_bar_idx is not None and trade_date:
        for source in ['rithmic', 'live']:
            cur.execute("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                       cvd_close, ts_start, ts_end
                FROM es_range_bars
                WHERE trade_date = %s AND bar_idx = %s AND source = %s
                LIMIT 1
            """, (trade_date, trigger_bar_idx, source))
            row = cur.fetchone()
            if row:
                trig_data = {
                    'bar_idx': row[0], 'open': float(row[1]) if row[1] else None,
                    'high': float(row[2]) if row[2] else None,
                    'low': float(row[3]) if row[3] else None,
                    'close': float(row[4]) if row[4] else None,
                    'volume': int(row[5]) if row[5] else None,
                    'buy_vol': int(row[6]) if row[6] else None,
                    'sell_vol': int(row[7]) if row[7] else None,
                    'delta': int(row[8]) if row[8] else None,
                    'cvd': float(row[9]) if row[9] else None,
                    'ts_start': str(row[10]) if row[10] else None,
                    'source': source
                }
                break

    # Also get swing bar data from range bars
    sw1_bar_data = None
    sw2_bar_data = None
    best = abs_d.get('best_swing', {})
    ref_swing = best.get('ref_swing', {})
    recent_swing = best.get('swing', {})

    for sw_info, label in [(ref_swing, 'sw1'), (recent_swing, 'sw2')]:
        sw_bar_idx = sw_info.get('bar_idx')
        if sw_bar_idx is not None and trade_date:
            for source in ['rithmic', 'live']:
                cur.execute("""
                    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                           bar_volume, bar_buy_volume, bar_sell_volume, bar_delta,
                           cvd_close, ts_start, ts_end
                    FROM es_range_bars
                    WHERE trade_date = %s AND bar_idx = %s AND source = %s
                    LIMIT 1
                """, (trade_date, sw_bar_idx, source))
                row = cur.fetchone()
                if row:
                    bar_data = {
                        'bar_idx': row[0], 'open': float(row[1]) if row[1] else None,
                        'high': float(row[2]) if row[2] else None,
                        'low': float(row[3]) if row[3] else None,
                        'close': float(row[4]) if row[4] else None,
                        'volume': int(row[5]) if row[5] else None,
                        'buy_vol': int(row[6]) if row[6] else None,
                        'sell_vol': int(row[7]) if row[7] else None,
                        'delta': int(row[8]) if row[8] else None,
                        'cvd': float(row[9]) if row[9] else None,
                        'ts_start': str(row[10]) if row[10] else None,
                    }
                    if label == 'sw1': sw1_bar_data = bar_data
                    else: sw2_bar_data = bar_data
                    break

    results.append({
        'id': tid,
        'ts': str(ts),
        'direction': direction,
        'grade': grade,
        'score': score,
        'spot': float(spot) if spot else None,
        'result': result,
        'pnl': float(pnl) if pnl else 0,
        'max_profit': float(max_profit) if max_profit else 0,
        'max_loss': float(max_loss) if max_loss else 0,
        'paradigm': paradigm,
        'es_price': float(es_price) if es_price else None,
        'abs_details': abs_d,
        'comments': comments,
        'trigger_bar': trig_data,
        'sw1_bar': sw1_bar_data,
        'sw2_bar': sw2_bar_data,
    })

print(json.dumps(results, default=str))
conn.close()
