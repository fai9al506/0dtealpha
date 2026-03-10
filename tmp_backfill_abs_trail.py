"""
Backfill ES Absorption outcomes with updated trail gap=8 (was gap=5).
Replays the trail logic using actual ES range bars for each trade.
"""
import psycopg2, os, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Trail params (NEW)
BE_TRIGGER = 10
ACTIVATION = 10
GAP_OLD = 5
GAP_NEW = 8
STOP_PTS = 12
MAX_BARS = 200

# Get all resolved ES Absorption trades
cur.execute("""
SELECT id, ts, direction, spot, abs_es_price, abs_details,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       outcome_stop_level, outcome_elapsed_min, outcome_first_event,
       ts::date as trade_date
FROM setup_log
WHERE setup_name = 'ES Absorption'
  AND outcome_result IS NOT NULL
ORDER BY ts
""")
trades = cur.fetchall()
cols = [d[0] for d in cur.description]

print(f"Found {len(trades)} resolved ES Absorption trades\n")
print(f"{'ID':>4} {'Date':>10} {'Dir':>8} | {'OLD':>12} | {'NEW':>12} | {'Diff':>8} | Notes")
print("-" * 90)

total_old = 0
total_new = 0
updates = []

for row in trades:
    t = dict(zip(cols, row))
    tid = t['id']
    direction = t['direction']
    is_long = direction.lower() in ('long', 'bullish')
    es_price = t['abs_es_price']
    trade_date = str(t['trade_date'])

    det = t['abs_details']
    if isinstance(det, str):
        det = json.loads(det) if det else {}
    elif det is None:
        det = {}

    entry_bar_idx = det.get('bar_idx', 0)

    if not es_price:
        print(f"{tid:>4} {trade_date} {direction:>8} | no ES price, skipping")
        continue

    # Get range bars after entry
    # Prefer rithmic, fall back to live
    cur.execute("""
        SELECT DISTINCT source FROM es_range_bars WHERE trade_date = %s
    """, (trade_date,))
    sources = [r[0] for r in cur.fetchall()]
    src = 'rithmic' if 'rithmic' in sources else 'live'

    cur.execute("""
        SELECT bar_idx, bar_high, bar_low, bar_close,
               ts_start - interval '5 hours' as ts_et
        FROM es_range_bars
        WHERE trade_date = %s AND source = %s AND bar_idx >= %s
        ORDER BY bar_idx
        LIMIT %s
    """, (trade_date, src, entry_bar_idx, MAX_BARS))
    bars = cur.fetchall()

    if not bars:
        print(f"{tid:>4} {trade_date} {direction:>8} | no bars found, skipping")
        continue

    # Simulate trail for both old and new gap
    def simulate_trail(gap):
        entry = es_price
        stop = entry - STOP_PTS if is_long else entry + STOP_PTS
        max_fav = 0
        t1_hit = False
        seen_high = entry
        seen_low = entry

        for b in bars:
            bidx, bh, bl, bc, bts = b
            if bidx <= entry_bar_idx:
                continue

            if bh > seen_high:
                seen_high = bh
            if bl < seen_low:
                seen_low = bl

            fav = (seen_high - entry) if is_long else (entry - seen_low)
            if fav > max_fav:
                max_fav = fav

            if max_fav >= 10 and not t1_hit:
                t1_hit = True

            # Trail logic
            trail_lock = None
            if max_fav >= ACTIVATION:
                trail_lock = max_fav - gap
            elif max_fav >= BE_TRIGGER:
                trail_lock = 0

            if trail_lock is not None:
                if is_long:
                    new_stop = entry + trail_lock
                    if new_stop > stop:
                        stop = new_stop
                else:
                    new_stop = entry - trail_lock
                    if new_stop < stop:
                        stop = new_stop

            # Check stop hit
            if is_long and bl <= stop:
                pnl = stop - entry
                if t1_hit:
                    pnl = round((10.0 + pnl) / 2, 1)
                    return 'WIN', pnl, max_fav, seen_high - entry, entry - seen_low
                else:
                    result = 'WIN' if pnl >= 0 else 'LOSS'
                    return result, round(pnl, 1), max_fav, seen_high - entry, entry - seen_low
            elif not is_long and bh >= stop:
                pnl = entry - stop
                if t1_hit:
                    pnl = round((10.0 + pnl) / 2, 1)
                    return 'WIN', pnl, max_fav, entry - seen_low, seen_high - entry
                else:
                    result = 'WIN' if pnl >= 0 else 'LOSS'
                    return result, round(pnl, 1), max_fav, entry - seen_low, seen_high - entry

        # EXPIRED (ran out of bars / market close)
        last_close = bars[-1][3]
        pnl = (last_close - entry) if is_long else (entry - last_close)
        if t1_hit:
            pnl = round((10.0 + pnl) / 2, 1)
        else:
            pnl = round(pnl, 1)
        mp = (seen_high - entry) if is_long else (entry - seen_low)
        ml = (seen_low - entry) if is_long else (entry - seen_high)
        return 'EXPIRED', pnl, max_fav, round(mp, 2), round(ml, 2)

    old_result, old_pnl, old_maxfav, old_mp, old_ml = simulate_trail(GAP_OLD)
    new_result, new_pnl, new_maxfav, new_mp, new_ml = simulate_trail(GAP_NEW)

    diff = new_pnl - old_pnl
    total_old += old_pnl
    total_new += new_pnl

    notes = ""
    if diff != 0:
        notes = f"{'IMPROVED' if diff > 0 else 'WORSE'}"
    if new_result != old_result:
        notes += f" {old_result}->{new_result}"

    print(f"{tid:>4} {trade_date} {direction:>8} | {old_result:>4} {old_pnl:>+7.1f} | {new_result:>4} {new_pnl:>+7.1f} | {diff:>+7.1f} | {notes}")

    # Store update if changed
    if new_pnl != old_pnl or new_result != t['outcome_result']:
        updates.append({
            'id': tid,
            'result': new_result,
            'pnl': new_pnl,
            'max_profit': round(new_mp, 2),
            'max_loss': round(new_ml, 2),
            'stop_level': None,  # will be set by live trail going forward
        })

print(f"\n{'='*90}")
print(f"TOTAL OLD (gap=5): {total_old:+.1f} pts")
print(f"TOTAL NEW (gap=8): {total_new:+.1f} pts")
print(f"DIFFERENCE:        {total_new - total_old:+.1f} pts")
print(f"\nTrades to update: {len(updates)}")

# Apply updates
if updates:
    print("\nApplying updates to setup_log...")
    for u in updates:
        cur.execute("""
            UPDATE setup_log SET
                outcome_result = %s,
                outcome_pnl = %s,
                outcome_max_profit = %s,
                outcome_max_loss = %s
            WHERE id = %s
        """, (u['result'], u['pnl'], u['max_profit'], u['max_loss'], u['id']))
    conn.commit()
    print(f"Updated {len(updates)} trades.")
else:
    print("No updates needed.")

conn.close()
