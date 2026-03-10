"""Backfill the 3 unresolved Skew Charm trades (#359, #363, #365)."""
import psycopg2, os
from datetime import datetime
import pytz

NY = pytz.timezone("US/Eastern")
BE_TRIGGER = 10
ACTIVATION = 10
GAP = 8
INITIAL_SL = 20

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("""
SELECT id, ts, direction, spot, target
FROM setup_log
WHERE setup_name = 'Skew Charm' AND outcome_result IS NULL
ORDER BY ts
""")
trades = cur.fetchall()
cols = [d[0] for d in cur.description]
print("Found %d unresolved Skew Charm trades" % len(trades))

for row in trades:
    t = dict(zip(cols, row))
    tid = t['id']
    ts = t['ts']
    spot = t['spot']
    direction = t['direction']
    is_long = direction.lower() == 'long'

    alert_date = ts.astimezone(NY).date() if ts.tzinfo else NY.localize(ts).date()
    market_close_dt = NY.localize(datetime.combine(alert_date, datetime.strptime("16:00", "%H:%M").time()))

    cur.execute("""
        SELECT ts, spot FROM playback_snapshots
        WHERE ts >= %s AND ts <= %s
        ORDER BY ts ASC
    """, (ts, market_close_dt))
    prices = [(r[0], r[1]) for r in cur.fetchall() if r[1] is not None]

    if not prices:
        print("ID=%d: no price data" % tid)
        continue

    entry = spot
    stop = entry - INITIAL_SL if is_long else entry + INITIAL_SL
    max_fav = 0
    t1_hit = False
    trail_stopped = False
    final_pnl = None
    new_max_profit = 0
    new_max_loss = 0
    first_event = None
    elapsed_min = None

    for price_ts, price in prices:
        profit = (price - entry) if is_long else (entry - price)
        if profit > new_max_profit:
            new_max_profit = profit
        if profit < new_max_loss:
            new_max_loss = profit
        if profit > max_fav:
            max_fav = profit
        if max_fav >= 10 and not t1_hit:
            t1_hit = True

        trail_lock = None
        if max_fav >= ACTIVATION:
            trail_lock = max_fav - GAP
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

        if is_long and price <= stop:
            pnl = stop - entry
            if t1_hit:
                final_pnl = round((10.0 + pnl) / 2, 1)
            else:
                final_pnl = round(pnl, 1)
            first_event = "target" if pnl > 0 else "stop"
            trail_stopped = True
            elapsed_min = (price_ts - ts).total_seconds() / 60
            break
        elif not is_long and price >= stop:
            pnl = entry - stop
            if t1_hit:
                final_pnl = round((10.0 + pnl) / 2, 1)
            else:
                final_pnl = round(pnl, 1)
            first_event = "target" if pnl > 0 else "stop"
            trail_stopped = True
            elapsed_min = (price_ts - ts).total_seconds() / 60
            break

    if not trail_stopped:
        last_price = prices[-1][1]
        pnl = (last_price - entry) if is_long else (entry - last_price)
        if t1_hit:
            final_pnl = round((10.0 + pnl) / 2, 1)
        else:
            final_pnl = round(pnl, 1)
        first_event = "timeout"
        elapsed_min = (prices[-1][0] - ts).total_seconds() / 60

    new_result = "WIN" if final_pnl > 0 else ("LOSS" if final_pnl < 0 else "EXPIRED")
    if t1_hit and final_pnl >= 0:
        new_result = "WIN"

    print("ID=%d %s %s: spot=%.2f max_fav=%.1f t1=%s -> %s %+.1f (stop=%.2f, %d prices, %.0f min)" % (
        tid, str(alert_date), direction, spot, max_fav, t1_hit,
        new_result, final_pnl, stop, len(prices), elapsed_min or 0))

    cur.execute("""
        UPDATE setup_log SET
            outcome_result = %s,
            outcome_pnl = %s,
            outcome_max_profit = %s,
            outcome_max_loss = %s,
            outcome_first_event = %s,
            outcome_elapsed_min = %s,
            outcome_stop_level = %s
        WHERE id = %s
    """, (new_result, final_pnl, round(new_max_profit, 2), round(new_max_loss, 2),
          first_event, round(elapsed_min, 1) if elapsed_min else None,
          round(stop, 2), tid))

conn.commit()
print("\nDone. Updated %d trades." % len(trades))
conn.close()
