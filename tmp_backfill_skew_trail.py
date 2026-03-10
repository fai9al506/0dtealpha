"""
Backfill Skew Charm outcomes with new trailing stop logic.
Old: fixed 10pt target / 20pt stop.
New: hybrid trail (BE@+10, activation=10, gap=8, initial SL=20).
     Split-target P&L: T1=+10 fixed, T2=trail, combined=(10+trail)/2.
"""
import psycopg2, os
from datetime import datetime, timedelta
import pytz

NY = pytz.timezone("US/Eastern")

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Trail params
BE_TRIGGER = 10
ACTIVATION = 10
GAP = 8
INITIAL_SL = 20

# Get all resolved Skew Charm trades
cur.execute("""
SELECT id, ts, direction, spot, target,
       outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
       outcome_first_event, outcome_elapsed_min
FROM setup_log
WHERE setup_name = 'Skew Charm'
  AND outcome_result IS NOT NULL
ORDER BY ts
""")
trades = cur.fetchall()
cols = [d[0] for d in cur.description]

print("Found %d resolved Skew Charm trades\n" % len(trades))
print("%4s %10s %6s | %12s | %12s | %8s | Notes" % ("ID", "Date", "Dir", "OLD", "NEW", "Diff"))
print("-" * 90)

total_old = 0
total_new = 0
updates = []

for row in trades:
    t = dict(zip(cols, row))
    tid = t['id']
    direction = t['direction']
    is_long = direction.lower() == 'long'
    spot = t['spot']
    ts = t['ts']

    if not spot or not ts:
        print("%4d  no spot/ts, skipping" % tid)
        continue

    # Get alert date in ET
    if ts.tzinfo:
        alert_date = ts.astimezone(NY).date()
    else:
        alert_date = NY.localize(ts).date()

    market_close_dt = NY.localize(datetime.combine(alert_date, datetime.strptime("16:00", "%H:%M").time()))

    # Get playback_snapshots from entry to market close
    cur.execute("""
        SELECT ts, spot FROM playback_snapshots
        WHERE ts >= %s AND ts <= %s
        ORDER BY ts ASC
    """, (ts, market_close_dt))
    prices = [(r[0], r[1]) for r in cur.fetchall() if r[1] is not None]

    if not prices:
        print("%4d %s %6s | no price data, skipping" % (tid, str(alert_date), direction))
        continue

    old_result = t['outcome_result']
    old_pnl = float(t['outcome_pnl']) if t['outcome_pnl'] is not None else 0

    # Simulate new trailing stop
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
        if is_long:
            profit = price - entry
        else:
            profit = entry - price

        if profit > new_max_profit:
            new_max_profit = profit
        if profit < new_max_loss:
            new_max_loss = profit

        if profit > max_fav:
            max_fav = profit

        # T1 hit at +10
        if max_fav >= 10 and not t1_hit:
            t1_hit = True

        # Trail logic
        trail_lock = None
        if max_fav >= ACTIVATION:
            trail_lock = max_fav - GAP
        elif max_fav >= BE_TRIGGER:
            trail_lock = 0  # breakeven

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
        if is_long and price <= stop:
            pnl = stop - entry
            if t1_hit:
                final_pnl = round((10.0 + pnl) / 2, 1)
                first_event = "target"
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
                first_event = "target"
            else:
                final_pnl = round(pnl, 1)
                first_event = "target" if pnl > 0 else "stop"
            trail_stopped = True
            elapsed_min = (price_ts - ts).total_seconds() / 60
            break

    # If not stopped: EOD mark-to-market
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
    # If T1 hit, overall is WIN
    if t1_hit and final_pnl >= 0:
        new_result = "WIN"

    diff = final_pnl - old_pnl
    total_old += old_pnl
    total_new += final_pnl

    notes = ""
    if diff != 0:
        notes = "IMPROVED" if diff > 0 else "WORSE"
    if new_result != old_result:
        notes += " %s->%s" % (old_result, new_result)

    print("%4d %s %6s | %4s %+7.1f | %4s %+7.1f | %+7.1f | %s" % (
        tid, str(alert_date), direction,
        old_result, old_pnl,
        new_result, final_pnl,
        diff, notes))

    updates.append({
        'id': tid,
        'result': new_result,
        'pnl': final_pnl,
        'max_profit': round(new_max_profit, 2),
        'max_loss': round(new_max_loss, 2),
        'first_event': first_event,
        'elapsed': round(elapsed_min, 1) if elapsed_min else None,
        'stop_level': round(stop, 2),
    })

print("\n" + "=" * 90)
print("TOTAL OLD (fixed 10pt): %+.1f pts" % total_old)
print("TOTAL NEW (trail gap=8): %+.1f pts" % total_new)
print("DIFFERENCE:              %+.1f pts" % (total_new - total_old))
print("\nTrades to update: %d" % len(updates))

# Apply updates
if updates:
    print("\nApplying updates to setup_log...")
    for u in updates:
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
        """, (u['result'], u['pnl'], u['max_profit'], u['max_loss'],
              u['first_event'], u['elapsed'], u['stop_level'], u['id']))
    conn.commit()
    print("Updated %d trades." % len(updates))
else:
    print("No updates needed.")

conn.close()
