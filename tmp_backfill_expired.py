"""Backfill EXPIRED trades with outcome_pnl = 0.0 - APPLY UPDATE"""
import os
from datetime import date as dt_date
import psycopg2

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

dates = [dt_date(2026, 2, 24), dt_date(2026, 2, 25), dt_date(2026, 2, 26)]

def is_long(direction):
    return direction in ('long', 'bullish', 'BUY')

# Get closing SPX spot from playback_snapshots (last before 16:00 ET)
closing_spx = {}
for d in dates:
    cur.execute("""
        SELECT spot FROM playback_snapshots
        WHERE (ts AT TIME ZONE 'America/New_York')::date = %s
          AND (ts AT TIME ZONE 'America/New_York')::time <= '16:00'
          AND spot IS NOT NULL
        ORDER BY ts DESC LIMIT 1;
    """, (d,))
    row = cur.fetchone()
    if row:
        closing_spx[d] = float(row[0])

# Get closing ES price from es_range_bars (last before 16:05 ET)
closing_es = {}
for d in dates:
    cur.execute("""
        SELECT bar_close FROM es_range_bars
        WHERE trade_date = %s
          AND (ts_end AT TIME ZONE 'America/New_York')::time <= '16:05'
        ORDER BY ts_end DESC LIMIT 1;
    """, (d,))
    row = cur.fetchone()
    if row:
        closing_es[d] = float(row[0])

print("Closing SPX:", {str(k): v for k, v in closing_spx.items()})
print("Closing ES:", {str(k): v for k, v in closing_es.items()})

# Get all affected trades
cur.execute("""
    SELECT id, (ts AT TIME ZONE 'America/New_York')::date as trade_date,
           setup_name, direction, spot, abs_es_price,
           outcome_pnl, outcome_max_profit, outcome_max_loss
    FROM setup_log
    WHERE outcome_result = 'EXPIRED' AND outcome_pnl = 0.0
    ORDER BY ts;
""")
trades = cur.fetchall()

# Compute and apply updates
updates = []
for t in trades:
    tid, trade_date, setup_name, direction, entry_spot, abs_es_price = t[0], t[1], t[2], t[3], float(t[4]), t[5]

    if setup_name == 'ES Absorption' and abs_es_price:
        es_entry = float(abs_es_price)
        if trade_date in closing_es:
            es_close = closing_es[trade_date]
            pnl = round(es_close - es_entry, 2) if is_long(direction) else round(es_entry - es_close, 2)
            updates.append((tid, pnl))
    else:
        if trade_date in closing_spx:
            close = closing_spx[trade_date]
            pnl = round(close - entry_spot, 2) if is_long(direction) else round(entry_spot - close, 2)
            updates.append((tid, pnl))

print(f"\nApplying {len(updates)} updates...")

# Apply the UPDATE
for tid, pnl in updates:
    cur.execute("""
        UPDATE setup_log SET outcome_pnl = %s WHERE id = %s;
    """, (pnl, tid))
    print(f"  Updated id={tid}: outcome_pnl = {pnl}")

conn.commit()
print(f"\nCOMMITTED {len(updates)} updates.")

# Verify the updates
print(f"\n{'='*140}")
print("VERIFICATION - Updated trades:")
print(f"{'='*140}")
cur.execute("""
    SELECT id, (ts AT TIME ZONE 'America/New_York')::date as trade_date,
           setup_name, direction, spot,
           outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
           ts AT TIME ZONE 'America/New_York' as time_et
    FROM setup_log
    WHERE id = ANY(%s)
    ORDER BY ts;
""", ([tid for tid, _ in updates],))

rows = cur.fetchall()
print(f"{'id':>4} | {'date':>10} | {'setup':<20} | {'dir':>7} | {'entry':>10} | {'result':>8} | {'pnl':>8} | {'max_p':>7} | {'max_l':>7}")
print("-" * 120)
for r in rows:
    print(f"{r[0]:>4} | {str(r[1]):>10} | {r[2]:<20} | {r[3]:>7} | {r[4]:>10} | {r[5]:>8} | {r[6]:>8.2f} | {r[7]:>7} | {r[8]:>7}")

# Check no more 0-PnL expired trades remain
cur.execute("SELECT count(*) FROM setup_log WHERE outcome_result = 'EXPIRED' AND outcome_pnl = 0.0;")
remaining = cur.fetchone()[0]
print(f"\nRemaining EXPIRED trades with outcome_pnl = 0.0: {remaining}")

# Grand total PnL
cur.execute("SELECT SUM(outcome_pnl) FROM setup_log WHERE outcome_pnl IS NOT NULL;")
grand_total = cur.fetchone()[0]
print(f"Grand total PnL (all trades): {grand_total:+.2f} pts")

# Per-setup summary
print("\nPer-setup PnL summary (all trades):")
cur.execute("""
    SELECT setup_name,
           COUNT(*) as trades,
           SUM(CASE WHEN outcome_result = 'WIN' THEN 1 ELSE 0 END) as wins,
           SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
           SUM(CASE WHEN outcome_result = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
           ROUND(SUM(outcome_pnl)::numeric, 2) as total_pnl
    FROM setup_log
    WHERE outcome_pnl IS NOT NULL
    GROUP BY setup_name
    ORDER BY total_pnl DESC;
""")
for r in cur.fetchall():
    print(f"  {r[0]:<20}: {r[1]:>3} trades, {r[2]}W/{r[3]}L/{r[4]}E, {r[5]:>+8.2f} pts")

conn.close()
