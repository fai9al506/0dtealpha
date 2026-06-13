"""S179 FIFO-vs-OID per-lid attribution audit.

Bot tracks each lid's close_fill_price via the OID it submitted. But TS broker
uses FIFO accounting: a sell pairs with the OLDEST still-open buy in the
account's net position. When multiple positions are open and closes fire
near-simultaneously, the per-OID price stored against each lid != the FIFO
price the broker actually books to that lid.

Net broker P&L is always correct. But per-lid (and therefore per-setup) P&L
attribution in setup_log/real_trade_orders is shuffled.

This script:
  1. For each trading day in 2026, find days where >=2 positions were
     concurrently open on the same account.
  2. Compare bot's per-lid close_fill_price (OID-based) vs FIFO-derived close.
  3. Quantify how much per-setup P&L is shuffled vs broker truth.
"""
import psycopg2, json
from collections import defaultdict

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB); cur = conn.cursor()

# Pull every closed trade from real_trade_orders
cur.execute("""
    SELECT setup_log_id,
           state->>'account_id' AS acct,
           state->>'setup_name' AS setup,
           state->>'direction' AS dir,
           (state->>'fill_price')::numeric AS fill,
           (state->>'close_fill_price')::numeric AS close,
           state->>'ts_placed' AS placed,
           state->>'close_reason' AS reason,
           created_at,
           updated_at
    FROM real_trade_orders
    WHERE state->>'status' = 'closed'
      AND state->>'fill_price' IS NOT NULL
      AND (state->>'fill_price')::numeric > 0
    ORDER BY created_at
""")
rows = cur.fetchall()
print(f"Pulled {len(rows)} closed real trades total\n")

# Group by (acct, date)
groups = defaultdict(list)
for r in rows:
    sid, acct, setup, direction, fill, close, placed, reason, created, updated = r
    if not acct or not fill:
        continue
    date = created.date()
    groups[(acct, date)].append({
        "lid": sid, "setup": setup, "dir": direction,
        "fill": float(fill), "close": float(close) if close else None,
        "placed_ts": created, "updated_ts": updated, "reason": reason,
    })

# For each group, detect concurrent overlap
# (a position is "open" between created_at and updated_at).
concurrent_days = []
for (acct, date), trades in groups.items():
    if len(trades) < 2:
        continue
    # Sort by created
    trades.sort(key=lambda t: t["placed_ts"])
    # Pairwise overlap check
    max_concurrent = 1
    for i in range(len(trades)):
        cnt = 1
        for j in range(i+1, len(trades)):
            if trades[j]["placed_ts"] < trades[i]["updated_ts"]:
                cnt += 1
        max_concurrent = max(max_concurrent, cnt)
    if max_concurrent >= 2:
        concurrent_days.append((acct, date, len(trades), max_concurrent, trades))

print(f"Days with >=2 concurrent positions on same account: {len(concurrent_days)}\n")

# For each concurrent day, FIFO-match and compare
total_lids_affected = 0
total_pnl_shift_abs = 0.0  # sum of |bot_pnl - fifo_pnl| in pts
days_with_shift = 0
big_shifts = []  # lids where shift > 5 pts

for acct, date, total, peak, trades in concurrent_days:
    # FIFO match: sort entries by placed_ts ASC; sort exits by updated_ts (close time) ASC.
    entries_sorted = sorted(trades, key=lambda t: t["placed_ts"])  # FIFO order
    exits_sorted = sorted([t for t in trades if t["close"] is not None],
                          key=lambda t: t["updated_ts"])
    # FIFO pairing: i-th exit closes i-th entry
    pairs = list(zip(entries_sorted[:len(exits_sorted)], exits_sorted))
    day_shift = 0.0
    day_affected = 0
    for entry_lid, exit_fill_holder in pairs:
        is_long = entry_lid["dir"] in ("long", "bullish")
        bot_pnl = (entry_lid["close"] - entry_lid["fill"]) if entry_lid["close"] is not None else None
        if not is_long and bot_pnl is not None:
            bot_pnl = -bot_pnl
        # FIFO: entry closed at exit_fill_holder.close
        fifo_close = exit_fill_holder["close"]
        fifo_pnl = (fifo_close - entry_lid["fill"]) if fifo_close is not None else None
        if not is_long and fifo_pnl is not None:
            fifo_pnl = -fifo_pnl
        if bot_pnl is None or fifo_pnl is None:
            continue
        shift = fifo_pnl - bot_pnl
        if abs(shift) > 0.01:
            day_affected += 1
            day_shift += abs(shift)
            if abs(shift) > 5.0:
                big_shifts.append({
                    "acct": acct, "date": str(date), "lid": entry_lid["lid"],
                    "setup": entry_lid["setup"], "dir": entry_lid["dir"],
                    "bot_pnl": bot_pnl, "fifo_pnl": fifo_pnl, "shift": shift,
                })
    if day_affected > 0:
        days_with_shift += 1
        total_lids_affected += day_affected
        total_pnl_shift_abs += day_shift

print(f"=== Summary ===")
print(f"Concurrent-position days analyzed: {len(concurrent_days)}")
print(f"Days where bot vs FIFO attribution diverges: {days_with_shift}")
print(f"Total lids with shifted attribution: {total_lids_affected}")
print(f"Total |shift| sum: {total_pnl_shift_abs:.2f} pts (= ${total_pnl_shift_abs * 5:.2f} at 1 MES)")
print(f"Big shifts (|shift| > 5 pts): {len(big_shifts)} lids\n")

if big_shifts:
    print("Top 15 biggest shifts:")
    big_shifts.sort(key=lambda x: -abs(x["shift"]))
    for b in big_shifts[:15]:
        print(f"  {b['date']} acct={b['acct']} lid={b['lid']:5d} {b['setup']:25s} {b['dir']:8s} "
              f"bot={b['bot_pnl']:+6.2f} fifo={b['fifo_pnl']:+6.2f} shift={b['shift']:+6.2f}")

conn.close()
