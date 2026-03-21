"""Fix setup_log #951 — SB Absorption LONG with wrong outcome."""
import os, sqlalchemy
from datetime import datetime, timezone

url = os.environ['DATABASE_URL']
engine = sqlalchemy.create_engine(url)

es_entry = 6678.75
target = es_entry + 10  # 6688.75
stop = es_entry - 8     # 6670.75

with engine.connect() as conn:
    row = conn.execute(sqlalchemy.text(
        "SELECT ts FROM setup_log WHERE id = 951"
    )).fetchone()
    entry_ts = row[0]
    print(f"Entry ts: {entry_ts} ({entry_ts.strftime('%H:%M ET') if entry_ts else '?'})")

    # Try Mar 18 trade date
    bars = conn.execute(sqlalchemy.text("""
        SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, ts_start, ts_end
        FROM es_range_bars
        WHERE trade_date = '2026-03-18' AND source = 'rithmic'
        ORDER BY bar_idx
    """)).fetchall()
    print(f"Rithmic bars on Mar 18: {len(bars)}")

    if not bars:
        # Try without date filter, just near the time
        bars = conn.execute(sqlalchemy.text("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, ts_start, ts_end, trade_date
            FROM es_range_bars
            WHERE source = 'rithmic' AND ts_start BETWEEN '2026-03-18 19:00:00+00' AND '2026-03-18 21:00:00+00'
            ORDER BY ts_start
        """)).fetchall()
        print(f"Bars near entry time: {len(bars)}")
        for b in bars[:5]:
            print(f"  idx={b[0]} O={b[1]} H={b[2]} L={b[3]} C={b[4]} ts={b[5]} date={b[7]}")

    # Find entry bar — closest ts_start <= entry_ts
    entry_bar_idx = None
    for b in bars:
        idx, o, h, l, c, ts_s, ts_e = b[:7]
        if ts_s and ts_s <= entry_ts:
            entry_bar_idx = idx
    print(f"Entry bar idx: {entry_bar_idx}")

    if entry_bar_idx is None:
        # Signal fired near market close (15:59 ET), might be after last bar
        # Show last few bars
        print("Last 5 bars:")
        for b in bars[-5:]:
            print(f"  idx={b[0]} O={b[1]} H={b[2]} L={b[3]} C={b[4]} ts={b[5]}")
        # Use the last bar as entry reference
        if bars:
            entry_bar_idx = bars[-1][0]
            print(f"Using last bar idx={entry_bar_idx}")

    if entry_bar_idx is not None:
        # Scan bars after entry
        seen_high = es_entry
        seen_low = es_entry
        target_hit_bar = None
        stop_hit_bar = None

        for b in bars:
            idx, o, h, l, c, ts_s, ts_e = b[:7]
            if idx <= entry_bar_idx:
                continue
            if h is not None:
                seen_high = max(seen_high, h)
            if l is not None:
                seen_low = min(seen_low, l)
            if stop_hit_bar is None and seen_low <= stop:
                stop_hit_bar = (idx, ts_s)
            if target_hit_bar is None and seen_high >= target:
                target_hit_bar = (idx, ts_s)
            if target_hit_bar and stop_hit_bar:
                break

        print(f"\nTarget hit: {target_hit_bar}")
        print(f"Stop hit: {stop_hit_bar}")

        # If signal was at 15:59 ET (near close), likely expired
        if not target_hit_bar and not stop_hit_bar:
            # Market closes at 16:00 ET, signal at 15:59 — only 1 min of bars
            result = "EXPIRED"
            # Use the ES price at close (last bar after entry)
            after_entry = [b for b in bars if b[0] > entry_bar_idx]
            if after_entry:
                close_price = after_entry[-1][4]
            else:
                close_price = es_entry
            pnl = round(close_price - es_entry, 1)
            max_profit = round(seen_high - es_entry, 1)
            max_loss = round(seen_low - es_entry, 1)
            elapsed = 1  # ~1 min to market close
            print(f"=> EXPIRED (near close), P&L: {pnl}, MP: {max_profit}, ML: {max_loss}")
        elif target_hit_bar and (not stop_hit_bar or target_hit_bar[0] <= stop_hit_bar[0]):
            result = "WIN"
            pnl = round(target - es_entry, 1)
            resolve_idx = target_hit_bar[0]
            mp_h, mp_l = es_entry, es_entry
            for b in bars:
                if b[0] <= entry_bar_idx or b[0] > resolve_idx:
                    continue
                if b[2] is not None: mp_h = max(mp_h, b[2])
                if b[3] is not None: mp_l = min(mp_l, b[3])
            max_profit = round(mp_h - es_entry, 1)
            max_loss = round(mp_l - es_entry, 1)
            elapsed = int((target_hit_bar[1] - entry_ts).total_seconds() / 60.0)
            print(f"=> WIN, P&L: {pnl}, MP: {max_profit}, ML: {max_loss}, elapsed: {elapsed}")
        else:
            result = "LOSS"
            pnl = round(stop - es_entry, 1)
            resolve_idx = stop_hit_bar[0]
            mp_h, mp_l = es_entry, es_entry
            for b in bars:
                if b[0] <= entry_bar_idx or b[0] > resolve_idx:
                    continue
                if b[2] is not None: mp_h = max(mp_h, b[2])
                if b[3] is not None: mp_l = min(mp_l, b[3])
            max_profit = round(mp_h - es_entry, 1)
            max_loss = round(mp_l - es_entry, 1)
            elapsed = int((stop_hit_bar[1] - entry_ts).total_seconds() / 60.0)
            print(f"=> LOSS, P&L: {pnl}, MP: {max_profit}, ML: {max_loss}, elapsed: {elapsed}")

        # Update
        conn.execute(sqlalchemy.text("""
            UPDATE setup_log SET
                outcome_result = :result,
                outcome_pnl = :pnl,
                outcome_max_profit = :mp,
                outcome_max_loss = :ml,
                outcome_elapsed_min = :elapsed
            WHERE id = 951
        """), {"result": result, "pnl": pnl, "mp": max_profit, "ml": max_loss, "elapsed": elapsed})
        conn.commit()

        row = conn.execute(sqlalchemy.text(
            "SELECT outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss, outcome_elapsed_min FROM setup_log WHERE id = 951"
        )).fetchone()
        print(f"\nUPDATED: result={row[0]}, pnl={row[1]}, max_profit={row[2]}, max_loss={row[3]}, elapsed={row[4]}")
    else:
        print("Could not find entry bar — cannot reconstruct")
