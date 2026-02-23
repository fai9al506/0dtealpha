"""Test the absorption outcome calculation after the fix"""
import os, sys
from datetime import datetime
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
eng = create_engine(DB_URL)

# Simulate what _calculate_absorption_outcome does
with eng.begin() as conn:
    trades = conn.execute(text("""
        SELECT id, ts, setup_name, direction, spot, abs_es_price, paradigm, target
        FROM setup_log
        WHERE setup_name = 'ES Absorption'
        ORDER BY ts ASC
    """)).mappings().all()

    for t in trades:
        ts = t['ts']
        es_entry = float(t['abs_es_price'])
        is_long = t['direction'].lower() in ('long', 'bullish')
        ten_pt_level = es_entry + 10 if is_long else es_entry - 10
        stop_level = es_entry - 12 if is_long else es_entry + 12

        alert_date = ts.strftime('%Y-%m-%d')

        bar_rows = conn.execute(text("""
            SELECT bar_idx, bar_high, bar_low, bar_close, ts_start, ts_end
            FROM es_range_bars
            WHERE trade_date = :td AND symbol = '@ES' AND status = 'closed'
            ORDER BY bar_idx ASC
        """), {"td": alert_date}).mappings().all()

        # NEW matching: last bar that started before trade ts
        signal_bar_idx = None
        for r in bar_rows:
            bar_start = r['ts_start']
            if bar_start <= ts:
                signal_bar_idx = r['bar_idx']
            else:
                break

        print(f"\nTrade #{t['id']}: ts={ts} es_entry={es_entry} {t['direction']}")
        print(f"  signal_bar_idx={signal_bar_idx} (total bars={len(bar_rows)})")

        if signal_bar_idx is None:
            print(f"  NO MATCHING BAR!")
            continue

        # Walk bars after signal
        hit_10pt = False
        hit_target = False
        hit_stop = False
        first_event = None
        max_profit = 0
        max_loss = 0

        for r in bar_rows:
            if r['bar_idx'] <= signal_bar_idx:
                continue
            bh = float(r['bar_high'])
            bl = float(r['bar_low'])

            if is_long:
                if not hit_10pt and bh >= ten_pt_level:
                    hit_10pt = True
                    if not first_event: first_event = "10pt"
                if not hit_stop and bl <= stop_level:
                    hit_stop = True
                    if not first_event: first_event = "stop"
                profit = bh - es_entry
                loss = bl - es_entry
            else:
                if not hit_10pt and bl <= ten_pt_level:
                    hit_10pt = True
                    if not first_event: first_event = "10pt"
                if not hit_stop and bh >= stop_level:
                    hit_stop = True
                    if not first_event: first_event = "stop"
                profit = es_entry - bl
                loss = es_entry - bh

            if profit > max_profit: max_profit = profit
            if loss < max_loss: max_loss = loss

        print(f"  hit_10pt={hit_10pt} hit_target={hit_target} hit_stop={hit_stop}")
        print(f"  first_event={first_event} max_profit={max_profit:.1f} max_loss={max_loss:.1f}")

        # Determine result
        if first_event == "10pt":
            result = "WIN"
        elif first_event == "stop":
            result = "LOSS"
        else:
            result = "EXPIRED"
        print(f"  result={result}")
