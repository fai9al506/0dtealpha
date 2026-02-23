"""Check both ES Absorption trades for the SPX fallback bug"""
import os
from sqlalchemy import create_engine, text

DB_URL = os.getenv("DATABASE_URL", "")
if "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)
engine = create_engine(DB_URL)

with engine.begin() as conn:
    trades = conn.execute(text("""
        SELECT id, ts, setup_name, direction, spot, abs_es_price, grade, score,
               outcome_result, outcome_pnl, outcome_target_level, outcome_stop_level,
               outcome_elapsed_min, outcome_first_event
        FROM setup_log
        WHERE setup_name = 'ES Absorption'
        ORDER BY ts ASC
    """)).mappings().all()

    for t in trades:
        es_entry = float(t['abs_es_price'])
        spx_spot = float(t['spot'])
        is_bearish = t['direction'].lower() in ('short', 'bearish')
        target = es_entry - 10 if is_bearish else es_entry + 10
        stop = es_entry + 12 if is_bearish else es_entry - 12

        print(f"=== Trade #{t['id']} ({t['direction']}) ===")
        print(f"  Time: {t['ts']}")
        print(f"  ES entry: {es_entry}, SPX spot: {spx_spot}")
        print(f"  Spread (ES - SPX): {es_entry - spx_spot:.1f}")
        print(f"  Target: {target:.1f} (ES {'- 10' if is_bearish else '+ 10'})")
        print(f"  Stop: {stop:.1f} (ES {'+ 12' if is_bearish else '- 12'})")
        print(f"  Outcome: {t['outcome_result']} PNL={t['outcome_pnl']} elapsed={t['outcome_elapsed_min']}min")
        print(f"  Stored target/stop: {t['outcome_target_level']}/{t['outcome_stop_level']}")

        # Key check: would SPX spot at entry hit the ES target?
        if is_bearish:
            spx_hits_es_target = spx_spot <= target
        else:
            spx_hits_es_target = spx_spot >= target
        print(f"\n  ** BUG CHECK: SPX spot ({spx_spot:.1f}) vs ES target ({target:.1f})")
        print(f"  ** SPX already {'AT/PAST' if spx_hits_es_target else 'NOT at'} ES target at entry time!")

        if spx_hits_es_target:
            print(f"  ** THIS MEANS: if live tracker ever used SPX instead of ES, it would falsely resolve as WIN!")

        # Show ES bars around the trade
        print(f"\n  ES bars from trade onwards:")
        bars = conn.execute(text("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, ts_start, ts_end
            FROM es_range_bars
            WHERE trade_date = :tdate AND symbol = '@ES'
              AND ts_start >= :start
            ORDER BY bar_idx ASC
            LIMIT 15
        """), {"start": t['ts'], "tdate": str(t['ts'])[:10]}).mappings().all()

        first_target_bar = None
        first_stop_bar = None
        for b in bars:
            hit = ""
            if is_bearish:
                if float(b['bar_low']) <= target:
                    hit += " TARGET"
                    if not first_target_bar:
                        first_target_bar = b
                if float(b['bar_high']) >= stop:
                    hit += " STOP"
                    if not first_stop_bar:
                        first_stop_bar = b
            else:
                if float(b['bar_high']) >= target:
                    hit += " TARGET"
                    if not first_target_bar:
                        first_target_bar = b
                if float(b['bar_low']) <= stop:
                    hit += " STOP"
                    if not first_stop_bar:
                        first_stop_bar = b
            print(f"    bar#{b['bar_idx']} {str(b['ts_start'])[11:19]} H={b['bar_high']} L={b['bar_low']} C={b['bar_close']}{' **'+hit+'**' if hit else ''}")

        # How long until ES actually hit target?
        if first_target_bar:
            entry_ts = t['ts']
            target_ts = first_target_bar['ts_start']
            actual_elapsed = (target_ts - entry_ts).total_seconds() / 60
            print(f"\n  ES actually hit target at bar#{first_target_bar['bar_idx']} ({first_target_bar['ts_start']})")
            print(f"  Actual elapsed: {actual_elapsed:.1f} min (stored says {t['outcome_elapsed_min']} min)")
        else:
            print(f"\n  ES NEVER hit target in shown bars!")

        if first_stop_bar:
            print(f"  ES hit stop at bar#{first_stop_bar['bar_idx']} ({first_stop_bar['ts_start']})")

        print()
