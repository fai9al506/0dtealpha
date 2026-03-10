"""Deep analysis of trades #349 and #353 — opposite direction problem."""
import psycopg2, os, json
from datetime import timedelta

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

for tid in [349, 353]:
    print(f"\n{'='*100}")
    print(f"TRADE #{tid} — DEEP ANALYSIS")
    print(f"{'='*100}")

    # Get trade details
    cur.execute("""
        SELECT id, ts, direction, grade, score, spot, abs_es_price,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               abs_details::text, paradigm, comments, abs_vol_ratio
        FROM setup_log WHERE id = %s
    """, (tid,))
    r = cur.fetchone()
    ts = r[1]
    ts_et = ts - timedelta(hours=5)
    abs_d = json.loads(r[11]) if r[11] else {}
    best = abs_d.get('best_swing', {})
    ref_sw = best.get('ref_swing', {})
    rec_sw = best.get('swing', {})

    print(f"\n--- TRADE INFO ---")
    print(f"Time ET:     {ts_et.strftime('%H:%M:%S')}")
    print(f"Direction:   {r[2]}")
    print(f"Pattern:     {abs_d.get('pattern', '?')}")
    print(f"Grade:       {r[3]} (score={r[4]})")
    print(f"SPX Spot:    {r[5]}")
    print(f"ES Price:    {r[6]}")
    print(f"Paradigm:    {r[12]}")
    print(f"Result:      {r[7]} (pnl={r[8]}, maxP={r[9]}, maxL={r[10]})")
    print(f"Vol Ratio:   {abs_d.get('vol_ratio', '?')}")
    print(f"CVD Z-score: {best.get('cvd_z', '?')}")
    print(f"CVD Std:     {abs_d.get('cvd_std', '?')}")
    print(f"ATR:         {abs_d.get('atr', '?')}")
    print(f"Trigger Bar: {abs_d.get('bar_idx', '?')}")
    print(f"Pattern Tier: {abs_d.get('pattern_tier', '?')}")
    print(f"Resolution:  {abs_d.get('resolution_reason', '?')}")

    print(f"\n--- SWING PAIR (used for signal) ---")
    print(f"Ref Swing (Sw1): type={ref_sw.get('type')} bar={ref_sw.get('bar_idx')} price={ref_sw.get('price')} cvd={ref_sw.get('cvd')} vol={ref_sw.get('volume')} ts={ref_sw.get('ts','?')}")
    print(f"Rec Swing (Sw2): type={rec_sw.get('type')} bar={rec_sw.get('bar_idx')} price={rec_sw.get('price')} cvd={rec_sw.get('cvd')} vol={rec_sw.get('volume')} ts={rec_sw.get('ts','?')}")

    sw1_price = ref_sw.get('price', 0)
    sw2_price = rec_sw.get('price', 0)
    sw1_cvd = ref_sw.get('cvd', 0)
    sw2_cvd = rec_sw.get('cvd', 0)
    if sw1_price and sw2_price:
        print(f"\nPrice: {sw1_price} -> {sw2_price} (delta={sw2_price-sw1_price:+.2f})")
    if sw1_cvd is not None and sw2_cvd is not None:
        print(f"CVD:   {sw1_cvd} -> {sw2_cvd} (delta={sw2_cvd-sw1_cvd:+.0f})")

    sw_type = ref_sw.get('type', '')
    pattern = abs_d.get('pattern', '')
    if sw_type == 'H':
        if sw2_price > sw1_price:
            print(f"  => Higher High (price up)")
        else:
            print(f"  => Lower High (price down)")
        if sw2_cvd > sw1_cvd:
            print(f"  => Higher CVD (buying pressure up)")
        else:
            print(f"  => Lower CVD (buying pressure down)")
    elif sw_type == 'L':
        if sw2_price < sw1_price:
            print(f"  => Lower Low (price down)")
        else:
            print(f"  => Higher Low (price up)")
        if sw2_cvd > sw1_cvd:
            print(f"  => Higher CVD (buying pressure up)")
        else:
            print(f"  => Lower CVD (buying pressure down)")

    print(f"\n  PATTERN LOGIC: {pattern}")
    if pattern == 'buy_exhaustion':
        print(f"  = Higher high + Lower CVD => buyers exhausted => SELL")
    elif pattern == 'buy_absorption':
        print(f"  = Lower high + Higher CVD => passive sellers absorbing => SELL")
    elif pattern == 'sell_exhaustion':
        print(f"  = Lower low + Higher CVD => sellers exhausted => BUY")
    elif pattern == 'sell_absorption':
        print(f"  = Higher low + Lower CVD => passive buyers absorbing => BUY")

    # Show ALL divergences found on this bar
    print(f"\n--- ALL DIVERGENCES FOUND ON THIS BAR ---")
    all_bull = abs_d.get('all_bull_divs', [])
    all_bear = abs_d.get('all_bear_divs', [])
    print(f"Bullish divergences: {len(all_bull)}")
    for d in all_bull:
        print(f"  {d.get('pattern'):25s} | z={d.get('cvd_z',0):5.2f} | score={d.get('score',0):5.1f} | type={d.get('swing_type')} @ {d.get('swing_price')}")
    print(f"Bearish divergences: {len(all_bear)}")
    for d in all_bear:
        print(f"  {d.get('pattern'):25s} | z={d.get('cvd_z',0):5.2f} | score={d.get('score',0):5.1f} | type={d.get('swing_type')} @ {d.get('swing_price')}")

    rejected = abs_d.get('rejected_divergence')
    if rejected:
        print(f"\nRejected divergence: {rejected}")

    # Get surrounding bars (20 before, 20 after trigger)
    trigger_idx = abs_d.get('bar_idx')
    if trigger_idx:
        print(f"\n--- SURROUNDING BARS (trigger bar #{trigger_idx}) ---")
        cur.execute("""
            SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                   bar_volume, bar_delta, cvd_close, ts_start
            FROM es_range_bars
            WHERE trade_date = '2026-03-02' AND source = 'rithmic'
                  AND bar_idx BETWEEN %s AND %s
            ORDER BY bar_idx
        """, (trigger_idx - 20, trigger_idx + 20))
        context_bars = cur.fetchall()

        print(f"\n{'Bar':>4} | {'Time ET':>8} | {'Open':>8} | {'High':>8} | {'Low':>8} | {'Close':>8} | {'Vol':>6} | {'Delta':>6} | {'CVD':>8} | Note")
        print("-" * 110)
        for cb in context_bars:
            bidx, bo, bh, bl, bc, bvol, bdelta, bcvd, bts = cb
            bts_et = bts - timedelta(hours=5) if bts else None
            t_str = bts_et.strftime('%H:%M:%S') if bts_et else '?'
            note = ''
            if bidx == trigger_idx:
                note = '<< TRIGGER'
            if ref_sw.get('bar_idx') == bidx:
                note += ' << SW1 (ref)'
            if rec_sw.get('bar_idx') == bidx:
                note += ' << SW2 (recent)'
            print(f"{bidx:>4} | {t_str:>8} | {float(bo):>8.2f} | {float(bh):>8.2f} | {float(bl):>8.2f} | {float(bc):>8.2f} | {int(bvol):>6} | {int(bdelta):>+6} | {float(bcvd):>+8.0f} | {note}")

    # Get ALL swings tracked at the time of this bar
    # We can reconstruct what swings existed by looking at nearby swing bars
    print(f"\n--- RECENT SWING POINTS (from bars around trigger) ---")
    cur.execute("""
        SELECT bar_idx, bar_high, bar_low, cvd_close, ts_start
        FROM es_range_bars
        WHERE trade_date = '2026-03-02' AND source = 'rithmic'
              AND bar_idx BETWEEN %s AND %s
        ORDER BY bar_idx
    """, (trigger_idx - 60, trigger_idx))
    nearby = cur.fetchall()

    # Simple pivot detection (left=2, right=2) to show what swings existed
    bars_list = [(r[0], float(r[1]), float(r[2]), float(r[3]), r[4]) for r in nearby]
    print(f"\nPivot highs and lows in 60 bars before trigger:")
    for i in range(2, len(bars_list) - 2):
        idx, high, low, cvd, bts = bars_list[i]
        # Check pivot high
        if (high >= bars_list[i-1][1] and high >= bars_list[i-2][1] and
            high >= bars_list[i+1][1] and high >= bars_list[i+2][1]):
            bts_et = bts - timedelta(hours=5) if bts else None
            t_str = bts_et.strftime('%H:%M') if bts_et else '?'
            marker = ' << USED' if idx in [ref_sw.get('bar_idx'), rec_sw.get('bar_idx')] else ''
            print(f"  HIGH: bar={idx} time={t_str} price={high:.2f} cvd={cvd:+.0f}{marker}")
        # Check pivot low
        if (low <= bars_list[i-1][2] and low <= bars_list[i-2][2] and
            low <= bars_list[i+1][2] and low <= bars_list[i+2][2]):
            bts_et = bts - timedelta(hours=5) if bts else None
            t_str = bts_et.strftime('%H:%M') if bts_et else None
            marker = ' << USED' if idx in [ref_sw.get('bar_idx'), rec_sw.get('bar_idx')] else ''
            print(f"  LOW:  bar={idx} time={t_str} price={low:.2f} cvd={cvd:+.0f}{marker}")

    # What happened AFTER the signal (next 30 bars)
    print(f"\n--- PRICE ACTION AFTER SIGNAL (next 30 bars) ---")
    cur.execute("""
        SELECT bar_idx, bar_close, bar_high, bar_low, cvd_close, ts_start
        FROM es_range_bars
        WHERE trade_date = '2026-03-02' AND source = 'rithmic'
              AND bar_idx BETWEEN %s AND %s
        ORDER BY bar_idx
    """, (trigger_idx, trigger_idx + 30))
    after_bars = cur.fetchall()
    if after_bars:
        entry_price = float(after_bars[0][1])
        max_up = 0
        max_down = 0
        for ab in after_bars:
            ab_high = float(ab[2])
            ab_low = float(ab[3])
            up_from_entry = ab_high - entry_price
            down_from_entry = entry_price - ab_low
            if up_from_entry > max_up: max_up = up_from_entry
            if down_from_entry > max_down: max_down = down_from_entry
        print(f"  Entry price: {entry_price}")
        print(f"  Max move UP:   +{max_up:.2f} pts")
        print(f"  Max move DOWN: -{max_down:.2f} pts")
        print(f"  Signal was {r[2]} -> {'CORRECT' if (r[2]=='bearish' and max_down>max_up) or (r[2]=='bullish' and max_up>max_down) else 'WRONG'}")
        print(f"  If OPPOSITE direction: {'SELL' if r[2]=='bullish' else 'BUY'} would have captured {max_down if r[2]=='bullish' else max_up:.1f} pts")

    print()

conn.close()
