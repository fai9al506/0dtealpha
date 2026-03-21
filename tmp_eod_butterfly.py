"""
EOD Butterfly Target Study using Aggregate DD Hedging
=====================================================
Wizard: "where DD is neutral = perfect close for dealers"
DD > 0 = bearish (dealers sell). DD < 0 = bullish (dealers buy).
DD -> 0 = equilibrium = price settles.

Note: DD value has NO sign prefix in our DB. Positive = hedging long (bullish).
Need to check actual sign convention from raw data.
"""

from sqlalchemy import create_engine, text
from collections import defaultdict
import json, re

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)


def parse_dd(dd_str):
    """Parse '$1,244,694,042' or '-$500,000,000' to float."""
    if not dd_str or dd_str == '$0': return 0.0
    s = dd_str.replace('$', '').replace(',', '').strip()
    try:
        return float(s)
    except ValueError:
        return None


def main():
    # Fetch all volland snapshots with DD data
    print("Fetching volland_snapshots...")
    sql = text("""
        SELECT ts,
               payload->'statistics'->>'delta_decay_hedging' as dd,
               payload->>'current_price' as cp,
               payload->'statistics'->>'paradigm' as paradigm,
               payload->'statistics'->>'aggregatedCharm' as charm
        FROM volland_snapshots
        WHERE ts::date >= '2026-02-11'
          AND payload->'statistics'->>'delta_decay_hedging' IS NOT NULL
          AND payload->'statistics'->>'delta_decay_hedging' != '$0'
          AND payload->>'current_price' IS NOT NULL
        ORDER BY ts
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    print(f"  {len(rows)} snapshots with DD + price")

    # Group by date and build intraday trajectory
    daily = defaultdict(list)
    for r in rows:
        dd = parse_dd(r.dd)
        if dd is None: continue
        try:
            spot = float(r.cp)
        except (ValueError, TypeError):
            continue

        d = str(r.ts.date())
        # ET offset (EDT after Mar 9, EST before)
        if r.ts.month > 3 or (r.ts.month == 3 and r.ts.day >= 9):
            et_h = r.ts.hour - 4
        else:
            et_h = r.ts.hour - 5
        et_decimal = et_h + r.ts.minute / 60.0

        daily[d].append({
            'ts': r.ts, 'dd': dd, 'spot': spot,
            'et': et_decimal, 'paradigm': r.paradigm,
            'charm_str': r.charm,
        })

    print(f"  {len(daily)} trading days\n")

    # Fetch actual closes
    sql2 = text("""
        SELECT DISTINCT ON (ts::date) ts::date as d, spot
        FROM chain_snapshots
        WHERE ts::date >= '2026-02-11' AND spot IS NOT NULL
        ORDER BY ts::date, ts DESC
    """)
    with engine.connect() as conn:
        closes = {str(r.d): float(r.spot) for r in conn.execute(sql2).fetchall() if r.spot}

    # ================================================================
    #  1. DD SIGN CONVENTION CHECK
    # ================================================================
    print("="*80)
    print("1. DD SIGN CONVENTION (sample values through Mar 19)")
    print("="*80 + "\n")

    d19 = daily.get('2026-03-19', [])
    market_hours = [s for s in d19 if 9.5 <= s['et'] <= 16.0]
    print(f"  Mar 19 samples ({len(market_hours)} snapshots during market hours):")
    print(f"  {'ET':>6} {'DD Value':>15} {'DD $B':>8} {'Spot':>8}")
    for s in market_hours[::15]:  # every 15th snapshot
        print(f"  {s['et']:>6.1f} {s['dd']:>15,.0f} {s['dd']/1e9:>+8.2f}B {s['spot']:>8.1f}")

    # ================================================================
    #  2. DD TRAJECTORY THROUGH THE DAY
    # ================================================================
    print(f"\n{'='*80}")
    print("2. DD TRAJECTORY + ACTUAL CLOSE")
    print("="*80 + "\n")

    def get_at(snaps, target_et, window=0.5):
        nearby = [s for s in snaps if abs(s['et'] - target_et) <= window]
        if not nearby: return None, None
        best = min(nearby, key=lambda s: abs(s['et'] - target_et))
        return best['dd'], best['spot']

    print(f"  {'Date':>12} {'DD@10':>9} {'DD@12':>9} {'DD@14':>9} {'DD@15':>9} {'DD@15:30':>9} "
          f"{'Spot@14':>8} {'Close':>8} {'PM Move':>8}")
    print(f"  {'-'*12} {'-'*9} {'-'*9} {'-'*9} {'-'*9} {'-'*9} {'-'*8} {'-'*8} {'-'*8}")

    analysis = []
    for d in sorted(daily.keys()):
        actual = closes.get(d)
        if not actual: continue
        snaps = daily[d]
        mkt = [s for s in snaps if 9.5 <= s['et'] <= 16.0]
        if len(mkt) < 10: continue

        dd10, sp10 = get_at(mkt, 10.0)
        dd12, sp12 = get_at(mkt, 12.0)
        dd14, sp14 = get_at(mkt, 14.0)
        dd15, sp15 = get_at(mkt, 15.0)
        dd1530, sp1530 = get_at(mkt, 15.5)

        def fdd(v):
            if v is None: return "---"
            return f"{v/1e9:+.1f}B"

        pm_move = f"{actual - sp14:+.0f}" if sp14 else "---"
        print(f"  {d:>12} {fdd(dd10):>9} {fdd(dd12):>9} {fdd(dd14):>9} {fdd(dd15):>9} "
              f"{fdd(dd1530):>9} {sp14 or 0:>8.1f} {actual:>8.1f} {pm_move:>8}")

        if dd14 is not None and sp14 is not None:
            analysis.append({
                'd': d, 'dd14': dd14, 'sp14': sp14, 'actual': actual,
                'dd15': dd15, 'dd1530': dd1530, 'sp15': sp15,
                'dd10': dd10, 'sp10': sp10,
                'pm_move': actual - sp14,
            })

    if not analysis:
        print("\n  No data for analysis!")
        return

    # ================================================================
    #  3. DD DIRECTION vs AFTERNOON MOVE
    # ================================================================
    print(f"\n{'='*80}")
    print("3. DD@14:00 DIRECTION vs ACTUAL PM MOVE")
    print("="*80 + "\n")

    # DD positive in our system = hedging long = BULLISH (dealers buy)
    # DD negative = hedging short = BEARISH (dealers sell)
    # Check: does DD sign predict PM direction?
    correct = 0
    for a in analysis:
        dd_bullish = a['dd14'] > 0
        price_up = a['pm_move'] > 0
        if dd_bullish == price_up:
            correct += 1

    n = len(analysis)
    print(f"  DD positive = bullish? Correct: {correct}/{n} ({correct/n*100:.0f}%)")

    # Try opposite: DD positive = bearish
    correct2 = n - correct
    print(f"  DD positive = bearish? Correct: {correct2}/{n} ({correct2/n*100:.0f}%)")

    # Let's just show the data and let the pattern speak
    print(f"\n  {'Date':>12} {'DD@14':>10} {'PM Move':>8} {'DD>0 = Up?':>11} {'DD>0 = Down?':>12}")
    for a in analysis:
        dd_pos = a['dd14'] > 0
        up = a['pm_move'] > 0
        print(f"  {a['d']:>12} {a['dd14']/1e9:>+10.2f}B {a['pm_move']:>+8.1f} "
              f"{'YES' if dd_pos == up else 'NO':>11} {'YES' if dd_pos != up else 'NO':>12}")

    # ================================================================
    #  4. DD MAGNITUDE vs MOVE SIZE
    # ================================================================
    print(f"\n{'='*80}")
    print("4. |DD@14| vs AFTERNOON |MOVE|")
    print("="*80 + "\n")

    for lo, hi, label in [(0, 1e9, "<1B"), (1e9, 3e9, "1-3B"),
                          (3e9, 7e9, "3-7B"), (7e9, 1e15, ">7B")]:
        bucket = [a for a in analysis if lo <= abs(a['dd14']) < hi]
        if bucket:
            avg_move = sum(abs(a['pm_move']) for a in bucket) / len(bucket)
            print(f"  |DD| {label}: {len(bucket)} days, avg |PM move| {avg_move:.1f} pts")

    # ================================================================
    #  5. DD CONVERGENCE TO ZERO = SETTLING
    # ================================================================
    print(f"\n{'='*80}")
    print("5. DD CONVERGENCE: |DD| Shrinking = Price Settling?")
    print("="*80 + "\n")

    for a in analysis:
        if a['dd15'] is not None:
            a['dd_shrinking'] = abs(a['dd15']) < abs(a['dd14'])
        else:
            a['dd_shrinking'] = None

    shrinking = [a for a in analysis if a.get('dd_shrinking') == True]
    growing = [a for a in analysis if a.get('dd_shrinking') == False]

    if shrinking:
        avg_r = sum(abs(a['pm_move']) for a in shrinking) / len(shrinking)
        print(f"  DD shrinking 14->15 ({len(shrinking)} days): avg |PM move| {avg_r:.1f} pts")
    if growing:
        avg_r = sum(abs(a['pm_move']) for a in growing) / len(growing)
        print(f"  DD growing 14->15   ({len(growing)} days): avg |PM move| {avg_r:.1f} pts")

    # ================================================================
    #  6. BUTTERFLY SIMULATION
    # ================================================================
    print(f"\n{'='*80}")
    print("6. BUTTERFLY SIMULATION: Enter at 14:00, Expire at Close")
    print("="*80 + "\n")

    print("  Strategy: Buy SPX 0DTE butterfly centered at spot@14:00")
    print("  Typical cost: $5-wide ~$2.50/contract, $10-wide ~$4.50/contract")
    print("  Max profit: (width - cost) per $1 multiplier if close = center")
    print("  Breakeven: center +/- (width - cost)\n")

    print(f"  {'Date':>12} {'Center':>8} {'Close':>8} {'Error':>7} "
          f"{'$5w P/L':>8} {'$10w P/L':>9} {'$5w ROI':>8} {'$10w ROI':>9}")
    print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*7} {'-'*8} {'-'*9} {'-'*8} {'-'*9}")

    total_5 = total_10 = 0
    cost_5 = 250  # $2.50 x 100 multiplier
    cost_10 = 450  # $4.50 x 100 multiplier

    for a in analysis:
        center = round(a['sp14'] / 5) * 5  # round to nearest $5 strike
        err = abs(a['actual'] - center)

        # $5-wide butterfly
        payout_5 = max(0, (5 - err)) * 100
        pnl_5 = payout_5 - cost_5
        roi_5 = pnl_5 / cost_5 * 100

        # $10-wide butterfly
        payout_10 = max(0, (10 - err)) * 100
        pnl_10 = payout_10 - cost_10
        roi_10 = pnl_10 / cost_10 * 100

        total_5 += pnl_5
        total_10 += pnl_10

        win_5 = "WIN" if pnl_5 > 0 else "LOSS"
        print(f"  {a['d']:>12} {center:>8.0f} {a['actual']:>8.1f} {err:>7.1f} "
              f"{pnl_5:>+8.0f} {pnl_10:>+9.0f} {roi_5:>+7.0f}% {roi_10:>+8.0f}%")

    n = len(analysis)
    print(f"\n  TOTAL ($5-wide):  ${total_5:+,.0f} over {n} days (${total_5/n:+,.0f}/day)")
    print(f"  TOTAL ($10-wide): ${total_10:+,.0f} over {n} days (${total_10/n:+,.0f}/day)")

    # ── Selective: only when DD is shrinking ──
    print(f"\n  --- Selective: Only enter when |DD| shrinking (14->15) ---")
    if shrinking:
        t5_s = t10_s = 0
        wins_5 = wins_10 = 0
        for a in shrinking:
            center = round(a['sp14'] / 5) * 5
            err = abs(a['actual'] - center)
            p5 = max(0, (5 - err)) * 100 - cost_5
            p10 = max(0, (10 - err)) * 100 - cost_10
            t5_s += p5; t10_s += p10
            if p5 > 0: wins_5 += 1
            if p10 > 0: wins_10 += 1
        print(f"  Shrinking ({len(shrinking)} days): $5w ${t5_s:+,.0f} ({wins_5}W), "
              f"$10w ${t10_s:+,.0f} ({wins_10}W)")

    # ── Selective: only when |DD| < 3B (small = settling) ──
    print(f"\n  --- Selective: Only enter when |DD@14| < 3B (low DD = settling) ---")
    low_dd = [a for a in analysis if abs(a['dd14']) < 3e9]
    high_dd = [a for a in analysis if abs(a['dd14']) >= 3e9]
    if low_dd:
        t5 = t10 = 0
        for a in low_dd:
            center = round(a['sp14'] / 5) * 5
            err = abs(a['actual'] - center)
            t5 += max(0, (5-err))*100 - cost_5
            t10 += max(0, (10-err))*100 - cost_10
        avg_err = sum(abs(a['actual'] - round(a['sp14']/5)*5) for a in low_dd) / len(low_dd)
        print(f"  Low DD ({len(low_dd)} days): avg error {avg_err:.1f} pts, "
              f"$5w ${t5:+,.0f}, $10w ${t10:+,.0f}")
    if high_dd:
        t5 = t10 = 0
        for a in high_dd:
            center = round(a['sp14'] / 5) * 5
            err = abs(a['actual'] - round(a['sp14']/5)*5)
            t5 += max(0, (5-err))*100 - cost_5
            t10 += max(0, (10-err))*100 - cost_10
        avg_err = sum(abs(a['actual'] - round(a['sp14']/5)*5) for a in high_dd) / len(high_dd)
        print(f"  High DD ({len(high_dd)} days): avg error {avg_err:.1f} pts, "
              f"$5w ${t5:+,.0f}, $10w ${t10:+,.0f}")

    # ── Directional shift ──
    print(f"\n  --- Directional butterfly (shift center by DD direction) ---")
    # If DD bullish -> shift center up 5. If DD bearish -> shift center down 5.
    # Try both conventions
    for convention, shift_label in [(1, "DD>0 = shift UP"), (-1, "DD>0 = shift DOWN")]:
        t10 = 0
        for a in analysis:
            base = round(a['sp14'] / 5) * 5
            shift = 5 * convention if a['dd14'] > 0 else -5 * convention
            center = base + shift
            err = abs(a['actual'] - center)
            t10 += max(0, (10-err))*100 - cost_10
        print(f"  {shift_label}: $10w ${t10:+,.0f}")

    neutral_t10 = sum(max(0, (10-abs(a['actual']-round(a['sp14']/5)*5)))*100 - cost_10 for a in analysis)
    print(f"  Neutral (no shift): $10w ${neutral_t10:+,.0f}")

    # ================================================================
    #  7. 15:00 ENTRY (LATER, CLOSER TO CLOSE)
    # ================================================================
    print(f"\n{'='*80}")
    print("7. BUTTERFLY AT 15:00 (Cheaper, Closer to Expiry)")
    print("="*80 + "\n")

    print("  Later entry = cheaper premium but less time for adjustment")
    print("  Typical cost: $5-wide ~$1.00, $10-wide ~$2.00 at 15:00\n")

    cost_5_late = 100
    cost_10_late = 200
    total_5l = total_10l = 0
    wins_5l = wins_10l = 0

    for a in analysis:
        if a['sp15'] is None: continue
        center = round(a['sp15'] / 5) * 5
        err = abs(a['actual'] - center)
        p5 = max(0, (5-err))*100 - cost_5_late
        p10 = max(0, (10-err))*100 - cost_10_late
        total_5l += p5; total_10l += p10
        if p5 > 0: wins_5l += 1
        if p10 > 0: wins_10l += 1

    n15 = sum(1 for a in analysis if a['sp15'] is not None)
    print(f"  $5-wide @15:00:  ${total_5l:+,.0f} over {n15} days (${total_5l/n15:+,.0f}/day) [{wins_5l}W/{n15-wins_5l}L]")
    print(f"  $10-wide @15:00: ${total_10l:+,.0f} over {n15} days (${total_10l/n15:+,.0f}/day) [{wins_10l}W/{n15-wins_10l}L]")

    # Compare 14:00 vs 15:00 entry
    n14 = len(analysis)
    print(f"\n  Comparison (per day):")
    print(f"  {'':>20} {'$5w':>10} {'$10w':>10}")
    print(f"  {'14:00 entry':>20} ${total_5/n14:+,.0f}/day ${total_10/n14:+,.0f}/day")
    print(f"  {'15:00 entry':>20} ${total_5l/n15:+,.0f}/day ${total_10l/n15:+,.0f}/day")

    # ================================================================
    #  SUMMARY
    # ================================================================
    print(f"\n{'='*80}")
    print("SUMMARY")
    print("="*80 + "\n")

    avg_err_14 = sum(abs(a['actual'] - round(a['sp14']/5)*5) for a in analysis) / len(analysis)
    within5 = sum(1 for a in analysis if abs(a['actual'] - round(a['sp14']/5)*5) <= 5)
    within10 = sum(1 for a in analysis if abs(a['actual'] - round(a['sp14']/5)*5) <= 10)

    print(f"  Days analyzed: {len(analysis)}")
    print(f"  Avg |close - spot@14|: {avg_err_14:.1f} pts")
    print(f"  Close within 5 pts of spot@14: {within5}/{n} ({within5/n*100:.0f}%)")
    print(f"  Close within 10 pts of spot@14: {within10}/{n} ({within10/n*100:.0f}%)")

    # Direction accuracy
    bull_correct = sum(1 for a in analysis if (a['dd14'] > 0) == (a['pm_move'] > 0))
    bear_correct = n - bull_correct
    best = max(bull_correct, bear_correct)
    conv = "DD>0=BULL" if bull_correct > bear_correct else "DD>0=BEAR"
    print(f"  DD@14 direction accuracy: {best}/{n} ({best/n*100:.0f}%) [{conv}]")


if __name__ == '__main__':
    main()
