"""
OUT-OF-SAMPLE VALIDATION: Test bearish CVD flip logic.
Split: Feb 27 (14 trades) vs Mar 2 (17 trades).
Logic discovered on Mar 2 — does it hold on Feb 27?
Also test each day independently.
"""
import json, sys
sys.stdout.reconfigure(encoding='utf-8')

data = json.load(open('abs_data_all.json'))

# Split by date
feb27 = [t for t in data if t['trade_date'] == '2026-02-27']
mar02 = [t for t in data if t['trade_date'] == '2026-03-02']

print(f"Feb 27: {len(feb27)} trades")
print(f"Mar 02: {len(mar02)} trades")
print(f"Total:  {len(data)} trades")

def analyze_flip(trades, label):
    """Analyze bearish CVD flip + z>=0.75 on a set of trades."""
    print(f"\n{'='*120}")
    print(f"  {label}")
    print(f"{'='*120}")

    # Separate swing-to-swing from zone-revisit
    swing_trades = []
    zone_trades = []
    for t in trades:
        abs_d = t.get('abs_details', {})
        best = abs_d.get('best_swing', {})
        if not best:
            continue
        sw_type = best.get('ref_swing', {}).get('type', '')
        if sw_type == 'Z':
            zone_trades.append(t)
        else:
            swing_trades.append(t)

    print(f"\n  Swing-to-swing: {len(swing_trades)}, Zone-revisit: {len(zone_trades)}")

    # ---- BASELINE ----
    base_w = sum(1 for t in trades if t.get('abs_details',{}).get('best_swing') and t['result'] == 'WIN')
    base_l = sum(1 for t in trades if t.get('abs_details',{}).get('best_swing') and t['result'] == 'LOSS')
    base_e = sum(1 for t in trades if t.get('abs_details',{}).get('best_swing') and (t['result'] or 'OPEN') not in ('WIN','LOSS'))
    base_pnl = sum(t['pnl'] for t in trades if t.get('abs_details',{}).get('best_swing'))
    base_wr = round(100*base_w/(base_w+base_l), 1) if (base_w+base_l) else 0

    # ---- Identify bearish CVD overshoot ----
    print(f"\n  {'ID':>4} | {'Dir':>8} | {'Pattern':>22} | {'Type':>4} | {'Res':>7} | {'PnL':>6} | {'Trig CVD':>10} | {'Min SW CVD':>10} | {'Overshoot':>10} | Action")
    print("  " + "-"*115)

    kept_w = kept_l = kept_e = kept_pnl = 0
    flip_w = flip_l = flip_e = flip_pnl = 0
    blocked_pnl = 0

    for t in trades:
        abs_d = t.get('abs_details', {})
        best = abs_d.get('best_swing', {})
        if not best:
            continue
        ref_sw = best.get('ref_swing', {})
        rec_sw = best.get('swing', {})
        sw_type = ref_sw.get('type', '')
        result = t['result'] or 'OPEN'
        pnl = t['pnl']
        direction = t['direction']
        z = best.get('cvd_z', 0)
        max_profit = t.get('max_profit', 0) or 0
        max_loss = t.get('max_loss', 0) or 0

        trig_bar = t.get('trigger_bar') or {}
        trig_cvd = trig_bar.get('cvd')
        sw1_cvd = ref_sw.get('cvd', 0)
        sw2_cvd = rec_sw.get('cvd', 0)

        # Check bearish CVD overshoot
        bear_overshoot = False
        overshoot_str = ''
        min_sw_cvd = ''
        if direction == 'bearish' and sw_type == 'H' and trig_cvd is not None:
            min_cv = min(sw1_cvd, sw2_cvd)
            min_sw_cvd = f"{min_cv:+.0f}"
            if trig_cvd < min_cv:
                bear_overshoot = True
                pct = round(100 * (min_cv - trig_cvd) / abs(min_cv), 1) if min_cv != 0 else 0
                overshoot_str = f"{pct}%"

        action = ''
        if sw_type == 'Z':
            # Zone-revisit: always keep
            kept_pnl += pnl
            if result == 'WIN': kept_w += 1
            elif result == 'LOSS': kept_l += 1
            else: kept_e += 1
            action = 'KEEP (zone)'
        elif bear_overshoot:
            # FLIP to bullish
            if result == 'LOSS':
                fp = 10.0; flip_w += 1; fr = 'WIN'
            elif result == 'EXPIRED' or result == 'OPEN':
                if max_loss >= 10:
                    fp = 10.0; flip_w += 1; fr = 'WIN'
                elif max_profit >= 12:
                    fp = -12.0; flip_l += 1; fr = 'LOSS'
                else:
                    fp = max_loss - max_profit; flip_e += 1; fr = 'EXP'
            else:  # WIN (bearish won = bad for bullish flip)
                fp = -12.0; flip_l += 1; fr = 'LOSS'
            flip_pnl += fp
            action = f'FLIP->BULL {fr} {fp:+.1f}'
        elif z < 0.75 and sw_type != 'Z':
            # Z-score too low: block
            blocked_pnl += pnl
            action = f'BLOCK z={z:.2f}'
        else:
            # Keep as-is
            kept_pnl += pnl
            if result == 'WIN': kept_w += 1
            elif result == 'LOSS': kept_l += 1
            else: kept_e += 1
            action = 'KEEP'

        trig_cvd_str = f"{trig_cvd:+.0f}" if trig_cvd is not None else 'N/A'
        print(f"  #{t['id']:>4} | {direction:>8} | {abs_d.get('pattern',''):>22} | {sw_type:>4} | {result:>7} | {pnl:>+6.1f} | {trig_cvd_str:>10} | {min_sw_cvd:>10} | {overshoot_str:>10} | {action}")

    total_w = kept_w + flip_w
    total_l = kept_l + flip_l
    total_e = kept_e + flip_e
    total_pnl = kept_pnl + flip_pnl
    total_n = total_w + total_l + total_e
    wr = round(100*total_w/(total_w+total_l), 1) if (total_w+total_l) else 0

    print(f"\n  RESULTS FOR {label}:")
    print(f"  {'':>30} | {'Trades':>6} | {'W/L/E':>12} | {'WR':>6} | {'PnL':>8}")
    print(f"  {'BASELINE':<30} | {base_w+base_l+base_e:>6} | {base_w}W/{base_l}L/{base_e}E | {base_wr:>5.1f}% | {base_pnl:>+8.1f}")
    print(f"  {'WITH FLIP + Z>=0.75':<30} | {total_n:>6} | {total_w}W/{total_l}L/{total_e}E | {wr:>5.1f}% | {total_pnl:>+8.1f}")
    print(f"  {'IMPROVEMENT':<30} | {'':>6} | {'':>12} | {wr-base_wr:>+5.1f}% | {total_pnl-base_pnl:>+8.1f}")

    return {
        'base_pnl': base_pnl, 'base_wr': base_wr,
        'new_pnl': total_pnl, 'new_wr': wr,
        'flipped': flip_w + flip_l + flip_e,
        'flip_w': flip_w, 'flip_l': flip_l
    }

# Run on each day separately
r1 = analyze_flip(feb27, "FEB 27 (out-of-sample — logic discovered on Mar 2)")
r2 = analyze_flip(mar02, "MAR 02 (in-sample — where #349 and #353 were found)")
r3 = analyze_flip(data, "ALL TRADES COMBINED")

# Final verdict
print(f"\n{'='*120}")
print("CROSS-VALIDATION SUMMARY")
print(f"{'='*120}")
print(f"\n  {'Day':<30} | {'Base PnL':>8} | {'Base WR':>7} | {'New PnL':>8} | {'New WR':>7} | {'Flips':>5} | {'Flip W/L':>8} | {'Improve':>8}")
print(f"  {'-'*100}")
print(f"  {'Feb 27 (out-of-sample)':<30} | {r1['base_pnl']:>+8.1f} | {r1['base_wr']:>6.1f}% | {r1['new_pnl']:>+8.1f} | {r1['new_wr']:>6.1f}% | {r1['flipped']:>5} | {r1['flip_w']}W/{r1['flip_l']}L | {r1['new_pnl']-r1['base_pnl']:>+8.1f}")
print(f"  {'Mar 02 (in-sample)':<30} | {r2['base_pnl']:>+8.1f} | {r2['base_wr']:>6.1f}% | {r2['new_pnl']:>+8.1f} | {r2['new_wr']:>6.1f}% | {r2['flipped']:>5} | {r2['flip_w']}W/{r2['flip_l']}L | {r2['new_pnl']-r2['base_pnl']:>+8.1f}")
print(f"  {'ALL COMBINED':<30} | {r3['base_pnl']:>+8.1f} | {r3['base_wr']:>6.1f}% | {r3['new_pnl']:>+8.1f} | {r3['new_wr']:>6.1f}% | {r3['flipped']:>5} | {r3['flip_w']}W/{r3['flip_l']}L | {r3['new_pnl']-r3['base_pnl']:>+8.1f}")

print(f"\n  LOGIC VALIDITY CHECK:")
if r1['flip_l'] == 0 and r1['flipped'] > 0:
    print(f"  [PASS] Feb 27 out-of-sample: {r1['flip_w']}W / {r1['flip_l']}L on flips")
elif r1['flipped'] == 0:
    print(f"  [N/A]  Feb 27: no bearish CVD overshoot trades to flip")
else:
    print(f"  [WARN] Feb 27 out-of-sample: {r1['flip_w']}W / {r1['flip_l']}L — has flip losses")

if r2['flip_l'] == 0:
    print(f"  [PASS] Mar 02 in-sample: {r2['flip_w']}W / {r2['flip_l']}L on flips")
else:
    print(f"  [WARN] Mar 02 in-sample: {r2['flip_w']}W / {r2['flip_l']}L — has flip losses")

# Logic explanation
print(f"""
  LOGIC EXPLANATION:
  -----------------
  A bearish signal on swing HIGHS says: "buyers exhausted, price should drop"

  CVD overshoot means: trigger bar CVD is BELOW both swing high CVDs.
  Translation: the selling ALREADY HAPPENED. CVD dropped past both highs.
  The market already did what the divergence predicted.

  At this point, the market is OVERSOLD relative to the divergence.
  The selling move is DONE. Price is near the bottom. -> BUY (flip)

  This is NOT curve-fitting — it is detecting that a bearish divergence
  has RESOLVED (played out) and the reversal is now due.

  Key: this only applies to BEARISH signals (swing highs).
  For BULLISH signals (swing lows), CVD rising above swing CVDs is
  CONFIRMATION of the bullish move, not resolution — so no flip.
""")
