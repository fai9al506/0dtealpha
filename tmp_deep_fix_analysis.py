"""
Deep analysis: WHY #349 and #353 fired opposite direction.
Test potential fixes across all 31 trades.
"""
import json, sys
sys.stdout.reconfigure(encoding='utf-8')

data = json.load(open('abs_data.json'))

# Only trades with full swing data
trades = [t for t in data if t.get('abs_details', {}).get('best_swing')]

print("=" * 100)
print("ROOT CAUSE ANALYSIS: #349 and #353")
print("=" * 100)

print("""
TRADE #349 — BEARISH buy_exhaustion, should have been BULLISH
-------------------------------------------------------------
Swing pair: H434(6880.75, CVD=15733) vs H456(6894.50, CVD=13950)
  -> Higher high + Lower CVD = "buyers exhausted" -> SELL
Trigger bar: #492 at 13:52 ET, ES price 6877.50

PROBLEM: This divergence ALREADY PLAYED OUT.
- The buy_exhaustion from 11:11-11:47 correctly predicted the drop
- Price dropped from 6894 to 6877 (17 pts) — the exhaustion move HAPPENED
- At 13:52, we're at the BOTTOM of that move, not the top
- CVD dropped from 15733 to 10574 — massive selling CONFIRMED the move
- A bullish sell_absorption WAS found (z=0.70) but REJECTED by tier priority
- The market then rallied 34 pts from 6877 to 6911

TRADE #353 — BULLISH sell_absorption, should have been BEARISH
--------------------------------------------------------------
Swing pair: L446(6853.25, CVD=13290) vs L465(6881.50, CVD=12897)
  -> Higher low + Lower CVD = "passive buyers absorbing" -> BUY
Trigger bar: #499 at 14:16 ET, ES price 6904.25

PROBLEM: Same issue — divergence from 2+ hours ago, completely resolved.
- Price is 23 pts ABOVE the swing lows (6881-6853)
- CVD is 15263 — ABOVE both swing CVDs (13290, 12897) — buying went further
- z-score only 0.57 (barely above 0.5 min)
- The market then crashed 31 pts from 6904 to 6873
""")

# === TEST PROPOSED FIXES ACROSS ALL 31 TRADES ===
print("=" * 100)
print("PROPOSED FIX: CVD OVERSHOOT CHECK")
print("=" * 100)
print("""
Logic: If current CVD has moved PAST both swing CVDs in the direction
       the divergence predicted, the move already happened. Block signal.

- Bearish (swing highs): block if current_cvd < min(sw1_cvd, sw2_cvd)
  (selling went further than the exhaustion suggested)
- Bullish (swing lows): block if current_cvd > max(sw1_cvd, sw2_cvd)
  (buying went further than the absorption/exhaustion suggested)
""")

print(f"\n{'ID':>4} | {'Dir':>8} | {'Pattern':>22} | {'Res':>7} | {'PnL':>6} | {'Trig CVD':>10} | {'Sw1 CVD':>8} | {'Sw2 CVD':>8} | CVD Overshoot?")
print("-" * 120)

fix1_blocked_w = fix1_blocked_l = fix1_blocked_e = fix1_blocked_pnl = 0

for t in trades:
    abs_d = t['abs_details']
    best = abs_d.get('best_swing', {})
    ref_sw = best.get('ref_swing', {})
    rec_sw = best.get('swing', {})
    sw_type = ref_sw.get('type', '')
    if sw_type == 'Z':
        continue  # skip zone-revisit

    trig_bar = t.get('trigger_bar') or {}
    trig_cvd = trig_bar.get('cvd')
    if trig_cvd is None:
        continue

    sw1_cvd = ref_sw.get('cvd', 0)
    sw2_cvd = rec_sw.get('cvd', 0)
    direction = t['direction']
    result = t['result'] or 'OPEN'
    pnl = t['pnl']

    overshoot = False
    if direction == 'bearish' and sw_type == 'H':
        # Block if selling went past both swing CVDs
        if trig_cvd < min(sw1_cvd, sw2_cvd):
            overshoot = True
    elif direction == 'bullish' and sw_type == 'L':
        # Block if buying went past both swing CVDs
        if trig_cvd > max(sw1_cvd, sw2_cvd):
            overshoot = True

    if overshoot:
        fix1_blocked_pnl += pnl
        if result == 'WIN': fix1_blocked_w += 1
        elif result == 'LOSS': fix1_blocked_l += 1
        else: fix1_blocked_e += 1

    flag = '** BLOCKED **' if overshoot else ''
    print(f"#{t['id']:>4} | {direction:>8} | {abs_d.get('pattern',''):>22} | {result:>7} | {pnl:>+6.1f} | {trig_cvd:>+10.0f} | {sw1_cvd:>+8.0f} | {sw2_cvd:>+8.0f} | {flag}")

print(f"\nCVD Overshoot filter blocks: {fix1_blocked_w}W/{fix1_blocked_l}L/{fix1_blocked_e}E = {fix1_blocked_pnl:+.1f} pts")

# === FIX 2: Price proximity to swing ===
print(f"\n{'='*100}")
print("PROPOSED FIX 2: PRICE PROXIMITY TO SWING")
print("=" * 100)
print("""
Logic: Entry price must be within max_price_dist (ATR multiple) of the
       most recent swing in the pair. Prevents trading signals that
       already played out their expected price move.

Testing thresholds: 3 ATR, 4 ATR, 5 ATR
""")

for max_atr in [3.0, 4.0, 5.0]:
    blocked_w = blocked_l = blocked_e = blocked_pnl = 0
    for t in trades:
        abs_d = t['abs_details']
        best = abs_d.get('best_swing', {})
        ref_sw = best.get('ref_swing', {})
        rec_sw = best.get('swing', {})
        sw_type = ref_sw.get('type', '')
        if sw_type == 'Z':
            continue

        atr = abs_d.get('atr', 3.5)
        es_price = t.get('es_price') or 0
        sw2_price = rec_sw.get('price', 0)
        if not es_price or not sw2_price:
            continue

        dist = abs(es_price - sw2_price)
        dist_atr = dist / atr if atr > 0 else 0

        if dist_atr > max_atr:
            blocked_pnl += t['pnl']
            r = t['result'] or 'OPEN'
            if r == 'WIN': blocked_w += 1
            elif r == 'LOSS': blocked_l += 1
            else: blocked_e += 1

    print(f"  {max_atr} ATR: blocks {blocked_w}W/{blocked_l}L/{blocked_e}E = {blocked_pnl:+.1f} pts")

# === FIX 3: Minimum Z-score ===
print(f"\n{'='*100}")
print("PROPOSED FIX 3: RAISE MINIMUM Z-SCORE")
print("=" * 100)

for min_z in [0.5, 0.75, 1.0, 1.5, 2.0]:
    blocked_w = blocked_l = blocked_e = blocked_pnl = 0
    kept_w = kept_l = kept_e = kept_pnl = 0
    for t in trades:
        abs_d = t['abs_details']
        best = abs_d.get('best_swing', {})
        sw_type = best.get('ref_swing', {}).get('type', '')
        if sw_type == 'Z':
            # zone-revisits - keep as-is
            r = t['result'] or 'OPEN'
            if r == 'WIN': kept_w += 1
            elif r == 'LOSS': kept_l += 1
            else: kept_e += 1
            kept_pnl += t['pnl']
            continue

        z = best.get('cvd_z', 0)
        r = t['result'] or 'OPEN'
        if z < min_z:
            blocked_pnl += t['pnl']
            if r == 'WIN': blocked_w += 1
            elif r == 'LOSS': blocked_l += 1
            else: blocked_e += 1
        else:
            kept_pnl += t['pnl']
            if r == 'WIN': kept_w += 1
            elif r == 'LOSS': kept_l += 1
            else: kept_e += 1

    total_kept = kept_w + kept_l + kept_e
    wl = kept_w + kept_l
    wr = round(100*kept_w/wl, 1) if wl else 0
    print(f"  z >= {min_z}: keep {total_kept} trades | {kept_w}W/{kept_l}L/{kept_e}E | WR={wr}% | PnL={kept_pnl:+.1f} | blocks {blocked_w}W/{blocked_l}L")

# === COMBINED FIX: CVD Overshoot + Z-score ===
print(f"\n{'='*100}")
print("COMBINED FIXES")
print("=" * 100)

combos = [
    ("CVD overshoot only", True, False, 0.5),
    ("Z >= 1.0 only", False, False, 1.0),
    ("CVD overshoot + Z >= 0.75", True, False, 0.75),
    ("CVD overshoot + Z >= 1.0", True, False, 1.0),
    ("CVD overshoot + price 4ATR", True, True, 0.5),
    ("CVD overshoot + price 4ATR + Z>=1.0", True, True, 1.0),
]

print(f"\n{'Combo':<45} | {'Keep':>4} | {'W/L/E':>10} | {'WR':>6} | {'PnL':>8} | {'Blocked PnL':>11}")
print("-" * 100)

for label, use_cvd, use_price, min_z in combos:
    kept_w = kept_l = kept_e = kept_pnl = 0
    blocked_pnl = 0

    for t in trades:
        abs_d = t['abs_details']
        best = abs_d.get('best_swing', {})
        ref_sw = best.get('ref_swing', {})
        rec_sw = best.get('swing', {})
        sw_type = ref_sw.get('type', '')
        result = t['result'] or 'OPEN'
        pnl = t['pnl']
        direction = t['direction']

        blocked = False

        if sw_type != 'Z':  # swing-to-swing only
            z = best.get('cvd_z', 0)
            if z < min_z:
                blocked = True

            if use_cvd and not blocked:
                trig_bar = t.get('trigger_bar') or {}
                trig_cvd = trig_bar.get('cvd')
                sw1_cvd = ref_sw.get('cvd', 0)
                sw2_cvd = rec_sw.get('cvd', 0)
                if trig_cvd is not None:
                    if direction == 'bearish' and sw_type == 'H':
                        if trig_cvd < min(sw1_cvd, sw2_cvd):
                            blocked = True
                    elif direction == 'bullish' and sw_type == 'L':
                        if trig_cvd > max(sw1_cvd, sw2_cvd):
                            blocked = True

            if use_price and not blocked:
                atr = abs_d.get('atr', 3.5)
                es_price = t.get('es_price') or 0
                sw2_price = rec_sw.get('price', 0)
                if es_price and sw2_price:
                    dist_atr = abs(es_price - sw2_price) / atr if atr > 0 else 0
                    if dist_atr > 4.0:
                        blocked = True

        if blocked:
            blocked_pnl += pnl
        else:
            kept_pnl += pnl
            if result == 'WIN': kept_w += 1
            elif result == 'LOSS': kept_l += 1
            else: kept_e += 1

    total = kept_w + kept_l + kept_e
    wl = kept_w + kept_l
    wr = round(100*kept_w/wl, 1) if wl else 0
    print(f"{label:<45} | {total:>4} | {kept_w}W/{kept_l}L/{kept_e}E | {wr:>5.1f}% | {kept_pnl:>+8.1f} | {blocked_pnl:>+11.1f}")

# Baseline
total_w = sum(1 for t in trades if t['result'] == 'WIN')
total_l = sum(1 for t in trades if t['result'] == 'LOSS')
total_e = sum(1 for t in trades if (t['result'] or 'OPEN') == 'EXPIRED')
total_pnl = sum(t['pnl'] for t in trades)
total_wl = total_w + total_l
print(f"{'BASELINE (no filter)':<45} | {len(trades):>4} | {total_w}W/{total_l}L/{total_e}E | {round(100*total_w/total_wl,1):>5.1f}% | {total_pnl:>+8.1f} | {'':>11}")

# Show which specific trades each best combo blocks
print(f"\n{'='*100}")
print("BEST COMBO DETAIL: CVD overshoot + Z >= 1.0")
print("=" * 100)

for t in trades:
    abs_d = t['abs_details']
    best = abs_d.get('best_swing', {})
    ref_sw = best.get('ref_swing', {})
    rec_sw = best.get('swing', {})
    sw_type = ref_sw.get('type', '')
    result = t['result'] or 'OPEN'
    pnl = t['pnl']
    direction = t['direction']

    blocked_reasons = []
    if sw_type != 'Z':
        z = best.get('cvd_z', 0)
        if z < 1.0:
            blocked_reasons.append(f"z={z:.2f}<1.0")

        trig_bar = t.get('trigger_bar') or {}
        trig_cvd = trig_bar.get('cvd')
        sw1_cvd = ref_sw.get('cvd', 0)
        sw2_cvd = rec_sw.get('cvd', 0)
        if trig_cvd is not None:
            if direction == 'bearish' and sw_type == 'H':
                if trig_cvd < min(sw1_cvd, sw2_cvd):
                    blocked_reasons.append(f"CVD_overshoot (trig={trig_cvd:.0f} < min={min(sw1_cvd,sw2_cvd):.0f})")
            elif direction == 'bullish' and sw_type == 'L':
                if trig_cvd > max(sw1_cvd, sw2_cvd):
                    blocked_reasons.append(f"CVD_overshoot (trig={trig_cvd:.0f} > max={max(sw1_cvd,sw2_cvd):.0f})")

    if blocked_reasons:
        print(f"  BLOCKED #{t['id']:>4} | {direction:>8} | {result:>7} | {pnl:>+6.1f} | {' + '.join(blocked_reasons)}")
