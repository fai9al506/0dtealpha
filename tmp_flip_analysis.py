"""
Test: Instead of BLOCKING CVD overshoot trades, FLIP their direction.
If bearish + CVD went below both swing CVDs -> selling is done -> FLIP to BULLISH
If bullish + CVD went above both swing CVDs -> buying is done -> FLIP to BEARISH
"""
import json, sys
sys.stdout.reconfigure(encoding='utf-8')

data = json.load(open('abs_data.json'))
trades = [t for t in data if t.get('abs_details', {}).get('best_swing')]

print("=" * 120)
print("DIRECTION FLIP ANALYSIS: When CVD overshoot detected, flip to opposite direction")
print("=" * 120)

print("""
INSIGHT: #349 and #353 fired at the RIGHT TIME but WRONG DIRECTION.
The divergence had already played out. CVD overshoot = the move is COMPLETE.
Instead of blocking, flip the signal = trade the reversal.

For bearish with CVD overshoot (selling done) -> FLIP to BULLISH
For bullish with CVD overshoot (buying done)  -> FLIP to BEARISH
""")

print(f"{'ID':>4} | {'Orig Dir':>8} | {'Pattern':>22} | {'Orig Res':>8} | {'Orig PnL':>8} | {'MaxP':>6} | {'MaxL':>6} | {'Flip Dir':>8} | {'Flip Est':>8} | {'Flip PnL':>8} | Notes")
print("-" * 140)

orig_pnl_total = 0
flip_pnl_total = 0
flip_count = 0
flip_wins = 0
flip_losses = 0
flip_expired = 0
no_flip_wins = 0
no_flip_losses = 0
no_flip_expired = 0
no_flip_pnl = 0

for t in trades:
    abs_d = t['abs_details']
    best = abs_d.get('best_swing', {})
    ref_sw = best.get('ref_swing', {})
    rec_sw = best.get('swing', {})
    sw_type = ref_sw.get('type', '')
    result = t['result'] or 'OPEN'
    pnl = t['pnl']
    direction = t['direction']

    if sw_type == 'Z':
        no_flip_pnl += pnl
        if result == 'WIN': no_flip_wins += 1
        elif result == 'LOSS': no_flip_losses += 1
        else: no_flip_expired += 1
        orig_pnl_total += pnl
        flip_pnl_total += pnl
        continue

    trig_bar = t.get('trigger_bar') or {}
    trig_cvd = trig_bar.get('cvd')
    if trig_cvd is None:
        no_flip_pnl += pnl
        if result == 'WIN': no_flip_wins += 1
        elif result == 'LOSS': no_flip_losses += 1
        else: no_flip_expired += 1
        orig_pnl_total += pnl
        flip_pnl_total += pnl
        continue

    sw1_cvd = ref_sw.get('cvd', 0)
    sw2_cvd = rec_sw.get('cvd', 0)
    max_profit = t.get('max_profit', 0) or 0
    max_loss = t.get('max_loss', 0) or 0

    overshoot = False
    if direction == 'bearish' and sw_type == 'H':
        if trig_cvd < min(sw1_cvd, sw2_cvd):
            overshoot = True
    elif direction == 'bullish' and sw_type == 'L':
        if trig_cvd > max(sw1_cvd, sw2_cvd):
            overshoot = True

    if not overshoot:
        orig_pnl_total += pnl
        flip_pnl_total += pnl
        no_flip_pnl += pnl
        if result == 'WIN': no_flip_wins += 1
        elif result == 'LOSS': no_flip_losses += 1
        else: no_flip_expired += 1
        continue

    # FLIP this trade
    flip_count += 1
    orig_pnl_total += pnl
    flip_dir = 'bullish' if direction == 'bearish' else 'bearish'

    # Estimate flipped outcome:
    # Original bearish: max_profit = max downward move, max_loss = max upward move
    # Flipped to bullish: upside potential = orig max_loss, downside risk = orig max_profit
    # Original bullish: max_profit = max upward move, max_loss = max downward move
    # Flipped to bearish: downside potential = orig max_loss, upside risk = orig max_profit

    if result == 'LOSS':
        # Original hit stop = price moved AGAINST original = FOR flipped
        # The upside (for flip) was at least 12 pts (hit the 12pt stop)
        flip_result = 'WIN'
        flip_pnl_est = 10.0
        flip_wins += 1
    elif result == 'WIN':
        # Original hit target = price moved FOR original = AGAINST flipped
        flip_result = 'LOSS'
        flip_pnl_est = -12.0
        flip_losses += 1
    else:  # EXPIRED
        # Check which direction had more movement
        if direction == 'bearish':
            # Flipping to bullish: upside = max_loss (how far UP price went)
            up_potential = max_loss
            down_risk = max_profit
        else:
            # Flipping to bearish: downside = max_loss (how far DOWN price went)
            up_potential = max_loss
            down_risk = max_profit

        if up_potential >= 10:
            flip_result = 'WIN'
            flip_pnl_est = 10.0
            flip_wins += 1
        elif down_risk >= 12:
            flip_result = 'LOSS'
            flip_pnl_est = -12.0
            flip_losses += 1
        else:
            flip_result = 'EXPIRED'
            flip_pnl_est = up_potential - down_risk
            flip_expired += 1

    flip_pnl_total += flip_pnl_est

    note = ''
    if t['id'] in (349, 353):
        note = '<< KEY TRADE'

    print(f"#{t['id']:>4} | {direction:>8} | {abs_d.get('pattern',''):>22} | {result:>8} | {pnl:>+8.1f} | {max_profit:>6.1f} | {max_loss:>6.1f} | {flip_dir:>8} | {flip_result:>8} | {flip_pnl_est:>+8.1f} | {note}")

print(f"\n{'='*120}")
print("ALL-DIRECTION FLIP SUMMARY")
print(f"{'='*120}")
print(f"Trades flipped: {flip_count}")
print(f"Flipped: {flip_wins}W / {flip_losses}L / {flip_expired}E")
print(f"Original total PnL: {orig_pnl_total:+.1f}")
print(f"After flip PnL:     {flip_pnl_total:+.1f}")
print(f"Improvement:        {flip_pnl_total - orig_pnl_total:+.1f}")

total_w = no_flip_wins + flip_wins
total_l = no_flip_losses + flip_losses
total_e = no_flip_expired + flip_expired
wr = round(100*total_w/(total_w+total_l), 1) if (total_w+total_l) else 0
print(f"Combined: {total_w}W/{total_l}L/{total_e}E | WR={wr}% | PnL={flip_pnl_total:+.1f}")

# ========== BEARISH-ONLY FLIP (safer) ==========
print(f"\n{'='*120}")
print("BEARISH-ONLY FLIP: Only flip bearish CVD overshoot to bullish")
print(f"{'='*120}")
print("(Bullish CVD overshoot is unreliable - many winning trades falsely flagged)")

bear_flip_count = 0
bear_flip_pnl = 0
bear_orig_pnl = 0
bear_flip_wins = 0
bear_flip_losses = 0
bear_flip_expired = 0

for t in trades:
    abs_d = t['abs_details']
    best = abs_d.get('best_swing', {})
    ref_sw = best.get('ref_swing', {})
    sw_type = ref_sw.get('type', '')
    if sw_type != 'H':
        continue

    trig_bar = t.get('trigger_bar') or {}
    trig_cvd = trig_bar.get('cvd')
    if trig_cvd is None:
        continue

    direction = t['direction']
    if direction != 'bearish':
        continue

    rec_sw = best.get('swing', {})
    sw1_cvd = ref_sw.get('cvd', 0)
    sw2_cvd = rec_sw.get('cvd', 0)

    if trig_cvd >= min(sw1_cvd, sw2_cvd):
        continue  # no overshoot

    result = t['result'] or 'OPEN'
    pnl = t['pnl']
    max_profit = t.get('max_profit', 0) or 0
    max_loss = t.get('max_loss', 0) or 0
    bear_flip_count += 1
    bear_orig_pnl += pnl

    # Flip to bullish: profit = how far UP = orig max_loss
    if result == 'LOSS':
        flip_pnl_est = 10.0
        bear_flip_wins += 1
        flip_res = 'WIN'
    elif result == 'EXPIRED':
        if max_loss >= 10:
            flip_pnl_est = 10.0
            bear_flip_wins += 1
            flip_res = 'WIN'
        elif max_profit >= 12:
            flip_pnl_est = -12.0
            bear_flip_losses += 1
            flip_res = 'LOSS'
        else:
            flip_pnl_est = max_loss - max_profit
            bear_flip_expired += 1
            flip_res = 'EXPIRED'
    else:  # original WIN (bearish won = price went down = bad for flip)
        flip_pnl_est = -12.0
        bear_flip_losses += 1
        flip_res = 'LOSS'

    bear_flip_pnl += flip_pnl_est

    overshoot_pct = round(100 * (min(sw1_cvd, sw2_cvd) - trig_cvd) / abs(min(sw1_cvd, sw2_cvd)), 1) if min(sw1_cvd, sw2_cvd) != 0 else 0
    print(f"  #{t['id']:>4} | orig {result:>7} {pnl:>+6.1f} | maxP={max_profit:>5.1f} maxL={max_loss:>5.1f} | CVD overshoot: trig={trig_cvd:.0f} vs min_sw={min(sw1_cvd,sw2_cvd):.0f} ({overshoot_pct}% past) | FLIP -> BULL {flip_res} {flip_pnl_est:>+6.1f}")

print(f"\nBearish-only flip: {bear_flip_count} trades")
print(f"  Original PnL:  {bear_orig_pnl:+.1f}")
print(f"  Flipped PnL:   {bear_flip_pnl:+.1f}")
print(f"  Swing:         {bear_orig_pnl:+.1f} -> {bear_flip_pnl:+.1f} = {bear_flip_pnl - bear_orig_pnl:+.1f} improvement")
print(f"  Flip results:  {bear_flip_wins}W / {bear_flip_losses}L / {bear_flip_expired}E")

baseline_pnl = sum(t['pnl'] for t in trades)
grand_total = baseline_pnl - bear_orig_pnl + bear_flip_pnl
print(f"\n  BASELINE (all 31 as-is):    {baseline_pnl:+.1f} pts")
print(f"  WITH BEARISH FLIP:          {grand_total:+.1f} pts")
print(f"  NET IMPROVEMENT:            {grand_total - baseline_pnl:+.1f} pts")

# ========== COMBINED: bearish flip + z >= 0.75 ==========
print(f"\n{'='*120}")
print("COMBINED: Bearish CVD flip + Z >= 0.75 filter")
print(f"{'='*120}")

kept_w = kept_l = kept_e = kept_pnl = 0
flipped_w = flipped_l = flipped_e = flipped_pnl = 0
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

    if sw_type == 'Z':
        # Zone-revisit: keep as-is
        kept_pnl += pnl
        if result == 'WIN': kept_w += 1
        elif result == 'LOSS': kept_l += 1
        else: kept_e += 1
        continue

    z = best.get('cvd_z', 0)
    trig_bar = t.get('trigger_bar') or {}
    trig_cvd = trig_bar.get('cvd')
    sw1_cvd = ref_sw.get('cvd', 0)
    sw2_cvd = rec_sw.get('cvd', 0)
    max_profit = t.get('max_profit', 0) or 0
    max_loss = t.get('max_loss', 0) or 0

    # Check bearish CVD overshoot -> FLIP
    bear_overshoot = False
    if direction == 'bearish' and sw_type == 'H' and trig_cvd is not None:
        if trig_cvd < min(sw1_cvd, sw2_cvd):
            bear_overshoot = True

    if bear_overshoot:
        # FLIP to bullish
        if result == 'LOSS':
            fp = 10.0; flipped_w += 1
        elif result == 'EXPIRED':
            if max_loss >= 10: fp = 10.0; flipped_w += 1
            elif max_profit >= 12: fp = -12.0; flipped_l += 1
            else: fp = max_loss - max_profit; flipped_e += 1
        else:
            fp = -12.0; flipped_l += 1
        flipped_pnl += fp
        continue

    # Z-score filter (non-zone, non-flipped)
    if z < 0.75:
        blocked_pnl += pnl
        continue

    # Keep as-is
    kept_pnl += pnl
    if result == 'WIN': kept_w += 1
    elif result == 'LOSS': kept_l += 1
    else: kept_e += 1

total_w = kept_w + flipped_w
total_l = kept_l + flipped_l
total_e = kept_e + flipped_e
total_pnl = kept_pnl + flipped_pnl
total_n = total_w + total_l + total_e
wr = round(100*total_w/(total_w+total_l), 1) if (total_w+total_l) else 0

print(f"\n  Kept as-is:  {kept_w}W/{kept_l}L/{kept_e}E = {kept_pnl:+.1f}")
print(f"  Flipped:     {flipped_w}W/{flipped_l}L/{flipped_e}E = {flipped_pnl:+.1f}")
print(f"  Blocked:     {blocked_pnl:+.1f}")
print(f"  -----------")
print(f"  TOTAL:       {total_w}W/{total_l}L/{total_e}E = {total_pnl:+.1f} pts | WR={wr}% | {total_n} trades")
print(f"  BASELINE:    {baseline_pnl:+.1f} pts | 58.6% WR | 31 trades")
print(f"  IMPROVEMENT: {total_pnl - baseline_pnl:+.1f} pts | +{wr - 58.6:.1f}pp WR")
