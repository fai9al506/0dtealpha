"""Full Charm S/R Entry Improvement Report — Option B vs Option B + Charm S/R"""
import sqlalchemy as sa
import os, statistics
from datetime import timedelta

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    trades = conn.execute(sa.text("""
        SELECT id, ts, setup_name, direction, grade, score, spot,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               greek_alignment
        FROM setup_log
        WHERE outcome_result IS NOT NULL AND spot IS NOT NULL
        ORDER BY ts
    """)).fetchall()

    results = []
    for t in trades:
        spot = float(t.spot)
        ts = t.ts
        is_long = t.direction in ('long', 'bullish')
        pnl = float(t.outcome_pnl or 0)
        max_profit = float(t.outcome_max_profit or 0)
        max_loss = float(t.outcome_max_loss or 0)
        result = t.outcome_result
        align = t.greek_alignment

        # Option B filter
        optb = True
        if is_long:
            if (align or 0) < 3:
                optb = False
        else:
            if t.setup_name == 'ES Absorption':
                optb = False
            if t.setup_name == 'BofA Scalp':
                optb = False
            if t.setup_name == 'DD Exhaustion' and (align or 0) == 0:
                optb = False
            if t.setup_name == 'AG Short' and (align or 0) == -3:
                optb = False

        # Get charm strikes
        charm_rows = conn.execute(sa.text("""
            SELECT strike, value FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc BETWEEN :s AND :e
              AND strike BETWEEN :lo AND :hi AND value != 0
            ORDER BY ts_utc DESC, abs(value) DESC
        """), {'s': ts - timedelta(minutes=5), 'e': ts + timedelta(minutes=1),
               'lo': spot - 25, 'hi': spot + 25}).fetchall()

        has_sr = False
        pos_pct = 50
        ideal_entry = spot
        entry_imp = 0
        would_fill = False
        s_strike = r_strike = spot
        s_val = r_val = 0.0
        rng = 0

        if charm_rows:
            seen = set()
            strikes = []
            for cr in charm_rows:
                sk = float(cr.strike)
                if sk not in seen:
                    seen.add(sk)
                    strikes.append({'strike': sk, 'value': float(cr.value)})

            pos_above = [x for x in strikes if x['strike'] > spot and x['value'] > 0]
            neg_below = [x for x in strikes if x['strike'] <= spot and x['value'] < 0]

            if pos_above and neg_below:
                resistance = max(pos_above, key=lambda x: abs(x['value']))
                support = max(neg_below, key=lambda x: abs(x['value']))
                rng = resistance['strike'] - support['strike']
                if rng >= 10:
                    has_sr = True
                    s_strike = support['strike']
                    r_strike = resistance['strike']
                    s_val = support['value'] / 1e6
                    r_val = resistance['value'] / 1e6
                    pos_pct = (spot - s_strike) / rng * 100

                    if is_long:
                        ideal_entry = s_strike + rng * 0.3
                        entry_imp = spot - ideal_entry
                        lowest = spot + max_loss
                        would_fill = lowest <= ideal_entry
                    else:
                        ideal_entry = r_strike - rng * 0.3
                        entry_imp = ideal_entry - spot
                        highest = spot + max_profit
                        would_fill = highest >= ideal_entry

        already_good = has_sr and ((is_long and pos_pct <= 30) or (not is_long and pos_pct >= 70))

        stop_pts = {'DD Exhaustion': 12, 'Skew Charm': 20, 'GEX Long': 8,
                    'AG Short': 20, 'BofA Scalp': 10, 'ES Absorption': 8,
                    'Paradigm Reversal': 15}.get(t.setup_name, 12)

        if not has_sr or already_good:
            new_pnl = pnl
            new_result = result
            status = 'ALREADY_GOOD' if already_good else 'NO_DATA'
        elif not would_fill:
            new_pnl = 0
            new_result = 'SKIPPED'
            status = 'SKIPPED'
        else:
            adj_max_loss = max_loss + entry_imp
            adj_max_profit = max_profit + entry_imp
            if adj_max_profit >= 10:
                new_result = 'WIN'
                new_pnl = pnl + entry_imp
            elif adj_max_loss <= -stop_pts:
                new_result = 'LOSS'
                new_pnl = -stop_pts
            else:
                new_result = result
                new_pnl = pnl + entry_imp
            status = 'IMPROVED'

        results.append({
            'id': t.id, 'ts': ts, 'date': str(ts)[:10], 'setup': t.setup_name,
            'dir': t.direction, 'spot': spot, 'is_long': is_long,
            'optb': optb, 'align': align,
            'has_sr': has_sr, 'pos_pct': pos_pct, 'range': rng,
            's_strike': s_strike, 'r_strike': r_strike,
            'ideal': ideal_entry, 'imp': entry_imp, 'would_fill': would_fill,
            'status': status,
            'orig_pnl': pnl, 'orig_result': result,
            'new_pnl': new_pnl, 'new_result': new_result,
            'max_profit': max_profit, 'max_loss': max_loss,
            'is_win_orig': 'WIN' in result,
            'is_loss_orig': 'LOSS' in result,
            'is_win_new': 'WIN' in (new_result or ''),
            'is_loss_new': 'LOSS' in (new_result or ''),
        })

    # =====================================================
    optb_all = [r for r in results if r['optb']]

    def calc_metrics(subset, use_new=False):
        n = len(subset)
        if n == 0:
            return None
        pk = 'new_pnl' if use_new else 'orig_pnl'
        wk = 'is_win_new' if use_new else 'is_win_orig'
        lk = 'is_loss_new' if use_new else 'is_loss_orig'
        pnl = sum(r[pk] for r in subset)
        w = sum(1 for r in subset if r[wk])
        l = sum(1 for r in subset if r[lk])
        wr = w / (w + l) * 100 if (w + l) else 0
        gw = sum(r[pk] for r in subset if r[pk] > 0)
        gl = sum(r[pk] for r in subset if r[pk] < 0)
        pf = gw / abs(gl) if gl else 999
        avg_w = gw / w if w else 0
        avg_l = gl / l if l else 0
        cum = 0
        peak = 0
        dd = 0
        for r in subset:
            cum += r[pk]
            if cum > peak:
                peak = cum
            if cum - peak < dd:
                dd = cum - peak
        avg_dd = sum(r['max_loss'] for r in subset) / n
        return {'n': n, 'pnl': pnl, 'w': w, 'l': l, 'wr': wr, 'pf': pf,
                'avg_win': avg_w, 'avg_loss': avg_l, 'dd': dd, 'ppt': pnl / n,
                'avg_dd': avg_dd, 'gw': gw, 'gl': gl}

    # =====================================================
    print("=" * 100)
    print("CHARM S/R ENTRY IMPROVEMENT - FULL STUDY REPORT")
    print("=" * 100)

    print("""
HOW IT WORKS:
  Step 1: Find charm exposure per-STRIKE within +/-25 pts of spot
  Step 2: Identify RESISTANCE = strongest POSITIVE charm ABOVE spot
          (positive charm = bearish = dealers sell = price ceiling)
  Step 3: Identify SUPPORT = strongest NEGATIVE charm BELOW spot
          (negative charm = bullish = dealers buy = price floor)
  Step 4: Require minimum 10 pt range between S/R levels
  Step 5: Calculate ideal entry at 30% of range:
          - LONGS: enter at support + range*0.3 (near floor, away from ceiling)
          - SHORTS: enter at resistance - range*0.3 (near ceiling, away from floor)
  Step 6: If price reaches ideal entry -> use improved entry price
          If already in good zone -> no change needed
          If price never reaches ideal -> trade SKIPPED (no fill)
""")

    # =====================================================
    print("=" * 100)
    print("SECTION 1: OPTION B vs OPTION B + CHARM S/R")
    print("=" * 100)

    mo = calc_metrics(optb_all, False)
    mn = calc_metrics(optb_all, True)

    rows_table = [
        ("Trades",          f"{mo['n']}",         f"{mn['n']}",         f"{mn['n']-mo['n']:+d}"),
        ("Total PnL",       f"{mo['pnl']:+.1f}",  f"{mn['pnl']:+.1f}",  f"{mn['pnl']-mo['pnl']:+.1f}"),
        ("Win Rate",        f"{mo['wr']:.1f}%",    f"{mn['wr']:.1f}%",    f"{mn['wr']-mo['wr']:+.1f}%"),
        ("Wins / Losses",   f"{mo['w']}W / {mo['l']}L", f"{mn['w']}W / {mn['l']}L", ""),
        ("Profit Factor",   f"{mo['pf']:.2f}x",   f"{mn['pf']:.2f}x",   f"{mn['pf']-mo['pf']:+.2f}x"),
        ("Avg Win",         f"{mo['avg_win']:+.1f}", f"{mn['avg_win']:+.1f}", f"{mn['avg_win']-mo['avg_win']:+.1f}"),
        ("Avg Loss",        f"{mo['avg_loss']:+.1f}", f"{mn['avg_loss']:+.1f}", f"{mn['avg_loss']-mo['avg_loss']:+.1f}"),
        ("Points/Trade",    f"{mo['ppt']:+.2f}",  f"{mn['ppt']:+.2f}",  f"{mn['ppt']-mo['ppt']:+.2f}"),
        ("Max Drawdown",    f"{mo['dd']:+.1f}",   f"{mn['dd']:+.1f}",   f"{mn['dd']-mo['dd']:+.1f}"),
        ("Gross Wins",      f"{mo['gw']:+.1f}",   f"{mn['gw']:+.1f}",   f"{mn['gw']-mo['gw']:+.1f}"),
        ("Gross Losses",    f"{mo['gl']:+.1f}",   f"{mn['gl']:+.1f}",   f"{mn['gl']-mo['gl']:+.1f}"),
    ]

    print(f"\n  {'Metric':25s} {'Option B':>15s} {'B + Charm S/R':>15s} {'Change':>12s}")
    print(f"  {'-'*25} {'-'*15} {'-'*15} {'-'*12}")
    for label, vo, vn, vd in rows_table:
        print(f"  {label:25s} {vo:>15s} {vn:>15s} {vd:>12s}")

    # =====================================================
    print("\n" + "=" * 100)
    print("SECTION 2: WHAT HAPPENS TO EACH TRADE")
    print("=" * 100)

    for s in ['ALREADY_GOOD', 'IMPROVED', 'SKIPPED', 'NO_DATA']:
        sub = [r for r in optb_all if r['status'] == s]
        if not sub:
            continue
        n = len(sub)
        orig = sum(r['orig_pnl'] for r in sub)
        new = sum(r['new_pnl'] for r in sub)
        ow = sum(1 for r in sub if r['is_win_orig'])
        ol = sum(1 for r in sub if r['is_loss_orig'])
        nw = sum(1 for r in sub if r['is_win_new'])
        nl = sum(1 for r in sub if r['is_loss_new'])
        print(f"  {s:15s}: {n:3d} trades | orig {orig:+8.1f}pts ({ow}W/{ol}L) -> new {new:+8.1f}pts ({nw}W/{nl}L)")

    flipped_lw = [r for r in optb_all if r['status'] == 'IMPROVED' and r['is_loss_orig'] and r['is_win_new']]
    flipped_wl = [r for r in optb_all if r['status'] == 'IMPROVED' and r['is_win_orig'] and r['is_loss_new']]
    skipped_w = [r for r in optb_all if r['status'] == 'SKIPPED' and r['is_win_orig']]
    skipped_l = [r for r in optb_all if r['status'] == 'SKIPPED' and r['is_loss_orig']]

    print(f"\n  LOSS->WIN flips:    {len(flipped_lw)} trades (saved {sum(abs(r['orig_pnl']) + r['new_pnl'] for r in flipped_lw):+.1f} pts)")
    print(f"  WIN->LOSS flips:    {len(flipped_wl)} trades")
    print(f"  Skipped winners:    {len(skipped_w)} ({sum(r['orig_pnl'] for r in skipped_w):+.1f} missed PnL)")
    print(f"  Skipped losers:     {len(skipped_l)} ({sum(r['orig_pnl'] for r in skipped_l):+.1f} avoided)")

    # =====================================================
    print("\n" + "=" * 100)
    print("SECTION 3: DAILY BREAKDOWN (March)")
    print("=" * 100)

    dates = sorted(set(r['date'] for r in optb_all if '2026-03' in r['date']))
    print(f"\n  {'Date':12s} {'#':>4s} {'Orig PnL':>10s} {'Charm PnL':>10s} {'Delta':>10s} {'O-WR':>7s} {'C-WR':>7s} {'Skip':>5s} {'Flip':>5s}")
    print(f"  {'-'*12} {'-'*4} {'-'*10} {'-'*10} {'-'*10} {'-'*7} {'-'*7} {'-'*5} {'-'*5}")

    total_orig = 0
    total_new = 0
    for d in dates:
        day = [r for r in optb_all if r['date'] == d]
        n = len(day)
        orig = sum(r['orig_pnl'] for r in day)
        new = sum(r['new_pnl'] for r in day)
        delta = new - orig
        ow = sum(1 for r in day if r['is_win_orig'])
        ol = sum(1 for r in day if r['is_loss_orig'])
        nw = sum(1 for r in day if r['is_win_new'])
        nl = sum(1 for r in day if r['is_loss_new'])
        owr = ow / (ow + ol) * 100 if (ow + ol) else 0
        nwr = nw / (nw + nl) * 100 if (nw + nl) else 0
        skipped = sum(1 for r in day if r['status'] == 'SKIPPED')
        flipped = sum(1 for r in day if r['status'] == 'IMPROVED' and r['is_loss_orig'] and r['is_win_new'])
        total_orig += orig
        total_new += new
        marker = ' <<<' if delta > 20 else (' !!!' if delta < -20 else '')
        print(f"  {d:12s} {n:4d} {orig:+10.1f} {new:+10.1f} {delta:+10.1f} {owr:6.0f}% {nwr:6.0f}% {skipped:5d} {flipped:5d}{marker}")

    print(f"  {'TOTAL':12s} {sum(len([r for r in optb_all if r['date']==d]) for d in dates):4d} {total_orig:+10.1f} {total_new:+10.1f} {total_new-total_orig:+10.1f}")

    # =====================================================
    print("\n" + "=" * 100)
    print("SECTION 4: BY SETUP")
    print("=" * 100)

    for setup in sorted(set(r['setup'] for r in optb_all)):
        sub = [r for r in optb_all if r['setup'] == setup]
        if len(sub) < 3:
            continue
        mo2 = calc_metrics(sub, False)
        mn2 = calc_metrics(sub, True)
        fl = sum(1 for r in sub if r['status'] == 'IMPROVED' and r['is_loss_orig'] and r['is_win_new'])
        sk = sum(1 for r in sub if r['status'] == 'SKIPPED')
        print(f"\n  {setup}:")
        print(f"    Option B:      {mo2['n']:3d}t {mo2['pnl']:+8.1f}pts {mo2['wr']:.1f}%WR {mo2['w']}W/{mo2['l']}L PF={mo2['pf']:.2f}x ppt={mo2['ppt']:+.1f}")
        print(f"    B + Charm S/R: {mn2['n']:3d}t {mn2['pnl']:+8.1f}pts {mn2['wr']:.1f}%WR {mn2['w']}W/{mn2['l']}L PF={mn2['pf']:.2f}x ppt={mn2['ppt']:+.1f}")
        print(f"    Change:        {mn2['pnl']-mo2['pnl']:+8.1f}pts  WR {mn2['wr']-mo2['wr']:+.1f}%  Flipped={fl} Skipped={sk}")

    # =====================================================
    print("\n" + "=" * 100)
    print("SECTION 5: BY DIRECTION")
    print("=" * 100)

    for d_label, d_filt in [('LONGS', True), ('SHORTS', False)]:
        sub = [r for r in optb_all if r['is_long'] == d_filt]
        mo2 = calc_metrics(sub, False)
        mn2 = calc_metrics(sub, True)
        if mo2 is None:
            continue
        print(f"\n  {d_label}:")
        print(f"    Option B:      {mo2['n']:3d}t {mo2['pnl']:+8.1f}pts {mo2['wr']:.1f}%WR PF={mo2['pf']:.2f}x DD={mo2['dd']:+.1f}")
        print(f"    B + Charm S/R: {mn2['n']:3d}t {mn2['pnl']:+8.1f}pts {mn2['wr']:.1f}%WR PF={mn2['pf']:.2f}x DD={mn2['dd']:+.1f}")
        print(f"    Change:        {mn2['pnl']-mo2['pnl']:+8.1f}pts  WR {mn2['wr']-mo2['wr']:+.1f}%")

    # =====================================================
    print("\n" + "=" * 100)
    print("SECTION 6: RISK METRICS")
    print("=" * 100)

    dates_all = sorted(set(r['date'] for r in optb_all))
    daily_orig = [sum(r['orig_pnl'] for r in optb_all if r['date'] == d) for d in dates_all]
    daily_new = [sum(r['new_pnl'] for r in optb_all if r['date'] == d) for d in dates_all]
    daily_orig = [d for d in daily_orig if d != 0]
    daily_new = [d for d in daily_new if d != 0]

    sharpe_o = (statistics.mean(daily_orig) / statistics.stdev(daily_orig)) if len(daily_orig) > 1 else 0
    sharpe_n = (statistics.mean(daily_new) / statistics.stdev(daily_new)) if len(daily_new) > 1 else 0
    avg_daily_o = statistics.mean(daily_orig) if daily_orig else 0
    avg_daily_n = statistics.mean(daily_new) if daily_new else 0
    losing_days_o = sum(1 for d in daily_orig if d < 0)
    losing_days_n = sum(1 for d in daily_new if d < 0)
    worst_day_o = min(daily_orig) if daily_orig else 0
    worst_day_n = min(daily_new) if daily_new else 0
    best_day_o = max(daily_orig) if daily_orig else 0
    best_day_n = max(daily_new) if daily_new else 0

    improved_trades = [r for r in optb_all if r['status'] == 'IMPROVED']
    avg_imp = sum(r['imp'] for r in improved_trades) / len(improved_trades) if improved_trades else 0

    print(f"\n  {'Metric':25s} {'Option B':>15s} {'B + Charm S/R':>15s}")
    print(f"  {'-'*25} {'-'*15} {'-'*15}")
    print(f"  {'Avg Daily PnL':25s} {avg_daily_o:>+14.1f} {avg_daily_n:>+14.1f}")
    print(f"  {'Sharpe Ratio':25s} {sharpe_o:>14.2f} {sharpe_n:>14.2f}")
    print(f"  {'Losing Days':25s} {losing_days_o:>15d} {losing_days_n:>15d}")
    print(f"  {'Worst Day':25s} {worst_day_o:>+14.1f} {worst_day_n:>+14.1f}")
    print(f"  {'Best Day':25s} {best_day_o:>+14.1f} {best_day_n:>+14.1f}")
    print(f"  {'Trading Days':25s} {len(daily_orig):>15d} {len(daily_new):>15d}")
    print(f"\n  Avg entry improvement:  {avg_imp:+.1f} pts per trade")
    print(f"  Trades improved:        {len(improved_trades)}/{len(optb_all)} ({len(improved_trades)/len(optb_all)*100:.0f}%)")
    print(f"  Trades skipped:         {sum(1 for r in optb_all if r['status']=='SKIPPED')}")
    print(f"  Already good entry:     {sum(1 for r in optb_all if r['status']=='ALREADY_GOOD')}")
    print(f"  No charm data:          {sum(1 for r in optb_all if r['status']=='NO_DATA')}")

    # =====================================================
    print("\n" + "=" * 100)
    print("SECTION 7: IMPLEMENTATION NOTES")
    print("=" * 100)
    print("""
  CURRENT SYSTEM: Setup fires -> MARKET ORDER immediately

  PROPOSED SYSTEM: Setup fires -> check charm S/R position:
    Case A: Already in good zone (<=30% for longs, >=70% for shorts)
            -> MARKET ORDER as usual (no change)
    Case B: Outside good zone, range >= 10 pts
            -> LIMIT ORDER at ideal entry (30% level)
            -> If filled within N minutes -> trade executes at better price
            -> If not filled -> SKIP (no trade)
    Case C: No valid charm S/R data or range < 10 pts
            -> MARKET ORDER as usual (no change)

  REQUIRES:
    1. Query volland_exposure_points for charm strikes near spot
    2. Calculate S/R levels and ideal entry price
    3. Place LIMIT order instead of MARKET order
    4. Timeout mechanism (cancel unfilled limit after N minutes)
    5. Handle: what if setup becomes invalid while waiting?

  CAVEATS:
    - Simulation assumes limit orders fill at exact price (real fills may slip)
    - Simulation uses max_loss/max_profit to determine if price reached ideal
      (doesn't account for order of events within the bar)
    - Some "SKIPPED" trades might have filled with a looser threshold (35-40%)
    - Winning trades that are skipped represent genuine missed opportunity
    - Need to decide timeout duration (10min? 20min? 30min?)
""")
