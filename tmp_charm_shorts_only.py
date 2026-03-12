"""Final Report: Option B + Charm S/R for SHORTS only (longs unchanged)"""
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
        spot = float(t.spot); ts = t.ts
        is_long = t.direction in ('long', 'bullish')
        pnl = float(t.outcome_pnl or 0)
        mp = float(t.outcome_max_profit or 0)
        ml = float(t.outcome_max_loss or 0)
        result = t.outcome_result
        align = t.greek_alignment or 0

        # Option B filter
        optb = True
        if is_long and align < 3: optb = False
        elif not is_long:
            if t.setup_name == 'ES Absorption': optb = False
            if t.setup_name == 'BofA Scalp': optb = False
            if t.setup_name == 'DD Exhaustion' and align == 0: optb = False
            if t.setup_name == 'AG Short' and align == -3: optb = False
        if not optb:
            continue

        # Longs: no charm filter, keep original
        if is_long:
            results.append({
                'id': t.id, 'ts': ts, 'date': str(ts)[:10],
                'setup': t.setup_name, 'dir': t.direction,
                'is_long': True, 'spot': spot, 'align': align,
                'status': 'LONG_UNCHANGED',
                'orig_pnl': pnl, 'new_pnl': pnl,
                'orig_result': result, 'new_result': result,
                'max_profit': mp, 'max_loss': ml,
                'pos_pct': 0, 'imp': 0,
            })
            continue

        # Shorts: apply charm S/R
        cr = conn.execute(sa.text("""
            SELECT strike, value FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc BETWEEN :s AND :e
              AND strike BETWEEN :lo AND :hi AND value != 0
            ORDER BY ts_utc DESC, abs(value) DESC
        """), {'s': ts - timedelta(minutes=5), 'e': ts + timedelta(minutes=1),
               'lo': spot - 25, 'hi': spot + 25}).fetchall()

        has_sr = False
        if cr:
            seen = set(); strikes = []
            for r in cr:
                sk = float(r.strike)
                if sk not in seen:
                    seen.add(sk)
                    strikes.append({'strike': sk, 'value': float(r.value)})
            pa = [x for x in strikes if x['strike'] > spot and x['value'] > 0]
            nb = [x for x in strikes if x['strike'] <= spot and x['value'] < 0]
            if pa and nb:
                resistance = max(pa, key=lambda x: abs(x['value']))
                support = max(nb, key=lambda x: abs(x['value']))
                rng = resistance['strike'] - support['strike']
                if rng >= 10:
                    has_sr = True
                    pos_pct = (spot - support['strike']) / rng * 100
                    ideal = resistance['strike'] - rng * 0.3
                    entry_imp = ideal - spot
                    highest = spot + mp
                    would_fill = highest >= ideal
                    already_good = pos_pct >= 70

        if not has_sr:
            # No charm data: keep original
            results.append({
                'id': t.id, 'ts': ts, 'date': str(ts)[:10],
                'setup': t.setup_name, 'dir': t.direction,
                'is_long': False, 'spot': spot, 'align': align,
                'status': 'SHORT_NO_DATA',
                'orig_pnl': pnl, 'new_pnl': pnl,
                'orig_result': result, 'new_result': result,
                'max_profit': mp, 'max_loss': ml,
                'pos_pct': 0, 'imp': 0,
            })
            continue

        stop_pts = {'DD Exhaustion': 12, 'Skew Charm': 20, 'AG Short': 20,
                    'Paradigm Reversal': 15}.get(t.setup_name, 12)

        if already_good:
            new_pnl = pnl; new_result = result; status = 'SHORT_GOOD'
        elif not would_fill:
            new_pnl = 0; new_result = 'SKIPPED'; status = 'SHORT_SKIPPED'
        else:
            adj_ml = ml + entry_imp
            adj_mp = mp + entry_imp
            if adj_mp >= 10:
                new_result = 'WIN'
                new_pnl = pnl + entry_imp
            elif adj_ml <= -stop_pts:
                new_result = 'LOSS'
                new_pnl = -stop_pts
            else:
                new_result = result
                new_pnl = pnl + entry_imp
            status = 'SHORT_IMPROVED'

        results.append({
            'id': t.id, 'ts': ts, 'date': str(ts)[:10],
            'setup': t.setup_name, 'dir': t.direction,
            'is_long': False, 'spot': spot, 'align': align,
            'status': status, 'pos_pct': pos_pct, 'imp': entry_imp,
            'orig_pnl': pnl, 'new_pnl': new_pnl,
            'orig_result': result, 'new_result': new_result,
            'max_profit': mp, 'max_loss': ml,
        })

    # =====================================================
    def calc(subset, use_new=False):
        n = len(subset)
        if n == 0: return None
        pk = 'new_pnl' if use_new else 'orig_pnl'
        wk = lambda r: 'WIN' in (r['new_result'] if use_new else r['orig_result'])
        lk = lambda r: 'LOSS' in (r['new_result'] if use_new else r['orig_result'])
        pnl = sum(r[pk] for r in subset)
        w = sum(1 for r in subset if wk(r))
        l = sum(1 for r in subset if lk(r))
        wr = w / (w + l) * 100 if (w + l) else 0
        gw = sum(r[pk] for r in subset if r[pk] > 0)
        gl = sum(r[pk] for r in subset if r[pk] < 0)
        pf = gw / abs(gl) if gl else 999
        cum = 0; peak = 0; dd = 0
        for r in subset:
            cum += r[pk]
            if cum > peak: peak = cum
            if cum - peak < dd: dd = cum - peak
        return {'n': n, 'pnl': pnl, 'w': w, 'l': l, 'wr': wr, 'pf': pf,
                'dd': dd, 'ppt': pnl / n, 'gw': gw, 'gl': gl,
                'avg_w': gw / w if w else 0, 'avg_l': gl / l if l else 0}

    # =====================================================
    print("=" * 105)
    print("FINAL REPORT: OPTION B + CHARM S/R (SHORTS ONLY)")
    print("=" * 105)
    print()
    print("STRATEGY: Longs = market order (unchanged)")
    print("          Shorts = limit order at 30% from resistance (charm S/R)")
    print()

    # HEAD TO HEAD
    print("=" * 105)
    print("SECTION 1: HEAD-TO-HEAD COMPARISON")
    print("=" * 105)

    mo = calc(results, False)
    mn = calc(results, True)

    rows_t = [
        ("Trades",          mo['n'],    mn['n']),
        ("Total PnL",       mo['pnl'],  mn['pnl']),
        ("Win Rate",        mo['wr'],   mn['wr']),
        ("Wins",            mo['w'],    mn['w']),
        ("Losses",          mo['l'],    mn['l']),
        ("Profit Factor",   mo['pf'],   mn['pf']),
        ("Avg Win",         mo['avg_w'], mn['avg_w']),
        ("Avg Loss",        mo['avg_l'], mn['avg_l']),
        ("Points/Trade",    mo['ppt'],  mn['ppt']),
        ("Max Drawdown",    mo['dd'],   mn['dd']),
        ("Gross Wins",      mo['gw'],   mn['gw']),
        ("Gross Losses",    mo['gl'],   mn['gl']),
    ]

    print(f"\n  {'Metric':25s} {'Option B':>15s} {'B+CharmShorts':>15s} {'Change':>12s}")
    print(f"  {'-'*25} {'-'*15} {'-'*15} {'-'*12}")
    for label, vo, vn in rows_t:
        delta = vn - vo
        if 'Rate' in label:
            print(f"  {label:25s} {vo:>14.1f}% {vn:>14.1f}% {delta:>+11.1f}%")
        elif 'Factor' in label:
            print(f"  {label:25s} {vo:>14.2f}x {vn:>14.2f}x {delta:>+11.2f}x")
        elif label in ('Trades', 'Wins', 'Losses'):
            print(f"  {label:25s} {int(vo):>15d} {int(vn):>15d} {int(delta):>+12d}")
        else:
            print(f"  {label:25s} {vo:>+14.1f} {vn:>+14.1f} {delta:>+11.1f}")

    # =====================================================
    print(f"\n{'='*105}")
    print("SECTION 2: WHAT HAPPENS TO EACH TRADE")
    print("=" * 105)

    for s in ['LONG_UNCHANGED', 'SHORT_GOOD', 'SHORT_IMPROVED', 'SHORT_SKIPPED', 'SHORT_NO_DATA']:
        sub = [r for r in results if r['status'] == s]
        if not sub: continue
        n = len(sub)
        op = sum(r['orig_pnl'] for r in sub)
        np2 = sum(r['new_pnl'] for r in sub)
        ow = sum(1 for r in sub if 'WIN' in r['orig_result'])
        ol = sum(1 for r in sub if 'LOSS' in r['orig_result'])
        nw = sum(1 for r in sub if 'WIN' in r['new_result'])
        nl = sum(1 for r in sub if 'LOSS' in r['new_result'])
        print(f"  {s:20s}: {n:3d}t | orig {op:+8.1f}pts ({ow}W/{ol}L) -> new {np2:+8.1f}pts ({nw}W/{nl}L)")

    flipped = [r for r in results if r['status'] == 'SHORT_IMPROVED'
               and 'LOSS' in r['orig_result'] and 'WIN' in r['new_result']]
    skipped_w = [r for r in results if r['status'] == 'SHORT_SKIPPED'
                 and 'WIN' in r['orig_result']]
    skipped_l = [r for r in results if r['status'] == 'SHORT_SKIPPED'
                 and 'LOSS' in r['orig_result']]

    print(f"\n  Short LOSS->WIN flips:  {len(flipped)} trades")
    print(f"  Short skipped winners:  {len(skipped_w)} ({sum(r['orig_pnl'] for r in skipped_w):+.1f} missed)")
    print(f"  Short skipped losers:   {len(skipped_l)} ({sum(r['orig_pnl'] for r in skipped_l):+.1f} avoided)")

    # =====================================================
    print(f"\n{'='*105}")
    print("SECTION 3: DAILY BREAKDOWN (all dates)")
    print("=" * 105)

    dates = sorted(set(r['date'] for r in results))
    print(f"\n  {'Date':12s} {'#':>4s} {'Orig':>10s} {'Charm':>10s} {'Delta':>10s} {'O-WR':>7s} {'C-WR':>7s}")
    print(f"  {'-'*12} {'-'*4} {'-'*10} {'-'*10} {'-'*10} {'-'*7} {'-'*7}")

    for d in dates:
        day = [r for r in results if r['date'] == d]
        n = len(day)
        orig = sum(r['orig_pnl'] for r in day)
        new = sum(r['new_pnl'] for r in day)
        delta = new - orig
        ow = sum(1 for r in day if 'WIN' in r['orig_result'])
        ol = sum(1 for r in day if 'LOSS' in r['orig_result'])
        nw = sum(1 for r in day if 'WIN' in r['new_result'])
        nl = sum(1 for r in day if 'LOSS' in r['new_result'])
        owr = ow / (ow + ol) * 100 if (ow + ol) else 0
        nwr = nw / (nw + nl) * 100 if (nw + nl) else 0
        marker = ' <<<' if delta > 15 else (' !!!' if delta < -15 else '')
        print(f"  {d:12s} {n:4d} {orig:+10.1f} {new:+10.1f} {delta:+10.1f} {owr:6.0f}% {nwr:6.0f}%{marker}")

    total_orig = sum(r['orig_pnl'] for r in results)
    total_new = sum(r['new_pnl'] for r in results)
    print(f"  {'TOTAL':12s} {len(results):4d} {total_orig:+10.1f} {total_new:+10.1f} {total_new-total_orig:+10.1f}")

    # Count better/worse/same days
    better = worse = same = 0
    for d in dates:
        day = [r for r in results if r['date'] == d]
        orig = sum(r['orig_pnl'] for r in day)
        new = sum(r['new_pnl'] for r in day)
        if new > orig + 1: better += 1
        elif new < orig - 1: worse += 1
        else: same += 1
    print(f"\n  Better days: {better} | Worse days: {worse} | Same: {same}")

    # =====================================================
    print(f"\n{'='*105}")
    print("SECTION 4: BY SETUP")
    print("=" * 105)

    for setup in sorted(set(r['setup'] for r in results)):
        sub = [r for r in results if r['setup'] == setup]
        if len(sub) < 3: continue
        mo2 = calc(sub, False)
        mn2 = calc(sub, True)
        fl = sum(1 for r in sub if r['status'] == 'SHORT_IMPROVED'
                 and 'LOSS' in r['orig_result'] and 'WIN' in r['new_result'])
        sk = sum(1 for r in sub if r['status'] == 'SHORT_SKIPPED')
        print(f"\n  {setup}:")
        print(f"    Option B:        {mo2['n']:3d}t {mo2['pnl']:+8.1f}pts {mo2['wr']:.1f}%WR {mo2['w']}W/{mo2['l']}L PF={mo2['pf']:.2f}x DD={mo2['dd']:+.1f}")
        print(f"    B+CharmShorts:   {mn2['n']:3d}t {mn2['pnl']:+8.1f}pts {mn2['wr']:.1f}%WR {mn2['w']}W/{mn2['l']}L PF={mn2['pf']:.2f}x DD={mn2['dd']:+.1f}")
        print(f"    Change:          {mn2['pnl']-mo2['pnl']:+8.1f}pts  WR {mn2['wr']-mo2['wr']:+.1f}%  Flipped={fl} Skipped={sk}")

    # =====================================================
    print(f"\n{'='*105}")
    print("SECTION 5: SHORTS ONLY COMPARISON")
    print("=" * 105)

    shorts = [r for r in results if not r['is_long']]
    mos = calc(shorts, False)
    mns = calc(shorts, True)

    print(f"\n  {'Metric':25s} {'Shorts Orig':>15s} {'Shorts Charm':>15s} {'Change':>12s}")
    print(f"  {'-'*25} {'-'*15} {'-'*15} {'-'*12}")
    for label, vo, vn in [
        ("Trades", mos['n'], mns['n']),
        ("Total PnL", mos['pnl'], mns['pnl']),
        ("Win Rate", mos['wr'], mns['wr']),
        ("Wins / Losses", f"{mos['w']}W/{mos['l']}L", f"{mns['w']}W/{mns['l']}L"),
        ("Profit Factor", mos['pf'], mns['pf']),
        ("Max Drawdown", mos['dd'], mns['dd']),
        ("Points/Trade", mos['ppt'], mns['ppt']),
    ]:
        if isinstance(vo, str):
            print(f"  {label:25s} {vo:>15s} {vn:>15s}")
        elif 'Rate' in label:
            print(f"  {label:25s} {vo:>14.1f}% {vn:>14.1f}% {vn-vo:>+11.1f}%")
        elif 'Factor' in label:
            print(f"  {label:25s} {vo:>14.2f}x {vn:>14.2f}x {vn-vo:>+11.2f}x")
        elif label in ('Trades',):
            print(f"  {label:25s} {int(vo):>15d} {int(vn):>15d} {int(vn-vo):>+12d}")
        else:
            print(f"  {label:25s} {vo:>+14.1f} {vn:>+14.1f} {vn-vo:>+11.1f}")

    # =====================================================
    print(f"\n{'='*105}")
    print("SECTION 6: RISK METRICS")
    print("=" * 105)

    dates_all = sorted(set(r['date'] for r in results))
    daily_orig = [sum(r['orig_pnl'] for r in results if r['date'] == d) for d in dates_all]
    daily_new = [sum(r['new_pnl'] for r in results if r['date'] == d) for d in dates_all]
    daily_orig = [d for d in daily_orig if d != 0]
    daily_new = [d for d in daily_new if d != 0]

    sharpe_o = statistics.mean(daily_orig) / statistics.stdev(daily_orig) if len(daily_orig) > 1 else 0
    sharpe_n = statistics.mean(daily_new) / statistics.stdev(daily_new) if len(daily_new) > 1 else 0

    losing_o = sum(1 for d in daily_orig if d < 0)
    losing_n = sum(1 for d in daily_new if d < 0)

    print(f"\n  {'Metric':25s} {'Option B':>15s} {'B+CharmShorts':>15s}")
    print(f"  {'-'*25} {'-'*15} {'-'*15}")
    print(f"  {'Avg Daily PnL':25s} {statistics.mean(daily_orig):>+14.1f} {statistics.mean(daily_new):>+14.1f}")
    print(f"  {'Sharpe Ratio':25s} {sharpe_o:>14.2f} {sharpe_n:>14.2f}")
    print(f"  {'Losing Days':25s} {losing_o:>15d} {losing_n:>15d}")
    print(f"  {'Worst Day':25s} {min(daily_orig):>+14.1f} {min(daily_new):>+14.1f}")
    print(f"  {'Best Day':25s} {max(daily_orig):>+14.1f} {max(daily_new):>+14.1f}")
    print(f"  {'Trading Days':25s} {len(daily_orig):>15d} {len(daily_new):>15d}")

    improved = [r for r in results if r['status'] == 'SHORT_IMPROVED']
    avg_imp = sum(r['imp'] for r in improved) / len(improved) if improved else 0
    print(f"\n  Avg short entry improvement: {avg_imp:+.1f} pts")
    print(f"  Shorts improved:  {len(improved)}")
    print(f"  Shorts skipped:   {sum(1 for r in results if r['status']=='SHORT_SKIPPED')}")
    print(f"  Shorts already good: {sum(1 for r in results if r['status']=='SHORT_GOOD')}")
    print(f"  Shorts no data:   {sum(1 for r in results if r['status']=='SHORT_NO_DATA')}")
    print(f"  Longs unchanged:  {sum(1 for r in results if r['status']=='LONG_UNCHANGED')}")
